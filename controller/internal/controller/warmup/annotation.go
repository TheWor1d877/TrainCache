package warmup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"traincache/controller/internal/logging"
	"traincache/controller/internal/scorefmt"
)

// scoreWriteAttempts is the read-modify-write retry bound for resourceVersion
// conflicts required by D27 and plan M8 criterion ⑤.
const scoreWriteAttempts = 5

// ErrScoreConflict reports that writing a task's Node score annotation kept
// conflicting for scoreWriteAttempts attempts.
var ErrScoreConflict = errors.New("warmup: score annotation conflict")

// TaskRef identifies the WarmupTask whose annotation is written and the
// generation frozen into the value.
type TaskRef struct {
	Namespace  string
	Name       string
	Generation int64
}

// ScoreEntry is one Pod's computed score together with the parameters that
// produced it. The parameters are only used for detail logging; storage keeps
// the integer score.
type ScoreEntry struct {
	PodIndex int32
	K        int32
	H        float64
	C        int32
	Score    int32
}

// ScoreWriter merges per-Pod scores into the per-task Node annotation
// (docs/8.1 design.md §6.2, D27). It only touches the Node annotation whose
// key is scorefmt.Key(task.Namespace, task.Name); other tasks' keys, other
// annotation keys and all Pod entries not present in the current write are
// preserved.
type ScoreWriter struct {
	Client client.Client
	// Now returns the write instant; nil means time.Now. Tests inject a
	// deterministic clock.
	Now func() time.Time
	// Prefix is the configured annotation prefix. Empty uses scorefmt's
	// default. Controller and Scheduler must share this value (D27).
	Prefix string
}

// Write merges entries into the task's score annotation on nodeName.
//
// Each attempt performs Get -> merge -> Update. Entries for Pod indexes not in
// entries are retained ("过期条目保留"); entries for indexes in entries
// overwrite the old score. The value's g is always the task's current
// generation. resourceVersion conflicts are retried up to scoreWriteAttempts
// times before ErrScoreConflict is returned.
func (w *ScoreWriter) Write(ctx context.Context, task TaskRef, nodeName string, entries []ScoreEntry) error {
	if w == nil || w.Client == nil {
		return fmt.Errorf("warmup: write node scores: nil client")
	}
	if err := validateTaskRef(task); err != nil {
		return err
	}
	if nodeName == "" {
		return fmt.Errorf("warmup: write node scores: empty node name")
	}
	if err := validateEntries(entries); err != nil {
		return err
	}

	taskString := task.Namespace + "/" + task.Name
	logger := logging.From(ctx).With(slog.String("warmup_task", taskString))
	key := scorefmt.Key(task.Namespace, task.Name)
	if w.Prefix != "" {
		key = scorefmt.KeyWithPrefix(w.Prefix, task.Namespace, task.Name)
	}
	now := time.Now
	if w.Now != nil {
		now = w.Now
	}

	for _, entry := range entries {
		logger.Debug("pod score detail",
			slog.Int("pod", int(entry.PodIndex)),
			slog.String("node", nodeName),
			slog.Int("k", int(entry.K)),
			slog.Float64("h", entry.H),
			slog.Int("c", int(entry.C)),
			slog.Int("score", int(entry.Score)))
	}

	var lastErr error
	for attempt := 1; attempt <= scoreWriteAttempts; attempt++ {
		node := &corev1.Node{}
		if err := w.Client.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
			logger.Error("score annotation read failed",
				slog.String("node", nodeName),
				slog.String("annotation_key", key),
				slog.Int64("generation", task.Generation),
				slog.String("err", err.Error()))
			return fmt.Errorf("warmup: get node %q: %w", nodeName, err)
		}

		p := mergeScores(node.Annotations[key], taskString, entries)
		value, err := scorefmt.EncodeValue(scorefmt.Value{
			V:    1,
			Task: taskString,
			G:    task.Generation,
			T:    now().UTC(),
			P:    p,
		})
		if err != nil {
			return fmt.Errorf("warmup: encode scores for %s on node %q: %w", taskString, nodeName, err)
		}

		if node.Annotations == nil {
			node.Annotations = map[string]string{}
		}
		node.Annotations[key] = value

		if err := w.Client.Update(ctx, node); err != nil {
			lastErr = err
			if apierrors.IsConflict(err) {
				logger.Debug("score annotation conflict, retrying",
					slog.String("node", nodeName),
					slog.String("annotation_key", key),
					slog.Int64("generation", task.Generation),
					slog.Int("attempt", attempt))
				continue
			}
			logger.Error("score annotation write failed",
				slog.String("node", nodeName),
				slog.String("annotation_key", key),
				slog.Int64("generation", task.Generation),
				slog.String("err", err.Error()))
			return fmt.Errorf("warmup: update node %q: %w", nodeName, err)
		}

		logger.Info("score annotation written",
			slog.String("node", nodeName),
			slog.String("annotation_key", key),
			slog.Int64("generation", task.Generation),
			slog.Int("entries", len(entries)),
			slog.Any("pods", entryIndexes(entries)))
		return nil
	}

	logger.Error("score annotation conflict exhausted",
		slog.String("node", nodeName),
		slog.String("annotation_key", key),
		slog.Int64("generation", task.Generation),
		slog.Int("attempts", scoreWriteAttempts),
		slog.String("err", lastErrString(lastErr)))
	return fmt.Errorf("%w: node %q, task %s/%s, attempts %d: %w",
		ErrScoreConflict, nodeName, task.Namespace, task.Name, scoreWriteAttempts, lastErr)
}

func validateTaskRef(task TaskRef) error {
	if task.Namespace == "" || task.Name == "" {
		return fmt.Errorf("warmup: write node scores: empty task namespace or name")
	}
	return nil
}

func validateEntries(entries []ScoreEntry) error {
	for _, entry := range entries {
		if entry.PodIndex < 0 {
			return fmt.Errorf("warmup: write node scores: negative pod index %d", entry.PodIndex)
		}
		if entry.Score < 0 || entry.Score > 100 {
			return fmt.Errorf("warmup: write node scores: pod %d score %d out of range 0..100",
				entry.PodIndex, entry.Score)
		}
	}
	return nil
}

// mergeScores decodes the existing value and returns a new map with entries
// applied. An undecodable or mismatched value is treated as empty: the key
// belongs to this task, so replacing a corrupt payload is safe. Old Pod
// entries are kept as required by the generation semantics (M8 task 8.3).
func mergeScores(existing, task string, entries []ScoreEntry) map[string]int32 {
	merged := map[string]int32{}
	if existing != "" {
		if current, err := scorefmt.DecodeValue(existing); err == nil && current.Task == task {
			for index, score := range current.P {
				merged[index] = score
			}
		}
	}
	for _, entry := range entries {
		merged[strconv.FormatInt(int64(entry.PodIndex), 10)] = entry.Score
	}
	return merged
}

func entryIndexes(entries []ScoreEntry) []int32 {
	indexes := make([]int32, 0, len(entries))
	for _, entry := range entries {
		indexes = append(indexes, entry.PodIndex)
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i] < indexes[j] })
	return indexes
}

func lastErrString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
