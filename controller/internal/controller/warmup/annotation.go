package warmup

import (
	"fmt"
	"time"

	"traincache/controller/internal/scorefmt"
)

// TaskRef identifies the WarmupTask snapshot written into each Pod's score
// annotation.
type TaskRef struct {
	Namespace  string
	Name       string
	Generation int64
}

// ScoreAnnotationKey returns the configured Pod score annotation key, falling
// back to the default. Controller and Scheduler must use the same key (D49).
func ScoreAnnotationKey(configured string) string {
	if configured == "" {
		return scorefmt.DefaultAnnotation
	}
	return configured
}

// EncodePodScores encodes the node→score snapshot for one Pod. The value is
// written in the same patch that removes the Pod's scheduling gate
// (docs/7.1 design.md §6.2).
func EncodePodScores(task TaskRef, nodeScores map[string]int32, now time.Time) (string, error) {
	if task.Namespace == "" || task.Name == "" {
		return "", fmt.Errorf("warmup: score annotation: empty task namespace or name")
	}
	return scorefmt.EncodeValue(scorefmt.Value{
		V:    1,
		Task: task.Namespace + "/" + task.Name,
		G:    task.Generation,
		T:    now.UTC(),
		S:    nodeScores,
	})
}
