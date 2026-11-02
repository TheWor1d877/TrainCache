package dataset

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
)

// m12BaseSpec returns a spec that passes every check in ValidateSpec. Its
// shards total three chunks, which the maxChunks boundary cases rely on.
func m12BaseSpec() traincachev1alpha1.DataSetSpec {
	return traincachev1alpha1.DataSetSpec{
		Endpoint:    "minio:9000",
		Bucket:      "bucket",
		DatasetName: "dataset",
		SecretRef:   corev1.LocalObjectReference{Name: "creds"},
		ChunkSize:   100,
		Shards: []traincachev1alpha1.ShardSpec{
			{Name: "a", Count: 2, LastChunkSize: 100},
			{Name: "b", Count: 1, LastChunkSize: 50},
		},
	}
}

func m12CloneSpec(spec traincachev1alpha1.DataSetSpec) traincachev1alpha1.DataSetSpec {
	spec.Shards = append([]traincachev1alpha1.ShardSpec(nil), spec.Shards...)
	return spec
}

// TestValidateSpecM12FullFieldTable covers every field ValidateSpec inspects,
// including required-string emptiness, numeric zero/negative/type-boundary
// values, selector handling, per-shard index reporting, and the maxChunks
// boundary. Each failing case asserts the exact reason substring (field name)
// instead of only err != nil.
func TestValidateSpecM12FullFieldTable(t *testing.T) {
	const (
		maxInt32 = int32(1<<31 - 1)
		minInt32 = int32(-1 << 31)
		minInt64 = int64(-1 << 63)
		maxInt64 = int64(1<<63 - 1)
	)

	tests := []struct {
		name       string
		mutate     func(*traincachev1alpha1.DataSetSpec)
		maxChunks  int64
		wantCount  int
		wantReason []string
	}{
		// Positive cases.
		{
			name:      "all fields valid",
			mutate:    func(*traincachev1alpha1.DataSetSpec) {},
			maxChunks: 100,
		},
		{
			name: "all fields valid with selector",
			mutate: func(s *traincachev1alpha1.DataSetSpec) {
				s.AccessibleNodeSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"zone": "a"}}
			},
			maxChunks: 100,
		},
		{
			name: "empty selector struct passes",
			mutate: func(s *traincachev1alpha1.DataSetSpec) {
				s.AccessibleNodeSelector = &metav1.LabelSelector{}
			},
			maxChunks: 100,
		},
		{
			name: "chunkSize and lastChunkSize at lower boundary 1",
			mutate: func(s *traincachev1alpha1.DataSetSpec) {
				s.ChunkSize = 1
				s.Shards[0].LastChunkSize = 1
				s.Shards[1].LastChunkSize = 1
			},
			maxChunks: 100,
		},
		{
			name: "lastChunkSize equals chunkSize at upper boundary",
			mutate: func(s *traincachev1alpha1.DataSetSpec) {
				s.Shards[1].LastChunkSize = s.ChunkSize
			},
			maxChunks: 100,
		},
		{
			name: "lastChunkSize 1 below chunkSize",
			mutate: func(s *traincachev1alpha1.DataSetSpec) {
				s.Shards[0].LastChunkSize = 1
			},
			maxChunks: 100,
		},
		{
			name: "count at int32 upper boundary with cap disabled",
			mutate: func(s *traincachev1alpha1.DataSetSpec) {
				s.Shards[0].Count = maxInt32
			},
			maxChunks: 0,
		},
		{
			name:      "maxChunks equal to total chunks",
			mutate:    func(*traincachev1alpha1.DataSetSpec) {},
			maxChunks: 3,
		},
		{
			name:      "maxChunks zero disables cap",
			mutate:    func(*traincachev1alpha1.DataSetSpec) {},
			maxChunks: 0,
		},
		{
			name:      "maxChunks negative disables cap",
			mutate:    func(*traincachev1alpha1.DataSetSpec) {},
			maxChunks: -7,
		},

		// Required string fields.
		{
			name:       "endpoint empty",
			mutate:     func(s *traincachev1alpha1.DataSetSpec) { s.Endpoint = "" },
			maxChunks:  100,
			wantCount:  1,
			wantReason: []string{"spec.endpoint must not be empty"},
		},
		{
			name:       "bucket empty",
			mutate:     func(s *traincachev1alpha1.DataSetSpec) { s.Bucket = "" },
			maxChunks:  100,
			wantCount:  1,
			wantReason: []string{"spec.bucket must not be empty"},
		},
		{
			name:       "datasetName empty",
			mutate:     func(s *traincachev1alpha1.DataSetSpec) { s.DatasetName = "" },
			maxChunks:  100,
			wantCount:  1,
			wantReason: []string{"spec.datasetName must not be empty"},
		},
		{
			name:       "secretRef name empty",
			mutate:     func(s *traincachev1alpha1.DataSetSpec) { s.SecretRef.Name = "" },
			maxChunks:  100,
			wantCount:  1,
			wantReason: []string{"spec.secretRef.name must not be empty"},
		},
		{
			name: "all required strings empty reported together",
			mutate: func(s *traincachev1alpha1.DataSetSpec) {
				s.Endpoint = ""
				s.Bucket = ""
				s.DatasetName = ""
				s.SecretRef.Name = ""
			},
			maxChunks: 100,
			wantCount: 4,
			wantReason: []string{
				"spec.endpoint must not be empty",
				"spec.bucket must not be empty",
				"spec.datasetName must not be empty",
				"spec.secretRef.name must not be empty",
			},
		},

		// Shards list.
		{
			name:       "shards nil",
			mutate:     func(s *traincachev1alpha1.DataSetSpec) { s.Shards = nil },
			maxChunks:  100,
			wantCount:  1,
			wantReason: []string{"spec.shards must not be empty"},
		},
		{
			name:       "shards empty non-nil",
			mutate:     func(s *traincachev1alpha1.DataSetSpec) { s.Shards = []traincachev1alpha1.ShardSpec{} },
			maxChunks:  100,
			wantCount:  1,
			wantReason: []string{"spec.shards must not be empty"},
		},

		// ChunkSize numeric boundaries.
		{
			name:       "chunkSize zero",
			mutate:     func(s *traincachev1alpha1.DataSetSpec) { s.ChunkSize = 0 },
			maxChunks:  100,
			wantCount:  1,
			wantReason: []string{"spec.chunkSize must be > 0", "got 0"},
		},
		{
			name:       "chunkSize negative",
			mutate:     func(s *traincachev1alpha1.DataSetSpec) { s.ChunkSize = -1 },
			maxChunks:  100,
			wantCount:  1,
			wantReason: []string{"spec.chunkSize must be > 0", "got -1"},
		},
		{
			name:       "chunkSize int64 lower boundary",
			mutate:     func(s *traincachev1alpha1.DataSetSpec) { s.ChunkSize = minInt64 },
			maxChunks:  100,
			wantCount:  1,
			wantReason: []string{"spec.chunkSize must be > 0"},
		},
		{
			name:       "chunkSize at int64 upper boundary",
			mutate:     func(s *traincachev1alpha1.DataSetSpec) { s.ChunkSize = maxInt64 },
			maxChunks:  100,
			wantCount:  0,
			wantReason: nil,
		},
		{
			name: "non-positive chunkSize suppresses exceeds comparison",
			mutate: func(s *traincachev1alpha1.DataSetSpec) {
				s.ChunkSize = 0 // lastChunkSize 100 would "exceed" it, but the guard skips the comparison.
			},
			maxChunks:  100,
			wantCount:  1,
			wantReason: []string{"spec.chunkSize must be > 0"},
		},

		// Shard name.
		{
			name:       "shard 0 name empty",
			mutate:     func(s *traincachev1alpha1.DataSetSpec) { s.Shards[0].Name = "" },
			maxChunks:  100,
			wantCount:  1,
			wantReason: []string{"spec.shards[0].name must not be empty"},
		},
		{
			name:       "shard 1 name empty reports index 1",
			mutate:     func(s *traincachev1alpha1.DataSetSpec) { s.Shards[1].Name = "" },
			maxChunks:  100,
			wantCount:  1,
			wantReason: []string{"spec.shards[1].name must not be empty"},
		},
		{
			name:       "two empty names are not counted as duplicates",
			mutate:     func(s *traincachev1alpha1.DataSetSpec) { s.Shards[0].Name = ""; s.Shards[1].Name = "" },
			maxChunks:  100,
			wantCount:  2,
			wantReason: []string{"spec.shards[0].name must not be empty", "spec.shards[1].name must not be empty"},
		},
		{
			name:       "duplicate shard name reports later index",
			mutate:     func(s *traincachev1alpha1.DataSetSpec) { s.Shards[1].Name = "a" },
			maxChunks:  100,
			wantCount:  1,
			wantReason: []string{`spec.shards[1].name "a" is duplicated`},
		},
		{
			name: "non-adjacent duplicate reports last occurrence",
			mutate: func(s *traincachev1alpha1.DataSetSpec) {
				s.Shards = append(s.Shards, traincachev1alpha1.ShardSpec{Name: "a", Count: 1, LastChunkSize: 10})
			},
			maxChunks:  100,
			wantCount:  1,
			wantReason: []string{`spec.shards[2].name "a" is duplicated`},
		},

		// Shard Count numeric boundaries.
		{
			name:       "shard 0 count zero",
			mutate:     func(s *traincachev1alpha1.DataSetSpec) { s.Shards[0].Count = 0 },
			maxChunks:  100,
			wantCount:  1,
			wantReason: []string{"spec.shards[0].count must be > 0", "got 0"},
		},
		{
			name:       "shard 1 count negative reports index 1",
			mutate:     func(s *traincachev1alpha1.DataSetSpec) { s.Shards[1].Count = -1 },
			maxChunks:  100,
			wantCount:  1,
			wantReason: []string{"spec.shards[1].count must be > 0", "got -1"},
		},
		{
			name:       "shard count int32 lower boundary",
			mutate:     func(s *traincachev1alpha1.DataSetSpec) { s.Shards[0].Count = minInt32 },
			maxChunks:  100,
			wantCount:  1,
			wantReason: []string{"spec.shards[0].count must be > 0"},
		},
		{
			name:       "shard count 1 at lower boundary",
			mutate:     func(s *traincachev1alpha1.DataSetSpec) { s.Shards[0].Count = 1 },
			maxChunks:  3,
			wantCount:  0,
			wantReason: nil,
		},

		// Shard LastChunkSize numeric boundaries.
		{
			name:       "shard 0 lastChunkSize zero",
			mutate:     func(s *traincachev1alpha1.DataSetSpec) { s.Shards[0].LastChunkSize = 0 },
			maxChunks:  100,
			wantCount:  1,
			wantReason: []string{"spec.shards[0].lastChunkSize must be > 0", "got 0"},
		},
		{
			name:       "shard 1 lastChunkSize negative reports index 1",
			mutate:     func(s *traincachev1alpha1.DataSetSpec) { s.Shards[1].LastChunkSize = -1 },
			maxChunks:  100,
			wantCount:  1,
			wantReason: []string{"spec.shards[1].lastChunkSize must be > 0", "got -1"},
		},
		{
			name:       "shard lastChunkSize int64 lower boundary",
			mutate:     func(s *traincachev1alpha1.DataSetSpec) { s.Shards[0].LastChunkSize = minInt64 },
			maxChunks:  100,
			wantCount:  1,
			wantReason: []string{"spec.shards[0].lastChunkSize must be > 0"},
		},
		{
			name:       "lastChunkSize at int64 upper boundary exceeds chunkSize",
			mutate:     func(s *traincachev1alpha1.DataSetSpec) { s.Shards[0].LastChunkSize = maxInt64 },
			maxChunks:  100,
			wantCount:  1,
			wantReason: []string{"exceeds chunkSize 100"},
		},
		{
			name:       "lastChunkSize one above chunkSize",
			mutate:     func(s *traincachev1alpha1.DataSetSpec) { s.Shards[0].LastChunkSize = 101 },
			maxChunks:  100,
			wantCount:  1,
			wantReason: []string{"spec.shards[0].lastChunkSize 101 exceeds chunkSize 100"},
		},

		// Multi-error aggregation.
		{
			name: "independent violations all reported",
			mutate: func(s *traincachev1alpha1.DataSetSpec) {
				s.Bucket = ""
				s.Shards[0].Count = 0
				s.Shards[1].LastChunkSize = 101
			},
			maxChunks: 100,
			wantCount: 3,
			wantReason: []string{
				"spec.bucket must not be empty",
				"spec.shards[0].count must be > 0",
				"spec.shards[1].lastChunkSize 101 exceeds chunkSize 100",
			},
		},

		// maxChunks boundary.
		{
			name:       "total chunks exceed maxChunks by one",
			mutate:     func(*traincachev1alpha1.DataSetSpec) {},
			maxChunks:  2,
			wantCount:  1,
			wantReason: []string{"total chunks 3 exceed configured maximum 2"},
		},

		// AccessibleNodeSelector.
		{
			name: "invalid selector reports field name",
			mutate: func(s *traincachev1alpha1.DataSetSpec) {
				s.AccessibleNodeSelector = &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
					{Key: "zone", Operator: "Bogus"},
				}}
			},
			maxChunks:  100,
			wantCount:  1,
			wantReason: []string{"spec.accessibleNodeSelector is invalid"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := m12CloneSpec(m12BaseSpec())
			tt.mutate(&spec)

			got := ValidateSpec(spec, tt.maxChunks)
			if len(got) != tt.wantCount {
				t.Fatalf("ValidateSpec reasons = %v, want %d reason(s)", got, tt.wantCount)
			}
			joined := strings.Join(got, "\n")
			for _, want := range tt.wantReason {
				if !strings.Contains(joined, want) {
					t.Errorf("ValidateSpec reasons = %v, want substring %q", got, want)
				}
			}
		})
	}
}
