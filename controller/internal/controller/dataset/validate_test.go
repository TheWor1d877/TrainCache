package dataset

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
)

func TestValidateSpec(t *testing.T) {
	valid := validSpec()

	tests := []struct {
		name   string
		mutate func(*traincachev1alpha1.DataSetSpec)
		max    int64
		want   int
	}{
		{name: "valid", mutate: func(*traincachev1alpha1.DataSetSpec) {}, max: 100, want: 0},
		{name: "zero chunk size", mutate: func(s *traincachev1alpha1.DataSetSpec) { s.ChunkSize = 0 }, max: 100, want: 1},
		{name: "no shards", mutate: func(s *traincachev1alpha1.DataSetSpec) { s.Shards = nil }, max: 100, want: 1},
		{name: "empty shard name", mutate: func(s *traincachev1alpha1.DataSetSpec) { s.Shards[0].Name = "" }, max: 100, want: 1},
		{name: "duplicate shard name", mutate: func(s *traincachev1alpha1.DataSetSpec) {
			s.Shards = append(s.Shards, s.Shards[0])
		}, max: 100, want: 1},
		{name: "zero count", mutate: func(s *traincachev1alpha1.DataSetSpec) { s.Shards[0].Count = 0 }, max: 100, want: 1},
		{name: "zero last chunk size", mutate: func(s *traincachev1alpha1.DataSetSpec) { s.Shards[0].LastChunkSize = 0 }, max: 100, want: 1},
		{name: "last chunk larger than chunk size", mutate: func(s *traincachev1alpha1.DataSetSpec) {
			s.Shards[0].LastChunkSize = s.ChunkSize + 1
		}, max: 100, want: 1},
		{name: "exceeds configured maximum", mutate: func(*traincachev1alpha1.DataSetSpec) {}, max: 1, want: 1},
		{name: "empty endpoint", mutate: func(s *traincachev1alpha1.DataSetSpec) { s.Endpoint = "" }, max: 100, want: 1},
		{name: "empty secret ref", mutate: func(s *traincachev1alpha1.DataSetSpec) { s.SecretRef.Name = "" }, max: 100, want: 1},
		{name: "invalid selector", mutate: func(s *traincachev1alpha1.DataSetSpec) {
			s.AccessibleNodeSelector = &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
				{Key: "zone", Operator: "Bogus"},
			}}
		}, max: 100, want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := valid
			spec.Shards = append([]traincachev1alpha1.ShardSpec(nil), valid.Shards...)
			tt.mutate(&spec)
			if got := len(ValidateSpec(spec, tt.max)); got != tt.want {
				t.Fatalf("ValidateSpec reasons = %v, want %d", ValidateSpec(spec, tt.max), tt.want)
			}
		})
	}
}

func validSpec() traincachev1alpha1.DataSetSpec {
	return traincachev1alpha1.DataSetSpec{
		Endpoint:    "minio:9000",
		Bucket:      "bucket",
		DatasetName: "ds",
		SecretRef:   corev1.LocalObjectReference{Name: "creds"},
		ChunkSize:   100,
		Shards: []traincachev1alpha1.ShardSpec{
			{Name: "a", Count: 2, LastChunkSize: 100},
			{Name: "b", Count: 1, LastChunkSize: 50},
		},
	}
}
