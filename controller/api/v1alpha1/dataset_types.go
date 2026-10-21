package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DataSetSpec declares a published dataset snapshot and the chunk layout the
// control plane must freeze in status.
//
// Chunk numbering is frozen exactly once from this spec (D3/D4): the global
// chunk number is the running sum of Shards[].Count in declaration order.
type DataSetSpec struct {
	// Endpoint is the S3-compatible endpoint, for example s3.example.com:9000.
	// +kubebuilder:validation:MinLength=1
	Endpoint string `json:"endpoint"`

	// Bucket is the bucket that holds the published dataset.
	// +kubebuilder:validation:MinLength=1
	Bucket string `json:"bucket"`

	// DatasetName is the dataset path prefix inside the bucket.
	// +kubebuilder:validation:MinLength=1
	DatasetName string `json:"datasetName"`

	// SecretRef points at a same-namespace Secret holding accessKey/secretKey.
	SecretRef corev1.LocalObjectReference `json:"secretRef"`

	// SnapshotID selects a specific snapshot; empty means CURRENT.
	// +optional
	SnapshotID string `json:"snapshotID,omitempty"`

	// ChunkSize is the byte size of every non-final chunk.
	// +kubebuilder:validation:Minimum=1
	ChunkSize int64 `json:"chunkSize"`

	// Shards is the ordered shard list; global chunk numbers accumulate across it.
	// +kubebuilder:validation:MinItems=1
	Shards []ShardSpec `json:"shards"`

	// AccessibleNodeSelector optionally narrows the default
	// traincache.io/s3-access=true node set. The label is maintained by humans
	// and is never written by any TrainCache component (D30).
	// +optional
	AccessibleNodeSelector *metav1.LabelSelector `json:"accessibleNodeSelector,omitempty"`

	// Shuffle records the dataset's default shuffle capability.
	// +optional
	Shuffle bool `json:"shuffle,omitempty"`

	// Metadata carries dataset registration metadata. It is informational only;
	// neither the Controller nor the Agent reads it.
	// +optional
	Metadata *DataSetMetadata `json:"metadata,omitempty"`
}

// DataSetMetadata records dataset metadata. It is informational only; neither
// the Controller nor the Agent reads it.
type DataSetMetadata struct {
	// SampleCount is the number of training samples.
	// +optional
	SampleCount int64 `json:"sampleCount,omitempty"`

	// Format is the dataset serialization format.
	// +optional
	Format string `json:"format,omitempty"`

	// Description is a human-readable description.
	// +optional
	Description string `json:"description,omitempty"`

	// Version is the dataset version label.
	// +optional
	Version string `json:"version,omitempty"`

	// Source is the provenance of the dataset.
	// +optional
	Source string `json:"source,omitempty"`

	// Purpose is the intended training purpose.
	// +optional
	Purpose string `json:"purpose,omitempty"`
}

// ShardSpec describes one publish shard.
type ShardSpec struct {
	// Name is the shard name used by core/publish.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Count is the number of chunks in this shard.
	// +kubebuilder:validation:Minimum=1
	Count int32 `json:"count"`

	// LastChunkSize is the byte size of the final chunk in this shard.
	// +kubebuilder:validation:Minimum=1
	LastChunkSize int64 `json:"lastChunkSize"`
}

// BlockLayout freezes the global chunk range of one shard.
type BlockLayout struct {
	// Shard is the shard name.
	Shard string `json:"shard"`

	// StartChunk is the global chunk number of this shard's first chunk.
	StartChunk int32 `json:"startChunk"`

	// Count is the shard's chunk count.
	Count int32 `json:"count"`

	// LastChunkSize is the shard's final chunk size in bytes.
	LastChunkSize int64 `json:"lastChunkSize"`
}

// DataSetStatus is written by the DataSet controller only.
type DataSetStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`

	// SnapshotID is the resolved concrete snapshot (never CURRENT).
	SnapshotID string `json:"snapshotID,omitempty"`

	// TotalChunks is the sum of Shards[].Count.
	TotalChunks int32 `json:"totalChunks,omitempty"`

	// BlockLayout is the frozen global chunk layout, in spec shard order.
	BlockLayout []BlockLayout `json:"blockLayout,omitempty"`

	// AccessibleNodes is the resolved, ascending node list.
	AccessibleNodes []string `json:"accessibleNodes,omitempty"`
}

// DataSet is the schema for the datasets API.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Snapshot",type=string,JSONPath=`.status.snapshotID`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type DataSet struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DataSetSpec   `json:"spec,omitempty"`
	Status DataSetStatus `json:"status,omitempty"`
}

// DataSetList contains a list of DataSet objects.
//
// +kubebuilder:object:root=true
type DataSetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DataSet `json:"items"`
}
