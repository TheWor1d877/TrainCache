package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AffinityPolicy records the user's affinity intent. v1 does not feed it into
// the GWAA algorithm.
//
// +kubebuilder:validation:Enum=Any;Pack;Spread
type AffinityPolicy string

const (
	AffinityAny    AffinityPolicy = "Any"
	AffinityPack   AffinityPolicy = "Pack"
	AffinitySpread AffinityPolicy = "Spread"
)

// TrainingType is the training phase the task is prepared for.
//
// +kubebuilder:validation:Enum=epoch;shuffle;curriculum;static
type TrainingType string

const (
	TrainingTypeEpoch      TrainingType = "epoch"
	TrainingTypeShuffle    TrainingType = "shuffle"
	TrainingTypeCurriculum TrainingType = "curriculum"
	TrainingTypeStatic     TrainingType = "static"
)

// WarmupPhase is the WarmupTask lifecycle phase.
//
// +kubebuilder:validation:Enum=Pending;Filtering;Preheating;Gating;Released;Degraded;Failed
type WarmupPhase string

const (
	WarmupPhasePending    WarmupPhase = "Pending"
	WarmupPhaseFiltering  WarmupPhase = "Filtering"
	WarmupPhasePreheating WarmupPhase = "Preheating"
	WarmupPhaseGating     WarmupPhase = "Gating"
	WarmupPhaseReleased   WarmupPhase = "Released"
	WarmupPhaseDegraded   WarmupPhase = "Degraded"
	WarmupPhaseFailed     WarmupPhase = "Failed"
)

// ConsumeOrderSource reports where the effective consume order came from.
//
// +kubebuilder:validation:Enum=Spec;Default
type ConsumeOrderSource string

const (
	ConsumeOrderSourceSpec    ConsumeOrderSource = "Spec"
	ConsumeOrderSourceDefault ConsumeOrderSource = "Default"
)

// GateState is the gate decision.
//
// +kubebuilder:validation:Enum=Hold;Released;Degraded
type GateState string

const (
	GateStateHold     GateState = "Hold"
	GateStateReleased GateState = "Released"
	GateStateDegraded GateState = "Degraded"
)

// PreheatState is the per-(pod,node) preheat state.
//
// +kubebuilder:validation:Enum=Preheating;Ready;Stopped;Failed
type PreheatState string

const (
	PreheatStatePreheating PreheatState = "Preheating"
	PreheatStateReady      PreheatState = "Ready"
	PreheatStateStopped    PreheatState = "Stopped"
	PreheatStateFailed     PreheatState = "Failed"
)

// TargetRef identifies the workload whose Pods this WarmupTask preheats.
type TargetRef struct {
	// APIVersion is the target workload's API version.
	// +kubebuilder:validation:MinLength=1
	APIVersion string `json:"apiVersion"`

	// Kind is Job, StatefulSet or Pod.
	// +kubebuilder:validation:Enum=Job;StatefulSet;Pod
	Kind string `json:"kind"`

	// Name is the target workload name.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// PodLabelSelector optionally selects the target Pods.
	// +optional
	PodLabelSelector *metav1.LabelSelector `json:"podLabelSelector,omitempty"`
}

// PodDetail describes one training Pod's window and resources.
type PodDetail struct {
	// Index is the Pod ordinal in 0..PodCount-1; it must be dense.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=0
	Index int32 `json:"index"`

	// Resources are the Pod's requests, used for node capacity filtering.
	Resources corev1.ResourceRequirements `json:"resources"`

	// InitialConsumeOrder is the global chunk order. Empty means "use the
	// default split"; if any Pod's order is invalid the whole set falls back
	// to the default split (D29).
	// +optional
	// +kubebuilder:validation:UniqueItems=true
	InitialConsumeOrder []int32 `json:"initialConsumeOrder,omitempty"`

	// K is this Pod's startup window length and scoring plateau length.
	// +kubebuilder:validation:Minimum=1
	K int32 `json:"k"`
}

// GatePolicy configures the three-condition gate.
type GatePolicy struct {
	// Threshold is the required ready fraction, in [0,1].
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	Threshold float64 `json:"threshold"`

	// GapMin is the marginal-gain gap threshold, in [0,1].
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	GapMin float64 `json:"gapMin"`

	// SlopeMin is the marginal-gain slope threshold, fraction per second.
	SlopeMin float64 `json:"slopeMin"`

	// TMax is the absolute gate timeout.
	TMax metav1.Duration `json:"tMax"`

	// MinInterval is the minimum slope sampling interval.
	MinInterval metav1.Duration `json:"minInterval"`
}

// ScorePolicy configures the global decay score. There is no window field:
// each Pod's K is the single window (D25).
type ScorePolicy struct {
	// HalfLife is H, the decay half-life in chunks.
	// +kubebuilder:validation:Minimum=0
	HalfLife float64 `json:"halfLife"`

	// Cutoff is C, the truncation distance in chunks.
	// +kubebuilder:validation:Minimum=0
	Cutoff int32 `json:"cutoff"`
}

// CandidatePolicy restricts candidate nodes for preheating.
type CandidatePolicy struct {
	// RequireReady requires a candidate node to be Ready.
	RequireReady bool `json:"requireReady"`

	// IgnoreNoSchedule ignores the NoSchedule taint when filtering candidates.
	IgnoreNoSchedule bool `json:"ignoreNoSchedule"`

	// MinFreeDiskBytes is the minimum free disk required on a candidate node.
	// +optional
	MinFreeDiskBytes int64 `json:"minFreeDiskBytes,omitempty"`
}

// WarmupTaskSpec is immutable in podCount and dataSetRef; all other fields may
// be updated. The controller never creates, deletes or scales Pods (D31).
//
// +kubebuilder:validation:XValidation:rule="self.preheatFanoutMin <= self.preheatFanout",message="preheatFanoutMin must not exceed preheatFanout"
type WarmupTaskSpec struct {
	// DataSetRef points at a same-namespace DataSet.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="dataSetRef is immutable"
	DataSetRef corev1.LocalObjectReference `json:"dataSetRef"`

	TargetRef TargetRef `json:"targetRef"`

	// PodCount is the fixed number of training Pods.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="podCount is immutable"
	PodCount int32 `json:"podCount"`

	// PreheatFanout is the upper bound on the number of preheat nodes each Pod
	// claims: default 1, typical value 2. Zero and negative values are rejected
	// by the CRD. GWAA chooses the target count within
	// [PreheatFanoutMin, PreheatFanout]. The actual Assignment may be below
	// the lower bound when conflicts, capacity limits or out-of-target fallback
	// cause MinUnmet, but it never exceeds this upper bound.
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	PreheatFanout int32 `json:"preheatFanout,omitempty"`

	// PreheatFanoutMin is the target lower bound on the number of preheat nodes
	// each Pod claims. Default 1; values below 1 are rejected by the CRD. GWAA
	// chooses the target count within [PreheatFanoutMin, PreheatFanout]. The
	// actual Assignment may fall below this bound when conflicts, capacity
	// limits or out-of-target fallback cause MinUnmet; it is not a hard
	// guarantee. The actual Assignment never exceeds PreheatFanout.
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	PreheatFanoutMin int32 `json:"preheatFanoutMin,omitempty"`

	// Affinity is recorded for v1 and does not affect GWAA.
	// +optional
	Affinity AffinityPolicy `json:"affinity,omitempty"`

	TrainingType TrainingType `json:"trainingType"`

	PodDetails []PodDetail `json:"podDetails"`

	Gate GatePolicy `json:"gate"`

	Score ScorePolicy `json:"score"`

	// Candidate optionally configures candidate node filtering.
	// +optional
	Candidate CandidatePolicy `json:"candidate,omitempty"`
}

// PodNodeReadiness is one Pod's readiness fact on one of its preheat nodes.
// Status stores one entry per actual (Pod, preheat node) pair. The entry count
// per Pod is usually between spec.preheatFanoutMin and spec.preheatFanout, but
// it may fall below the minimum when too few nodes are feasible, conflicts
// leave a Pod with fewer claims, or capacity limits the assignment; that case
// is reported as MinUnmet. It never exceeds spec.preheatFanout. A Pod with no
// feasible node gets a single summary entry. Per-candidate-node ready-block
// counts live in WarmupTaskStatus.ReadinessTable.
type PodNodeReadiness struct {
	PodIndex int32  `json:"podIndex"`
	PodName  string `json:"podName,omitempty"`
	// NodeName is this entry's preheat node.
	NodeName string `json:"nodeName,omitempty"`
	// ActualNode is the Pod's actual landing node; all entries of the same Pod
	// carry the same value.
	ActualNode string `json:"actualNode,omitempty"`
	// ReadyBlockBitmap is the per-request-chunk readiness bitmap reported by
	// the Agent (bit i == submitted chunks[i]); 1 means persisted and verified.
	ReadyBlockBitmap []byte `json:"readyBlockBitmap,omitempty"`
	// StartupWindowReady is ReadyInWindow's source value; both are assigned in
	// the same computation (D41) and this field is kept for compatibility.
	StartupWindowReady int32 `json:"startupWindowReady,omitempty"`
	// ReadyInWindow mirrors StartupWindowReady for the 8.1 gate contract.
	ReadyInWindow int32 `json:"readyInWindow,omitempty"`
	// RequiredReady is floor(threshold*K_i) for the owning Pod.
	RequiredReady int32 `json:"requiredReady,omitempty"`
	// StartupWindowReadyRate is observability only and never gates.
	StartupWindowReadyRate float64 `json:"startupWindowReadyRate,omitempty"`
	GlobalDecayScore       int32   `json:"globalDecayScore,omitempty"`
	// State is this preheat node's state. When a Pod has no feasible node it
	// gets one summary entry with an empty NodeName and the Pod-level state.
	State          PreheatState `json:"state,omitempty"`
	LastReportTime *metav1.Time `json:"lastReportTime,omitempty"`
	// MinUnmet reports whether this Pod's final GWAA assignment claims fewer
	// than spec.preheatFanoutMin nodes (D40). All entries of the same Pod
	// carry the same value; the Poll loop never recomputes it.
	MinUnmet bool `json:"minUnmet,omitempty"`
}

// GateDecision is the frozen gate verdict.
type GateDecision struct {
	State     GateState    `json:"state,omitempty"`
	Reason    string       `json:"reason,omitempty"`
	DecidedAt *metav1.Time `json:"decidedAt,omitempty"`
	PodRates  []PodRate    `json:"podRates,omitempty"`
}

// PodRate is one Pod's marginal-gain observation at gate time.
type PodRate struct {
	PodIndex int32   `json:"podIndex"`
	Rate     float64 `json:"rate"`
	Gap      float64 `json:"gap"`
	Slope    float64 `json:"slope"`
}

// WarmupTaskStatus is written by the WarmupTask controller only.
type WarmupTaskStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	Phase              WarmupPhase        `json:"phase,omitempty"`

	// ConsumeOrderSource is Spec or Default.
	ConsumeOrderSource ConsumeOrderSource `json:"consumeOrderSource,omitempty"`

	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// CandidateNodes is the stable, ascending node list and the readiness-table
	// column order.
	CandidateNodes []string `json:"candidateNodes,omitempty"`

	// ReadinessTable is P x N window ready-block counts (not ratios). Rows are
	// indexed by PodDetail.Index and columns follow CandidateNodes order.
	ReadinessTable [][]int32 `json:"readinessTable,omitempty"`

	// PodNodeReadiness has one entry per actual (Pod, preheat node) pair.
	// Entries per Pod are usually between spec.preheatFanoutMin and
	// spec.preheatFanout and never exceed the maximum. They can fall below the
	// minimum when infeasible nodes, conflicts or capacity limits leave a Pod
	// short; MinUnmet reports that case. A Pod with no feasible node gets one
	// summary entry.
	PodNodeReadiness []PodNodeReadiness `json:"podNodeReadiness,omitempty"`

	GateDecision GateDecision `json:"gateDecision,omitempty"`

	LastUpdated *metav1.Time `json:"lastUpdated,omitempty"`
}

// WarmupTask is the schema for the warmuptasks API.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Gate",type=string,JSONPath=`.status.gateDecision.state`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type WarmupTask struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WarmupTaskSpec   `json:"spec,omitempty"`
	Status WarmupTaskStatus `json:"status,omitempty"`
}

// WarmupTaskList contains a list of WarmupTask objects.
//
// +kubebuilder:object:root=true
type WarmupTaskList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WarmupTask `json:"items"`
}
