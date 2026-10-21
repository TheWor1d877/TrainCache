// Package v1alpha1 contains the TrainCache control-plane API types.
//
// The package deliberately depends on k8s.io/apimachinery only: the API
// contract must stay usable by webhook and scheduler binaries without pulling
// in controller-runtime client machinery.
//
// +kubebuilder:object:generate=true
// +groupName=traincache.io
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is the group and version used to register these objects.
	GroupVersion = schema.GroupVersion{Group: "traincache.io", Version: "v1alpha1"}

	// SchemeBuilder registers the CRD types with a runtime.Scheme.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion,
		&DataSet{},
		&DataSetList{},
		&WarmupTask{},
		&WarmupTaskList{},
	)
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}
