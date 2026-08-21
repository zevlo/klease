/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// GPULeaseSpec defines the desired state of GPULease.
//
// A lease points at an existing workload (never defines one), queues FIFO by
// creationTimestamp, and holds the GPU for a time-boxed slot that starts at
// admission — not at creation.
//
// +kubebuilder:validation:XValidation:rule="!has(self.gracePeriod) || duration(self.gracePeriod) <= duration(self.duration)",message="gracePeriod must not exceed duration"
type GPULeaseSpec struct {
	// workloadRef identifies the workload that holds the GPU while this lease
	// is Active. It must live in the same namespace as the lease.
	// Changing what a lease points at would hand the GPU to a different
	// workload without a drain, so it is immutable: create a new lease.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="workloadRef is immutable"
	WorkloadRef WorkloadRef `json:"workloadRef"`

	// duration is how long the lease holds the GPU once admitted. The timer
	// starts at admission (status.activeSince), not at creation — a queued
	// lease does not burn its slot waiting.
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s') && duration(self) <= duration('24h')",message="duration must be greater than 0 and at most 24h"
	Duration metav1.Duration `json:"duration"`

	// gracePeriod is how long the drain may take after expiry before the
	// holder's pods are force-deleted. Defaults to 5m.
	// +kubebuilder:validation:Default="5m"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('0s')",message="gracePeriod must not be negative"
	// +optional
	GracePeriod *metav1.Duration `json:"gracePeriod,omitempty"`
}

// WorkloadRef points to an existing workload that the lease arbitrates.
type WorkloadRef struct {
	// kind of the workload. Only Deployment is supported in v0.1.
	// +kubebuilder:validation:Enum=Deployment
	Kind string `json:"kind"`

	// name of the workload, in the same namespace as the lease.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
}

// GPULeaseState is the lifecycle phase of a lease.
// +kubebuilder:validation:Enum=Pending;Active;Draining;Expired
type GPULeaseState string

const (
	// GPULeaseStatePending means the lease is queued, waiting for the GPU.
	GPULeaseStatePending GPULeaseState = "Pending"
	// GPULeaseStateActive means the lease currently holds the GPU.
	GPULeaseStateActive GPULeaseState = "Active"
	// GPULeaseStateDraining means the lease expired and its workload is being
	// reclaimed: replicas are driven to 0 and leftover pods are force-deleted
	// at status.drainDeadline. No new lease is admitted while a drain runs.
	GPULeaseStateDraining GPULeaseState = "Draining"
	// GPULeaseStateExpired means the lease is finished and the GPU has been handed off.
	GPULeaseStateExpired GPULeaseState = "Expired"
)

// Condition types for GPULease.
const (
	// GPULeaseConditionWorkloadNotFound is True when the lease's workloadRef
	// target does not exist (or is not opted in to klease). The lease holds
	// Pending until the target is admissible.
	GPULeaseConditionWorkloadNotFound = "WorkloadNotFound"
)

// ManagedLabelKey is the label that opts a workload into klease arbitration.
// Workloads carrying this label obey the invariant: no active lease
// referencing them -> replicas 0.
const ManagedLabelKey = "klease.zachallen.io/managed"

// GPULeaseStatus defines the observed state of GPULease.
type GPULeaseStatus struct {
	// state is the current lifecycle phase: Pending, Active, Draining, or Expired.
	// +optional
	State GPULeaseState `json:"state,omitempty"`

	// activeSince is when the lease was admitted (Pending -> Active).
	// +optional
	ActiveSince *metav1.Time `json:"activeSince,omitempty"`

	// expiresAt is activeSince + spec.duration; drain starts when it is reached.
	// +optional
	ExpiresAt *metav1.Time `json:"expiresAt,omitempty"`

	// drainDeadline is expiresAt + spec.gracePeriod, stamped when the lease
	// enters Draining; leftover pods are force-deleted when it is reached.
	// +optional
	DrainDeadline *metav1.Time `json:"drainDeadline,omitempty"`

	// queuePosition is the lease's position in the global FIFO queue
	// (0 for the queue head or the Active lease).
	// +optional
	QueuePosition int32 `json:"queuePosition,omitempty"`

	// conditions represent the current state of the GPULease resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=gl
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="Workload",type=string,JSONPath=`.spec.workloadRef.name`
// +kubebuilder:printcolumn:name="Duration",type=string,JSONPath=`.spec.duration`
// +kubebuilder:printcolumn:name="Expires",type=string,JSONPath=`.status.expiresAt`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// GPULease is the Schema for the gpuleases API. It arbitrates access to a
// shared GPU: workloads labeled klease.zachallen.io/managed="true" obey the
// invariant "no active lease -> replicas 0". At most one lease is Active
// cluster-wide at any moment.
type GPULease struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of GPULease
	// +required
	Spec GPULeaseSpec `json:"spec"`

	// status defines the observed state of GPULease
	// +optional
	Status GPULeaseStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// GPULeaseList contains a list of GPULease
type GPULeaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []GPULease `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &GPULease{}, &GPULeaseList{})
		return nil
	})
}
