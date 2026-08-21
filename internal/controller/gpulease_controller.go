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

package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kleasev1alpha1 "github.com/zevlo/klease/api/v1alpha1"
)

// workloadNotFoundRequeue is how long a lease with a missing workloadRef waits
// before checking again. Controlled requeue instead of error backoff: a missing
// Deployment is an expected state, not a failure.
const workloadNotFoundRequeue = 30 * time.Second

// GPULeaseReconciler reconciles a GPULease object
type GPULeaseReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=klease.zachallen.io,resources=gpuleases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=klease.zachallen.io,resources=gpuleases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=klease.zachallen.io,resources=gpuleases/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;update;patch

// Reconcile moves the cluster toward the desired state of a GPULease.
//
// Phase 1 scope (pointer mechanism): resolve spec.workloadRef to a Deployment
// in the lease's namespace; if missing, set WorkloadNotFound and requeue; if
// present, ensure it is scaled to 1. The four-state lifecycle, FIFO queue,
// and invariant enforcement arrive in Phase 2.
func (r *GPULeaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithValues("gpulease", req.NamespacedName)

	lease := &kleasev1alpha1.GPULease{}
	if err := r.Get(ctx, req.NamespacedName, lease); err != nil {
		if apierrors.IsNotFound(err) {
			// Deleted — nothing to do.
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	deploy := &appsv1.Deployment{}
	deployKey := types.NamespacedName{Namespace: lease.Namespace, Name: lease.Spec.WorkloadRef.Name}
	if err := r.Get(ctx, deployKey, deploy); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("workloadRef target not found; holding lease with WorkloadNotFound")
			r.Recorder.Eventf(lease, "Warning", "WorkloadNotFound",
				"workloadRef %s/%s (kind %s) does not exist",
				lease.Namespace, lease.Spec.WorkloadRef.Name, lease.Spec.WorkloadRef.Kind)
			changed := meta.SetStatusCondition(&lease.Status.Conditions, metav1.Condition{
				Type:   kleasev1alpha1.GPULeaseConditionWorkloadNotFound,
				Status: metav1.ConditionTrue,
				Reason: "WorkloadRefTargetMissing",
				Message: fmt.Sprintf("referenced %s %s/%s not found",
					lease.Spec.WorkloadRef.Kind, lease.Namespace, lease.Spec.WorkloadRef.Name),
			})
			if changed {
				if err := r.Status().Update(ctx, lease); err != nil {
					return ctrl.Result{}, err
				}
			}
			return ctrl.Result{RequeueAfter: workloadNotFoundRequeue}, nil
		}
		return ctrl.Result{}, err
	}

	// Workload exists: clear a stale WorkloadNotFound condition if present.
	clearNotFound := meta.SetStatusCondition(&lease.Status.Conditions, metav1.Condition{
		Type:   kleasev1alpha1.GPULeaseConditionWorkloadNotFound,
		Status: metav1.ConditionFalse,
		Reason: "WorkloadRefTargetFound",
		Message: fmt.Sprintf("referenced %s %s/%s found",
			lease.Spec.WorkloadRef.Kind, lease.Namespace, lease.Spec.WorkloadRef.Name),
	})
	if clearNotFound {
		if err := r.Status().Update(ctx, lease); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Scale the holder's workload to 1 (idempotent — only write on drift).
	if deploy.Spec.Replicas == nil || *deploy.Spec.Replicas != 1 {
		log.Info("scaling workload up", "deployment", deployKey, "from", deploy.Spec.Replicas)
		patch := client.MergeFrom(deploy.DeepCopy())
		one := int32(1)
		deploy.Spec.Replicas = &one
		if err := r.Patch(ctx, deploy, patch); err != nil {
			return ctrl.Result{}, err
		}
		r.Recorder.Eventf(lease, "Normal", "ScaledUp",
			"scaled %s %s/%s to 1",
			lease.Spec.WorkloadRef.Kind, lease.Namespace, lease.Spec.WorkloadRef.Name)
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *GPULeaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kleasev1alpha1.GPULease{}).
		Named("gpulease").
		Complete(r)
}
