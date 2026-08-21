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
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kleasev1alpha1 "github.com/zevlo/klease/api/v1alpha1"
)

const (
	// pendingRequeue is the safety-net cadence when leases are queued: it
	// also drives promotion after an Active lease is deleted with no other
	// trigger, and retries admission for leases with missing workloads.
	pendingRequeue = 30 * time.Second
	// minExpiryRequeue clamps the Active requeue so a lease expiring right
	// now schedules a small positive delay instead of zero or negative.
	minExpiryRequeue = time.Second
)

// GPULeaseReconciler reconciles a GPULease object.
//
// Every reconcile is a global pass over the whole queue: FIFO admission,
// the single-active invariant, and replica enforcement all run level-triggered
// regardless of which lease triggered the reconcile.
type GPULeaseReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	// NowFn overrides the clock for tests. Defaults to time.Now.
	NowFn func() time.Time
}

// +kubebuilder:rbac:groups=klease.zachallen.io,resources=gpuleases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=klease.zachallen.io,resources=gpuleases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=klease.zachallen.io,resources=gpuleases/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

// Reconcile drives the cluster toward the lease queue's desired state:
//
//   - At most one lease Active cluster-wide (FIFO by creationTimestamp)
//   - The active holder's workloadRef scaled to 1
//   - Every other managed Deployment scaled to 0 — drift of any kind
//     (manual scale-ups, pre-existing pods) is corrected every pass
//
// The Active lease requeues at its expiry; Pending leases requeue on a
// safety-net cadence so promotion never waits on an external event.
func (r *GPULeaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithValues("gpulease", req.NamespacedName)
	now := r.now()

	// The queue spans namespaces: every pass sees every lease.
	leaseList := &kleasev1alpha1.GPULeaseList{}
	if err := r.List(ctx, leaseList); err != nil {
		return ctrl.Result{}, err
	}
	leases := make([]*kleasev1alpha1.GPULease, len(leaseList.Items))
	for i := range leaseList.Items {
		leases[i] = &leaseList.Items[i]
	}

	// Maintain WorkloadNotFound conditions; they gate admission.
	if err := r.maintainWorkloadConditions(ctx, leases); err != nil {
		return ctrl.Result{}, err
	}

	// Pure scheduling pass.
	result := computeQueue(leases, now, func(l *kleasev1alpha1.GPULease) bool {
		return !conditionTrue(l, kleasev1alpha1.GPULeaseConditionWorkloadNotFound)
	})

	// Persist status mutations and emit transition events.
	for _, l := range result.Changed {
		if err := r.Status().Update(ctx, l); err != nil {
			return ctrl.Result{}, err
		}
	}
	for _, t := range result.Transitions {
		log.Info("lease transition", "lease", types.NamespacedName{Namespace: t.Lease.Namespace, Name: t.Lease.Name}, "kind", t.Kind)
		r.Recorder.Event(t.Lease, "Normal", t.Kind, t.Message)
	}

	// Level-triggered invariant enforcement across managed workloads.
	if err := r.enforceInvariant(ctx, result.Active); err != nil {
		return ctrl.Result{}, err
	}

	// Requeue against global state, not just the requested lease: whoever's
	// reconcile admitted a lease must carry the next expiry timer.
	switch {
	case result.Active != nil && result.Active.Status.ExpiresAt != nil:
		d := result.Active.Status.ExpiresAt.Time.Sub(now)
		if d < minExpiryRequeue {
			d = minExpiryRequeue
		}
		return ctrl.Result{RequeueAfter: d}, nil
	case len(pendingLeases(leases)) > 0:
		return ctrl.Result{RequeueAfter: pendingRequeue}, nil
	}
	return ctrl.Result{}, nil
}

func (r *GPULeaseReconciler) now() time.Time {
	if r.NowFn != nil {
		return r.NowFn()
	}
	return time.Now()
}

// maintainWorkloadConditions sets or clears WorkloadNotFound on every
// non-expired lease by resolving its workloadRef: a missing target or one
// without the managed label blocks admission; a transient lookup error
// leaves the condition untouched for this pass.
func (r *GPULeaseReconciler) maintainWorkloadConditions(ctx context.Context, leases []*kleasev1alpha1.GPULease) error {
	for _, l := range leases {
		if l.Status.State == kleasev1alpha1.GPULeaseStateExpired ||
			l.Status.State == kleasev1alpha1.GPULeaseStateDraining {
			continue
		}
		deploy := &appsv1.Deployment{}
		err := r.Get(ctx, types.NamespacedName{Namespace: l.Namespace, Name: l.Spec.WorkloadRef.Name}, deploy)

		var cond metav1.Condition
		switch {
		case apierrors.IsNotFound(err):
			cond = metav1.Condition{
				Type:    kleasev1alpha1.GPULeaseConditionWorkloadNotFound,
				Status:  metav1.ConditionTrue,
				Reason:  "WorkloadRefTargetMissing",
				Message: fmt.Sprintf("referenced %s %s/%s not found", l.Spec.WorkloadRef.Kind, l.Namespace, l.Spec.WorkloadRef.Name),
			}
		case err != nil:
			continue
		case deploy.Labels[kleasev1alpha1.ManagedLabelKey] != "true":
			cond = metav1.Condition{
				Type:    kleasev1alpha1.GPULeaseConditionWorkloadNotFound,
				Status:  metav1.ConditionTrue,
				Reason:  "WorkloadNotManaged",
				Message: fmt.Sprintf("referenced %s %s/%s is not labeled %s=true", l.Spec.WorkloadRef.Kind, l.Namespace, l.Spec.WorkloadRef.Name, kleasev1alpha1.ManagedLabelKey),
			}
		default:
			cond = metav1.Condition{
				Type:    kleasev1alpha1.GPULeaseConditionWorkloadNotFound,
				Status:  metav1.ConditionFalse,
				Reason:  "WorkloadRefTargetFound",
				Message: fmt.Sprintf("referenced %s %s/%s found", l.Spec.WorkloadRef.Kind, l.Namespace, l.Spec.WorkloadRef.Name),
			}
		}

		if meta.SetStatusCondition(&l.Status.Conditions, cond) {
			eventType, reason := "Normal", "WorkloadFound"
			if cond.Status == metav1.ConditionTrue {
				eventType, reason = "Warning", "WorkloadNotFound"
			}
			r.Recorder.Event(l, eventType, reason, cond.Message)
			if err := r.Status().Update(ctx, l); err != nil {
				return err
			}
		}
	}
	return nil
}

// enforceInvariant drives every managed Deployment to the replica count the
// queue demands: 1 for the active holder's workloadRef, 0 for everything
// else. Scale-downs are recorded on the Deployment so unauthorized GPU use
// is visible in events.
func (r *GPULeaseReconciler) enforceInvariant(ctx context.Context, active *kleasev1alpha1.GPULease) error {
	managed := &appsv1.DeploymentList{}
	if err := r.List(ctx, managed, client.MatchingLabels{kleasev1alpha1.ManagedLabelKey: "true"}); err != nil {
		return err
	}
	for i := range managed.Items {
		deploy := &managed.Items[i]
		want := int32(0)
		holder := active != nil &&
			active.Spec.WorkloadRef.Kind == "Deployment" &&
			active.Spec.WorkloadRef.Name == deploy.Name &&
			active.Namespace == deploy.Namespace
		if holder {
			want = 1
		}
		if deploy.Spec.Replicas != nil && *deploy.Spec.Replicas == want {
			continue
		}
		patch := client.MergeFrom(deploy.DeepCopy())
		deploy.Spec.Replicas = &want
		if err := r.Patch(ctx, deploy, patch); err != nil {
			return err
		}
		if holder {
			r.Recorder.Eventf(active, "Normal", "ScaledUp",
				"scaled %s %s/%s to 1", active.Spec.WorkloadRef.Kind, active.Namespace, active.Spec.WorkloadRef.Name)
		} else {
			r.Recorder.Event(deploy, "Normal", "ScaledDown",
				"scaled to 0: no active lease holds this workload (managed by klease)")
		}
	}
	return nil
}

func conditionTrue(l *kleasev1alpha1.GPULease, condType string) bool {
	c := meta.FindStatusCondition(l.Status.Conditions, condType)
	return c != nil && c.Status == metav1.ConditionTrue
}

// SetupWithManager sets up the controller with the Manager.
func (r *GPULeaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kleasev1alpha1.GPULease{}).
		Watches(&appsv1.Deployment{},
			handler.EnqueueRequestsFromMapFunc(r.leasesForManagedDeployment),
			builder.WithPredicates(predicate.NewPredicateFuncs(func(o client.Object) bool {
				return o.GetLabels()[kleasev1alpha1.ManagedLabelKey] == "true"
			}))).
		Named("gpulease").
		Complete(r)
}

// leasesForManagedDeployment enqueues every lease on any managed Deployment
// event. Reconciliation is a global pass, so any lease key drives the same
// invariant enforcement — this catches drift on managed workloads even when
// the drifted Deployment is referenced by no lease. Known v0.1 limit: if no
// leases exist at all, there is no key to enqueue and drift waits for the
// first lease event.
func (r *GPULeaseReconciler) leasesForManagedDeployment(ctx context.Context, obj client.Object) []reconcile.Request {
	leaseList := &kleasev1alpha1.GPULeaseList{}
	if err := r.List(ctx, leaseList); err != nil {
		return nil
	}
	reqs := make([]reconcile.Request, 0, len(leaseList.Items))
	for i := range leaseList.Items {
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: leaseList.Items[i].Namespace,
			Name:      leaseList.Items[i].Name,
		}})
	}
	return reqs
}
