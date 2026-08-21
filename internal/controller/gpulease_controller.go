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
	corev1 "k8s.io/api/core/v1"
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
	// drainPollInterval caps the requeue while a drain is in progress so
	// graceful pod termination is noticed well before the deadline.
	drainPollInterval = 5 * time.Second
	// minRequeueDelay clamps requeues so a timer firing right now still
	// schedules a small positive delay instead of zero or negative.
	minRequeueDelay = time.Second
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
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

// Reconcile drives the cluster toward the lease queue's desired state:
//
//   - At most one lease Active cluster-wide (FIFO by creationTimestamp)
//   - The active holder's workloadRef scaled to 1
//   - Every other managed Deployment scaled to 0 — drift of any kind
//     (manual scale-ups, pre-existing pods) is corrected every pass
//   - Expired holders drain before the next lease is admitted: the lease
//     goes Draining until its pods are gone (force-deleted at
//     drainDeadline), then Expired hands the GPU to the queue head
//
// The Active lease requeues at its expiry; a Draining lease requeues on a
// poll capped by its drain deadline; Pending leases requeue on a safety-net
// cadence so promotion never waits on an external event.
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

	// Finish drains first so a lease completing in this pass releases the
	// GPU for admission below in the same reconcile.
	if err := r.completeDrains(ctx, leases, now); err != nil {
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
	// reconcile admitted a lease must carry the next expiry timer, and a
	// drain in progress must carry its deadline.
	switch {
	case result.Active != nil && result.Active.Status.ExpiresAt != nil:
		d := max(result.Active.Status.ExpiresAt.Sub(now), minRequeueDelay)
		return ctrl.Result{RequeueAfter: d}, nil
	case len(result.Draining) > 0:
		d := drainPollInterval
		for _, l := range result.Draining {
			if l.Status.DrainDeadline == nil {
				continue // missing stamp: force-delete path, poll now
			}
			if wait := l.Status.DrainDeadline.Sub(now); wait < d {
				d = wait
			}
		}
		if d < minRequeueDelay {
			d = minRequeueDelay
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

// completeDrains advances Draining leases whose workload has been
// reclaimed to Expired. A drain is complete when the referenced Deployment
// is gone or no pods match its selector; leftover pods are force-deleted
// once the drain deadline passes, with completion confirmed next pass.
func (r *GPULeaseReconciler) completeDrains(ctx context.Context, leases []*kleasev1alpha1.GPULease, now time.Time) error {
	for _, l := range leases {
		if l.Status.State != kleasev1alpha1.GPULeaseStateDraining {
			continue
		}
		done, err := r.drainStep(ctx, l, now)
		if err != nil {
			return err
		}
		if !done {
			continue
		}
		l.Status.State = kleasev1alpha1.GPULeaseStateExpired
		if err := r.Status().Update(ctx, l); err != nil {
			return err
		}
		r.Recorder.Event(l, "Normal", TransitionExpired,
			"drain complete; workload reclaimed and GPU released")
	}
	return nil
}

// drainStep performs one progress check on a Draining lease, force-deleting
// stragglers once the deadline is reached. It reports whether the drain is
// complete.
func (r *GPULeaseReconciler) drainStep(ctx context.Context, l *kleasev1alpha1.GPULease, now time.Time) (bool, error) {
	deploy := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Namespace: l.Namespace, Name: l.Spec.WorkloadRef.Name}, deploy)
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(deploy.Namespace),
		client.MatchingLabels(deploy.Spec.Selector.MatchLabels)); err != nil {
		return false, err
	}
	if len(pods.Items) == 0 {
		return true, nil
	}
	if l.Status.DrainDeadline != nil && now.Before(l.Status.DrainDeadline.Time) {
		return false, nil
	}

	for i := range pods.Items {
		if err := r.Delete(ctx, &pods.Items[i], client.GracePeriodSeconds(0)); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
	}
	r.Recorder.Eventf(l, "Warning", "ForceDrain",
		"grace period exceeded; force-deleted %d Pod(s) of %s %s/%s",
		len(pods.Items), l.Spec.WorkloadRef.Kind, l.Namespace, l.Spec.WorkloadRef.Name)
	return false, nil
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

// drainQueueKey is a synthetic reconcile key used when a managed
// Deployment event fires with no leases in the cluster: reconciliation is
// a global pass over all leases, so any key drives the same invariant
// enforcement and drift is corrected immediately.
var drainQueueKey = types.NamespacedName{Namespace: "klease", Name: "queue"}

// leasesForManagedDeployment enqueues every lease on any managed Deployment
// event. Reconciliation is a global pass, so any lease key drives the same
// invariant enforcement — this catches drift on managed workloads even when
// the drifted Deployment is referenced by no lease. With no leases at all,
// a synthetic key drives the pass anyway so the invariant still holds.
func (r *GPULeaseReconciler) leasesForManagedDeployment(ctx context.Context, obj client.Object) []reconcile.Request {
	leaseList := &kleasev1alpha1.GPULeaseList{}
	if err := r.List(ctx, leaseList); err != nil {
		return nil
	}
	if len(leaseList.Items) == 0 {
		return []reconcile.Request{{NamespacedName: drainQueueKey}}
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
