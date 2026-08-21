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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kleasev1alpha1 "github.com/zevlo/klease/api/v1alpha1"
)

const (
	// testNamespace hosts all fixtures; leases and workloads colocate here.
	testNamespace = "default"
	// appLabelKey is the selector label shared by test workloads.
	appLabelKey = "app"
)

var _ = Describe("GPULease Controller", func() {
	var (
		r        *GPULeaseReconciler
		recorder *record.FakeRecorder
	)

	makeLease := func(name, deployName string, duration time.Duration) *kleasev1alpha1.GPULease {
		return &kleasev1alpha1.GPULease{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Spec: kleasev1alpha1.GPULeaseSpec{
				WorkloadRef: kleasev1alpha1.WorkloadRef{Kind: kleasev1alpha1.KindDeployment, Name: deployName},
				Duration:    metav1.Duration{Duration: duration},
			},
		}
	}

	makeDeployment := func(name string, replicas int32, managed bool) *appsv1.Deployment {
		labels := map[string]string{appLabelKey: name}
		if managed {
			labels[kleasev1alpha1.ManagedLabelKey] = kleasev1alpha1.ManagedLabelValue
		}
		return &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace, Labels: labels},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{appLabelKey: name}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{appLabelKey: name}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "registry.k8s.io/pause:3.10"}}},
				},
			},
		}
	}

	key := func(name string) types.NamespacedName {
		return types.NamespacedName{Name: name, Namespace: testNamespace}
	}

	makePod := func(name, deployName string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace, Labels: map[string]string{appLabelKey: deployName}},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "registry.k8s.io/pause:3.10"}}},
		}
	}

	getLease := func(name string) *kleasev1alpha1.GPULease {
		l := &kleasev1alpha1.GPULease{}
		ExpectWithOffset(1, k8sClient.Get(ctx, key(name), l)).To(Succeed())
		return l
	}
	getDeploy := func(name string) *appsv1.Deployment {
		d := &appsv1.Deployment{}
		ExpectWithOffset(1, k8sClient.Get(ctx, key(name), d)).To(Succeed())
		return d
	}
	replicas := func(name string) int32 {
		d := getDeploy(name)
		ExpectWithOffset(1, d.Spec.Replicas).NotTo(BeNil())
		return *d.Spec.Replicas
	}
	doReconcile := func(name string) reconcile.Result {
		result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key(name)})
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
		return result
	}
	// deleteLeaseNow removes a lease without waiting for its drain: the
	// finalizer is stripped so the object is reclaimed immediately. Test
	// cleanup only — operators should let the controller drain.
	deleteLeaseNow := func(name string) {
		l := &kleasev1alpha1.GPULease{}
		if err := k8sClient.Get(ctx, key(name), l); err != nil {
			return // already gone
		}
		if l.DeletionTimestamp == nil {
			ExpectWithOffset(1, k8sClient.Delete(ctx, l)).To(Succeed())
			// A lease without the finalizer is reclaimed immediately; one
			// with it lingers until the finalizer is stripped below.
			if err := k8sClient.Get(ctx, key(name), l); err != nil {
				return
			}
		}
		if controllerutil.RemoveFinalizer(l, kleasev1alpha1.FinalizerDrain) {
			ExpectWithOffset(1, k8sClient.Update(ctx, l)).To(Succeed())
		}
	}

	BeforeEach(func() {
		recorder = record.NewFakeRecorder(64)
		r = &GPULeaseReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: recorder,
		}
	})

	Context("single lease", func() {
		const (
			leaseName = "single-lease"
			deploy    = "single-deploy"
		)

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, makeLease(leaseName, deploy, 30*time.Minute))).To(Succeed())
			Expect(k8sClient.Create(ctx, makeDeployment(deploy, 0, true))).To(Succeed())
		})

		AfterEach(func() {
			deleteLeaseNow(leaseName)
			Expect(k8sClient.Delete(ctx, getDeploy(deploy))).To(Succeed())
		})

		It("admits the lease and scales its workload to 1", func() {
			doReconcile(leaseName)

			l := getLease(leaseName)
			Expect(l.Status.State).To(Equal(kleasev1alpha1.GPULeaseStateActive))
			Expect(l.Status.QueuePosition).To(Equal(int32(0)))
			Expect(l.Status.ActiveSince).NotTo(BeNil())
			Expect(l.Status.ExpiresAt).NotTo(BeNil())
			Expect(replicas(deploy)).To(Equal(int32(1)))
		})

		It("requeues at the lease expiry and is idempotent", func() {
			first := doReconcile(leaseName)
			Expect(first.RequeueAfter).To(BeNumerically(">", 25*time.Minute))

			second := doReconcile(leaseName)
			Expect(second.RequeueAfter).To(BeNumerically(">", 25*time.Minute))
			l := getLease(leaseName)
			Expect(l.Status.State).To(Equal(kleasev1alpha1.GPULeaseStateActive))
			Expect(replicas(deploy)).To(Equal(int32(1)))
		})
	})

	Context("drain with a deleted workload", func() {
		const (
			leaseName = "vanishing-lease"
			deploy    = "vanishing-deploy"
		)

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, makeLease(leaseName, deploy, 30*time.Minute))).To(Succeed())
			Expect(k8sClient.Create(ctx, makeDeployment(deploy, 0, true))).To(Succeed())
		})

		AfterEach(func() {
			Expect(k8sClient.Delete(ctx, getLease(leaseName))).To(Succeed())
			// By reference: the test may have deleted it already.
			_ = k8sClient.Delete(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deploy, Namespace: testNamespace}})
		})

		It("completes the drain immediately when the workload no longer exists", func() {
			fakeNow := time.Now()
			r.NowFn = func() time.Time { return fakeNow }

			doReconcile(leaseName) // admits

			// Past expiry, and the workload was deleted out from under the lease.
			fakeNow = fakeNow.Add(31 * time.Minute)
			Expect(k8sClient.Delete(ctx, getDeploy(deploy))).To(Succeed())

			doReconcile(leaseName) // expiry pass: Active -> Draining (drain check ran while still Active)

			result := doReconcile(leaseName) // drain check: Deployment gone -> Expired

			Expect(getLease(leaseName).Status.State).To(Equal(kleasev1alpha1.GPULeaseStateExpired))
			Expect(result.RequeueAfter).To(Equal(time.Duration(0))) // nothing queued, draining, or active
		})
	})

	Context("spec mutation on a live lease", func() {
		const (
			leaseName = "mutable-lease"
			deploy    = "mutable-deploy"
		)

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, makeLease(leaseName, deploy, 30*time.Minute))).To(Succeed())
			Expect(k8sClient.Create(ctx, makeDeployment(deploy, 0, true))).To(Succeed())
			doReconcile(leaseName) // admits
			Expect(getLease(leaseName).Status.State).To(Equal(kleasev1alpha1.GPULeaseStateActive))
		})

		AfterEach(func() {
			deleteLeaseNow(leaseName)
			Expect(k8sClient.Delete(ctx, getDeploy(deploy))).To(Succeed())
		})

		It("rejects workloadRef mutation at the API server", func() {
			l := getLease(leaseName)
			l.Spec.WorkloadRef.Name = "some-other-deploy"
			err := k8sClient.Update(ctx, l)

			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsInvalid(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("workloadRef is immutable"))
		})

		It("extends the live expiry when spec.duration grows", func() {
			l := getLease(leaseName)
			l.Spec.Duration = metav1.Duration{Duration: time.Hour}
			Expect(k8sClient.Update(ctx, l)).To(Succeed())

			doReconcile(leaseName)

			l = getLease(leaseName)
			Expect(l.Status.State).To(Equal(kleasev1alpha1.GPULeaseStateActive))
			Expect(l.Status.ExpiresAt.Time).To(Equal(l.Status.ActiveSince.Add(time.Hour)))
			Expect(replicas(deploy)).To(Equal(int32(1))) // still the holder
		})
	})

	Context("lease deletion", func() {
		const (
			holder    = "del-holder"
			successor = "del-successor"
			depA      = "del-deploy-a"
			depB      = "del-deploy-b"
		)

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, makeLease(holder, depA, 30*time.Minute))).To(Succeed())
			Expect(k8sClient.Create(ctx, makeDeployment(depA, 0, true))).To(Succeed())
			Expect(k8sClient.Create(ctx, makeDeployment(depB, 0, true))).To(Succeed())
			doReconcile(holder) // admits holder; the sweep stamps the finalizer
			Expect(k8sClient.Create(ctx, makeLease(successor, depB, 30*time.Minute))).To(Succeed())
			doReconcile(successor) // steady state: holder Active, successor queued
		})

		AfterEach(func() {
			deleteLeaseNow(holder)
			deleteLeaseNow(successor)
			_ = k8sClient.Delete(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: depA, Namespace: testNamespace}})
			_ = k8sClient.Delete(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: depB, Namespace: testNamespace}})
		})

		It("carries the drain finalizer while holding the GPU", func() {
			Expect(getLease(holder).Finalizers).To(ContainElement(kleasev1alpha1.FinalizerDrain))
		})

		It("drains a deleted Active lease before releasing it and admitting the successor", func() {
			fakeNow := time.Now()
			r.NowFn = func() time.Time { return fakeNow }

			pod := makePod("del-holder-pod", depA)
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pod) })

			// Deleting the holder mid-slot: the finalizer holds the object.
			Expect(k8sClient.Delete(ctx, getLease(holder))).To(Succeed())
			doReconcile(successor)

			h := getLease(holder) // still exists: finalizer holds it
			Expect(h.DeletionTimestamp).NotTo(BeNil())
			Expect(h.Status.State).To(Equal(kleasev1alpha1.GPULeaseStateDraining))
			Expect(h.Status.DrainDeadline.Time).To(BeTemporally("~", fakeNow.Add(5*time.Minute), time.Second)) // grace runs from deletion
			Expect(getLease(successor).Status.State).To(Equal(kleasev1alpha1.GPULeaseStatePending))
			Expect(replicas(depA)).To(Equal(int32(0)))
			Expect(replicas(depB)).To(Equal(int32(0)))

			// The pod terminates: drain completes, the object is released,
			// and the successor is admitted in the same pass.
			Expect(k8sClient.Delete(ctx, pod)).To(Succeed())
			doReconcile(successor)

			err := k8sClient.Get(ctx, key(holder), &kleasev1alpha1.GPULease{})
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
			Expect(getLease(successor).Status.State).To(Equal(kleasev1alpha1.GPULeaseStateActive))
			Expect(replicas(depB)).To(Equal(int32(1)))
		})

		It("force-deletes a stuck pod of a deleted lease at the drain deadline", func() {
			fakeNow := time.Now()
			r.NowFn = func() time.Time { return fakeNow }

			stuck := makePod("del-stuck-pod", depA)
			Expect(k8sClient.Create(ctx, stuck)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, stuck) })

			Expect(k8sClient.Delete(ctx, getLease(holder))).To(Succeed())
			doReconcile(successor)
			Expect(getLease(holder).Status.State).To(Equal(kleasev1alpha1.GPULeaseStateDraining))

			// Past the 5m drain deadline: the straggler is force-deleted.
			fakeNow = fakeNow.Add(10 * time.Minute)
			doReconcile(successor)
			Expect(k8sClient.Get(ctx, key("del-stuck-pod"), &corev1.Pod{})).To(HaveOccurred())

			// Deletion is confirmed on the next pass; then the successor runs.
			doReconcile(successor)
			Expect(apierrors.IsNotFound(k8sClient.Get(ctx, key(holder), &kleasev1alpha1.GPULease{}))).To(BeTrue())
			Expect(getLease(successor).Status.State).To(Equal(kleasev1alpha1.GPULeaseStateActive))
		})
	})

	Context("deletion of a Pending lease", func() {
		const leaseName = "del-pending"

		It("removes the object immediately without ever admitting it", func() {
			Expect(k8sClient.Create(ctx, makeLease(leaseName, "never-created-deploy", 30*time.Minute))).To(Succeed())
			doReconcile(leaseName) // Pending: workload missing, no finalizer

			l := getLease(leaseName)
			Expect(l.Status.State).To(Equal(kleasev1alpha1.GPULeaseStatePending))
			Expect(l.Finalizers).To(BeEmpty())

			Expect(k8sClient.Delete(ctx, l)).To(Succeed())
			doReconcile(leaseName)

			err := k8sClient.Get(ctx, key(leaseName), &kleasev1alpha1.GPULease{})
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})

	Context("workloadRef target missing", func() {
		const leaseName = "orphan-lease"

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, makeLease(leaseName, "does-not-exist", 30*time.Minute))).To(Succeed())
		})

		AfterEach(func() {
			Expect(k8sClient.Delete(ctx, getLease(leaseName))).To(Succeed())
		})

		It("holds Pending with WorkloadNotFound=True and requeues on the safety net", func() {
			result := doReconcile(leaseName)

			Expect(result.RequeueAfter).To(Equal(pendingRequeue))
			l := getLease(leaseName)
			Expect(l.Status.State).To(Equal(kleasev1alpha1.GPULeaseStatePending))
			cond := meta.FindStatusCondition(l.Status.Conditions, kleasev1alpha1.GPULeaseConditionWorkloadNotFound)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal("WorkloadRefTargetMissing"))
		})

		It("recovers when the Deployment appears later", func() {
			doReconcile(leaseName)

			Expect(k8sClient.Create(ctx, makeDeployment("does-not-exist", 0, true))).To(Succeed())
			DeferCleanup(func() { Expect(k8sClient.Delete(ctx, getDeploy("does-not-exist"))).To(Succeed()) })

			doReconcile(leaseName)

			l := getLease(leaseName)
			Expect(l.Status.State).To(Equal(kleasev1alpha1.GPULeaseStateActive))
			cond := meta.FindStatusCondition(l.Status.Conditions, kleasev1alpha1.GPULeaseConditionWorkloadNotFound)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(replicas("does-not-exist")).To(Equal(int32(1)))
		})
	})

	Context("workload not labeled managed", func() {
		const (
			leaseName = "unlabeled-lease"
			deploy    = "unlabeled-deploy"
		)

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, makeLease(leaseName, deploy, 30*time.Minute))).To(Succeed())
			Expect(k8sClient.Create(ctx, makeDeployment(deploy, 3, false))).To(Succeed())
		})

		AfterEach(func() {
			Expect(k8sClient.Delete(ctx, getLease(leaseName))).To(Succeed())
			Expect(k8sClient.Delete(ctx, getDeploy(deploy))).To(Succeed())
		})

		It("blocks admission and leaves the unmanaged Deployment untouched", func() {
			doReconcile(leaseName)

			l := getLease(leaseName)
			Expect(l.Status.State).To(Equal(kleasev1alpha1.GPULeaseStatePending))
			cond := meta.FindStatusCondition(l.Status.Conditions, kleasev1alpha1.GPULeaseConditionWorkloadNotFound)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal("WorkloadNotManaged"))
			Expect(replicas(deploy)).To(Equal(int32(3))) // not ours to touch
		})
	})

	Context("two leases contend for the GPU", func() {
		const (
			first  = "first-lease"
			second = "second-lease"
			depA   = "deploy-a"
			depB   = "deploy-b"
		)

		BeforeEach(func() {
			// first created (and admitted) before second: deterministic FIFO.
			Expect(k8sClient.Create(ctx, makeLease(first, depA, 30*time.Minute))).To(Succeed())
			Expect(k8sClient.Create(ctx, makeDeployment(depA, 0, true))).To(Succeed())
			Expect(k8sClient.Create(ctx, makeDeployment(depB, 0, true))).To(Succeed())
			doReconcile(first) // admits first
			Expect(k8sClient.Create(ctx, makeLease(second, depB, 30*time.Minute))).To(Succeed())
		})

		AfterEach(func() {
			deleteLeaseNow(first)
			deleteLeaseNow(second)
			Expect(k8sClient.Delete(ctx, getDeploy(depA))).To(Succeed())
			Expect(k8sClient.Delete(ctx, getDeploy(depB))).To(Succeed())
		})

		It("holds single-active: the second lease stays Pending at position 0, its workload at 0", func() {
			doReconcile(second)

			Expect(getLease(first).Status.State).To(Equal(kleasev1alpha1.GPULeaseStateActive))
			l := getLease(second)
			Expect(l.Status.State).To(Equal(kleasev1alpha1.GPULeaseStatePending))
			Expect(l.Status.QueuePosition).To(Equal(int32(0)))
			Expect(replicas(depA)).To(Equal(int32(1)))
			Expect(replicas(depB)).To(Equal(int32(0)))
		})

		It("hands off strictly: the successor is admitted only after the drain completes", func() {
			fakeNow := time.Now()
			r.NowFn = func() time.Time { return fakeNow }

			doReconcile(second) // admits first, second queued

			// A pod of the holder's workload is still running at expiry.
			pod := makePod("holder-pod", depA)
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, pod)
			})

			// Advance past expiry: holder moves to Draining, its Deployment
			// scales to 0, and the successor must not be admitted yet.
			fakeNow = fakeNow.Add(31 * time.Minute)
			doReconcile(second)

			Expect(getLease(first).Status.State).To(Equal(kleasev1alpha1.GPULeaseStateDraining))
			Expect(getLease(first).Status.DrainDeadline).NotTo(BeNil())
			l := getLease(second)
			Expect(l.Status.State).To(Equal(kleasev1alpha1.GPULeaseStatePending))
			Expect(replicas(depA)).To(Equal(int32(0)))
			Expect(replicas(depB)).To(Equal(int32(0)))

			// The pod terminates gracefully before the drain deadline.
			Expect(k8sClient.Delete(ctx, pod)).To(Succeed())

			doReconcile(second)

			Expect(getLease(first).Status.State).To(Equal(kleasev1alpha1.GPULeaseStateExpired))
			Expect(getLease(second).Status.State).To(Equal(kleasev1alpha1.GPULeaseStateActive))
			Expect(replicas(depA)).To(Equal(int32(0)))
			Expect(replicas(depB)).To(Equal(int32(1)))
		})

		It("force-deletes stuck pods at the drain deadline", func() {
			fakeNow := time.Now()
			r.NowFn = func() time.Time { return fakeNow }

			doReconcile(second) // admits first

			stuck := makePod("stuck-pod", depA)
			Expect(k8sClient.Create(ctx, stuck)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, stuck)
			})

			// Past expiry but within the 5m default grace: Draining, pod untouched.
			fakeNow = fakeNow.Add(31 * time.Minute)
			doReconcile(second)

			Expect(getLease(first).Status.State).To(Equal(kleasev1alpha1.GPULeaseStateDraining))
			Expect(k8sClient.Get(ctx, key("stuck-pod"), &corev1.Pod{})).To(Succeed())

			// Past the drain deadline: stragglers force-deleted, lease still
			// Draining this pass; completion lands next pass.
			fakeNow = fakeNow.Add(10 * time.Minute)
			doReconcile(second)

			Expect(getLease(first).Status.State).To(Equal(kleasev1alpha1.GPULeaseStateDraining))
			Expect(k8sClient.Get(ctx, key("stuck-pod"), &corev1.Pod{})).To(HaveOccurred())

			doReconcile(second)

			Expect(getLease(first).Status.State).To(Equal(kleasev1alpha1.GPULeaseStateExpired))
			Expect(getLease(second).Status.State).To(Equal(kleasev1alpha1.GPULeaseStateActive))
			Expect(replicas(depB)).To(Equal(int32(1)))
		})

		It("corrects drift: a manually scaled-up non-holder snaps back to 0", func() {
			doReconcile(second)

			d := getDeploy(depB)
			three := int32(3)
			d.Spec.Replicas = &three
			Expect(k8sClient.Update(ctx, d)).To(Succeed())

			doReconcile(second)
			Expect(replicas(depB)).To(Equal(int32(0)))
			Expect(replicas(depA)).To(Equal(int32(1)))
		})
	})

	Context("no lease exists", func() {
		const deploy = "idle-managed-deploy"

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, makeDeployment(deploy, 2, true))).To(Succeed())
		})

		AfterEach(func() {
			Expect(k8sClient.Delete(ctx, getDeploy(deploy))).To(Succeed())
		})

		It("still scales managed workloads to 0 when a reconcile runs (invariant with empty queue)", func() {
			// Reconcile is triggered by some other lease's request in
			// practice; here we drive it with an arbitrary key.
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key("any-lease")})
			Expect(err).NotTo(HaveOccurred())
			Expect(replicas(deploy)).To(Equal(int32(0)))
		})

		It("maps managed Deployment events to the synthetic queue key when no leases exist", func() {
			reqs := r.leasesForManagedDeployment(ctx, getDeploy(deploy))

			Expect(reqs).To(HaveLen(1))
			Expect(reqs[0].NamespacedName).To(Equal(drainQueueKey))
		})
	})
})
