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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kleasev1alpha1 "github.com/zevlo/klease/api/v1alpha1"
)

var _ = Describe("GPULease Controller", func() {
	var (
		r        *GPULeaseReconciler
		recorder *record.FakeRecorder
	)

	makeLease := func(name, deployName string, duration time.Duration) *kleasev1alpha1.GPULease {
		return &kleasev1alpha1.GPULease{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: kleasev1alpha1.GPULeaseSpec{
				WorkloadRef: kleasev1alpha1.WorkloadRef{Kind: "Deployment", Name: deployName},
				Duration:    metav1.Duration{Duration: duration},
			},
		}
	}

	makeDeployment := func(name string, replicas int32, managed bool) *appsv1.Deployment {
		labels := map[string]string{"app": name}
		if managed {
			labels[kleasev1alpha1.ManagedLabelKey] = "true"
		}
		return &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: labels},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "registry.k8s.io/pause:3.10"}}},
				},
			},
		}
	}

	key := func(name string) types.NamespacedName {
		return types.NamespacedName{Name: name, Namespace: "default"}
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
			Expect(k8sClient.Delete(ctx, getLease(leaseName))).To(Succeed())
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
			Expect(k8sClient.Delete(ctx, getLease(first))).To(Succeed())
			Expect(k8sClient.Delete(ctx, getLease(second))).To(Succeed())
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

		It("hands off at expiry: first Expired, second Active, workloads swap", func() {
			fakeNow := time.Now()
			r.NowFn = func() time.Time { return fakeNow }

			doReconcile(second) // steady state under the fake clock

			// Advance past the holder's expiry and run any lease's reconcile.
			fakeNow = fakeNow.Add(31 * time.Minute)
			doReconcile(second)

			Expect(getLease(first).Status.State).To(Equal(kleasev1alpha1.GPULeaseStateExpired))
			Expect(getLease(second).Status.State).To(Equal(kleasev1alpha1.GPULeaseStateActive))
			Expect(replicas(depA)).To(Equal(int32(0)))
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
	})
})
