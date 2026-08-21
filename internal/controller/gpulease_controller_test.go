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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"time"

	kleasev1alpha1 "github.com/zevlo/klease/api/v1alpha1"
)

var _ = Describe("GPULease Controller", func() {
	var (
		r         *GPULeaseReconciler
		recorder  *record.FakeRecorder
		leaseName types.NamespacedName
		deployKey types.NamespacedName
	)

	makeLease := func(name, deployName string) *kleasev1alpha1.GPULease {
		return &kleasev1alpha1.GPULease{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: kleasev1alpha1.GPULeaseSpec{
				WorkloadRef: kleasev1alpha1.WorkloadRef{Kind: "Deployment", Name: deployName},
				Duration:    metav1.Duration{Duration: 30 * time.Minute},
			},
		}
	}

	makeDeployment := func(name string, replicas int32) *appsv1.Deployment {
		return &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
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

	BeforeEach(func() {
		recorder = record.NewFakeRecorder(32)
		r = &GPULeaseReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: recorder,
		}
	})

	Context("when the workloadRef target exists", func() {
		const (
			name = "scale-up-test"
			dep  = "scale-up-deploy"
		)
		leaseName = types.NamespacedName{Name: name, Namespace: "default"}
		deployKey = types.NamespacedName{Name: dep, Namespace: "default"}

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, makeLease(name, dep))).To(Succeed())
			Expect(k8sClient.Create(ctx, makeDeployment(dep, 0))).To(Succeed())
		})

		AfterEach(func() {
			lease := &kleasev1alpha1.GPULease{}
			Expect(k8sClient.Get(ctx, leaseName, lease)).To(Succeed())
			Expect(k8sClient.Delete(ctx, lease)).To(Succeed())
			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, deployKey, deploy)).To(Succeed())
			Expect(k8sClient.Delete(ctx, deploy)).To(Succeed())
		})

		It("scales the referenced Deployment from 0 to 1", func() {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: leaseName})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, deployKey, deploy)).To(Succeed())
			Expect(deploy.Spec.Replicas).NotTo(BeNil())
			Expect(*deploy.Spec.Replicas).To(Equal(int32(1)))
		})

		It("is idempotent — a second reconcile does not rescale or error", func() {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: leaseName})
			Expect(err).NotTo(HaveOccurred())
			result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: leaseName})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))
		})
	})

	Context("when the workloadRef target is missing", func() {
		const name = "not-found-test"

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, makeLease(name, "does-not-exist"))).To(Succeed())
		})

		AfterEach(func() {
			lease := &kleasev1alpha1.GPULease{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, lease)).To(Succeed())
			Expect(k8sClient.Delete(ctx, lease)).To(Succeed())
		})

		It("sets WorkloadNotFound=True and requeues without error", func() {
			result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: "default"}})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(30 * time.Second))

			lease := &kleasev1alpha1.GPULease{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, lease)).To(Succeed())
			cond := meta.FindStatusCondition(lease.Status.Conditions, kleasev1alpha1.GPULeaseConditionWorkloadNotFound)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal("WorkloadRefTargetMissing"))
		})

		It("clears WorkloadNotFound and scales up once the Deployment appears", func() {
			key := types.NamespacedName{Name: name, Namespace: "default"}
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Create(ctx, makeDeployment("does-not-exist", 0))).To(Succeed())
			DeferCleanup(func() {
				deploy := &appsv1.Deployment{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "does-not-exist", Namespace: "default"}, deploy)).To(Succeed())
				Expect(k8sClient.Delete(ctx, deploy)).To(Succeed())
			})

			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())

			lease := &kleasev1alpha1.GPULease{}
			Expect(k8sClient.Get(ctx, key, lease)).To(Succeed())
			cond := meta.FindStatusCondition(lease.Status.Conditions, kleasev1alpha1.GPULeaseConditionWorkloadNotFound)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "does-not-exist", Namespace: "default"}, deploy)).To(Succeed())
			Expect(*deploy.Spec.Replicas).To(Equal(int32(1)))
		})
	})
})
