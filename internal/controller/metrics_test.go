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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kleasev1alpha1 "github.com/zevlo/klease/api/v1alpha1"
)

// The gauges and histograms are package-global: each spec must reset or
// tolerate prior observations. Gauges are Set (last pass wins); histogram
// counts are cumulative across specs, so tests assert deltas.
var _ = Describe("queue metrics", Ordered, func() {
	var (
		r       *GPULeaseReconciler
		now     time.Time
		waitObs int // admissionWait count before the spec's actions
	)

	countWait := func() int {
		return histogramSamples(admissionWaitSeconds)
	}

	doReconcile := func(name string) reconcile.Result {
		result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: testNamespace}})
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
		return result
	}

	BeforeEach(func() {
		now = time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
		r = &GPULeaseReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: record.NewFakeRecorder(64),
			NowFn:    func() time.Time { return now },
		}
		waitObs = countWait()
	})

	Context("gauges", func() {
		const (
			leaseA = "metrics-lease-a"
			leaseB = "metrics-lease-b"
			leaseC = "metrics-lease-c"
			deploy = "metrics-deploy"
		)

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, &kleasev1alpha1.GPULease{
				ObjectMeta: metav1.ObjectMeta{Name: leaseA, Namespace: testNamespace,
					CreationTimestamp: metav1.NewTime(now.Add(-time.Minute))},
				Spec: kleasev1alpha1.GPULeaseSpec{
					WorkloadRef: kleasev1alpha1.WorkloadRef{Kind: kleasev1alpha1.KindDeployment, Name: deploy},
					Duration:    metav1.Duration{Duration: 30 * time.Minute},
				},
			})).To(Succeed())
			Expect(k8sClient.Create(ctx, &kleasev1alpha1.GPULease{
				ObjectMeta: metav1.ObjectMeta{Name: leaseB, Namespace: testNamespace,
					CreationTimestamp: metav1.NewTime(now.Add(-30 * time.Second))},
				Spec: kleasev1alpha1.GPULeaseSpec{
					WorkloadRef: kleasev1alpha1.WorkloadRef{Kind: kleasev1alpha1.KindDeployment, Name: deploy},
					Duration:    metav1.Duration{Duration: 30 * time.Minute},
				},
			})).To(Succeed())
			Expect(k8sClient.Create(ctx, &kleasev1alpha1.GPULease{
				ObjectMeta: metav1.ObjectMeta{Name: leaseC, Namespace: testNamespace,
					CreationTimestamp: metav1.NewTime(now.Add(-time.Second))},
				Spec: kleasev1alpha1.GPULeaseSpec{
					WorkloadRef: kleasev1alpha1.WorkloadRef{Kind: kleasev1alpha1.KindDeployment, Name: deploy},
					Duration:    metav1.Duration{Duration: 30 * time.Minute},
				},
			})).To(Succeed())
			Expect(k8sClient.Create(ctx, makeDeploymentForMetrics(deploy))).To(Succeed())
		})

		AfterEach(func() {
			cleanupMetricsLease(leaseA)
			cleanupMetricsLease(leaseB)
			cleanupMetricsLease(leaseC)
			Expect(k8sClient.Delete(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deploy, Namespace: testNamespace}})).To(Succeed())
		})

		It("reports one active holder and the pending backlog", func() {
			doReconcile(leaseA)

			Expect(testutil.ToFloat64(activeLeases)).To(Equal(float64(1)))
			Expect(testutil.ToFloat64(queueDepth)).To(Equal(float64(2)))
		})

		It("is idempotent across repeated passes", func() {
			doReconcile(leaseA)
			doReconcile(leaseA)

			Expect(testutil.ToFloat64(activeLeases)).To(Equal(float64(1)))
			Expect(testutil.ToFloat64(queueDepth)).To(Equal(float64(2)))
		})

		It("observes admission wait exactly once per admission", func() {
			doReconcile(leaseA)
			Expect(countWait() - waitObs).To(Equal(1))

			doReconcile(leaseA) // steady state: no new observations
			Expect(countWait() - waitObs).To(Equal(1))
		})

		It("drops to zero with an empty or expired queue", func() {
			doReconcile(leaseA) // a admitted; b, c queued

			// Drive the cascade to exhaustion: each expiry needs one pass
			// to enter Draining and one to complete (completeDrains runs
			// before computeQueue), after which the successor is admitted
			// with a fresh 30m slot.
			for range 3 {
				now = now.Add(35 * time.Minute)
				doReconcile(leaseA) // holder expires -> Draining
				doReconcile(leaseA) // drain completes, next admitted (or queue empties)
			}

			Expect(testutil.ToFloat64(activeLeases)).To(Equal(float64(0)))
			Expect(testutil.ToFloat64(queueDepth)).To(Equal(float64(0)))
		})
	})

	Context("drain duration", func() {
		const (
			leaseName = "metrics-drain-lease"
			deploy    = "metrics-drain-deploy"
		)

		It("observes the expiry-to-completion interval", func() {
			drainObs := histogramSamples(drainDurationSeconds)

			Expect(k8sClient.Create(ctx, &kleasev1alpha1.GPULease{
				ObjectMeta: metav1.ObjectMeta{Name: leaseName, Namespace: testNamespace},
				Spec: kleasev1alpha1.GPULeaseSpec{
					WorkloadRef: kleasev1alpha1.WorkloadRef{Kind: kleasev1alpha1.KindDeployment, Name: deploy},
					Duration:    metav1.Duration{Duration: 10 * time.Minute},
					GracePeriod: &metav1.Duration{Duration: time.Minute},
				},
			})).To(Succeed())
			Expect(k8sClient.Create(ctx, makeDeploymentForMetrics(deploy))).To(Succeed())
			doReconcile(leaseName) // admitted

			// Past expiry: the first pass enters Draining; completion (no
			// pods) lands on the next pass — completeDrains runs before
			// computeQueue within a reconcile.
			now = now.Add(11 * time.Minute)
			doReconcile(leaseName) // Active -> Draining
			doReconcile(leaseName) // drain completes

			Expect(histogramSamples(drainDurationSeconds) - drainObs).To(Equal(1))

			cleanupMetricsLease(leaseName)
			Expect(k8sClient.Delete(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deploy, Namespace: testNamespace}})).To(Succeed())
		})
	})
})

// histogramSamples returns the observed sample count of a histogram.
func histogramSamples(h prometheus.Histogram) int {
	var m dto.Metric
	if err := h.(prometheus.Metric).Write(&m); err != nil {
		return 0
	}
	return int(m.GetHistogram().GetSampleCount())
}

// makeDeploymentForMetrics builds a managed Deployment fixture.
func makeDeploymentForMetrics(name string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace,
			Labels: map[string]string{appLabelKey: name, kleasev1alpha1.ManagedLabelKey: kleasev1alpha1.ManagedLabelValue}},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptrTo(int32(0)),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{appLabelKey: name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{appLabelKey: name}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: pauseImage}}},
			},
		},
	}
}

// ptrTo is a tiny helper for literal pointers.
func ptrTo[T any](v T) *T { return &v }

// cleanupMetricsLease removes a lease fixture without waiting for its
// drain: delete, then strip the finalizer if the object lingers.
func cleanupMetricsLease(name string) {
	l := &kleasev1alpha1.GPULease{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNamespace}, l); err != nil {
		return // already gone
	}
	if l.DeletionTimestamp == nil {
		Expect(k8sClient.Delete(ctx, l)).To(Succeed())
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNamespace}, l); err != nil {
			return
		}
	}
	if controllerutil.RemoveFinalizer(l, kleasev1alpha1.FinalizerDrain) {
		Expect(k8sClient.Update(ctx, l)).To(Succeed())
	}
}
