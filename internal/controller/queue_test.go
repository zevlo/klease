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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kleasev1alpha1 "github.com/zevlo/klease/api/v1alpha1"
)

var _ = Describe("computeQueue", func() {
	var base time.Time

	BeforeEach(func() {
		base = time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	})

	// lease builds a Pending lease created at base+createdOffset.
	lease := func(name, ns string, createdOffset time.Duration, mutate func(*kleasev1alpha1.GPULease)) *kleasev1alpha1.GPULease {
		l := &kleasev1alpha1.GPULease{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				Namespace:         ns,
				CreationTimestamp: metav1.NewTime(base.Add(createdOffset)),
			},
			Spec: kleasev1alpha1.GPULeaseSpec{
				WorkloadRef: kleasev1alpha1.WorkloadRef{Kind: "Deployment", Name: "w-" + name},
				Duration:    metav1.Duration{Duration: 30 * time.Minute},
			},
		}
		if mutate != nil {
			mutate(l)
		}
		return l
	}

	active := func(l *kleasev1alpha1.GPULease, since time.Time) *kleasev1alpha1.GPULease {
		l.Status.State = kleasev1alpha1.GPULeaseStateActive
		l.Status.ActiveSince = &metav1.Time{Time: since}
		exp := metav1.NewTime(since.Add(l.Spec.Duration.Duration))
		l.Status.ExpiresAt = &exp
		return l
	}

	alwaysAdmissible := func(*kleasev1alpha1.GPULease) bool { return true }
	neverAdmissible := func(*kleasev1alpha1.GPULease) bool { return false }

	findTransition := func(res QueueResult, kind string) *Transition {
		for i := range res.Transitions {
			if res.Transitions[i].Kind == kind {
				return &res.Transitions[i]
			}
		}
		return nil
	}

	Context("admission", func() {
		It("admits the oldest lease and stamps its timer from now", func() {
			a := lease("a", "default", 0, nil)
			b := lease("b", "default", time.Minute, nil)
			res := computeQueue([]*kleasev1alpha1.GPULease{b, a}, base, alwaysAdmissible)

			Expect(res.Active).To(BeIdenticalTo(a))
			Expect(a.Status.State).To(Equal(kleasev1alpha1.GPULeaseStateActive))
			Expect(a.Status.ActiveSince.Time).To(Equal(base))
			Expect(a.Status.ExpiresAt.Time).To(Equal(base.Add(30 * time.Minute)))
			Expect(b.Status.State).To(Equal(kleasev1alpha1.GPULeaseStatePending))
			Expect(findTransition(res, TransitionAdmitted).Lease).To(BeIdenticalTo(a))
		})

		It("tie-breaks equal creationTimestamps by namespace then name", func() {
			a := lease("zeta", "default", 0, nil)
			b := lease("alpha", "kube-ns", 0, nil)
			res := computeQueue([]*kleasev1alpha1.GPULease{a, b}, base, alwaysAdmissible)

			// "default" < "kube-ns", so a wins despite the later-alphabet name.
			Expect(res.Active).To(BeIdenticalTo(a))
		})

		It("tie-breaks within one namespace by name", func() {
			a := lease("zeta", "default", 0, nil)
			b := lease("alpha", "default", 0, nil)
			res := computeQueue([]*kleasev1alpha1.GPULease{a, b}, base, alwaysAdmissible)

			Expect(res.Active).To(BeIdenticalTo(b))
		})

		It("keeps an existing Active lease and leaves the queue alone", func() {
			a := active(lease("a", "default", 0, nil), base.Add(-time.Minute))
			b := lease("b", "default", time.Minute, nil)
			res := computeQueue([]*kleasev1alpha1.GPULease{a, b}, base, alwaysAdmissible)

			Expect(res.Active).To(BeIdenticalTo(a))
			Expect(b.Status.State).To(Equal(kleasev1alpha1.GPULeaseStatePending))
			Expect(findTransition(res, TransitionAdmitted)).To(BeNil()) // no lifecycle event for queueing
		})

		It("skips an inadmissible head and admits the next lease", func() {
			a := lease("a", "default", 0, nil)
			b := lease("b", "default", time.Minute, nil)
			res := computeQueue([]*kleasev1alpha1.GPULease{a, b}, base, func(l *kleasev1alpha1.GPULease) bool {
				return l.Name != "a"
			})

			Expect(res.Active).To(BeIdenticalTo(b))
			Expect(a.Status.State).To(Equal(kleasev1alpha1.GPULeaseStatePending)) // skipped, does not hold the queue
		})

		It("leaves the GPU idle when every candidate is inadmissible", func() {
			a := lease("a", "default", 0, nil)
			res := computeQueue([]*kleasev1alpha1.GPULease{a}, base, neverAdmissible)

			Expect(res.Active).To(BeNil())
			Expect(a.Status.State).To(Equal(kleasev1alpha1.GPULeaseStatePending))
			Expect(res.Transitions).To(BeEmpty())
		})
	})

	Context("expiry", func() {
		It("expires an Active lease at expiresAt", func() {
			a := active(lease("a", "default", 0, nil), base.Add(-31*time.Minute))
			res := computeQueue([]*kleasev1alpha1.GPULease{a}, base.Add(time.Minute), alwaysAdmissible)

			Expect(a.Status.State).To(Equal(kleasev1alpha1.GPULeaseStateExpired))
			Expect(res.Active).To(BeNil())
			Expect(findTransition(res, TransitionExpired).Lease).To(BeIdenticalTo(a))
		})

		It("admits the next lease in the same pass that expires the holder", func() {
			a := active(lease("a", "default", 0, nil), base.Add(-31*time.Minute))
			b := lease("b", "default", time.Minute, nil)
			res := computeQueue([]*kleasev1alpha1.GPULease{a, b}, base.Add(time.Minute), alwaysAdmissible)

			Expect(a.Status.State).To(Equal(kleasev1alpha1.GPULeaseStateExpired))
			Expect(res.Active).To(BeIdenticalTo(b))
		})

		It("does not expire a lease one second early", func() {
			a := active(lease("a", "default", 0, nil), base)
			res := computeQueue([]*kleasev1alpha1.GPULease{a}, base.Add(29*time.Minute+59*time.Second), alwaysAdmissible)

			Expect(res.Active).To(BeIdenticalTo(a))
			Expect(res.Transitions).To(BeEmpty())
		})
	})

	Context("split-brain resolution", func() {
		It("keeps the earliest activeSince and demotes the rest", func() {
			a := active(lease("a", "default", 0, nil), base.Add(-2*time.Minute))
			b := active(lease("b", "default", time.Minute, nil), base.Add(-1*time.Minute))
			res := computeQueue([]*kleasev1alpha1.GPULease{a, b}, base, alwaysAdmissible)

			Expect(res.Active).To(BeIdenticalTo(a))
			Expect(b.Status.State).To(Equal(kleasev1alpha1.GPULeaseStatePending))
			Expect(b.Status.ActiveSince).To(BeNil())
			Expect(b.Status.ExpiresAt).To(BeNil())
			Expect(findTransition(res, TransitionDemoted).Lease).To(BeIdenticalTo(b))
		})

		It("ranks an Active lease missing activeSince last", func() {
			a := active(lease("a", "default", 0, nil), base.Add(-1*time.Minute))
			b := active(lease("b", "default", time.Minute, nil), base)
			b.Status.ActiveSince = nil
			res := computeQueue([]*kleasev1alpha1.GPULease{a, b}, base, alwaysAdmissible)

			Expect(res.Active).To(BeIdenticalTo(a))
			Expect(b.Status.State).To(Equal(kleasev1alpha1.GPULeaseStatePending))
		})
	})

	Context("queue positions", func() {
		It("numbers Pending leases by FIFO rank with head 0", func() {
			a := lease("a", "default", 0, nil)
			b := lease("b", "default", time.Minute, nil)
			c := lease("c", "default", 2*time.Minute, nil)
			computeQueue([]*kleasev1alpha1.GPULease{c, a, b}, base, alwaysAdmissible)

			Expect(a.Status.QueuePosition).To(Equal(int32(0))) // admitted, position 0
			Expect(b.Status.QueuePosition).To(Equal(int32(0))) // head of remaining queue
			Expect(c.Status.QueuePosition).To(Equal(int32(1)))
		})
	})

	Context("steady state", func() {
		It("mutates nothing when the queue is already correct", func() {
			a := active(lease("a", "default", 0, nil), base.Add(-time.Minute))
			a.Status.QueuePosition = 0
			b := lease("b", "default", time.Minute, nil)
			b.Status.State = kleasev1alpha1.GPULeaseStatePending
			b.Status.QueuePosition = 0
			res := computeQueue([]*kleasev1alpha1.GPULease{a, b}, base, alwaysAdmissible)

			Expect(res.Changed).To(BeEmpty())
			Expect(res.Transitions).To(BeEmpty())
		})
	})
})
