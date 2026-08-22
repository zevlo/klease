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

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	kleasev1alpha1 "github.com/zevlo/klease/api/v1alpha1"
)

// Queue-level metrics registered on the controller-runtime registry and
// served by the manager's metrics endpoint. Gauges are level-triggered
// (Set on every pass, idempotent across re-reconciles); histograms fire
// exactly once per lifecycle edge (admission, drain completion).
var (
	queueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "klease_queue_depth",
		Help: "Number of leases waiting for the GPU (Pending) after the latest scheduling pass.",
	})
	activeLeases = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "klease_active_leases",
		Help: "Number of leases currently holding the GPU (Active); 0 or 1.",
	})
	admissionWaitSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "klease_admission_wait_seconds",
		Help: "Time from lease creation to admission (Pending -> Active).",
		// Queues are human-scale: most waits land in seconds to hours.
		Buckets: []float64{1, 10, 60, 300, 900, 1800, 3600, 7200, 14400, 28800},
	})
	drainDurationSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "klease_drain_duration_seconds",
		Help: "Time from lease expiry to drain completion (workload fully reclaimed).",
		// Drains are bounded by gracePeriod (default 5m).
		Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600, 1800},
	})
)

func init() {
	metrics.Registry.MustRegister(
		queueDepth,
		activeLeases,
		admissionWaitSeconds,
		drainDurationSeconds,
	)
}

// setQueueGauges reports the level-triggered queue state after a pass.
// active is the lease holding the GPU (nil when idle or draining).
func setQueueGauges(active *kleasev1alpha1.GPULease, pendingCount int) {
	queueDepth.Set(float64(pendingCount))
	if active != nil {
		activeLeases.Set(1)
	} else {
		activeLeases.Set(0)
	}
}

// observeAdmissionWait records how long a newly admitted lease waited
// from creation to admission.
func observeAdmissionWait(l *kleasev1alpha1.GPULease) {
	if l.Status.ActiveSince == nil {
		return
	}
	admissionWaitSeconds.Observe(l.Status.ActiveSince.Time.Sub(l.CreationTimestamp.Time).Seconds())
}

// observeDrainDuration records how long a completed drain took from
// expiry to full workload reclamation.
func observeDrainDuration(expiredAt, completedAt time.Time) {
	drainDurationSeconds.Observe(completedAt.Sub(expiredAt).Seconds())
}
