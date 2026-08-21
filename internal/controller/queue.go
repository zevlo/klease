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
	"fmt"
	"slices"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	kleasev1alpha1 "github.com/zevlo/klease/api/v1alpha1"
)

// Transition kinds for lease lifecycle events.
const (
	TransitionAdmitted = "Admitted"
	TransitionDraining = "Draining"
	TransitionExpired  = "Expired"
	TransitionDemoted  = "Demoted"
	TransitionAdjusted = "Adjusted"
)

// defaultGracePeriod mirrors the CEL default on spec.gracePeriod; it backs
// leases whose gracePeriod is unset (e.g. written via the fake client).
const defaultGracePeriod = 5 * time.Minute

// Transition is a lifecycle change computed for a lease during a queue pass.
type Transition struct {
	Lease   *kleasev1alpha1.GPULease
	Kind    string
	Message string
}

// QueueResult is the outcome of one global queue pass.
type QueueResult struct {
	// Active is the lease holding the GPU after the pass, or nil if none.
	Active *kleasev1alpha1.GPULease
	// Draining lists every lease in the Draining state after the pass.
	Draining []*kleasev1alpha1.GPULease
	// Changed lists every lease whose status was mutated, deduplicated.
	Changed []*kleasev1alpha1.GPULease
	// Transitions lists lifecycle changes that should emit events.
	Transitions []Transition
}

// computeQueue runs the pure scheduling pass over all leases:
//
//  0. Derivation: Active and Draining leases have their timers re-derived
//     from spec every pass — spec.duration edits extend or shorten a live
//     lease's expiresAt, gracePeriod edits move a live drain's deadline,
//     and corrupted status self-heals.
//  1. Expiry: Active leases at or past expiresAt become Draining, with
//     drainDeadline = expiresAt + gracePeriod stamped for the reclaim.
//  2. Single-active: if multiple leases are Active (split brain), the
//     earliest activeSince wins; the rest are demoted to Pending.
//  3. Admission: with no Active and no Draining lease, the FIFO head
//     (creationTimestamp, tie-broken by namespace then name) whose
//     workload is admissible becomes Active, with the timer stamped from
//     now — a queued lease never burns its slot waiting.
//  4. Queue positions are recomputed for Pending leases (head = 0).
//
// admissible reports whether a lease's workloadRef can run now; leases that
// are not admissible are skipped without holding the rest of the queue.
//
// Draining-to-Expired completion is deliberately outside this pass: it
// depends on cluster state (pod termination), so the reconciler drives it
// before invoking computeQueue.
func computeQueue(leases []*kleasev1alpha1.GPULease, now time.Time, admissible func(*kleasev1alpha1.GPULease) bool) QueueResult {
	var result QueueResult
	seen := map[types.NamespacedName]bool{}
	changed := func(l *kleasev1alpha1.GPULease) {
		key := types.NamespacedName{Namespace: l.Namespace, Name: l.Name}
		if !seen[key] {
			seen[key] = true
			result.Changed = append(result.Changed, l)
		}
	}
	transition := func(l *kleasev1alpha1.GPULease, kind, message string) {
		result.Transitions = append(result.Transitions, Transition{Lease: l, Kind: kind, Message: message})
	}
	adjust := func(l *kleasev1alpha1.GPULease, field string, from, to time.Time) {
		changed(l)
		transition(l, TransitionAdjusted,
			fmt.Sprintf("%s adjusted from %s to %s by spec change or repair",
				field, from.UTC().Format(time.RFC3339), to.UTC().Format(time.RFC3339)))
	}

	// Pass 0: derivation. Timers are derived state, not write-once stamps:
	// every pass recomputes them from spec so edits to a live lease take
	// effect and corrupted status heals.
	for _, l := range leases {
		switch l.Status.State {
		case kleasev1alpha1.GPULeaseStateActive:
			if l.Status.ActiveSince == nil {
				continue // admission stamps it; nothing to derive from yet
			}
			want := metav1.NewTime(l.Status.ActiveSince.Add(l.Spec.Duration.Duration))
			if l.Status.ExpiresAt == nil {
				l.Status.ExpiresAt = &want
				changed(l) // repair, not an adjustment: nothing to move from
			} else if !want.Time.Equal(l.Status.ExpiresAt.Time) {
				from := l.Status.ExpiresAt.Time
				adjust(l, "expiresAt", from, want.Time)
				l.Status.ExpiresAt = &want
			}
		case kleasev1alpha1.GPULeaseStateDraining:
			if l.Status.ExpiresAt == nil {
				continue
			}
			want := metav1.NewTime(l.Status.ExpiresAt.Add(gracePeriodOf(l)))
			if l.Status.DrainDeadline == nil {
				l.Status.DrainDeadline = &want
				changed(l)
			} else if !want.Time.Equal(l.Status.DrainDeadline.Time) {
				from := l.Status.DrainDeadline.Time
				adjust(l, "drainDeadline", from, want.Time)
				l.Status.DrainDeadline = &want
			}
		}
	}

	// Pass 1: expiry. The holder keeps the GPU until its workload is
	// reclaimed, so expiry starts a drain rather than finishing the lease.
	for _, l := range leases {
		if l.Status.State != kleasev1alpha1.GPULeaseStateActive || l.Status.ExpiresAt == nil {
			continue
		}
		if now.Before(l.Status.ExpiresAt.Time) {
			continue
		}
		expiredAt := l.Status.ExpiresAt.Time
		deadline := metav1.NewTime(expiredAt.Add(gracePeriodOf(l)))
		l.Status.State = kleasev1alpha1.GPULeaseStateDraining
		l.Status.DrainDeadline = &deadline
		changed(l)
		transition(l, TransitionDraining,
			fmt.Sprintf("lease expired at %s; draining workload (force-delete at %s)",
				expiredAt.UTC().Format(time.RFC3339), deadline.Time.UTC().Format(time.RFC3339)))
	}
	for _, l := range leases {
		if l.Status.State == kleasev1alpha1.GPULeaseStateDraining {
			result.Draining = append(result.Draining, l)
		}
	}

	// Pass 2: single-active (split-brain resolution).
	var actives []*kleasev1alpha1.GPULease
	for _, l := range leases {
		if l.Status.State == kleasev1alpha1.GPULeaseStateActive {
			actives = append(actives, l)
		}
	}
	if len(actives) > 1 {
		slices.SortFunc(actives, func(a, b *kleasev1alpha1.GPULease) int {
			return activeRank(a).Compare(activeRank(b))
		})
		for _, l := range actives[1:] {
			l.Status.State = kleasev1alpha1.GPULeaseStatePending
			l.Status.ActiveSince = nil
			l.Status.ExpiresAt = nil
			changed(l)
			transition(l, TransitionDemoted,
				"multiple Active leases found; earliest admission kept, this lease returned to Pending")
		}
		actives = actives[:1]
	}

	// Pass 3: admission. Strict handoff: a Draining lease still owns the
	// GPU until its workload is reclaimed, so it gates admission too.
	if len(actives) == 0 && len(result.Draining) == 0 {
		for _, c := range pendingLeases(leases) {
			if admissible != nil && !admissible(c) {
				continue
			}
			activeSince := metav1.NewTime(now)
			expiresAt := metav1.NewTime(now.Add(c.Spec.Duration.Duration))
			c.Status.State = kleasev1alpha1.GPULeaseStateActive
			c.Status.ActiveSince = &activeSince
			c.Status.ExpiresAt = &expiresAt
			c.Status.QueuePosition = 0
			changed(c)
			transition(c, TransitionAdmitted,
				fmt.Sprintf("lease admitted; expires at %s", expiresAt.Time.UTC().Format(time.RFC3339)))
			actives = append(actives, c)
			break
		}
	}
	if len(actives) > 0 {
		result.Active = actives[0]
	}

	// Pass 4: queue positions. Materialize Pending on fresh leases so the
	// lifecycle is visible (unset state means "never reconciled").
	pending := pendingLeases(leases)
	for i, p := range pending {
		if p.Status.State == "" {
			p.Status.State = kleasev1alpha1.GPULeaseStatePending
			changed(p)
		}
		if p.Status.QueuePosition != int32(i) {
			p.Status.QueuePosition = int32(i)
			changed(p)
		}
	}

	return result
}

// pendingLeases returns leases awaiting admission (unset state or Pending),
// in global FIFO order.
func pendingLeases(leases []*kleasev1alpha1.GPULease) []*kleasev1alpha1.GPULease {
	pending := make([]*kleasev1alpha1.GPULease, 0, len(leases))
	for _, l := range leases {
		if l.Status.State == "" || l.Status.State == kleasev1alpha1.GPULeaseStatePending {
			pending = append(pending, l)
		}
	}
	slices.SortFunc(pending, fifoCompare)
	return pending
}

// fifoCompare ranks leases by creationTimestamp, then namespace, then
// name — a deterministic total order. Negative when a sorts before b.
func fifoCompare(a, b *kleasev1alpha1.GPULease) int {
	if c := a.CreationTimestamp.Compare(b.CreationTimestamp.Time); c != 0 {
		return c
	}
	if c := strings.Compare(a.Namespace, b.Namespace); c != 0 {
		return c
	}
	return strings.Compare(a.Name, b.Name)
}

// activeRank orders Active leases for split-brain resolution. Earliest
// activeSince wins; a lease missing the stamp ranks last.
func activeRank(l *kleasev1alpha1.GPULease) time.Time {
	if l.Status.ActiveSince == nil {
		return time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	}
	return l.Status.ActiveSince.Time
}

// gracePeriodOf returns the lease's grace period, defaulting when unset.
func gracePeriodOf(l *kleasev1alpha1.GPULease) time.Duration {
	if l.Spec.GracePeriod != nil {
		return l.Spec.GracePeriod.Duration
	}
	return defaultGracePeriod
}
