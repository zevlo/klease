# klease

klease is a Kubernetes operator that lets workloads take turns using a shared GPU.

Instead of fighting over who owns the accelerator, you label the GPU Deployment as managed, and each team (or job) creates a `GPULease` that reserves a time slot. klease runs the queue for you: the holder's Deployment is scaled to 1 for the duration of its lease, everyone else's stays at 0, and expired holders are drained before the next lease starts.

## How it works

A `GPULease` points at an existing Deployment — it never creates or modifies pods itself:

```yaml
apiVersion: klease.zachallen.io/v1alpha1
kind: GPULease
metadata:
  name: my-turn
spec:
  workloadRef:
    kind: Deployment
    name: vllm-server
  duration: 2h      # how long you hold the GPU once admitted
  gracePeriod: 5m   # how long your workload gets to shut down cleanly
```

The lease moves through four states:

```
Pending → Active → Draining → Expired
```

- **Pending** — queued. Leases are admitted first-come, first-served (by creation time). The clock starts at admission, not creation: waiting in line never burns your slot.
- **Active** — your Deployment is scaled to 1 and holds the GPU until `expiresAt`.
- **Draining** — your slot ended. The Deployment is scaled to 0 and klease waits for your pods to exit. If they are still running when `gracePeriod` elapses, they are force-deleted.
- **Expired** — done. The next lease in line is admitted only after your workload is fully drained, so two workloads never touch the GPU at the same time.

Deleting an Active lease follows the same path: the object sticks around (held by a finalizer) until the drain finishes, so deleting a lease never strands pods on the GPU.

### The managed-label contract

klease only touches Deployments labeled:

```yaml
klease.zachallen.io/managed: "true"
```

The rule is simple: a managed Deployment runs **exactly one replica when its lease is Active, zero replicas at every other time** — including manual scale-ups, which are reverted automatically. Unlabeled Deployments are ignored entirely.

If a lease points at a Deployment that is missing or not labeled, the lease stays Pending with a `WorkloadNotFound` condition and is admitted as soon as the target appears.

## Getting started

Prerequisites: a Kubernetes 1.30+ cluster, `kubectl`, and access to a container registry if deploying from source.

**Install the operator:**

```sh
# Build and push the manager image
make docker-build docker-push IMG=<registry>/klease:v0.1.0

# Install CRDs and deploy the manager
make install
make deploy IMG=<registry>/klease:v0.1.0
```

**Put a workload under lease arbitration:**

```sh
kubectl label deployment vllm-server klease.zachallen.io/managed=true
kubectl scale deployment vllm-server --replicas=0
```

> The label alone is enough — on the next reconcile klease parks the Deployment at 0 replicas until a lease holds it.

**Take a turn:**

```sh
kubectl apply -f - <<'EOF'
apiVersion: klease.zachallen.io/v1alpha1
kind: GPULease
metadata:
  name: my-turn
spec:
  workloadRef:
    kind: Deployment
    name: vllm-server
  duration: 2h
EOF
```

**Watch the queue:**

```sh
kubectl get gpuleases     # short name: gl
```

```
NAME      STATE     WORKLOAD       DURATION   EXPIRES                AGE
my-turn   Active    vllm-server    2h         2026-08-22T14:00:00Z   5m
next-up   Pending   vllm-server    1h                                2m
```

Events on each lease (`kubectl describe gpulease my-turn`) show every transition: `Admitted`, `Draining`, `Expired`.

### Changing a lease

- **Extend or shorten your slot**: edit `spec.duration` on an Active lease — `expiresAt` moves accordingly (shortening past the current time ends the slot).
- **Adjust shutdown time**: edit `spec.gracePeriod`, even while Draining.
- **Point elsewhere**: `workloadRef` is immutable. Delete the lease and create a new one.

### Spec reference

| Field | Description |
|---|---|
| `workloadRef.kind` | Must be `Deployment` |
| `workloadRef.name` | Deployment in the lease's namespace |
| `duration` | Slot length once admitted (`1s`–`24h`) |
| `gracePeriod` | Drain budget after expiry before force-delete (default `5m`, must not exceed `duration`) |

### Status reference

| Field | Description |
|---|---|
| `state` | `Pending`, `Active`, `Draining`, or `Expired` |
| `activeSince` / `expiresAt` | Slot start and end (timer starts at admission) |
| `drainDeadline` | Force-delete cutoff while Draining |
| `queuePosition` | Place in line (0 = head) |
| `conditions` | `WorkloadNotFound` when the target is missing or not managed |

## Metrics

The manager's metrics endpoint (HTTPS, authn/authz-protected by default) exposes:

| Metric | Type | Meaning |
|---|---|---|
| `klease_queue_depth` | gauge | Leases waiting for the GPU |
| `klease_active_leases` | gauge | Current holder (0 or 1) |
| `klease_admission_wait_seconds` | histogram | Wait from lease creation to admission |
| `klease_drain_duration_seconds` | histogram | Time from expiry to workload fully reclaimed |

## Uninstall

```sh
kubectl delete gpuleases -A           # drains any holder first
make undeploy
make uninstall
```

## Development

```sh
make test        # unit + envtest suite
make test-e2e    # full lifecycle on an isolated Kind cluster
make lint        # golangci-lint
```

See [AGENTS.md](AGENTS.md) for repository layout and conventions.

## License

Copyright 2026 Zachary Allen.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
