# CHIMERA Phase 11 — Distributed Architecture Design Proposal

> **Implementation Status: DESIGN PROPOSAL ONLY (Not Implemented)**
> CHIMERA `v0.2.0` is a **single-host** workflow orchestration engine backed by a local append-only file (`<workflow_id>.chimera.log`) and local OS file locks (`fcntl.flock` / `msvcrt.locking`). Local file locks and local event logs **do not** provide distributed consensus across multiple machines or network filesystems (NFS/SMB). This document specifies the architecture required to evolve CHIMERA into a genuinely distributed multi-node workflow engine when concrete multi-host scale requirements justify external infrastructure dependencies.

---

## 1. Why Sharing a Local Event Log Over NFS/SMB Fails

Attempting to "distribute" a single-host engine by placing `<workflow_id>.chimera.log` on a shared network filesystem (NFS, SMB/CIFS, EFS) violates fundamental distributed systems safety invariants:
1. **Unreliable Lock Leases**: Advisory file locks over network filesystems suffer from lease-expiration split-brain during transient network partitions. Client A may believe it still holds `LOCK_EX` while the NFS server expires Client A's lease and grants `LOCK_EX` to Client B.
2. **Non-Atomic Append Interleaving**: Concurrent `O_APPEND` writes from two network clients are not guaranteed to be atomic at the frame boundary, corrupting `@CHIMERA1`/`@CHIMERA2` frames.
3. **Page-Cache Incoherency**: Client B's `read_bytes()` may observe stale attribute/page caches even after Client A calls `os.fsync()`.

Consequently, a distributed backend must replace file-based coordination with a network-accessible storage/coordination engine that enforces **atomic compare-and-swap (CAS) fencing** on every state transition.

---

## 2. Core Distributed Architecture Requirements

### 2.1 Durable Shared Coordination & Storage
A distributed CHIMERA deployment separates roles into:
- **Stateless Coordinator / Scheduler Nodes**: Evaluate DAG readiness, promote `PENDING -> READY` tasks, and sweep expired worker leases.
- **Stateless Worker Nodes**: Poll or receive `READY` task claims, execute task callables in isolated child processes/containers, send periodic heartbeats, and submit fenced completions.
- **Authoritative State Store**: Stores workflow definitions (`definition_hash`), current task states (`claim_epoch`, `lease_expires_at`), and the append-only audit log of state transitions inside atomic transactions.

### 2.2 Atomic Task Claiming, Leases & Heartbeats
Instead of holding an open file descriptor lock for the duration of a workflow, task execution uses **time-bounded leases**:
1. **Atomic Claim**: A worker claims a `READY` task by atomically executing:
   - Increment `claim_epoch = claim_epoch + 1` and `attempt = attempt + 1`.
   - Set `state = 'RUNNING'`, `worker_id = :worker_id`, `lease_expires_at = NOW() + :lease_duration`.
   - Append `TASK_STATE_CHANGED(READY -> RUNNING, claim_epoch)` to the event log in the same transaction.
2. **Lease Renewal (Heartbeating)**: For tasks whose `timeout_seconds` exceeds `lease_duration` (e.g., a 30-minute task with a 30-second lease), the worker sends heartbeats every `lease_duration / 3`:
   ```sql
   UPDATE chimera_tasks
   SET lease_expires_at = NOW() + INTERVAL '30 seconds'
   WHERE workflow_id = :wf_id
     AND task_id = :task_id
     AND state = 'RUNNING'
     AND claim_epoch = :worker_claim_epoch
     AND NOW() < deadline_at;
   ```
   If the `UPDATE` affects `0` rows, the worker learns immediately that its lease was revoked or the workflow was cancelled, and terminates its local child process.

### 2.3 Monotonic Fencing Tokens Across Coordinator & Worker Restarts
Fencing tokens (`claim_epoch`) are persisted in the authoritative database row for `(workflow_id, task_id)`:
- Every state transition out of `RUNNING` (whether by worker completion, lease expiry, timeout, or cancellation) increments or checks `claim_epoch` inside a serializable/row-locked transaction:
  ```sql
  UPDATE chimera_tasks
  SET state = 'SUCCEEDED',
      result_json = :result_json,
      completed_at = NOW()
  WHERE workflow_id = :wf_id
    AND task_id = :task_id
    AND state = 'RUNNING'
    AND claim_epoch = :token_claim_epoch;
  ```
- If Worker 1 experiences a 60-second GC pause or network partition, the lease reaper increments `claim_epoch` from `1` to `2` and transitions the task to `RETRY_WAIT` / `READY`. When Worker 1 reconnects and submits its completion with `token_claim_epoch = 1`, the `WHERE claim_epoch = 1` predicate matches `0` rows, and the store records `STALE_COMPLETION_IGNORED`.

### 2.4 Leader Election vs. Lease-Based Timer Sweeping
Rather than requiring a single fragile cluster leader for correctness:
- **Correctness never depends on a single leader**: Because every claim, completion, promotion, and timeout transition is guarded by row-level locking (`SELECT ... FOR UPDATE`) and monotonic `claim_epoch` predicates in the database, multiple coordinator instances can safely run concurrently without corrupting state.
- **Work partitioning**: Coordinators use `SELECT ... FOR UPDATE SKIP LOCKED` to poll expired task leases (`state = 'RUNNING' AND (NOW() >= lease_expires_at OR NOW() >= deadline_at)`) and elapsed retry timers (`state = 'RETRY_WAIT' AND NOW() >= wake_at`) in bounded batches without coordinator contention.

### 2.5 Network Partition Handling & Split-Brain Behavior
- **Worker-to-Store Partition**: If a worker is partitioned from the state store, its lease renewal fails. Once `NOW() >= lease_expires_at` on the store's monotonic/database clock, the task is fenced (`claim_epoch++`) and rescheduled on a healthy worker. Meanwhile, the partitioned worker stops renewing its lease and kills its local subprocess after missing 2 consecutive heartbeats.
- **External Side-Effect Caveat**: Just as in the single-host engine, fencing protects CHIMERA's state store from stale commits, **not** external non-transactional systems. External APIs must validate `ctx.idempotency_key` (or `ctx.claim_epoch` if the external store supports fenced writes).

### 2.6 Worker Authentication, Authorization & Multi-Tenancy
- **Authentication**: Workers authenticate to the coordinator/API gateway using mTLS client certificates or short-lived workload identity JWTs (`worker_id` bound to the certificate SAN).
- **Authorization**: Workers are scoped to specific task queues/namespaces and may only renew leases or submit results for `(workflow_id, task_id, claim_epoch)` tokens explicitly issued to their authenticated `worker_id`.
- **Secrets Management**: Task `input_payload` should store secret references (e.g., `vault://...` or KMS key URIs) resolved just-in-time by the worker rather than persisting plaintext credentials in `chimera_events`.

### 2.7 Backpressure, Fairness & Capacity Limits
- **Global and Per-Workflow Concurrency**: Enforced via a concurrency counter or semaphore table in the transaction that claims `READY` tasks.
- **Weighted Fair Queuing Across Workflows**: Ready-task polling orders eligible tasks by `(tenant_active_running_count, priority, ready_at, workflow_id, task_id)` so a single tenant submitting a 10,000-task fan-out workflow cannot starve smaller workflows.
- **Bounded Queue Admission**: Rejects new `create_workflow` requests with HTTP `429 Too Many Requests` / `BackpressureExceededError` when pending task counts exceed configured cluster watermarks.

---

## 3. Backend Trade-Off Evaluation

| Dimension | Option A: Relational DB (PostgreSQL) | Option B: Message Broker + KV (Redis / RabbitMQ / Kafka) | Option C: Consensus Log (etcd / Raft) |
| :--- | :--- | :--- | :--- |
| **Atomic State + Event Log Commit** | **Native ACID**: Updating `chimera_tasks`, promoting downstream tasks, and inserting into `chimera_events` happen in one SQL transaction. | **Complex / Dual-Write Risk**: Brokers lack multi-key ACID transactions across task state, dependency joins, and event history unless scripted in single-shard Lua. | **Supported via Multi-Key Txn**: Supported in etcd/Raft, but strict payload size limits (e.g., 1.5 MiB per txn) restrict large workflows. |
| **Atomic Task Claiming** | **Native**: `SELECT ... FOR UPDATE SKIP LOCKED` provides low-contention concurrent work claiming. | **High Throughput**: Native consumer groups, but requires a separate durable state store for DAG join readiness (`all dependencies SUCCEEDED`). | **Poor Fit**: Consensus stores are not designed for high-churn queue polling. |
| **Fencing Token Enforcement** | **Exact**: `WHERE claim_epoch = :epoch` enforced atomically on commit. | Requires custom Lua scripts or an external relational DB to reject stale worker completions. | **Exact**: `mod_revision` / version comparisons. |
| **Operational Complexity** | **Low**: Single well-understood RDBMS with standard backup, PITR, and schema migration tooling. | **Medium–High**: Requires operating both a broker and a durable database, plus reconciling state between them. | **High**: Requires dedicated consensus cluster sizing and compaction tuning. |
| **Throughput Ceiling** | **~1,000–5,000 task transitions/sec** per primary PostgreSQL instance with batched commits. | **10,000+ transitions/sec**, at the cost of significantly higher architectural complexity. | **~500–2,000 writes/sec** (bounded by Raft quorum disk sync). |

### Recommendation: Relational Database (PostgreSQL) as First Distributed Backend
When CHIMERA requires multi-host execution, a **PostgreSQL-backed `RelationalWorkflowBackend`** is the recommended first implementation:
1. It preserves CHIMERA's exact state-machine and fencing invariants (`I_1`–`I_5`) inside single ACID transactions without dual-write anomalies between a broker and a database.
2. `SELECT ... FOR UPDATE SKIP LOCKED` natively solves concurrent worker claiming, lease expiration reaping, and retry timer polling.
3. Both the current `LocalEventLogBackend` (zero dependencies) and the proposed `PostgresWorkflowBackend` can share the exact same `TaskSpec`, `ClaimToken`, `TaskContext`, and `ProcessTaskExecutor` interfaces.
