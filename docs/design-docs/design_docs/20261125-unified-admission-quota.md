# Unified admission quota: one ledger for search and build

Related issue: (to be filled). This document covers the concurrency-side
companion of the existing rate-side quota system
(`internal/util/quota`, `internal/util/ratelimitutil`, proxy quota center).
The existing system limits request *rates* (tokens per second) per
cluster/database/collection/partition. This design adds a single,
work-conserving **concurrency admission ledger** with bounded per-collection
queues, covering DQL retrieval (`SearchTask`/`QueryTask`) and index build
tasks together, and is inert until explicitly enabled.

## 1. Problem

Today three mechanisms keep separate books and none can answer "how many
searches and builds of collection X are running right now, and where do the
overflow requests go?":

1. Proxy entry rate limiting (per-user pending caps, queue expiry).
2. QueryNode read-task scheduling (worker pool, queue merge, deadline
   progression, expiry).
3. DataCoord index build scheduling (per-node concurrency cap, memory
   admission, node selection).

A batch backfill (large index rebuilds on a few big collections) and a burst
of vector retrieval on the same collection therefore contend without any
shared accounting. Symptoms observed in production:

- retrieval p99 latency rises to several seconds;
- some requests fail with rate-limit errors whose retry advice says only
  "later", and retries fail again because the client does not know how long
  to wait;
- some requests hang inside nodes with neither result nor explicit failure;
- tuning client concurrency down and moving builds to night only hides the
  problem.

## 2. Goals and non-goals

### Goals

1. **Unified quota declaration.** One declaration gives cluster, database and
   collection scopes, each with retrieval and build concurrency limits. A
   collection-level limit constrains retrieval and build tasks of that
   collection against the same ledger.
2. **Work conservation.** Quota is not hard-partitioned per collection. Idle
   shares are borrowable; when the lender queues, it gets its reserved share
   back within a bounded number of scheduling rounds (no preemption of
   running tasks — reclaim happens at release time).
3. **Bounded per-collection queues.** Transient overflow enters the
   collection's own FIFO wait queue with length and max wait-time limits;
   only overflow beyond the queue, or wait timeout, rejects.
4. **Fair rotation.** When multiple collections queue, service rotates; one
   collection cannot occupy the service window for more than a configurable
   number of consecutive rounds. Inside a collection, FIFO.
5. **Two task classes, one ledger.** Search and build draw from one
   accounting book; each class keeps a guaranteed share even when the other
   saturates. Build tasks do **not** hold memory reservation while queued —
   memory admission stays in the build path and runs only at dispatch.
6. **Deadline and cancellation.** A queued item whose client deadline
   expires, or whose client disconnects, leaves the queue immediately,
   counters drop synchronously, and no token is ever acquired for it.
7. **Distinguishable overflow responses.** "saturated now, queueable" vs
   "hard reject" are different typed errors carrying scope, task class,
   current queue depth and a suggested retry delay. Underlying errors are
   never passed through to the client.
8. **Observability from the same source.** In-flight, queued, max wait,
   rejects and effective quota per scope/class are sampled from the same
   in-memory state the scheduler mutates; snapshots reconcile with internal
   counters.

### Non-goals

- Replacing the existing rate limiter tree, the proxy per-user pending cap
  and queue expiry, QueryNode read scheduling/merge/deadline logic, or
  DataCoord build concurrency/memory admission/node selection. All stay in
  force; the new gate is an additional admission point, default off.
- Cross-process distributed quotas. The ledger is per-process (a node-local
  gate). Cluster-level scope here means "the whole process's view", matching
  where the gate is instantiated; no external service is introduced
  (hard constraint: stdlib + existing dependencies only).
- Rate (tokens/sec) limiting. That remains the job of the existing quota
  system.
- Changing any wire proto or existing error code's meaning.

## 3. Hard constraints mapped to design decisions

| Constraint | Design decision |
|---|---|
| Single ledger; in-flight + reserved never exceeds quota at any instant | All admission decisions happen in one `Ledger` component under one mutex; one token object per admitted task at each of cluster/db/collection scopes. No second counter exists. |
| Same source of truth for queue/in-flight/reservation/counters/snapshots; failure paths return acquired quota; no leak or double-return | Queues, counters and the snapshot view live in one struct guarded by one lock. `Lease.Release()` is idempotent (atomic CAS); every waiter holds no token until dispatch, so cancel/timeout paths need no token return. |
| Compatibility: no new config => existing error codes, read scheduling/expiry, build concurrency/memory admission unchanged; config only added, never changed | Master switch `quotaAdmission.enabled` defaults `false`. Disabled path is a lock-free atomic read returning a no-op lease: no queue, no reject, no counting. Existing code paths are wrapped, not rewritten. |
| No external services; no vectors / PK values / raw filter expressions in logs, errors, metrics | Pure in-process package depending only on stdlib + `pkg/v3`. Errors and metrics carry IDs and counts only (dbID, collectionID, class, depth, retry-after). |
| No degradation without contention | Fast path: one atomic bool (enabled) then, when no queue exists and quota is free, an uncontended lock attempt; the dispatch goroutine parks on a condition/event channel while everything is idle. No extra cross-process round trips. |

## 4. Package layout

New package `internal/util/quota/admission` (the existing `quota` package
holds rate-side constants; the subpackage name does not collide):

- `types.go` — `ScopeKey`, `TaskClass`, `QuotaSpec`, `Lease`, `Snapshot` types.
- `errors.go` — internal sentinels + the structured `RejectInfo`; merr
  translation helper used at component boundaries.
- `ledger.go` — the ledger: quota declarations, token accounting, enable
  switch, `Acquire`/`UpdateQuota`/`Close`, snapshot.
- `queue.go` — per-collection FIFO wait queue, waiter lifecycle, timeout and
  cancel handling.
- `scheduler.go` — the single dispatch goroutine: expiry sweep + weighted
  fair rotation (deficit round robin) + admission checks at all three
  scopes.
- `clock.go` — injectable clock (`now func() time.Time`, async wake events),
  plus the production real clock.
- `metrics.go` — optional Prometheus registration fed by `Snapshot()`; the
  ledger itself stays metrics-free so unit tests need no prometheus state.
- `*_test.go` — table and concurrency tests with fake tasks and a
  controllable fake clock, in the repository's testify style.

The package imports only the Go standard library and `pkg/v3`
(`merr`, `mlog`, `metrics`, `typeutil`). It imports no `internal/*` package,
keeping the dependency edge acyclic the same way
`internal/proxy/scheduler` does.

## 5. Core model

### 5.1 Scopes and classes

```go
type ScopeKey struct {
    DatabaseID   int64 // 0 at cluster scope
    CollectionID int64 // 0 at cluster/database scope
}

type TaskClass int8
const (
    ClassSearch TaskClass = iota // DQL search/query retrieval
    ClassBuild                   // index build
)
```

Scopes form a tree: cluster -> databases -> collections. Every `Acquire`
reserves one token at **each** of the three scopes; admission requires all
three. A collection without an explicit declaration inherits database and
cluster limits and has no collection-level cap (infinite).

### 5.2 Quota declaration

```go
type QuotaSpec struct {
    // Total concurrency at this scope across both classes.
    Total int64
    // Guaranteed share per class; SearchReserve + BuildReserve <= Total.
    // Both 0 means "no class guarantee" (pure sharing).
    SearchReserve int64
    BuildReserve  int64
}
```

One declaration document carries specs for the three scopes:

```go
type Declaration struct {
    Cluster     QuotaSpec
    Databases   map[int64]QuotaSpec
    Collections map[int64]QuotaSpec // keyed by collection ID
}
```

"Same declaration, three levels" (requirement 一.1): search and build both
appear at every scope; a collection's spec constrains both classes of that
collection against one `Total`.

### 5.3 Accounting at one scope

At each scope node, per class c:

- `inFlight[c]` — admitted, currently running tasks of class c.
- `inFlightTotal = inFlight[Search] + inFlight[Build]`.

Admission rule for one task of class c at this scope:

1. If `inFlightTotal >= Total` => cannot admit now.
2. Class guarantee: if class c's in-flight is below its reserve
   (`inFlight[c] < reserve[c]`), the task is admitted with priority over
   borrowers of the other class — implemented by admitting reserve-eligible
   waiters before surplus waiters in the dispatch order (5.5).
3. Otherwise the task is a surplus (borrowing) task; it is admitted while
   `inFlightTotal < Total`.

Work conservation (requirement 一.2): idle reserve of either class is
usable by the other. Bounded reclaim (requirement 一.2): a queued task that
is reserve-eligible is dispatched before any surplus task; because every
release re-enters the scheduler, the lender waits at most until the next
release at that scope plus one scheduling round — never preempting a running
task, which cannot be safely killed mid-retrieval/mid-build.

Hierarchical check: a task is admissible only if the check passes at
collection, database **and** cluster scope. The tightest scope wins;
queuing is still per collection so fairness and FIFO remain well defined.

### 5.4 Lease: the only token handle

```go
type Lease interface {
    Release()          // idempotent; returns tokens at all three scopes
    Scope() ScopeKey
    Class() TaskClass
}
```

`Release` uses an atomic compare-and-swap so duplicate returns (defer plus
an explicit error-path call) are no-ops. On release the counters fall back
synchronously and the scheduler is woken. There is no separate
"reservation" object held by queued tasks: a queued waiter owns **zero**
tokens, which makes cancel/timeout leak-free by construction and keeps
build memory admission in the build path (a build reserves memory only
after the ledger admits it).

### 5.5 Scheduling: weighted fair rotation

One dispatch goroutine per ledger, woken by enqueue/release/quota-update
events and by a timer for timeout sweeps. Each scheduling pass:

1. **Expiry sweep first.** Every queue is scanned; waiters whose deadline
   (`min(client deadline, enqueueTime + maxQueueWait)`) has passed fail
   with the queue-timeout error and are removed; context cancellation is
   handled the same way via a per-watcher goroutine parked on
   `ctx.Done()`.
2. **Rotation over non-empty collection queues.** Collections are served in
   deficit round robin (DRR): each collection has a quantum equal to its
   declared reservation weight (default 1 when no collection spec
   exists). A collection with zero deficit cannot dispatch this round;
   deficit accrues each round, so long-term dispatch counts converge to
   reservation proportions.
3. **Per-turn cap.** A collection dispatches at most
   `maxConsecutiveServes` tasks per visit (default 1), guaranteeing no
   collection occupies consecutive service windows; within the visit the
   queue is strictly FIFO.
4. **Reserve before surplus.** At each dispatch attempt, reserve-eligible
   waiters (5.3 rule 2) are considered before surplus waiters across the
   whole pass, so guarantees win over borrowed capacity.
5. A task is dispatched only when all three scope checks pass; otherwise
   the round ends for that collection and the next collection rotates in.
   The goroutine parks when every queue is empty.

DRR gives both required fairness properties: service ratio approaches
reservation shares over a long two-collection run, and no collection serves
more than `maxConsecutiveServes` in a row.

### 5.6 Queue bounds and overflow

Each collection queue has:

- `maxQueueLength` — hard cap; a new `Acquire` when the queue is full
  returns the hard-reject error immediately (it never waits).
- `maxQueueWait` — maximum queued time, combined with the caller's ctx
  deadline.

Two distinguishable failure modes (requirement 一.7):

| Condition | merr code on the wire | Retriable | Fields |
|---|---|---|---|
| Queue full / hard reject | `ErrServiceRateLimit` (code 8, existing retriable rate-limit family) | yes | scope, class, queue depth, suggested retry delay |
| Wait timeout / cancel while queued | `ErrServiceQuotaExceeded` (code 9, existing) | no | scope, class, queue position at timeout, waited duration |
| Ledger closed (node stopping) | `ErrServiceQuotaExceeded` with reason `admission_closed` | no | scope, class |

No new numeric error code is introduced in this increment: the two existing
service-level sentinels already encode "retry soon" vs "exhausted", and
inventing a code in the 1–99 service family would break the one-code-one-
sentinel registry for a distinction the structured fields already carry.
The boundary helper builds these with `WrapErrServiceRateLimit` /
`WrapErrServiceQuotaExceeded`; the structured detail (scope, class, depth,
retry-after, position) travels in the message/fields and never contains
request payload. Suggested retry delay = EWMA of recent queue wait times
for that scope/class, clamped to `[minRetryAfter, maxRetryAfter]`; with no
sample yet it defaults to `maxQueueWait/2`.

Blame classification per the error handling guide: saturation is an
internal/transient condition reached by valid requests => System class,
which is exactly how the two existing sentinels behave (code 8 is
retriable; code 9 is not InputError-marked).

### 5.7 Master switch and fast path

```go
func (l *Ledger) Acquire(ctx context.Context, key ScopeKey, class TaskClass) (Lease, error)
```

1. One `atomic.Bool` load. If disabled => return a no-op lease whose
   `Release` does nothing. No lock, no queue, no counter (requirement 四.4,
   test 5.4).
2. Under `mu`: try all three scope checks. If all pass, token accounting
   updates and a real lease returns — when nothing is queued this lock is
   uncontended.
3. Otherwise enqueue onto the collection's bounded queue (or hard reject
   if full) and block on the waiter channel until dispatch, ctx cancel,
   timeout, or close.

### 5.8 Quota updates and reset

`UpdateQuota(Declaration)` replaces the spec table atomically under `mu`
and wakes the scheduler. In-flight tasks keep their leases: no task is
re-admitted or double-refunded; new requests are evaluated against the new
spec immediately. If a smaller quota leaves `inFlight > Total`, no new
admission happens at that scope until releases drain the excess; the
ledger still returns to zero because releases are unconditional and
idempotent (requirement 三.2).

### 5.9 Shutdown

`Close()` flips the closed flag, fails every queued waiter with the
closed error, wakes and parks the dispatch goroutine, and marks the
counters for the closure report. In-flight leases still release normally;
after all releases the snapshot reads all zeros (requirement 三.4).
Post-close `Acquire` fails immediately with the closed error instead of
queuing.

## 6. Observability

`Snapshot()` is taken under the same `mu` that mutates the state, so
snapshot and counters are the same source of truth (requirement 一.8):

```go
type ScopeSnapshot struct {
    Scope            ScopeKey
    EffectiveQuota   QuotaSpec
    InFlight         [2]int64
    Queued           [2]int64
    MaxWaitMillis    int64
    Rejected         [2]int64 // hard rejects + wait timeouts, split by reason
    InFlightPlusReserved int64 // always <= EffectiveQuota.Total while enabled
}
```

`metrics.go` optionally publishes gauges/counters
(`milvus_admission_inflight`, `..._queued`, `..._max_wait_seconds`,
`..._rejected_total`, `..._effective_quota`) labeled by
`nodeID, scope, dbID, collectionID, class` — low-cardinality IDs only, no
request content. Reconciliation is testable directly: every test asserts
`Snapshot()` equals the internal counters and that
`inFlight + queued-reserved == admitted - released`.

## 7. Integration plan (how the new execution point connects)

The ledger is constructed by each participating component and wrapped
around the existing admission sites; exact functions are filled in after
the read/build scheduler walkthrough and listed in section 11.

- Proxy/DQL: gate retrieval tasks at enqueue, ahead of the existing
  per-user pending cap. The cap and its queue-expiry logic remain the
  inner mechanism; when the gate is off, behavior is byte-identical.
- QueryNode read scheduler: optional second gate at task dequeue for
  node-local concurrency bookkeeping; merge/deadline/cleanup logic is
  untouched.
- DataCoord build scheduler: gate at enqueue/assignment, **before** memory
  admission and node selection, so queued builds hold neither ledger
  tokens nor memory; existing concurrency caps and memory checks run after
  a lease is acquired and remain authoritative for their own resources.
- Every call site releases the lease in a `defer` on the task's terminal
  path (success, failure, cancel), satisfying 三.3.

Configuration (new keys only, all under a new `quotaAdmission` block in
`configs/milvus.yaml`, defaults preserve current behavior):

```yaml
quotaAdmission:
  enabled: false
  maxQueueLength: 1024          # per collection
  maxQueueWait: 30s
  maxConsecutiveServes: 1
  minRetryAfter: 10ms
  maxRetryAfter: 5s
  cluster:    {total: 0, searchReserve: 0, buildReserve: 0}   # 0 = unlimited
  databases:  {}
  collections: {}
```

## 8. Failure and degradation behavior

1. Queue timeout is an explicit typed result with scope and queue
   position — never rendered as a downstream timeout or empty result.
2. Quota re-issue/reset never refunds in-flight tasks twice; new requests
   use the new quota; the ledger returns to zero after drains.
3. Build failure/cancel releases ledger tokens (and then memory
   reservation) before the failure is reported, unblocking following
   builds.
4. Node stop/queue offline fails all queued items explicitly and zeroes
   the counters once in-flight leases release.
5. Gate disabled: direct pass-through, no queue/reject/count.

## 9. Privacy

Errors, logs and metrics contain only IDs (db/collection), enum class,
counts, durations and positions. No query vectors, primary-key values,
filter expressions or request payloads are read, logged or labeled.

## 10. Testing and acceptance

Tests live beside the package, use testify plus a controllable fake clock
and fake tasks, and run with the repository's required flags
(`go test -tags dynamic,test -gcflags="all=-N -l"`).

1. **Two-collection concurrency test** — flood collection A with search
   tasks and B with build tasks; assert per-collection in-flight, queued,
   max-wait and reject counters at sampled times and at rest.
2. **Fairness** — both collections queue continuously; assert dispatch
   ratio converges to reservation shares and neither collection exceeds
   `maxConsecutiveServes` in a row.
3. **Conservation** — mixed success/failure/cancel/timeout run; assert the
   ledger returns to zero and every sample satisfies
   in-flight(+reserved) <= quota at all three scopes.
4. **Distinguishable responses** — hard reject vs queue timeout return
   different merr codes, both carrying scope, class, queue depth/retry
   hint or position.
5. **Same-source observability + master switch** — snapshot equals
   internal counters; with the switch off, requests pass through and no
   counter moves.

## 11. Existing-scheduler walkthrough and the new execution point

This section records what the code actually does today (file/function
references) and exactly where the ledger wraps each mechanism without
replacing it. All three wrap points are additive and gated by
`quotaAdmission.enabled` (default false).

### 11.1 Proxy entry: rate limiting and the DQL task queue

- gRPC rate limiting is `RateLimitInterceptorWithMetaCache`
  (`internal/proxy/rate_limit_interceptor.go`), which calls
  `types.Limiter.Check` (`internal/types/types.go`); `SimpleLimiter`
  (`internal/proxy/simple_rate_limiter.go`) walks the
  cluster→db→collection→partition token-bucket tree
  (`internal/util/ratelimitutil/rate_limiter_tree.go`) and rolls back
  partially consumed tokens on failure. This is *rate* (tokens/sec)
  limiting and is unchanged.
- The DQL task queue is `DqTaskQueue`
  (`internal/proxy/scheduler/task_scheduler.go`); its hard cap
  `proxy.maxTaskNum` (default 1024) rejects with
  `merr.WrapErrTooManyRequests` (code 4). DQL has no time-based TTL
  sweeper; items leave via ctx cancellation (`SetOnContextDone` →
  `removeUnissuedTask`), admin `ClearDQLQueue`, or normal dequeue.
- **Wrap point.** Decorate `types.Limiter` around `SimpleLimiter` at its
  construction in `internal/proxy/proxy.go` (the interceptor and the DML
  `Alloc` path both flow through that one interface, so a decorator needs
  no call-site changes). On a DQL Search/Query the decorator calls
  `AdmissionGate.Acquire(ctx, ScopeKey{db,coll}, ClassSearch)` before
  delegating to the inner limiter; the acquired lease is attached to the
  proxy task and released in its terminal path. The per-user pending cap,
  `maxTaskNum` and ctx-cancel cleanup remain the inner mechanisms. With
  the switch off the decorator returns the no-op lease, so behavior is
  identical to today.

### 11.2 QueryNode read scheduler

- The scheduler is the shared package
  `internal/util/searchutil/scheduler` (used by `querynodev2`). Entry is
  `Scheduler.Add` (`concurrent_safe_scheduler.go`); the global cap
  `queryNode.scheduler.unsolvedQueueSize` (default 1024) rejects with code
  4. Merge (`mergeTaskQueue`), deadline advance (`taskDeadlineAdvance`,
  default 50 ms), expiry (`cleanupReady`/`cleanupExpiredTasks`), and the
  default fifo vs optional user-task-polling policy all live here.
- **Wrap point.** Decorate the `Scheduler` interface at its construction
  in `querynodev2` `server.go`; the wrapper's `Add` calls
  `AdmissionGate.Acquire(..., ClassSearch)` and holds the lease until the
  task's `Done`. Merge, deadline progression and expiry are untouched
  because they operate on tasks that already hold a lease. This is a
  node-local ledger instance, complementary to the proxy one.

### 11.3 DataCoord index build scheduling

- Build tasks flow `buildIndexCh` → `IndexInspector`
  (`internal/datacoord/index/inspector.go`, `createIndexForSegment`) →
  the shared `globalTaskScheduler.Enqueue`
  (`internal/datacoord/task/global_scheduler.go`). The 100 ms
  `schedule()` loop queries DataNode slots, prices the task
  (`internal/datacoord/taskresource/resource.go`), and only then runs
  node selection (`task/node_picker.go`) which hard-filters on node
  memory. There is no index-specific pending cap today.
- Memory is reserved only on **admission**: the DataNode queue
  (`internal/datanode/index/scheduler.go`) counts only `admitted` tasks
  toward execution memory; queued tasks hold no execution reservation.
- **Wrap point.** In `globalTaskScheduler.schedule()`, immediately
  before `nodePicker.PickBounded`: `AdmissionGate.Acquire(...,
  ClassBuild)`. On failure the task is pushed back to `pendingTasks`,
  reusing the existing make-way path; on success the lease is released in
  the task's terminal handling (`check()`/`AbortAndRemoveTask`), which
  already centralizes failure/cancel cleanup — so requirement 三.3
  (release before reporting failure) is satisfied by construction.
  Because admission precedes memory pricing/picking, queued builds hold
  neither ledger tokens nor memory (requirement 一.5), and the existing
  concurrency cap, memory admission and node selection run after the gate
  and stay authoritative for their own resources.

### 11.4 Why the gate is a separate package

The ledger (`internal/util/quota/admission`) imports only stdlib and
`pkg/v3` (`merr`, `mlog`), so it has no dependency on proxy, querynode or
datacoord and can be embedded by all three without an import cycle. The
paramtable projection and lifecycle wrapper live in
`internal/util/quota/admission_gate.go`, which is the only file that
knows about config keys; the core ledger stays unit-testable with a fake
clock and no global state.

## 12. Validation and delivery boundary

This increment delivers, fully unit-tested behind the default-off switch:

1. The ledger package (`internal/util/quota/admission`): one-lock
   accounting, three-scope hierarchy, bounded FIFO queues, smooth
   weighted fair rotation, class reserves with bounded reclaim,
   deadline/cancel handling, typed distinguishable rejections, and
   same-source snapshots.
2. Configuration: additive `quotaAdmission.*` paramtable items plus the
   `milvus.yaml` block, all defaulting to preserve current behavior.
3. `AdmissionGate`: paramtable projection, hot-reload callbacks, and the
   lifecycle (start/close) each component will embed.

The three component integrations in section 11 are deliberately separate,
individually reviewable follow-up changes. Each must add its own
regression proving that, with the switch off, the existing error codes,
read scheduling/expiry and build concurrency/memory admission are
byte-for-byte unchanged. Wiring is not claimed as done in this increment
per the G3 rule (no end-to-end benefit asserted until a failure mode is
traced through the live component).

