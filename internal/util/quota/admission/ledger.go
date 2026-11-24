// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package admission

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// idlePark is the timer duration used when no waiter exists. The loop is
// additionally woken by an event channel, so the long duration costs nothing.
const idlePark = time.Hour

// scopeNode holds the single source of truth for one scope: spec, in-flight
// tokens, queued counts (needed for reserve headroom) and reject counters.
type scopeNode struct {
	dbID     int64 // 0 except on collection nodes, which remember their database
	spec     QuotaSpec
	inFlight [classCount]int64
	queued   [classCount]int64
	rejected map[RejectReason][classCount]int64
}

func newScopeNode(spec QuotaSpec) *scopeNode {
	return &scopeNode{
		spec:     spec,
		rejected: make(map[RejectReason][classCount]int64),
	}
}

func (n *scopeNode) totalInFlight() int64 { return n.inFlight[ClassSearch] + n.inFlight[ClassBuild] }

// Ledger is the unique admission execution point. Every acquire/release,
// every queue mutation and every snapshot goes through Ledger.mu, so the
// in-flight plus reserved total can never exceed the configured quota.
type Ledger struct {
	clock Clock

	// enabled is read on the Acquire fast path without taking mu. Writes
	// happen only under mu (config updates and Close).
	enabled atomic.Bool

	mu     sync.Mutex
	cfg    Config
	closed atomic.Bool

	cluster *scopeNode
	dbs     map[int64]*scopeNode
	colls   map[int64]*scopeNode

	// queues and the rotation ring are keyed by the full ScopeKey so
	// database-only requests never collide with collection requests. The
	// ring is append-only at runtime (members stay even when empty), which
	// keeps smooth-WRR accumulators stable across drain/refill cycles.
	queues  map[ScopeKey]*collectionQueue
	rrOrder []ScopeKey

	// lastWinner and streak bound how many consecutive passes the same
	// collection may win. The bound is weight*MaxConsecutiveServes, so with
	// the default multiplier 1 a collection can run at most its own declared
	// weight back to back before another non-empty queue gets a turn.
	lastWinner ScopeKey
	streak     int

	waitSeq uint64

	// waitEwma is the exponentially weighted recent queue wait per
	// collection, used to derive the hard-reject retry hint.
	waitEwma map[ScopeKey]time.Duration

	wake      chan struct{}
	done      chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// NewLedger builds a ledger with the given clock and initial config. Pass
// RealClock{} in production and a FakeClock in tests. The scheduler
// goroutine starts only when the config is enabled.
func NewLedger(clock Clock, cfg Config) *Ledger {
	if clock == nil {
		clock = RealClock{}
	}
	l := &Ledger{
		clock:    clock,
		dbs:      make(map[int64]*scopeNode),
		colls:    make(map[int64]*scopeNode),
		queues:   make(map[ScopeKey]*collectionQueue),
		waitEwma: make(map[ScopeKey]time.Duration),
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	l.cluster = newScopeNode(QuotaSpec{})
	l.applyConfigLocked(cfg)
	return l
}

// noopLease is returned on the disabled fast path. It does no accounting.
type noopLease struct {
	key   ScopeKey
	class TaskClass
}

func (n noopLease) Release()         {}
func (n noopLease) Scope() ScopeKey  { return n.key }
func (n noopLease) Class() TaskClass { return n.class }

// realLease is the only token handle. Release is idempotent via an atomic
// CAS, so a defer plus an explicit error-path release can never double-
// return tokens.
type realLease struct {
	ledger   *Ledger
	key      ScopeKey
	class    TaskClass
	released atomic.Bool
}

func (l *realLease) Release() {
	if !l.released.CompareAndSwap(false, true) {
		return
	}
	l.ledger.release(l.key, l.class)
}

func (l *realLease) Scope() ScopeKey  { return l.key }
func (l *realLease) Class() TaskClass { return l.class }

// Acquire returns a lease when the task is admitted, or a typed merr error
// when it is hard-rejected, times out in the queue, is canceled while
// queued, or the ledger is closed. When the master switch is off it is a
// lock-free pass-through that never queues or counts.
func (l *Ledger) Acquire(ctx context.Context, key ScopeKey, class TaskClass) (Lease, error) {
	// Closed takes precedence over the disabled fast path: a shutting-down
	// ledger must fail new work instead of handing out no-op leases.
	if l.closed.Load() {
		l.mu.Lock()
		err := l.rejectLocked(key, class, ReasonClosed, 0, 0, 0)
		l.mu.Unlock()
		return nil, err.ToMerr()
	}
	if !l.enabled.Load() {
		return noopLease{key: key, class: class}, nil
	}

	l.mu.Lock()

	// Fast path: no waiting work for this collection and capacity exists at
	// all three scopes. The lock is uncontended while nothing is queued.
	q := l.queues[key]
	if q == nil || q.len() == 0 {
		if l.canAdmitLocked(key, class) {
			l.admitLocked(key, class)
			l.mu.Unlock()
			return &realLease{ledger: l, key: key, class: class}, nil
		}
	}

	// Bounded queue: a full queue hard-rejects instead of waiting.
	if q != nil && q.len() >= l.cfg.MaxQueueLength {
		retryAfter := l.suggestRetryAfterLocked(key)
		depth := q.len()
		err := l.rejectLocked(key, class, ReasonQueueFull, depth, 0, retryAfter)
		l.mu.Unlock()
		return nil, err.ToMerr()
	}

	now := l.clock.Now()
	w := newWaiter(l.nextSeq(), key, class, now, l.cfg.MaxQueueWait, ctx)
	l.enqueueLocked(w)
	l.signal()
	l.mu.Unlock()

	// Prompt cancellation: the watcher removes the item and drops the
	// queued counters synchronously when the client goes away.
	go l.watchCancel(w)

	select {
	case <-w.ready:
	case <-ctx.Done():
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if w.state == waiterAdmitted {
		return &realLease{ledger: l, key: key, class: class}, nil
	}
	if w.state == waiterQueued {
		// The select returned on ctx.Done before the watcher ran; settle the
		// item here on the same code path.
		l.removeWaiterLocked(w)
		err := l.rejectWaiterLocked(w, ReasonCanceled)
		return nil, err.ToMerr()
	}
	return nil, w.reject.ToMerr()
}

// release returns the tokens at all three scopes and wakes the scheduler so
// reserved shares can be reclaimed within one scheduling round.
func (l *Ledger) release(key ScopeKey, class TaskClass) {
	l.mu.Lock()
	l.cluster.inFlight[class]--
	if n := l.dbs[key.DatabaseID]; n != nil {
		n.inFlight[class]--
	}
	if n := l.colls[key.CollectionID]; n != nil {
		n.inFlight[class]--
	}
	l.signal()
	l.mu.Unlock()
}

// watchCancel resolves a queued item as soon as its caller context ends. It
// exits without action once the scheduler has settled the item.
func (l *Ledger) watchCancel(w *waiter) {
	select {
	case <-w.ctx.Done():
	case <-w.ready:
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if w.state != waiterQueued {
		return
	}
	l.removeWaiterLocked(w)
	err := l.rejectWaiterLocked(w, ReasonCanceled)
	mlog.Warn(context.TODO(), "admission waiter canceled while queued",
		mlog.Int64("collectionID", w.key.CollectionID),
		mlog.String("class", w.class.String()),
		mlog.Err(err.ToMerr()))
}

// canAdmitLocked evaluates capacity at all three scopes. The tightest scope
// wins. Besides the total cap, a surplus (borrowing) task must leave
// headroom for the other class's reserve while other-class work is queued;
// reserve-eligible tasks are never throttled by that headroom rule.
func (l *Ledger) canAdmitLocked(key ScopeKey, class TaskClass) bool {
	other := ClassSearch
	if class == ClassSearch {
		other = ClassBuild
	}
	nodes := [3]*scopeNode{
		l.cluster,
		l.dbs[key.DatabaseID],
		l.colls[key.CollectionID],
	}
	for _, n := range nodes {
		if n == nil || n.spec.Total <= 0 {
			continue // unlimited at this scope
		}
		if n.totalInFlight() >= n.spec.Total {
			return false
		}
		reserve := n.spec.BuildReserve
		if class == ClassSearch {
			reserve = n.spec.SearchReserve
		}
		// Reserve-eligible tasks only need a free slot.
		if n.inFlight[class] < reserve {
			continue
		}
		// Surplus tasks keep headroom for a starved, queued other class.
		if n.queued[other] > 0 {
			otherReserve := n.spec.SearchReserve
			if other == ClassBuild {
				otherReserve = n.spec.BuildReserve
			}
			if starved := otherReserve - n.inFlight[other]; starved > 0 {
				if n.totalInFlight()+starved >= n.spec.Total {
					return false
				}
			}
		}
	}
	return true
}

// admitLocked increments in-flight at the three scopes. Caller must have
// checked canAdmitLocked.
func (l *Ledger) admitLocked(key ScopeKey, class TaskClass) {
	l.cluster.inFlight[class]++
	if n := l.dbNodeLocked(key.DatabaseID); n != nil {
		n.inFlight[class]++
	}
	if n := l.collNodeLocked(key); n != nil {
		n.inFlight[class]++
	}
}

func (l *Ledger) dbNodeLocked(dbID int64) *scopeNode {
	if dbID == 0 {
		return nil
	}
	n, ok := l.dbs[dbID]
	if !ok {
		n = newScopeNode(l.cfg.Databases[dbID])
		l.dbs[dbID] = n
	}
	return n
}

func (l *Ledger) collNodeLocked(key ScopeKey) *scopeNode {
	if key.CollectionID == 0 {
		return nil
	}
	n, ok := l.colls[key.CollectionID]
	if !ok {
		n = newScopeNode(l.cfg.Collections[key.CollectionID])
		n.dbID = key.DatabaseID
		l.colls[key.CollectionID] = n
	} else if n.dbID == 0 && key.DatabaseID != 0 {
		// Nodes pre-created from a config declaration do not know their
		// database until the first request arrives; backfill it then.
		n.dbID = key.DatabaseID
	}
	return n
}

// enqueueLocked attaches the waiter to its collection queue and updates the
// queued counters at all three scopes.
func (l *Ledger) enqueueLocked(w *waiter) {
	q, ok := l.queues[w.key]
	if !ok {
		q = &collectionQueue{key: w.key}
		l.queues[w.key] = q
	}
	if !q.inRing {
		q.inRing = true
		l.rrOrder = append(l.rrOrder, w.key)
	}
	q.push(w)

	l.cluster.queued[w.class]++
	if n := l.dbNodeLocked(w.key.DatabaseID); n != nil {
		n.queued[w.class]++
	}
	if n := l.collNodeLocked(w.key); n != nil {
		n.queued[w.class]++
	}
}

// removeWaiterLocked deletes the waiter from its queue and drops the queued
// counters. It is the only removal path for timeout/cancel/close, so queued
// accounting can never drift.
func (l *Ledger) removeWaiterLocked(w *waiter) {
	q := l.queues[w.key]
	if q == nil {
		return
	}
	for i, item := range q.items {
		if item == w {
			q.remove(i)
			break
		}
	}
	if q.len() == 0 {
		q.currentWeight = 0
		// Ring membership is permanent; an empty member is skipped on passes.
	}
	l.cluster.queued[w.class]--
	if n := l.dbs[w.key.DatabaseID]; n != nil {
		n.queued[w.class]--
	}
	if n := l.colls[w.key.CollectionID]; n != nil {
		n.queued[w.class]--
	}
}

// rejectLocked records a rejection that happens without an enqueued waiter
// (queue full, closed-before-enqueue) at all three scopes.
func (l *Ledger) rejectLocked(key ScopeKey, class TaskClass, reason RejectReason,
	depth, position int, retryAfter time.Duration) *RejectError {
	e := &RejectError{
		Reason:        reason,
		Scope:         ScopeCollection,
		Key:           key,
		Class:         class,
		QueueDepth:    depth,
		QueuePosition: position,
		RetryAfter:    retryAfter,
	}
	counts := func(n *scopeNode) {
		if n == nil {
			return
		}
		c := n.rejected[reason]
		c[class]++
		n.rejected[reason] = c
	}
	counts(l.cluster)
	counts(l.dbs[key.DatabaseID])
	counts(l.colls[key.CollectionID])
	return e
}

// rejectWaiterLocked records a rejection of an item that was queued.
func (l *Ledger) rejectWaiterLocked(w *waiter, reason RejectReason) *RejectError {
	e := l.rejectLocked(w.key, w.class, reason, 0, w.queuePos, 0)
	e.Waited = l.clock.Now().Sub(w.enqueued)
	w.finish(waiterRejected, e)
	close(w.ready)
	return e
}

func (l *Ledger) suggestRetryAfterLocked(key ScopeKey) time.Duration {
	hint := l.waitEwma[key]
	if hint <= 0 {
		hint = l.cfg.MaxQueueWait / 2
	}
	if hint < l.cfg.MinRetryAfter {
		hint = l.cfg.MinRetryAfter
	}
	if hint > l.cfg.MaxRetryAfter {
		hint = l.cfg.MaxRetryAfter
	}
	return hint
}

func (l *Ledger) nextSeq() uint64 {
	l.waitSeq++
	return l.waitSeq
}

// signal wakes the dispatch loop. The channel is buffered and sends are
// non-blocking, so coalesced events never block the holder of mu.
func (l *Ledger) signal() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// UpdateConfig replaces the configuration atomically. In-flight leases are
// untouched: no task is re-admitted or double-refunded, and new requests
// observe the new quota immediately. A shrink that leaves in-flight above
// the new total simply blocks further admission until releases drain it.
func (l *Ledger) UpdateConfig(cfg Config) error {
	if err := validateConfig(cfg); err != nil {
		return err
	}
	l.mu.Lock()
	l.applyConfigLocked(cfg)
	l.signal()
	l.mu.Unlock()
	return nil
}

func (l *Ledger) applyConfigLocked(cfg Config) {
	wasEnabled := l.enabled.Load()
	cfg = normalizeConfig(cfg)
	l.cfg = cfg
	l.enabled.Store(cfg.Enabled)
	// Turning the switch off at runtime drains the queues explicitly: every
	// queued item gets a typed failure instead of waiting forever behind a
	// disabled gate. Re-enabling starts with empty, reconciled counters.
	if wasEnabled && !cfg.Enabled {
		l.failAllQueuedLocked(ReasonClosed)
	}
	l.cluster.spec = cfg.Cluster
	for id, spec := range cfg.Databases {
		n, ok := l.dbs[id]
		if !ok {
			l.dbs[id] = newScopeNode(spec)
			continue
		}
		n.spec = spec
	}
	for id, spec := range cfg.Collections {
		n, ok := l.colls[id]
		if !ok {
			l.colls[id] = newScopeNode(spec)
			continue
		}
		n.spec = spec
	}
	l.startLoopLocked()
}

// Close fails every queued item, stops accepting work and parks the dispatch
// loop. In-flight leases still release normally; after they all release the
// snapshot reads all zeros.
func (l *Ledger) Close() {
	l.closeOnce.Do(func() { close(l.done) })

	l.mu.Lock()
	l.closed.Store(true)
	l.enabled.Store(false)
	l.failAllQueuedLocked(ReasonClosed)
	l.signal()
	l.mu.Unlock()
	l.wg.Wait()
}

// failAllQueuedLocked drains every collection queue with a typed rejection,
// dropping the queued counters at all three scopes synchronously.
func (l *Ledger) failAllQueuedLocked(reason RejectReason) {
	now := l.clock.Now()
	for key, q := range l.queues {
		for q.len() > 0 {
			w := q.pop()
			l.cluster.queued[w.class]--
			if n := l.dbs[w.key.DatabaseID]; n != nil {
				n.queued[w.class]--
			}
			if n := l.colls[w.key.CollectionID]; n != nil {
				n.queued[w.class]--
			}
			e := l.rejectLocked(w.key, w.class, reason, 0, w.queuePos, 0)
			e.Waited = now.Sub(w.enqueued)
			w.finish(waiterRejected, e)
			close(w.ready)
		}
		delete(l.queues, key)
	}
	l.rrOrder = l.rrOrder[:0]
	l.lastWinner = ScopeKey{}
	l.streak = 0
}

// validateConfig enforces the declaration invariants. The zero config is
// valid and means the ledger stays disabled.
func validateConfig(cfg Config) error {
	check := func(where string, s QuotaSpec) error {
		if s.Total < 0 || s.SearchReserve < 0 || s.BuildReserve < 0 {
			return merr.WrapErrParameterInvalidMsg("admission quota at %s must be non-negative", where)
		}
		if s.Total > 0 && s.SearchReserve+s.BuildReserve > s.Total {
			return merr.WrapErrParameterInvalidMsg(
				"admission reserves exceed total at %s: search=%d build=%d total=%d",
				where, s.SearchReserve, s.BuildReserve, s.Total)
		}
		return nil
	}
	if err := check("cluster", cfg.Cluster); err != nil {
		return err
	}
	for id, s := range cfg.Databases {
		if err := check("database", s); err != nil {
			return merr.Wrapf(err, "databaseID=%d", id)
		}
	}
	for id, s := range cfg.Collections {
		if err := check("collection", s); err != nil {
			return merr.Wrapf(err, "collectionID=%d", id)
		}
	}
	return nil
}

func normalizeConfig(cfg Config) Config {
	if cfg.MaxQueueLength <= 0 {
		cfg.MaxQueueLength = 1024
	}
	if cfg.MaxQueueWait <= 0 {
		cfg.MaxQueueWait = 30 * time.Second
	}
	if cfg.MaxConsecutiveServes <= 0 {
		cfg.MaxConsecutiveServes = 1
	}
	if cfg.MinRetryAfter <= 0 {
		cfg.MinRetryAfter = 10 * time.Millisecond
	}
	if cfg.MaxRetryAfter <= 0 {
		cfg.MaxRetryAfter = 5 * time.Second
	}
	if cfg.MaxRetryAfter < cfg.MinRetryAfter {
		cfg.MaxRetryAfter = cfg.MinRetryAfter
	}
	return cfg
}
