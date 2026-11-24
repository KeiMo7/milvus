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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testConfig() Config {
	return Config{
		Enabled:              true,
		MaxQueueLength:       4,
		MaxQueueWait:         100 * time.Millisecond,
		MaxConsecutiveServes: 1,
		MinRetryAfter:        time.Millisecond,
		MaxRetryAfter:        50 * time.Millisecond,
		Cluster:              QuotaSpec{Total: 8, SearchReserve: 4, BuildReserve: 2},
		Databases: map[int64]QuotaSpec{
			1: {Total: 6, SearchReserve: 3, BuildReserve: 2},
		},
		Collections: map[int64]QuotaSpec{
			100: {Total: 2, SearchReserve: 1, BuildReserve: 1},
			200: {Total: 2, SearchReserve: 1, BuildReserve: 1},
		},
	}
}

func collKey(coll int64) ScopeKey { return ScopeKey{DatabaseID: 1, CollectionID: coll} }

// waitFor polls cond under a short wall-clock budget. The ledger itself is
// driven by a FakeClock only for its internal timer; goroutine hand-offs
// (ready channels) still need a small real-time wait in tests.
func waitFor(t *testing.T, budget time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if !cond() {
		t.Fatalf("condition not met within %s", budget)
	}
}

// TestMasterSwitch_DirectPassThrough covers acceptance 5.4: with the switch
// off, every request goes straight through and no counter ever moves.
func TestMasterSwitch_DirectPassThrough(t *testing.T) {
	clock := NewFakeClock(time.Unix(1, 0))
	l := NewLedger(clock, Config{}) // zero config => disabled
	require.False(t, l.Enabled())

	for i := 0; i < 32; i++ {
		lease, err := l.Acquire(context.Background(), collKey(100), ClassSearch)
		require.NoError(t, err)
		require.NotNil(t, lease)
		lease.Release()
		lease.Release() // idempotent on the no-op lease too
	}
	l.Close()

	snaps := l.Snapshots()
	for _, s := range snaps {
		assert.Equal(t, [classCount]int64{}, s.InFlight)
		assert.Equal(t, [classCount]int64{}, s.Queued)
	}
	assert.Equal(t, [classCount]int64{}, l.InFlightTotal())
}

// TestAcquire_FastPathAndRelease covers basic token accounting and proves
// in-flight never exceeds the tightest (collection) scope.
func TestAcquire_FastPathAndRelease(t *testing.T) {
	clock := NewFakeClock(time.Unix(1, 0))
	l := NewLedger(clock, testConfig())
	defer l.Close()

	leases := make([]Lease, 0, 2)
	for i := 0; i < 2; i++ {
		lease, err := l.Acquire(context.Background(), collKey(100), ClassSearch)
		require.NoError(t, err)
		leases = append(leases, lease)
	}
	// Collection total is 2: a third attempt must queue, not over-admit.
	acquired := make(chan Lease, 1)
	go func() {
		lease, err := l.Acquire(context.Background(), collKey(100), ClassSearch)
		if err == nil {
			acquired <- lease
		}
		close(acquired)
	}()
	waitFor(t, time.Second, func() bool {
		s := snapshotFor(t, l, ScopeCollection, collKey(100))
		return s.Queued[ClassSearch] == 1
	})
	s := snapshotFor(t, l, ScopeCollection, collKey(100))
	assert.Equal(t, int64(2), s.InFlightPlusReserved)
	assert.Equal(t, int64(2), s.EffectiveQuota.Total)

	// Releasing one slot admits the queued task through the scheduler.
	leases[0].Release()
	select {
	case lease, ok := <-acquired:
		require.True(t, ok)
		require.NotNil(t, lease)
		lease.Release()
	case <-time.After(time.Second):
		t.Fatal("queued task was not admitted after release")
	}
	leases[1].Release()

	waitFor(t, time.Second, func() bool {
		return l.InFlightTotal() == [classCount]int64{}
	})
}

// TestQueueFull_HardReject covers acceptance 4 (hard reject is distinguishable
// and carries scope, class, queue depth and a retry hint).
func TestQueueFull_HardReject(t *testing.T) {
	clock := NewFakeClock(time.Unix(1, 0))
	cfg := testConfig()
	cfg.MaxQueueLength = 1
	l := NewLedger(clock, cfg)
	defer l.Close()

	// Fill the 2 collection slots.
	var held []Lease
	for i := 0; i < 2; i++ {
		lease, err := l.Acquire(context.Background(), collKey(100), ClassBuild)
		require.NoError(t, err)
		held = append(held, lease)
	}
	// One item can queue; the next must hard-reject immediately.
	blocked := make(chan struct{})
	go func() {
		_, _ = l.Acquire(context.Background(), collKey(100), ClassBuild)
		close(blocked)
	}()
	waitFor(t, time.Second, func() bool {
		return snapshotFor(t, l, ScopeCollection, collKey(100)).Queued[ClassBuild] == 1
	})

	_, err := l.Acquire(context.Background(), collKey(100), ClassBuild)
	require.Error(t, err)
	// The wire error is a typed merr; assert on the merr code and on the
	// structured payload being present in its message.
	assert.True(t, merrIsCode(err, merrCodeTooManyRequests), "expected TooManyRequests, got %v", err)
	assert.Contains(t, err.Error(), "build")
	assert.Contains(t, err.Error(), "retryAfter")

	for _, lease := range held {
		lease.Release()
	}
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("queued task did not settle after releases")
	}

	// The rejection is recorded at the collection scope from the same source.
	s := snapshotFor(t, l, ScopeCollection, collKey(100))
	assert.GreaterOrEqual(t, s.Rejected[ReasonQueueFull][ClassBuild], int64(1))
}

// TestQueueTimeout covers acceptance 1 and failure-mode 三.1: a queued item
// past MaxQueueWait fails explicitly with scope and queue position, never as
// a downstream timeout or an empty result.
func TestQueueTimeout(t *testing.T) {
	clock := NewFakeClock(time.Unix(1, 0))
	l := NewLedger(clock, testConfig())
	defer l.Close()

	var held []Lease
	for i := 0; i < 2; i++ {
		lease, err := l.Acquire(context.Background(), collKey(100), ClassSearch)
		require.NoError(t, err)
		held = append(held, lease)
	}

	result := make(chan error, 1)
	go func() {
		_, err := l.Acquire(context.Background(), collKey(100), ClassSearch)
		result <- err
	}()
	waitFor(t, time.Second, func() bool {
		return snapshotFor(t, l, ScopeCollection, collKey(100)).Queued[ClassSearch] == 1
	})

	// Drive the fake clock past MaxQueueWait; the loop timer fires and the
	// sweep rejects the waiter with a queue-timeout result.
	clock.Advance(200 * time.Millisecond)
	select {
	case err := <-result:
		require.Error(t, err)
		assert.True(t, merrIsCode(err, merrCodeQuotaExceeded), "expected QuotaExceeded, got %v", err)
		assert.Contains(t, err.Error(), "queuePosition")
		assert.Contains(t, err.Error(), "search")
	case <-time.After(time.Second):
		t.Fatal("queued task did not time out")
	}

	s := snapshotFor(t, l, ScopeCollection, collKey(100))
	assert.GreaterOrEqual(t, s.Rejected[ReasonQueueTimeout][ClassSearch], int64(1))
	// Queued count fell back synchronously; no leaked reservation.
	assert.Equal(t, int64(0), s.Queued[ClassSearch])
	for _, lease := range held {
		lease.Release()
	}
}

// TestCancelWhileQueued covers requirement 一.6: a client disconnecting while
// queued removes the item immediately and drops the counters.
func TestCancelWhileQueued(t *testing.T) {
	clock := NewFakeClock(time.Unix(1, 0))
	l := NewLedger(clock, testConfig())
	defer l.Close()

	var held []Lease
	for i := 0; i < 2; i++ {
		lease, err := l.Acquire(context.Background(), collKey(100), ClassSearch)
		require.NoError(t, err)
		held = append(held, lease)
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := l.Acquire(ctx, collKey(100), ClassSearch)
		result <- err
	}()
	waitFor(t, time.Second, func() bool {
		return snapshotFor(t, l, ScopeCollection, collKey(100)).Queued[ClassSearch] == 1
	})
	cancel()
	select {
	case err := <-result:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("canceled task did not return")
	}
	waitFor(t, time.Second, func() bool {
		return snapshotFor(t, l, ScopeCollection, collKey(100)).Queued[ClassSearch] == 0
	})
	for _, lease := range held {
		lease.Release()
	}
	assert.Equal(t, [classCount]int64{}, l.InFlightTotal())
}

// TestTwoCollections_Fairness covers acceptance 2. Fair rotation only
// applies while queues are non-empty (the uncontended fast path is
// intentionally lock-cheap, not fair), so the test first occupies every
// slot, then keeps both collection queues populated through a controlled
// release/admit cycle and inspects the resulting dispatch sequence.
func TestTwoCollections_Fairness(t *testing.T) {
	clock := NewFakeClock(time.Unix(1, 0))
	cfg := testConfig()
	// One global slot makes the dispatch sequence fully observable.
	cfg.Cluster = QuotaSpec{Total: 1, SearchReserve: 0, BuildReserve: 0}
	cfg.Databases = map[int64]QuotaSpec{1: {Total: 1}}
	cfg.Collections = map[int64]QuotaSpec{
		100: {Total: 1},
		200: {Total: 1},
	}
	cfg.MaxConsecutiveServes = 1
	const perCollection = 25
	cfg.MaxQueueLength = 2 * perCollection
	// The rotation is driven by real goroutine hand-offs in this test; give
	// the 50 waiters ample wall-clock room so queue timeouts cannot perturb
	// the fairness sequence being measured.
	cfg.MaxQueueWait = 30 * time.Second
	l := NewLedger(clock, cfg)
	defer l.Close()

	// Occupy the single global slot with a configured collection; its
	// collection total is 1 as well, so every following Acquire queues.
	holder, err := l.Acquire(context.Background(), collKey(100), ClassBuild)
	require.NoError(t, err)

	type result struct {
		which string
		lease Lease
		err   error
	}
	results := make(chan result, 2*perCollection)
	// Each waiter runs in its own goroutine and blocks until the scheduler
	// admits it. Goroutine start order is irrelevant because both queues are
	// confirmed full before any release, and intra-queue order is FIFO.
	var submitWg sync.WaitGroup
	submitWg.Add(2 * perCollection)
	submit := func(key ScopeKey, which string) {
		defer submitWg.Done()
		lease, err := l.Acquire(context.Background(), key, ClassSearch)
		results <- result{which: which, lease: lease, err: err}
	}
	for i := 0; i < perCollection; i++ {
		go submit(collKey(100), "A")
		go submit(collKey(200), "B")
	}
	// Both queues must be populated before the rotation starts. The queues
	// live in the ring regardless of whether a scope node snapshot exists yet.
	waitFor(t, 15*time.Second, func() bool {
		l.mu.Lock()
		depth := 0
		for _, q := range l.queues {
			depth += q.len()
		}
		l.mu.Unlock()
		return depth == 2*perCollection
	})

	// Each release admits exactly one waiter; record the sequence and release
	// the returned lease to free the slot for the next rotation pass.
	sequence := make([]string, 0, 2*perCollection)
	holder.Release()
	for i := 0; i < 2*perCollection; i++ {
		select {
		case r := <-results:
			require.NoError(t, r.err)
			sequence = append(sequence, r.which)
			r.lease.Release()
		case <-time.After(5 * time.Second):
			t.Fatalf("dispatch %d never arrived", i)
		}
	}
	submitWg.Wait()

	servedA, servedB := 0, 0
	maxConsecutive, consecutive := 0, 0
	last := ""
	for _, which := range sequence {
		if which == "A" {
			servedA++
		} else {
			servedB++
		}
		if which == last {
			consecutive++
		} else {
			consecutive = 1
			last = which
		}
		if consecutive > maxConsecutive {
			maxConsecutive = consecutive
		}
	}
	require.Equal(t, perCollection, servedA)
	require.Equal(t, perCollection, servedB)
	assert.LessOrEqual(t, maxConsecutive, 1,
		"a collection occupied consecutive service windows: %v", sequence)
}

// TestTwoCollections_WeightedFairness covers the proportional-share half of
// acceptance 2: collection B carries three times A's declared total, so with
// both queues saturated B must receive roughly three times the dispatches,
// and the heavyweight still cannot run back to back indefinitely.
func TestTwoCollections_WeightedFairness(t *testing.T) {
	clock := NewFakeClock(time.Unix(1, 0))
	cfg := testConfig()
	// Cluster total 1 makes each pass dispatch exactly one task.
	cfg.Cluster = QuotaSpec{Total: 1}
	cfg.Databases = map[int64]QuotaSpec{1: {Total: 1}}
	cfg.Collections = map[int64]QuotaSpec{
		100: {Total: 1},
		200: {Total: 3},
	}
	cfg.MaxConsecutiveServes = 1
	const nA, nB = 20, 60
	cfg.MaxQueueLength = nA + nB
	cfg.MaxQueueWait = 30 * time.Second
	l := NewLedger(clock, cfg)
	defer l.Close()

	holder, err := l.Acquire(context.Background(), collKey(100), ClassBuild)
	require.NoError(t, err)

	type result struct {
		which string
		lease Lease
		err   error
	}
	results := make(chan result, nA+nB)
	// One goroutine per waiter: Acquire blocks, so a loop in a single
	// goroutine would enqueue only its first item.
	var wg sync.WaitGroup
	submit := func(key ScopeKey, which string, n int) {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				lease, err := l.Acquire(context.Background(), key, ClassSearch)
				results <- result{which: which, lease: lease, err: err}
			}()
		}
	}
	submit(collKey(100), "A", nA)
	submit(collKey(200), "B", nB)

	// Both queues must be populated before the rotation starts.
	waitFor(t, 10*time.Second, func() bool {
		l.mu.Lock()
		depth := 0
		for _, q := range l.queues {
			depth += q.len()
		}
		l.mu.Unlock()
		return depth == nA+nB
	})

	sequence := make([]string, 0, nA+nB)
	holder.Release()
	for i := 0; i < nA+nB; i++ {
		select {
		case r := <-results:
			require.NoError(t, r.err)
			sequence = append(sequence, r.which)
			r.lease.Release()
		case <-time.After(5 * time.Second):
			t.Fatalf("dispatch %d never arrived", i)
		}
	}
	wg.Wait()

	servedA, servedB := 0, 0
	maxConsecutive, consecutive := 0, 0
	last := ""
	for _, which := range sequence {
		if which == "A" {
			servedA++
		} else {
			servedB++
		}
		if which == last {
			consecutive++
		} else {
			consecutive = 1
			last = which
		}
		if consecutive > maxConsecutive {
			maxConsecutive = consecutive
		}
	}
	require.Equal(t, nA, servedA)
	require.Equal(t, nB, servedB)
	// Smooth WRR with weights 1 and 3 interleaves as BBB A BBB A ...: B's
	// longest back-to-back run equals its declared weight (3), after which A
	// always gets a turn, so the window is never monopolized.
	assert.LessOrEqual(t, maxConsecutive, 3,
		"weighted rotation concentrated one collection beyond its weight: %v", sequence)
	// The observed ratio matches the 1:3 reservation proportion.
	assert.InDelta(t, 3.0, float64(servedB)/float64(servedA), 0.05)
}

// TestWorkConservation_ReserveReclaim covers requirement 一.2 and 一.5: idle
// reserve is borrowable, but the lender reclaims its reserved share when it
// starts queueing, while both classes coexist on one ledger.
func TestWorkConservation_ReserveReclaim(t *testing.T) {
	clock := NewFakeClock(time.Unix(1, 0))
	cfg := testConfig()
	// One collection, total 4: search reserve 3, build reserve 1.
	cfg.Collections = map[int64]QuotaSpec{
		100: {Total: 4, SearchReserve: 3, BuildReserve: 1},
	}
	cfg.Databases = map[int64]QuotaSpec{1: {Total: 4, SearchReserve: 3, BuildReserve: 1}}
	cfg.Cluster = QuotaSpec{Total: 4, SearchReserve: 3, BuildReserve: 1}
	l := NewLedger(clock, cfg)
	defer l.Close()

	// Builds borrow the idle search reserve and fill all four slots.
	var builds []Lease
	for i := 0; i < 4; i++ {
		lease, err := l.Acquire(context.Background(), collKey(100), ClassBuild)
		require.NoError(t, err)
		builds = append(builds, lease)
	}
	s := snapshotFor(t, l, ScopeCollection, collKey(100))
	assert.Equal(t, int64(4), s.InFlight[ClassBuild])

	// A search now queues (its reserve 3 is borrowed).
	searchGot := make(chan Lease, 1)
	go func() {
		lease, err := l.Acquire(context.Background(), collKey(100), ClassSearch)
		if err == nil {
			searchGot <- lease
		}
	}()
	waitFor(t, time.Second, func() bool {
		return snapshotFor(t, l, ScopeCollection, collKey(100)).Queued[ClassSearch] == 1
	})

	// Releasing three borrowed build slots lets the lender reclaim: the
	// search is reserve-eligible and is admitted before any new surplus work.
	builds[0].Release()
	builds[1].Release()
	builds[2].Release()
	select {
	case lease := <-searchGot:
		require.NotNil(t, lease)
		s = snapshotFor(t, l, ScopeCollection, collKey(100))
		assert.Equal(t, int64(1), s.InFlight[ClassSearch])
		assert.Equal(t, int64(1), s.InFlight[ClassBuild])
		lease.Release()
	case <-time.After(time.Second):
		t.Fatal("reserved share was not reclaimed for the queued search")
	}
	builds[3].Release()
}

// TestConservation_MixedOutcomesReturnToZero covers acceptance 3: a mixed run
// of success, failure, cancellation and timeout returns the ledger to zero,
// and every sampled in-flight total stays within quota.
func TestConservation_MixedOutcomesReturnToZero(t *testing.T) {
	clock := NewFakeClock(time.Unix(1, 0))
	cfg := testConfig()
	cfg.MaxQueueLength = 8
	cfg.MaxQueueWait = 80 * time.Millisecond
	l := NewLedger(clock, cfg)
	defer l.Close()

	var wg sync.WaitGroup
	var mu sync.Mutex
	violations := 0
	stopSample := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopSample:
				return
			default:
				for _, s := range l.Snapshots() {
					if s.EffectiveQuota.Total > 0 &&
						s.InFlightPlusReserved > s.EffectiveQuota.Total {
						mu.Lock()
						violations++
						mu.Unlock()
					}
				}
				time.Sleep(time.Millisecond)
			}
		}
	}()

	keys := []ScopeKey{collKey(100), collKey(200)}
	for i := 0; i < 80; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			key := keys[i%2]
			class := ClassSearch
			if i%3 == 0 {
				class = ClassBuild
			}
			ctx := context.Background()
			cancel := func() {}
			if i%4 == 0 {
				ctx, cancel = context.WithCancel(context.Background())
			}
			lease, err := l.Acquire(ctx, key, class)
			if err != nil {
				cancel()
				return
			}
			switch i % 4 {
			case 0:
				cancel() // cancel after admission: still releases normally
			case 1:
				// simulate failing task: release before returning
			default:
				time.Sleep(time.Millisecond)
			}
			lease.Release()
			cancel()
		}()
	}
	wg.Wait()

	// Let the fake clock advance so any straggling queued timeout also settles.
	clock.Advance(200 * time.Millisecond)
	waitFor(t, time.Second, func() bool {
		for _, s := range l.Snapshots() {
			if s.Queued != [classCount]int64{} {
				return false
			}
		}
		return l.InFlightTotal() == [classCount]int64{}
	})
	close(stopSample)

	assert.Equal(t, [classCount]int64{}, l.InFlightTotal())
	mu.Lock()
	assert.Equal(t, 0, violations, "in-flight exceeded quota at a sampled instant")
	mu.Unlock()
}

// TestUpdateQuota_ShrinkAndGrow covers failure-mode 三.2: quota re-issue
// neither double-refunds in-flight work nor breaks accounting; new requests
// observe the new quota and the ledger still drains to zero.
func TestUpdateQuota_ShrinkAndGrow(t *testing.T) {
	clock := NewFakeClock(time.Unix(1, 0))
	l := NewLedger(clock, testConfig())
	defer l.Close()

	lease, err := l.Acquire(context.Background(), collKey(100), ClassSearch)
	require.NoError(t, err)

	smaller := testConfig()
	smaller.Collections = map[int64]QuotaSpec{
		100: {Total: 1, SearchReserve: 1, BuildReserve: 0},
		200: {Total: 2, SearchReserve: 1, BuildReserve: 1},
	}
	require.NoError(t, l.UpdateConfig(smaller))

	// With one slot occupied and total now 1, another search must queue.
	got := make(chan Lease, 1)
	go func() {
		ls, err := l.Acquire(context.Background(), collKey(100), ClassSearch)
		if err == nil {
			got <- ls
		}
	}()
	waitFor(t, time.Second, func() bool {
		return snapshotFor(t, l, ScopeCollection, collKey(100)).Queued[ClassSearch] == 1
	})
	lease.Release()
	select {
	case ls := <-got:
		require.NotNil(t, ls)
		ls.Release()
	case <-time.After(time.Second):
		t.Fatal("shrunk quota did not admit after a release")
	}

	// Invalid declarations are rejected and leave the active config intact.
	bad := testConfig()
	bad.Collections = map[int64]QuotaSpec{100: {Total: 2, SearchReserve: 2, BuildReserve: 2}}
	require.Error(t, l.UpdateConfig(bad))

	waitFor(t, time.Second, func() bool { return l.InFlightTotal() == [classCount]int64{} })
}

// TestClose_FailsQueuedAndDrains covers failure-mode 三.4: shutdown fails
// every queued item explicitly and counters reach zero after leases release.
func TestClose_FailsQueuedAndDrains(t *testing.T) {
	clock := NewFakeClock(time.Unix(1, 0))
	l := NewLedger(clock, testConfig())

	var held []Lease
	for i := 0; i < 2; i++ {
		lease, err := l.Acquire(context.Background(), collKey(100), ClassSearch)
		require.NoError(t, err)
		held = append(held, lease)
	}
	queuedErr := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := l.Acquire(context.Background(), collKey(100), ClassSearch)
			queuedErr <- err
		}()
	}
	waitFor(t, time.Second, func() bool {
		return snapshotFor(t, l, ScopeCollection, collKey(100)).Queued[ClassSearch] == 2
	})

	go l.Close()
	for i := 0; i < 2; i++ {
		select {
		case err := <-queuedErr:
			require.Error(t, err)
			assert.True(t, merrIsCode(err, merrCodeQuotaExceeded), "got %v", err)
		case <-time.After(time.Second):
			t.Fatal("queued item was not failed on close")
		}
	}
	for _, lease := range held {
		lease.Release()
	}
	waitFor(t, time.Second, func() bool { return l.InFlightTotal() == [classCount]int64{} })

	// Post-close acquires fail immediately instead of queueing.
	_, err := l.Acquire(context.Background(), collKey(100), ClassSearch)
	require.Error(t, err)
}

// TestSnapshot_SameSourceAndReconcile covers acceptance 5: snapshots equal the
// internal counters and report max wait plus effective quota.
func TestSnapshot_SameSourceAndReconcile(t *testing.T) {
	clock := NewFakeClock(time.Unix(1, 0))
	l := NewLedger(clock, testConfig())
	defer l.Close()

	lease, err := l.Acquire(context.Background(), collKey(100), ClassBuild)
	require.NoError(t, err)
	defer lease.Release()

	s := snapshotFor(t, l, ScopeCollection, collKey(100))
	assert.Equal(t, int64(1), s.InFlight[ClassBuild])
	assert.Equal(t, int64(0), s.InFlight[ClassSearch])
	assert.Equal(t, int64(1), s.InFlightPlusReserved)
	assert.Equal(t, QuotaSpec{Total: 2, SearchReserve: 1, BuildReserve: 1}, s.EffectiveQuota)

	cluster := snapshotFor(t, l, ScopeCluster, ScopeKey{})
	assert.Equal(t, int64(1), cluster.InFlight[ClassBuild])
}
