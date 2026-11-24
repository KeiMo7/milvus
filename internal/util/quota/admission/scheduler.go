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

import "time"

// startLoopLocked launches the single dispatch goroutine once. It parks
// until the first enabled config arrives; Close waits for it to exit.
func (l *Ledger) startLoopLocked() {
	l.startOnce.Do(func() {
		l.wg.Add(1)
		go l.loop()
	})
}

// loop is the only goroutine that dispatches queued items. Every wake runs
// one expiry sweep and dispatches at most one task, then parks again on the
// timer (next deadline) or the event channel. Bounding a pass to one
// dispatch is what makes back-to-back slot releases rotate across
// collections instead of draining one queue: each release is its own pass,
// and every pass advances the rotation cursor.
func (l *Ledger) loop() {
	defer l.wg.Done()
	timer := l.clock.NewTimer(idlePark)
	defer timer.Stop()

	for {
		select {
		case <-l.done:
			return
		case <-timer.C():
		case <-l.wake:
		}

		l.mu.Lock()
		if l.closed.Load() {
			l.mu.Unlock()
			return
		}
		if !l.enabled.Load() {
			// Disabled at runtime: nothing to schedule; reset to idle park.
			timer.Reset(idlePark)
			l.mu.Unlock()
			continue
		}
		nextDeadline := l.schedulePassLocked()
		l.mu.Unlock()

		timer.Reset(nextDeadline)
	}
}

// schedulePassLocked sweeps expired waiters and then dispatches at most one
// task through fair rotation. It returns the duration until the next
// deadline (idlePark when nothing is queued).
func (l *Ledger) schedulePassLocked() time.Duration {
	now := l.clock.Now()
	next := l.sweepExpiredLocked(now)
	l.dispatchOneLocked(now)
	if next.IsZero() {
		return idlePark
	}
	d := next.Sub(l.clock.Now())
	if d <= 0 {
		// A deadline is already due; re-enter the pass immediately.
		return time.Nanosecond
	}
	if d > idlePark {
		return idlePark
	}
	return d
}

// sweepExpiredLocked fails every item whose deadline passed. The collection
// queue order is preserved, and the queued counters fall back here.
func (l *Ledger) sweepExpiredLocked(now time.Time) time.Time {
	var next time.Time
	for _, q := range l.queues {
		kept := q.items[:0]
		for _, w := range q.items {
			if w.expired(now) {
				l.cluster.queued[w.class]--
				if n := l.dbs[w.key.DatabaseID]; n != nil {
					n.queued[w.class]--
				}
				if n := l.colls[w.key.CollectionID]; n != nil {
					n.queued[w.class]--
				}
				l.rejectWaiterLocked(w, ReasonQueueTimeout)
				continue
			}
			kept = append(kept, w)
			if next.IsZero() || w.deadline.Before(next) {
				next = w.deadline
			}
		}
		q.items = kept
		if q.len() == 0 {
			q.currentWeight = 0
		}
	}
	return next
}

// dispatchOneLocked runs one smooth weighted round-robin pass and admits at
// most one task. Every non-empty queue gains its effective weight; the queue
// with the highest accumulator wins, dispatches its FIFO head, and pays its
// weight. Over a long run each collection's dispatch share converges to its
// reservation weight, and a queue's turns are spread evenly rather than
// served back to back. A queue whose head cannot be admitted now (no
// capacity at a scope) keeps its place and accumulator for the next pass.
func (l *Ledger) dispatchOneLocked(now time.Time) {
	if len(l.rrOrder) == 0 {
		return
	}

	// First collect every non-empty queue whose head is admittable now.
	// Accumulators are updated for all of them regardless of the cap, so a
	// queue that yields one pass keeps accruing and is not permanently
	// starved.
	type candidate struct {
		key    ScopeKey
		queue  *collectionQueue
		weight int64
	}
	candidates := make([]candidate, 0, len(l.rrOrder))
	for _, key := range l.rrOrder {
		q := l.queues[key]
		if q == nil || q.len() == 0 {
			continue
		}
		head := q.items[0]
		if head.expired(now) {
			continue // the next sweep settles it
		}
		if !l.canAdmitLocked(head.key, head.class) {
			continue
		}
		weight := l.weightLocked(key)
		q.currentWeight += weight
		candidates = append(candidates, candidate{key: key, queue: q, weight: weight})
	}
	if len(candidates) == 0 {
		// Nothing dispatchable: any prior window ownership is stale, so a
		// later refill starts its streak from one.
		l.lastWinner = ScopeKey{}
		l.streak = 0
		return
	}

	// Consecutive-window cap: with more than one eligible queue, a queue that
	// has already held the window for weight*MaxConsecutiveServes passes
	// yields this pass. With one candidate it is never capped, so a sole
	// workload cannot starve itself.
	best := -1
	for i, c := range candidates {
		if c.key == l.lastWinner &&
			l.streak >= int(c.weight)*l.cfg.MaxConsecutiveServes &&
			len(candidates) > 1 {
			continue
		}
		if best == -1 || c.queue.currentWeight > candidates[best].queue.currentWeight {
			best = i
		}
	}
	if best == -1 {
		// Every candidate is capped; reset the streak and pick the leader so
		// the loop still makes progress rather than idling.
		best = 0
		for i, c := range candidates {
			if c.queue.currentWeight > candidates[best].queue.currentWeight {
				best = i
			}
		}
	}
	chosen := candidates[best]

	// Track consecutive ownership of the service window.
	if chosen.key == l.lastWinner {
		l.streak++
	} else {
		l.lastWinner = chosen.key
		l.streak = 1
	}

	w := chosen.queue.pop()
	l.cluster.queued[w.class]--
	if db := l.dbs[w.key.DatabaseID]; db != nil {
		db.queued[w.class]--
	}
	if coll := l.colls[w.key.CollectionID]; coll != nil {
		coll.queued[w.class]--
	}
	l.admitLocked(w.key, w.class)
	l.observeWaitLocked(w, now)
	chosen.queue.currentWeight -= chosen.weight
	w.finish(waiterAdmitted, nil)
	close(w.ready)
}

// weightLocked returns the smooth-WRR weight of a collection: its effective
// total quota, or 1 when no positive total is declared so unconfigured
// collections rotate equally.
func (l *Ledger) weightLocked(key ScopeKey) int64 {
	if n := l.colls[key.CollectionID]; n != nil && n.spec.Total > 0 {
		return n.spec.Total
	}
	return 1
}

// observeWaitLocked updates the EWMA of queue wait used for retry hints.
func (l *Ledger) observeWaitLocked(w *waiter, now time.Time) {
	waited := now.Sub(w.enqueued)
	prev := l.waitEwma[w.key]
	if prev <= 0 {
		l.waitEwma[w.key] = waited
		return
	}
	// alpha = 1/4: recent waits move the hint without letting one outlier
	// dominate subsequent suggestions.
	l.waitEwma[w.key] = prev - prev/4 + waited/4
}
