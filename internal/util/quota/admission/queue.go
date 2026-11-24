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
	"time"
)

// waiterState is the lifecycle of one queued item. Transitions happen only
// while the ledger lock is held.
type waiterState int8

const (
	waiterQueued waiterState = iota
	waiterAdmitted
	waiterRejected
)

// waiter is one item in a collection's FIFO queue. It owns no tokens while
// queued: admission creates the lease only on dispatch, so cancellation and
// timeout paths never have to return tokens.
type waiter struct {
	id       uint64
	key      ScopeKey
	class    TaskClass
	enqueued time.Time
	deadline time.Time // min(ctx deadline, enqueued + MaxQueueWait)
	queuePos int       // 1-based position captured for timeout errors
	state    waiterState
	reject   *RejectError
	ready    chan struct{} // closed exactly once when the item leaves the queue
	ctx      context.Context
}

// newWaiter builds a queued item. A zero deadline means the caller context
// carries no deadline; the queue MaxQueueWait still applies.
func newWaiter(id uint64, key ScopeKey, class TaskClass, now time.Time,
	maxWait time.Duration, ctx context.Context) *waiter {
	deadline := now.Add(maxWait)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	return &waiter{
		id:       id,
		key:      key,
		class:    class,
		enqueued: now,
		deadline: deadline,
		state:    waiterQueued,
		ready:    make(chan struct{}),
		ctx:      ctx,
	}
}

// expired reports whether the item's queue deadline has passed at now.
func (w *waiter) expired(now time.Time) bool {
	return !w.deadline.IsZero() && !w.deadline.After(now)
}

// finish resolves the item exactly once. It must be called with the ledger
// lock held; the caller closes ready after dropping the lock to avoid
// waking the waiter while it still needs the lock to inspect state.
func (w *waiter) finish(state waiterState, reject *RejectError) bool {
	if w.state != waiterQueued {
		return false
	}
	w.state = state
	w.reject = reject
	return true
}

// collectionQueue is one collection's FIFO wait queue plus the smooth
// weighted round-robin bookkeeping the scheduler uses. All access is under
// the ledger lock. The queue object (and its ring membership) lives for the
// ledger's lifetime even when empty, which keeps rotation state stable
// across drain/refill cycles.
type collectionQueue struct {
	key   ScopeKey
	items []*waiter
	// currentWeight is the smooth-WRR accumulator (Nginx algorithm): each
	// scheduling pass adds the queue's effective weight; the queue with the
	// highest accumulator wins one dispatch and pays its weight. This yields
	// long-term dispatch proportions equal to the reservation weights while
	// spreading each queue's turns evenly (no back-to-back monopolization).
	currentWeight int64
	inRing        bool // membership in the rotation ring
}

// len returns the queue depth.
func (q *collectionQueue) len() int { return len(q.items) }

// push appends an item and stamps its 1-based position.
func (q *collectionQueue) push(w *waiter) {
	q.items = append(q.items, w)
	w.queuePos = len(q.items)
}

// pop removes and returns the head item, preserving FIFO order.
func (q *collectionQueue) pop() *waiter {
	if len(q.items) == 0 {
		return nil
	}
	w := q.items[0]
	q.items = q.items[1:]
	return w
}

// remove deletes the item at index i (used by the expiry/cancel sweep),
// keeping the remaining order intact.
func (q *collectionQueue) remove(i int) *waiter {
	w := q.items[i]
	q.items = append(q.items[:i], q.items[i+1:]...)
	return w
}

// maxWait returns the longest current waiting time at now.
func (q *collectionQueue) maxWait(now time.Time) time.Duration {
	if len(q.items) == 0 {
		return 0
	}
	return now.Sub(q.items[0].enqueued)
}

// depthByClass counts queued items per task class.
func (q *collectionQueue) depthByClass() [classCount]int64 {
	var out [classCount]int64
	for _, w := range q.items {
		out[w.class]++
	}
	return out
}
