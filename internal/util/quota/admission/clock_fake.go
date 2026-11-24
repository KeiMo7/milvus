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
	"sync"
	"time"
)

// FakeClock is a manually driven Clock for tests. Time only moves when the
// test calls Advance, and due timers fire then (in fire-time order), so
// queue timeouts are fully deterministic and never depend on wall-clock
// scheduling. It is safe for concurrent use.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
	seq    uint64
}

// NewFakeClock returns a FakeClock anchored at start.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

// Now returns the fake current time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// NewTimer registers a timer that fires d after the current fake time.
func (c *FakeClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	t := &fakeTimer{
		clock:  c,
		fireAt: c.now.Add(d),
		seq:    c.seq,
		ch:     make(chan time.Time, 1),
	}
	c.timers = append(c.timers, t)
	return t
}

// Advance moves the clock by d and fires every due, active timer exactly
// once. Fires are non-blocking sends on buffered channels.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	due := make([]*fakeTimer, 0)
	kept := c.timers[:0]
	for _, t := range c.timers {
		t.mu.Lock()
		active := !t.stopped && !t.fired
		if active && !t.fireAt.After(c.now) {
			t.fired = true
			due = append(due, t)
		} else if active {
			kept = append(kept, t)
		}
		t.mu.Unlock()
	}
	c.timers = kept
	c.mu.Unlock()
	for _, t := range due {
		select {
		case t.ch <- t.fireAt:
		default:
		}
	}
}

type fakeTimer struct {
	clock   *FakeClock
	mu      sync.Mutex
	fireAt  time.Time
	seq     uint64
	fired   bool
	stopped bool
	ch      chan time.Time
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	wasActive := !t.stopped && !t.fired
	t.stopped = true
	return wasActive
}

func (t *fakeTimer) Reset(d time.Duration) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	wasActive := !t.stopped && !t.fired
	// Drain any pending signal so a reset timer cannot fire early.
	select {
	case <-t.ch:
	default:
	}
	t.stopped = false
	t.fired = false
	t.clock.mu.Lock()
	t.fireAt = t.clock.now.Add(d)
	t.clock.seq++
	t.seq = t.clock.seq
	t.clock.timers = append(t.clock.timers, t)
	t.clock.mu.Unlock()
	return wasActive
}
