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

// Package admission provides a single, work-conserving concurrency ledger
// shared by retrieval (search/query) and index build tasks.
//
// The ledger is the only execution point that decides whether a task may run
// at a given moment: per-collection FIFO wait queues, fair rotation across
// collections, class-level reserved shares, and all in-flight accounting live
// in one place under one lock. It is the concurrency-side companion of the
// rate-side quota system (package quota); the two are complementary.
//
// The zero-value configuration disables the ledger, in which case Acquire is
// a lock-free pass-through that never queues, rejects or counts.
package admission

import "time"

// Clock abstracts time so queue timeouts and scheduling rounds can be driven
// deterministically in tests. Production code uses RealClock; tests inject a
// FakeClock whose timers only fire when the test advances the clock.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
}

// Timer is the minimal time.Timer surface the scheduler loop depends on.
type Timer interface {
	// C returns the channel the timer fires on.
	C() <-chan time.Time
	// Stop prevents the timer from firing. It returns false when the timer
	// has already fired or been stopped, matching time.Timer.
	Stop() bool
	// Reset re-arms the timer with a new duration.
	Reset(d time.Duration) bool
}

// RealClock wraps the wall clock and the standard library timer.
type RealClock struct{}

// Now returns the current wall-clock time.
func (RealClock) Now() time.Time { return time.Now() }

// NewTimer creates a timer backed by time.NewTimer.
func (RealClock) NewTimer(d time.Duration) Timer { return &realTimer{t: time.NewTimer(d)} }

type realTimer struct{ t *time.Timer }

func (r *realTimer) C() <-chan time.Time { return r.t.C }

func (r *realTimer) Stop() bool { return r.t.Stop() }

func (r *realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }
