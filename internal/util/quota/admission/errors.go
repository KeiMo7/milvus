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
	"time"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// RejectReason distinguishes the two client-visible outcomes plus the
// internal shutdown/cancel paths.
type RejectReason int8

const (
	// ReasonQueueFull is a hard reject: the per-collection queue is full, the
	// request never waited.
	ReasonQueueFull RejectReason = iota
	// ReasonQueueTimeout means the item waited past MaxQueueWait (or its
	// client deadline) without being admitted.
	ReasonQueueTimeout
	// ReasonCanceled means the caller's context ended while the item was
	// queued. It is reported to the caller as a retriable unavailable error.
	ReasonCanceled
	// ReasonClosed means the ledger is shutting down and takes no more work.
	ReasonClosed
)

// String provides stable labels for metrics and messages.
func (r RejectReason) String() string {
	switch r {
	case ReasonQueueFull:
		return "queue_full"
	case ReasonQueueTimeout:
		return "queue_timeout"
	case ReasonCanceled:
		return "canceled"
	case ReasonClosed:
		return "admission_closed"
	default:
		return "unknown"
	}
}

// RejectError is the structured, internal rejection vocabulary. It carries
// only IDs, counters and durations — never request payload (no vectors,
// primary-key values or filter expressions). Component boundaries convert it
// to a typed merr via ToMerr; it must not be returned raw across gRPC.
type RejectError struct {
	Reason        RejectReason
	Scope         ScopeName
	Key           ScopeKey
	Class         TaskClass
	QueueDepth    int           // current depth of the collection queue
	QueuePosition int           // position within the queue (timeout/close only)
	RetryAfter    time.Duration // suggested client wait (hard reject only)
	Waited        time.Duration // time spent queued (timeout/cancel only)
}

// Error implements error with a payload-free, structured message.
func (e *RejectError) Error() string {
	switch e.Reason {
	case ReasonQueueFull:
		return "admission queue full: scope=" + string(e.Scope) +
			" class=" + e.Class.String() +
			" queueDepth=" + itoa(e.QueueDepth) +
			" retryAfter=" + e.RetryAfter.String()
	case ReasonQueueTimeout:
		return "admission queue wait timeout: scope=" + string(e.Scope) +
			" class=" + e.Class.String() +
			" queuePosition=" + itoa(e.QueuePosition) +
			" waited=" + e.Waited.String()
	case ReasonCanceled:
		return "admission request canceled while queued: scope=" + string(e.Scope) +
			" class=" + e.Class.String() +
			" queuePosition=" + itoa(e.QueuePosition) +
			" waited=" + e.Waited.String()
	default:
		return "admission ledger closed: scope=" + string(e.Scope) +
			" class=" + e.Class.String() +
			" queuePosition=" + itoa(e.QueuePosition)
	}
}

// ToMerr translates the internal rejection into the typed merr the component
// boundary promises. Saturation on valid requests is a transient system
// condition, so every outcome stays in the retriable/non-Input service
// family; no new numeric error code is introduced.
//
//   - hard queue-full reject  -> ErrServiceTooManyRequests (code 4, the
//     existing "queue is full, retry later" family, with a retry hint)
//   - wait timeout / shutdown -> ErrServiceQuotaExceeded (code 9, an
//     explicit non-retriable terminal result, never a downstream timeout)
//   - caller cancellation     -> ErrServiceUnavailable (retriable)
func (e *RejectError) ToMerr() error {
	switch e.Reason {
	case ReasonQueueFull:
		return merr.WrapErrTooManyRequests(int32(e.QueueDepth),
			"admission saturated, scope="+string(e.Scope),
			"class="+e.Class.String(),
			"retryAfter="+e.RetryAfter.String(),
		)
	case ReasonCanceled:
		return merr.WrapErrServiceUnavailableMsg("admission request canceled while queued, scope=%s, class=%s, waited=%s",
			e.Scope, e.Class.String(), e.Waited.String())
	default:
		return merr.WrapErrServiceQuotaExceeded(e.Reason.String(),
			"scope="+string(e.Scope),
			"class="+e.Class.String(),
			"queuePosition="+itoa(e.QueuePosition),
			"waited="+e.Waited.String(),
		)
	}
}

// itoa avoids pulling in strconv at call sites while keeping messages
// allocation-light; strconv is also fine, this simply centralizes the format.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
