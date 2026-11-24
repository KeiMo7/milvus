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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// TestRejectError_ToMerr covers acceptance 4: hard reject and queue timeout
// map to distinct, typed merr codes, and every message carries the
// payload-free structured fields (scope, class, depth/position, hint).
func TestRejectError_ToMerr(t *testing.T) {
	key := ScopeKey{DatabaseID: 1, CollectionID: 100}

	t.Run("queue full maps to retriable TooManyRequests", func(t *testing.T) {
		e := &RejectError{
			Reason: ReasonQueueFull, Scope: ScopeCollection, Key: key,
			Class: ClassBuild, QueueDepth: 42, RetryAfter: 25 * time.Millisecond,
		}
		err := e.ToMerr()
		assert.True(t, errors.Is(err, merr.ErrServiceTooManyRequests))
		msg := err.Error()
		assert.Contains(t, msg, "build")
		assert.Contains(t, msg, "retryAfter")
	})

	t.Run("queue timeout maps to non-retriable QuotaExceeded", func(t *testing.T) {
		e := &RejectError{
			Reason: ReasonQueueTimeout, Scope: ScopeCollection, Key: key,
			Class: ClassSearch, QueuePosition: 7, Waited: 30 * time.Millisecond,
		}
		err := e.ToMerr()
		assert.True(t, errors.Is(err, merr.ErrServiceQuotaExceeded))
		assert.False(t, errors.Is(err, merr.ErrServiceTooManyRequests))
		msg := err.Error()
		assert.Contains(t, msg, "search")
		assert.Contains(t, msg, "queuePosition")
		assert.Contains(t, msg, "queue_timeout")
	})

	t.Run("cancel maps to ServiceUnavailable", func(t *testing.T) {
		e := &RejectError{Reason: ReasonCanceled, Scope: ScopeCollection, Key: key, Class: ClassSearch}
		err := e.ToMerr()
		assert.True(t, errors.Is(err, merr.ErrServiceUnavailable))
	})

	t.Run("closed maps to QuotaExceeded with closed reason", func(t *testing.T) {
		e := &RejectError{Reason: ReasonClosed, Scope: ScopeCollection, Key: key, Class: ClassBuild}
		err := e.ToMerr()
		assert.True(t, errors.Is(err, merr.ErrServiceQuotaExceeded))
		assert.Contains(t, err.Error(), "admission_closed")
	})
}

// TestRejectReason_String guards the stable metric/message labels.
func TestRejectReason_String(t *testing.T) {
	assert.Equal(t, "queue_full", ReasonQueueFull.String())
	assert.Equal(t, "queue_timeout", ReasonQueueTimeout.String())
	assert.Equal(t, "canceled", ReasonCanceled.String())
	assert.Equal(t, "admission_closed", ReasonClosed.String())
	assert.Equal(t, "search", ClassSearch.String())
	assert.Equal(t, "build", ClassBuild.String())
}
