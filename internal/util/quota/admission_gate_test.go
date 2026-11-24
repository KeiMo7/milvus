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

package quota

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"

	"github.com/milvus-io/milvus/internal/util/quota/admission"
)

func newTestComponentParam(t *testing.T) *paramtable.ComponentParam {
	t.Helper()
	params := &paramtable.ComponentParam{}
	// SkipRemote avoids needing an etcd endpoint; BaseTable has no public
	// Close and is used as a process-wide table, so no explicit cleanup.
	base := paramtable.NewBaseTable(paramtable.SkipRemote(true))
	params.Init(base)
	return params
}

// TestAdmissionGate_DefaultDisabled verifies the master switch defaults to
// off and the gate is a pass-through that never counts.
func TestAdmissionGate_DefaultDisabled(t *testing.T) {
	params := newTestComponentParam(t)
	require.False(t, params.AdmissionCfg.AdmissionEnabled.GetAsBool())

	gate := NewAdmissionGate(params, admission.RealClock{}, admission.Config{})
	defer gate.Close()

	key := admission.ScopeKey{DatabaseID: 1, CollectionID: 100}
	for i := 0; i < 16; i++ {
		lease, err := gate.Acquire(context.Background(), key, admission.ClassSearch)
		require.NoError(t, err)
		lease.Release()
	}
	assert.Equal(t, [2]int64{}, gate.Ledger().InFlightTotal())
	assert.False(t, gate.Ledger().Enabled())
}

// TestAdmissionConfigFromParam_Projection verifies paramtable scalars are
// projected onto the admission config while per-scope declarations survive.
func TestAdmissionConfigFromParam_Projection(t *testing.T) {
	params := newTestComponentParam(t)
	decl := admission.Config{
		Cluster: admission.QuotaSpec{Total: 8, SearchReserve: 4, BuildReserve: 2},
		Collections: map[int64]admission.QuotaSpec{
			100: {Total: 2, SearchReserve: 1, BuildReserve: 1},
		},
	}
	cfg := admissionConfigFromParam(params, decl)

	assert.False(t, cfg.Enabled)
	assert.Equal(t, 1024, cfg.MaxQueueLength)
	assert.Equal(t, 30*time.Second, cfg.MaxQueueWait)
	assert.Equal(t, 1, cfg.MaxConsecutiveServes)
	assert.Equal(t, 10*time.Millisecond, cfg.MinRetryAfter)
	assert.Equal(t, 5*time.Second, cfg.MaxRetryAfter)
	// Per-scope declarations are preserved.
	assert.Equal(t, int64(8), cfg.Cluster.Total)
	require.Contains(t, cfg.Collections, int64(100))
	assert.Equal(t, int64(2), cfg.Collections[100].Total)
}

// TestAdmissionGate_EnableAndEnforce verifies flipping the switch on makes
// the ledger enforce its declaration and queue overflow, and turning it back
// off drains the queue with typed failures.
func TestAdmissionGate_EnableAndEnforce(t *testing.T) {
	params := newTestComponentParam(t)
	decl := admission.Config{
		MaxQueueLength: 1,
		MaxQueueWait:   time.Second,
		Cluster:        admission.QuotaSpec{Total: 1},
		Collections: map[int64]admission.QuotaSpec{
			100: {Total: 1},
		},
	}
	gate := NewAdmissionGate(params, admission.RealClock{}, decl)
	defer gate.Close()

	// Enable via the underlying paramtable item.
	params.AdmissionCfg.AdmissionEnabled.SwapTempValue("true")
	defer params.AdmissionCfg.AdmissionEnabled.SwapTempValue("false")
	// Rebuild config from the now-enabled paramtable.
	require.NoError(t, gate.UpdateDeclaration(decl))
	assert.True(t, gate.Ledger().Enabled())

	key := admission.ScopeKey{DatabaseID: 1, CollectionID: 100}
	holder, err := gate.Acquire(context.Background(), key, admission.ClassBuild)
	require.NoError(t, err)

	// One item queues; the next hits the length-1 hard cap.
	blocked := make(chan error, 1)
	go func() {
		_, e := gate.Acquire(context.Background(), key, admission.ClassBuild)
		blocked <- e
	}()
	waitUntil(t, time.Second, func() bool {
		for _, s := range gate.Ledger().Snapshots() {
			if s.Key == key && s.Queued[admission.ClassBuild] == 1 {
				return true
			}
		}
		return false
	})

	_, err = gate.Acquire(context.Background(), key, admission.ClassBuild)
	require.Error(t, err)

	// Turning the switch off drains the queued item explicitly.
	params.AdmissionCfg.AdmissionEnabled.SwapTempValue("false")
	require.NoError(t, gate.UpdateDeclaration(decl))
	select {
	case e := <-blocked:
		require.Error(t, e)
	case <-time.After(2 * time.Second):
		t.Fatal("queued item was not failed on disable")
	}
	holder.Release()
}

func waitUntil(t *testing.T, budget time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met within %s", budget)
}
