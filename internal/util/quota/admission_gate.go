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
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"

	"github.com/milvus-io/milvus/internal/util/quota/admission"
)

// AdmissionGate wraps an admission.Ledger with the repository's paramtable
// configuration and hot-reload callbacks. It is the single object a
// component embeds or holds to enforce the unified search/build concurrency
// ledger. When quotaAdmission.enabled is false (default), Acquire is the
// ledger's lock-free pass-through and existing behavior is unchanged.
type AdmissionGate struct {
	pt     *paramtable.ComponentParam
	ledger *admission.Ledger

	// declaration supplies the per-scope quota; it is set at construction and
	// replaced atomically through UpdateDeclaration. It is guarded by mu.
	mu          sync.RWMutex
	declaration admission.Config
}

// NewAdmissionGate builds a gate from the current paramtable scalars and an
// optional per-scope declaration, then registers config callbacks so scalar
// changes (switch, queue bounds, retry hints) hot-reload without a restart.
// The declaration's scalar fields are always overwritten from paramtable;
// only its Cluster/Databases/Collections specs are caller-owned. Callers in
// production pass paramtable.Get(); tests inject their own ComponentParam.
func NewAdmissionGate(pt *paramtable.ComponentParam, clock admission.Clock,
	declaration admission.Config) *AdmissionGate {
	cfg := admissionConfigFromParam(pt, declaration)
	g := &AdmissionGate{
		pt:          pt,
		ledger:      admission.NewLedger(clock, cfg),
		declaration: cfg,
	}
	g.registerCallbacks(pt)
	return g
}

// Acquire delegates to the ledger; see admission.Ledger.Acquire.
func (g *AdmissionGate) Acquire(ctx context.Context, key admission.ScopeKey,
	class admission.TaskClass) (admission.Lease, error) {
	return g.ledger.Acquire(ctx, key, class)
}

// Ledger exposes the underlying ledger for snapshot/observability.
func (g *AdmissionGate) Ledger() *admission.Ledger { return g.ledger }

// Close releases the ledger's dispatch goroutine.
func (g *AdmissionGate) Close() { g.ledger.Close() }

// UpdateDeclaration replaces the per-scope quota specs while preserving the
// process-wide scalar knobs from paramtable. In-flight work is untouched.
func (g *AdmissionGate) UpdateDeclaration(decl admission.Config) error {
	cfg := admissionConfigFromParam(g.pt, decl)
	g.mu.Lock()
	g.declaration = cfg
	g.mu.Unlock()
	return g.ledger.UpdateConfig(cfg)
}

// registerCallbacks hot-reloads scalar changes. Each callback rebuilds the
// full config from current paramtable values plus the stored declaration.
func (g *AdmissionGate) registerCallbacks(pt *paramtable.ComponentParam) {
	reload := func(_ context.Context, _, _, _ string) error {
		g.mu.RLock()
		decl := g.declaration
		g.mu.RUnlock()
		return g.ledger.UpdateConfig(admissionConfigFromParam(pt, decl))
	}
	items := []*paramtable.ParamItem{
		&pt.AdmissionCfg.AdmissionEnabled,
		&pt.AdmissionCfg.AdmissionMaxQueueLength,
		&pt.AdmissionCfg.AdmissionMaxQueueWait,
		&pt.AdmissionCfg.AdmissionMaxConsecutiveServes,
		&pt.AdmissionCfg.AdmissionMinRetryAfter,
		&pt.AdmissionCfg.AdmissionMaxRetryAfter,
	}
	for _, item := range items {
		item.RegisterCallback(reload)
	}
}

// admissionConfigFromParam projects paramtable scalars onto an admission
// Config, keeping the caller's per-scope quota declarations intact.
func admissionConfigFromParam(pt *paramtable.ComponentParam, decl admission.Config) admission.Config {
	decl.Enabled = pt.AdmissionCfg.AdmissionEnabled.GetAsBool()
	decl.MaxQueueLength = pt.AdmissionCfg.AdmissionMaxQueueLength.GetAsInt()
	decl.MaxQueueWait = pt.AdmissionCfg.AdmissionMaxQueueWait.GetAsDurationByParse()
	decl.MaxConsecutiveServes = pt.AdmissionCfg.AdmissionMaxConsecutiveServes.GetAsInt()
	decl.MinRetryAfter = pt.AdmissionCfg.AdmissionMinRetryAfter.GetAsDurationByParse()
	decl.MaxRetryAfter = pt.AdmissionCfg.AdmissionMaxRetryAfter.GetAsDurationByParse()
	return decl
}
