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

// Snapshots returns the same-source observability view of every scope node
// and every non-empty collection queue. It takes the same lock the
// scheduler mutates under, so each snapshot reconciles exactly with the
// internal counters at the sampling instant.
func (l *Ledger) Snapshots() []Snapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock.Now()

	out := make([]Snapshot, 0, 1+len(l.dbs)+len(l.colls))
	out = append(out, l.snapshotNodeLocked(ScopeCluster, ScopeKey{}, l.cluster, 0))

	for id, n := range l.dbs {
		out = append(out, l.snapshotNodeLocked(ScopeDatabase, ScopeKey{DatabaseID: id}, n, 0))
	}
	for id, n := range l.colls {
		key := ScopeKey{DatabaseID: n.dbID, CollectionID: id}
		if q := l.queues[key]; q != nil {
			out = append(out, l.snapshotNodeLocked(ScopeCollection, key, n, q.maxWait(now)))
			continue
		}
		// A collection can hold in-flight work after its queue drained; its
		// queued view is still emitted so dashboards see the active scope.
		out = append(out, l.snapshotNodeLocked(ScopeCollection, key, n, 0))
	}

	// Surface queues for keys that have no collection spec node yet.
	for key, q := range l.queues {
		if _, ok := l.colls[key.CollectionID]; ok {
			continue
		}
		out = append(out, Snapshot{
			Scope:                ScopeCollection,
			Key:                  key,
			EffectiveQuota:       QuotaSpec{},
			Queued:               q.depthByClass(),
			MaxWait:              q.maxWait(now),
			Rejected:             map[RejectReason][classCount]int64{},
			InFlightPlusReserved: 0,
		})
	}
	return out
}

func (l *Ledger) snapshotNodeLocked(scope ScopeName, key ScopeKey, n *scopeNode,
	maxWait time.Duration) Snapshot {
	rejected := make(map[RejectReason][classCount]int64, len(n.rejected))
	for reason, counts := range n.rejected {
		rejected[reason] = counts
	}
	return Snapshot{
		Scope:                scope,
		Key:                  key,
		EffectiveQuota:       n.spec,
		InFlight:             n.inFlight,
		Queued:               n.queued,
		MaxWait:              maxWait,
		Rejected:             rejected,
		InFlightPlusReserved: n.totalInFlight(),
	}
}

// Enabled reports whether the master switch is on.
func (l *Ledger) Enabled() bool { return l.enabled.Load() }

// InFlightTotal returns the cluster-scope in-flight count per class; test and
// health checks use it to assert that the ledger returns to zero.
func (l *Ledger) InFlightTotal() [classCount]int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.cluster.inFlight
}
