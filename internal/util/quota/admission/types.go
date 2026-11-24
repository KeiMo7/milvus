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
)

// TaskClass identifies the two task families sharing one ledger.
type TaskClass int8

const (
	// ClassSearch is DQL retrieval: search and query tasks.
	ClassSearch TaskClass = iota
	// ClassBuild is index build work.
	ClassBuild
)

// classCount is the number of task classes; class-indexed arrays use it.
const classCount = 2

// String returns a stable lowercase label used in errors and metric labels.
func (c TaskClass) String() string {
	switch c {
	case ClassSearch:
		return "search"
	case ClassBuild:
		return "build"
	default:
		return "unknown"
	}
}

// ScopeName labels the three admission scopes in errors and metrics.
type ScopeName string

const (
	ScopeCluster    ScopeName = "cluster"
	ScopeDatabase   ScopeName = "database"
	ScopeCollection ScopeName = "collection"
)

// ScopeKey identifies a collection and its parent database. The zero value
// (both IDs 0) denotes the cluster scope; a zero CollectionID with a non-zero
// DatabaseID denotes the database scope.
type ScopeKey struct {
	DatabaseID   int64
	CollectionID int64
}

// QuotaSpec is one declaration entry at one scope.
type QuotaSpec struct {
	// Total caps in-flight tasks of both classes together at this scope.
	// Zero means unlimited.
	Total int64
	// SearchReserve and BuildReserve are the guaranteed shares per class.
	// Their sum must not exceed Total; zero means the class has no reserve
	// and only runs on surplus capacity.
	SearchReserve int64
	BuildReserve  int64
}

// Config is the full ledger configuration. The zero value is valid and means
// "disabled"; Ledger.UpdateConfig applies it atomically.
type Config struct {
	// Enabled is the master switch. When false, Acquire never blocks.
	Enabled bool
	// MaxQueueLength is the hard cap of every per-collection wait queue.
	MaxQueueLength int
	// MaxQueueWait bounds how long one item may wait before it fails.
	MaxQueueWait time.Duration
	// MaxConsecutiveServes bounds how many tasks one collection may dispatch
	// per scheduler visit. Must be >= 1 when the ledger is enabled.
	MaxConsecutiveServes int
	// MinRetryAfter / MaxRetryAfter clamp the suggested retry delay carried
	// by hard-reject errors.
	MinRetryAfter time.Duration
	MaxRetryAfter time.Duration
	// Cluster is the cluster-scope spec.
	Cluster QuotaSpec
	// Databases maps database ID to its spec.
	Databases map[int64]QuotaSpec
	// Collections maps collection ID to its spec.
	Collections map[int64]QuotaSpec
}

// Snapshot is the same-source observability view of one scope node. It is
// produced under the ledger lock that also mutates the counters, so a
// snapshot always reconciles with the internal accounting.
type Snapshot struct {
	Scope          ScopeName
	Key            ScopeKey
	EffectiveQuota QuotaSpec
	// InFlight per class; the sum never exceeds EffectiveQuota.Total while
	// the ledger is enabled.
	InFlight [classCount]int64
	// Queued per class.
	Queued [classCount]int64
	// MaxWait is the longest current queue wait at the sampling instant.
	MaxWait time.Duration
	// Rejected counts hard rejects and wait timeouts per class, split by
	// reject reason (see RejectReason).
	Rejected map[RejectReason][classCount]int64
	// InFlightPlusReserved is in-flight at this scope. Queued items hold no
	// reservation, so this is the value that must stay within Total.
	InFlightPlusReserved int64
}

// Lease is the only token handle. One lease represents one admitted task and
// its tokens at all three scopes. Release is idempotent.
type Lease interface {
	// Release returns the tokens. It is safe to call more than once and from
	// multiple goroutines; only the first call has an effect.
	Release()
	// Scope returns the collection/database key the lease was acquired for.
	Scope() ScopeKey
	// Class returns the task class the lease belongs to.
	Class() TaskClass
}
