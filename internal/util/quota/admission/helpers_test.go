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

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// merrIsCode checks the typed wire code through errors.Is, which is the
// repository-idiomatic match (milvusError matches by numeric code).
func merrIsCode(err error, target error) bool {
	return errors.Is(err, target)
}

var (
	merrCodeTooManyRequests = merr.ErrServiceTooManyRequests
	merrCodeQuotaExceeded   = merr.ErrServiceQuotaExceeded
)

// snapshotFor finds the snapshot of a scope, failing the test if absent.
func snapshotFor(t *testing.T, l *Ledger, scope ScopeName, key ScopeKey) Snapshot {
	t.Helper()
	for _, s := range l.Snapshots() {
		if s.Scope == scope && s.Key == key {
			return s
		}
	}
	t.Fatalf("snapshot for scope=%s key=%+v not found", scope, key)
	return Snapshot{}
}
