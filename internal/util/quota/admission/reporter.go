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

// Reporter receives same-source snapshots for external observability. The
// core ledger deliberately depends on no prometheus package; the wiring layer
// (component startup) implements this interface with the repository's
// centralized metric definitions and drives it on a fixed ticker. Report
// implementations must be cheap and must not block the ledger — the ticker
// goroutine calls it outside the ledger lock.
type Reporter interface {
	Report(snapshots []Snapshot, sampledAt time.Time)
}

// ReportOnce takes one snapshot through r when r is non-nil; nil reporters
// are a valid no-op so ledgers can run without observability wiring in tests.
func (l *Ledger) ReportOnce(r Reporter) {
	if r == nil {
		return
	}
	r.Report(l.Snapshots(), l.clock.Now())
}
