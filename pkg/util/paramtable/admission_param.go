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

package paramtable

// admissionConfig holds the scalar knobs of the unified admission ledger
// (package internal/util/quota/admission). Quota values per database and
// collection are supplied separately as a declaration; this group only owns
// process-wide switch and queue behavior. Every key is additive and the
// master switch defaults to false, so an absent configuration preserves all
// pre-existing rate-limit and scheduling behavior.
type admissionConfig struct {
	// AdmissionEnabled is the master switch. When false every Acquire is a
	// lock-free pass-through: no queue, no reject, no counting.
	AdmissionEnabled ParamItem

	// AdmissionMaxQueueLength bounds each per-collection wait queue.
	AdmissionMaxQueueLength ParamItem
	// AdmissionMaxQueueWait bounds how long one item may wait before it fails.
	AdmissionMaxQueueWait ParamItem
	// AdmissionMaxConsecutiveServes bounds how many consecutive scheduling
	// passes one collection may own (multiplied by its weight).
	AdmissionMaxConsecutiveServes ParamItem
	// AdmissionMinRetryAfter / AdmissionMaxRetryAfter clamp the suggested
	// retry delay carried by hard-reject errors.
	AdmissionMinRetryAfter ParamItem
	AdmissionMaxRetryAfter ParamItem
}

func (p *admissionConfig) init(base *BaseTable) {
	p.AdmissionEnabled = ParamItem{
		Key:          "quotaAdmission.enabled",
		DefaultValue: "false",
		Version:      "3.0.1",
		Doc: `master switch of the unified concurrency admission ledger shared by retrieval and index
build tasks. When false (default) requests bypass the ledger entirely; no queue, reject or counter is
introduced and existing rate limiting and task scheduling are unchanged.`,
		Export: true,
	}
	p.AdmissionEnabled.Init(base.mgr)

	p.AdmissionMaxQueueLength = ParamItem{
		Key:          "quotaAdmission.maxQueueLength",
		DefaultValue: "1024",
		Version:      "3.0.1",
		Doc:          "maximum number of items one collection's admission wait queue may hold; overflow is hard-rejected.",
		Export:       true,
	}
	p.AdmissionMaxQueueLength.Init(base.mgr)

	p.AdmissionMaxQueueWait = ParamItem{
		Key:          "quotaAdmission.maxQueueWait",
		DefaultValue: "30s",
		Version:      "3.0.1",
		Doc:          "maximum time an item may wait in the admission queue before it fails with a queue-timeout result.",
		Export:       true,
	}
	p.AdmissionMaxQueueWait.Init(base.mgr)

	p.AdmissionMaxConsecutiveServes = ParamItem{
		Key:          "quotaAdmission.maxConsecutiveServes",
		DefaultValue: "1",
		Version:      "3.0.1",
		Doc: `multiplier bounding how many consecutive scheduling passes one collection may own, expressed as its
fairness weight times this value; prevents one collection from monopolizing the service window.`,
		Export: true,
	}
	p.AdmissionMaxConsecutiveServes.Init(base.mgr)

	p.AdmissionMinRetryAfter = ParamItem{
		Key:          "quotaAdmission.minRetryAfter",
		DefaultValue: "10ms",
		Version:      "3.0.1",
		Doc:          "lower bound of the suggested retry-after duration returned with hard admission rejections.",
		Export:       true,
	}
	p.AdmissionMinRetryAfter.Init(base.mgr)

	p.AdmissionMaxRetryAfter = ParamItem{
		Key:          "quotaAdmission.maxRetryAfter",
		DefaultValue: "5s",
		Version:      "3.0.1",
		Doc:          "upper bound of the suggested retry-after duration returned with hard admission rejections.",
		Export:       true,
	}
	p.AdmissionMaxRetryAfter.Init(base.mgr)
}
