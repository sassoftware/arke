// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	met "github.com/hashicorp/go-metrics"
)

// LabelSet go-metrics Label. Avoid importing go-metrics anywhere but here
type LabelSet struct {
	Labels []met.Label
}

// Stats global Stats variable for access to the sinks
var (
	ClientActMessageGauge    = []string{"arke", "client", "active", "messages"}
	ClientStreamsGauge       = []string{"arke", "client", "streams"}
	ClientConsumedGauge      = []string{"arke", "client", "consumed"}
	ClientProducedGauge      = []string{"arke", "client", "produced"}
	RequestElapsedSummary    = []string{"arke", "request", "elapsed"}
	RequestTotalCounter      = []string{"arke", "request", "total"}
	RecvMsgCounter           = []string{"arke", "recvmsg", "total"}
	SendMsgCounter           = []string{"arke", "sendmsg", "total"}
	RateLimitEnforcedSummary = []string{"arke", "rate", "limit", "enforced", "summary"}
)

// NewLabelSet Create a new label set
func NewLabelSet() *LabelSet {
	metl := &LabelSet{}
	metl.Labels = make([]met.Label, 0)
	return metl
}

// AddLabel Add a label to your label set
func (metl *LabelSet) AddLabel(name string, value string) {
	label := met.Label{Name: name, Value: value}
	metl.Labels = append(metl.Labels, label)
}
