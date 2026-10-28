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

// Metric name segments reused across multiple metric definitions
const (
	metricNamespace = "arke"
	metricClient    = "client"
	metricTotal     = "total"
)

// Stats global Stats variable for access to the sinks
var (
	ClientActMessageGauge    = []string{metricNamespace, metricClient, "active", "messages"}
	ClientStreamsGauge       = []string{metricNamespace, metricClient, "streams"}
	ClientConsumedGauge      = []string{metricNamespace, metricClient, "consumed"}
	ClientProducedGauge      = []string{metricNamespace, metricClient, "produced"}
	RequestElapsedSummary    = []string{metricNamespace, "request", "elapsed"}
	RequestTotalCounter      = []string{metricNamespace, "request", metricTotal}
	RecvMsgCounter           = []string{metricNamespace, "recvmsg", metricTotal}
	SendMsgCounter           = []string{metricNamespace, "sendmsg", metricTotal}
	RateLimitEnforcedSummary = []string{metricNamespace, "rate", "limit", "enforced", "summary"}
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
