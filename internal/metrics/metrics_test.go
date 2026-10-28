// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Constants(t *testing.T) {
	assert.Equal(t, ClientActMessageGauge, []string{metricNamespace, metricClient, "active", "messages"})
	assert.Equal(t, ClientStreamsGauge, []string{metricNamespace, metricClient, "streams"})
	assert.Equal(t, ClientConsumedGauge, []string{metricNamespace, metricClient, "consumed"})
	assert.Equal(t, ClientProducedGauge, []string{metricNamespace, metricClient, "produced"})
	assert.Equal(t, RequestElapsedSummary, []string{metricNamespace, "request", "elapsed"})
	assert.Equal(t, RequestTotalCounter, []string{metricNamespace, "request", metricTotal})
	assert.Equal(t, RecvMsgCounter, []string{metricNamespace, "recvmsg", metricTotal})
	assert.Equal(t, SendMsgCounter, []string{metricNamespace, "sendmsg", metricTotal})
}

func Test_LabelSet(t *testing.T) {
	labelset := NewLabelSet()
	assert.NotNil(t, labelset)
	labelset.AddLabel("ClientUUID", "test")
	assert.Equal(t, len(labelset.Labels), 1)
}
