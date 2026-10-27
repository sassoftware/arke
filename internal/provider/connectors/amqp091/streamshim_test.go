// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp091

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_toStreamMessage_contains_compressed_body(t *testing.T) {
	orig := bytes.Repeat([]byte("x"), compressionSizeLimit+512)
	msg := streamMessage{Body: orig}

	cm, err := compressMessage(msg)
	assert.NoError(t, err)
	assert.Equal(t, "gzip", cm.Headers[transferEncodingHeaderName])
	assert.NotEqual(t, orig, cm.Body)

	sm := toStreamMessage(cm)
	props := sm.GetApplicationProperties()
	assert.Equal(t, "gzip", props[transferEncodingHeaderName])
	data := sm.GetData()
	assert.Greater(t, len(data), 0)
	assert.NotEqual(t, orig, data[0])
}
