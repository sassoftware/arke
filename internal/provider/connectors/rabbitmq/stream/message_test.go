// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_toStreamMessage_contains_compressed_body(t *testing.T) {
	original := bytes.Repeat([]byte("x"), compressionSizeLimit+512)
	compressed, err := compressMessage(Message{Body: original})
	assert.NoError(t, err)
	assert.Equal(t, "gzip", compressed.Headers[transferEncodingHeader])
	assert.NotEqual(t, original, compressed.Body)

	clientMessage := toStreamMessage(compressed)
	assert.Equal(t, "gzip", clientMessage.GetApplicationProperties()[transferEncodingHeader])
	assert.NotEmpty(t, clientMessage.GetData())
	assert.NotEqual(t, original, clientMessage.GetData())
}
