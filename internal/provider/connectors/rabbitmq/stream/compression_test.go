// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_decompressBody(t *testing.T) {
	orig := bytes.Repeat([]byte("x"), 1024)
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, err := gw.Write(orig)
	assert.NoError(t, err)
	_ = gw.Close()
	compressed := buf.Bytes()

	out, err := decompressBody(compressed)
	assert.NoError(t, err)
	assert.Equal(t, orig, out)
}

func Test_compressBody(t *testing.T) {
	orig := bytes.Repeat([]byte("x"), compressionSizeLimit+256)

	compressed, err := compressBody(orig)
	assert.NoError(t, err)
	assert.Less(t, len(compressed), len(orig))

	out, err := decompressBody(compressed)
	assert.NoError(t, err)
	assert.Equal(t, orig, out)
}

func Test_compressMessage(t *testing.T) {
	orig := bytes.Repeat([]byte("x"), compressionSizeLimit+512)
	msg := Message{Body: orig}

	cm, err := compressMessage(msg)
	assert.NoError(t, err)
	assert.Equal(t, "gzip", cm.Headers[transferEncodingHeader])
	assert.NotEqual(t, orig, cm.Body)
	assert.NotNil(t, cm.Headers)

	out, err := decompressBody(cm.Body)
	assert.NoError(t, err)
	assert.Equal(t, orig, out)
}

func Test_compressMessageSkipped(t *testing.T) {
	// create a large pseudo-random payload that is unlikely to compress
	orig := make([]byte, compressionSizeLimit+512)
	_, err := rand.Read(orig)
	assert.NoError(t, err)

	msg := Message{Body: orig}

	cm, cerr := compressMessage(msg)
	// compressMessage should attempt compression (body > limit) and then
	// skip it if the compressed size is not smaller than original.
	if cerr == nil {
		t.Fatalf("expected compressMessage to return an error when compression is not beneficial")
	}
	assert.Contains(t, cerr.Error(), "compression skipped")
	// ensure the returned message body was not replaced
	assert.Equal(t, orig, cm.Body)
}

func Test_decompressBody_non_gzip_returns_error(t *testing.T) {
	plain := []byte("not a gzip payload")
	_, err := decompressBody(plain)
	assert.Error(t, err)
}

func Test_compressBody_pool_concurrent(t *testing.T) {
	orig := bytes.Repeat([]byte("x"), compressionSizeLimit+256)
	var wg sync.WaitGroup
	n := 50
	errs := make(chan error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := compressBody(orig)
			if err != nil {
				errs <- err
				return
			}
			dec, err := decompressBody(out)
			if err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(dec, orig) {
				errs <- fmt.Errorf("decompressed mismatch")
				return
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		assert.NoError(t, err)
	}
}

func Test_bodyOverLimit(t *testing.T) {
	tests := []struct {
		name     string
		bodySize int
		expected bool
	}{
		{
			name:     "body at exactly the limit",
			bodySize: compressionSizeLimit,
			expected: false,
		},
		{
			name:     "body just below limit",
			bodySize: compressionSizeLimit - 1,
			expected: false,
		},
		{
			name:     "body just over limit",
			bodySize: compressionSizeLimit + 1,
			expected: true,
		},
		{
			name:     "body well over limit",
			bodySize: compressionSizeLimit * 2,
			expected: true,
		},
		{
			name:     "empty body",
			bodySize: 0,
			expected: false,
		},
		{
			name:     "small body",
			bodySize: 100,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := make([]byte, tt.bodySize)
			result := bodyOverLimit(body)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func Test_compressMessage_BelowLimit(t *testing.T) {
	// Messages below the compression limit should not be compressed
	small := bytes.Repeat([]byte("x"), compressionSizeLimit-100)
	msg := Message{Body: small}

	result, err := compressMessage(msg)
	assert.NoError(t, err)
	assert.Equal(t, small, result.Body, "Body should be unchanged for messages below limit")
	assert.Nil(t, result.Headers, "Headers should not be set for uncompressed messages")
}

func Test_compressMessage_AtLimit(t *testing.T) {
	// Messages at exactly the compression limit should not be compressed
	atLimit := bytes.Repeat([]byte("x"), compressionSizeLimit)
	msg := Message{Body: atLimit}

	result, err := compressMessage(msg)
	assert.NoError(t, err)
	assert.Equal(t, atLimit, result.Body, "Body should be unchanged for messages at limit")
	assert.Nil(t, result.Headers, "Headers should not be set for uncompressed messages")
}

func Test_compressMessage_PreservesExistingHeaders(t *testing.T) {
	orig := bytes.Repeat([]byte("x"), compressionSizeLimit+512)
	existingHeaders := map[string]string{
		"custom-header": "custom-value",
		"another":       "value",
	}
	msg := Message{Body: orig, Headers: existingHeaders}

	result, err := compressMessage(msg)
	assert.NoError(t, err)
	assert.Equal(t, "gzip", result.Headers[transferEncodingHeader])
	assert.Equal(t, "custom-value", result.Headers["custom-header"], "Existing headers should be preserved")
	assert.Equal(t, "value", result.Headers["another"], "Existing headers should be preserved")
}

func Test_compressDecompress_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		size int
	}{
		{"small payload", 1024},
		{"medium payload", compressionSizeLimit / 2},
		{"large payload", compressionSizeLimit + 1024},
		{"very large payload", compressionSizeLimit * 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orig := bytes.Repeat([]byte("test data "), tt.size/10)

			compressed, err := compressBody(orig)
			assert.NoError(t, err)

			decompressed, err := decompressBody(compressed)
			assert.NoError(t, err)
			assert.Equal(t, orig, decompressed)
		})
	}
}

func Test_decompressBody_InvalidGzip(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"random bytes", []byte{0x1f, 0x8b, 0x08, 0x00, 0xff, 0xff}},
		{"truncated gzip", []byte{0x1f, 0x8b}},
		{"empty data", []byte{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decompressBody(tt.data)
			assert.Error(t, err, "Should return error for invalid gzip data")
		})
	}
}
