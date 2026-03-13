// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp091

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"sync"
)

// The rabbitmq-streaming client library contains a framing limit that
// prevents publishing messages larger than approximately 1 MiB. To work
// around that third-party limitation we compress message bodies that exceed
// this limit inside Arke. Doing compression in Arke isolates the change to
// this service (avoiding updates to all client libraries).
const compressionSizeLimit = 1024 * 1024

var bufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}
var gzPool = sync.Pool{New: func() any { return gzip.NewWriter(nil) }}

// bodyOverLimit returns true when the payload exceeds the configured
// compression size limit.
func bodyOverLimit(body []byte) bool {
	return len(body) > compressionSizeLimit
}

func compressMessage(msg streamMessage) (streamMessage, error) {
	if bodyOverLimit(msg.Body) {
		compressed, err := compressBody(msg.Body)

		if err != nil {
			return msg, err
		}
		// Only use compressed body if it's smaller and the compressed
		// size is not over the limit.
		if len(compressed) < len(msg.Body) && !bodyOverLimit(compressed) {
			msg.Body = compressed
			if msg.Headers == nil {
				msg.Headers = make(map[string]string)
			}
			msg.Headers[transferEncodingHeaderName] = "gzip"
		} else {
			return msg, fmt.Errorf("compression skipped: original=%d compressed=%d", len(msg.Body), len(compressed))
		}
	}
	return msg, nil
}

func compressBody(b []byte) ([]byte, error) {
	// Use pooled buffers and gzip writers to reduce allocations.
	buf := bufPool.Get().(*bytes.Buffer)
	gw := gzPool.Get().(*gzip.Writer)
	gw.Reset(buf)
	defer releasePools(gw, buf)

	_, err := gw.Write(b)
	if err != nil {
		return b, err
	}
	if err := gw.Close(); err != nil {
		return b, err
	}

	// copy out compressed bytes because we will return the buffer to the pool
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())

	return out, nil
}

// releasePools detaches pooled objects from any external buffers and returns
// them to their pools.
func releasePools(gw *gzip.Writer, buf *bytes.Buffer) {
	if gw != nil {
		gw.Reset(nil)
		gzPool.Put(gw)
	}
	if buf != nil {
		buf.Reset()
		bufPool.Put(buf)
	}
}

// decompressBody detects gzip-encoded data (by encoding string or magic bytes)
// and returns the decompressed bytes. If data is not gzip, it returns the
// original bytes.
func decompressBody(b []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return out, nil
}
