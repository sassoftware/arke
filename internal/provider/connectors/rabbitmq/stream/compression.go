// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"sync"
)

const (
	compressionSizeLimit   = 1024 * 1024
	transferEncodingHeader = "Transfer-Encoding"
)

var bufferPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}
var gzipWriterPool = sync.Pool{New: func() any { return gzip.NewWriter(nil) }}

func bodyOverLimit(body []byte) bool { return len(body) > compressionSizeLimit }

func compressMessage(msg Message) (Message, error) {
	if !bodyOverLimit(msg.Body) {
		return msg, nil
	}
	compressed, err := compressBody(msg.Body)
	if err != nil {
		return msg, err
	}
	if len(compressed) >= len(msg.Body) || bodyOverLimit(compressed) {
		return msg, fmt.Errorf("compression skipped: original=%d compressed=%d", len(msg.Body), len(compressed))
	}
	msg.Body = compressed
	if msg.Headers == nil {
		msg.Headers = make(map[string]string)
	}
	msg.Headers[transferEncodingHeader] = "gzip"
	return msg, nil
}

func compressBody(body []byte) ([]byte, error) {
	buffer := bufferPool.Get().(*bytes.Buffer)
	writer := gzipWriterPool.Get().(*gzip.Writer)
	writer.Reset(buffer)
	defer func() {
		writer.Reset(nil)
		gzipWriterPool.Put(writer)
		buffer.Reset()
		bufferPool.Put(buffer)
	}()
	if _, err := writer.Write(body); err != nil {
		return body, err
	}
	if err := writer.Close(); err != nil {
		return body, err
	}
	compressed := make([]byte, buffer.Len())
	copy(compressed, buffer.Bytes())
	return compressed, nil
}

func decompressBody(body []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}
