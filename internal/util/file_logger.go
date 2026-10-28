// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"os"
)

type TestLogger struct {
	*ArkeLogger
	reader *os.File
	writer *os.File
}

// GetOutput reads the log output from the TestLogger's reader and returns it as a byte slice
// Note that this will close the writer and reader, so it should only be called once per
// TestLogger instance
func (l *TestLogger) GetOutput() []byte {
	_ = l.writer.Close()
	defer func() { _ = l.reader.Close() }()
	buf := make([]byte, 2048)
	n, _ := l.reader.Read(buf)
	return buf[:n]
}

// GetTestLoggerWithCleanup should only be used in tests when you need
// to capture the log output
func GetTestLoggerWithCleanup() (*TestLogger, func()) {
	ResetLogger()
	r, w, _ := os.Pipe()
	logger := NewArkeFileLogger(w)
	cleanup := func() {
		_ = w.Close()
		_ = r.Close()
		// Reset the logger so that other tests can expect an initialized logger
		NewArkeFileLogger(LogOutputFile)
	}
	return &TestLogger{
		ArkeLogger: logger,
		reader:     r,
		writer:     w,
	}, cleanup
}
