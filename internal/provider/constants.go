// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package provider

const (
	// DISCONNECTED Closed by the broker, retry connecting
	DISCONNECTED = iota
	// CONNECTED Connected to the broker
	CONNECTED = iota
	// CONNECTING Currently connecting to the broker
	CONNECTING = iota
	// CLOSED Closed by the client
	CLOSED = iota
)

const (
	// CONNECTTIMEOUT Default timeout for waiting for connection in WaitForConnect()
	CONNECTTIMEOUT = 15
	// ReconnectDelay Maximum time to wait before a if we failed to connect
	ReconnectDelay = 500
)
