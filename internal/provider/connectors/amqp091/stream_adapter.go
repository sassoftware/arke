// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp091

// activeStreamMessage owns provider Ack/Nack behavior for an active delivery.
type activeStreamMessage struct {
	Ack  func()
	Nack func()
}
