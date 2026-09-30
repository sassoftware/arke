// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

var clientIdentifierCtxKey = CtxKey{name: "clientIdentifier"}

type CtxKey struct {
	name string
}
