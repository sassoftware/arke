// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import pb "github.com/sassoftware/arke/api"

type AMQP10SessionShim struct {
	Source       *pb.Source
	stateMachine *subscribeStateMachine
}
