// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"context"

	ramqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	pb "github.com/sassoftware/arke/api"
)

type Subscription struct {
	ctx               context.Context
	source            *pb.Source
	bd                *BrokerDetails
	consumer          rabbitMQAMQP10ConsumerShim
	publisher         *ramqp.Publisher
	state             *subscribeStateMachine
	messageCh         chan<- *pb.Message
	recoveryAttempts  int
	lastRecoveryError *subscribeStateTransitionError
}

// activeDelivery lets consumer teardown remove only handles owned by one subscription.
type activeDelivery struct {
	subscription *Subscription
	delivery     ramqp.IDeliveryContext
}
