// Copyright © 2026, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"
	ramqp10 "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type subscriptionConsumerStub struct {
	receive     func(context.Context) (ramqp10.IDeliveryContext, error)
	closeOnce   sync.Once
	closeCalled chan struct{}
}

func (c *subscriptionConsumerStub) Receive(ctx context.Context) (ramqp10.IDeliveryContext, error) {
	return c.receive(ctx)
}

func (c *subscriptionConsumerStub) Close(context.Context) error {
	c.closeOnce.Do(func() { close(c.closeCalled) })
	return nil
}

type subscriptionDeliveryStub struct {
	message *amqp.Message
}

func (d *subscriptionDeliveryStub) Message() *amqp.Message                   { return d.message }
func (*subscriptionDeliveryStub) Accept(context.Context) error               { return nil }
func (*subscriptionDeliveryStub) Discard(context.Context, *amqp.Error) error { return nil }
func (*subscriptionDeliveryStub) DiscardWithAnnotations(context.Context, amqp.Annotations) error {
	return nil
}
func (*subscriptionDeliveryStub) Requeue(context.Context) error { return nil }
func (*subscriptionDeliveryStub) RequeueWithAnnotations(context.Context, amqp.Annotations) error {
	return nil
}
func (*subscriptionDeliveryStub) RequeueWithAnnotationsAndDeliveryFailed(context.Context, amqp.Annotations, bool) error {
	return nil
}
func (*subscriptionDeliveryStub) DelayRetry(context.Context, time.Duration, bool) error { return nil }

type waitForConnectProviderStub struct {
	provider.Provider
	wait func(context.Context) bool
}

func (p waitForConnectProviderStub) WaitForConnect(ctx context.Context) bool {
	return p.wait(ctx)
}

func newTestSubscription(t *testing.T, ctx context.Context, source *pb.Source, consumer rabbitMQAMQP10ConsumerShim, messageCh chan<- *pb.Message) (*Subscription, *rabbitMQAMQP10ConnectionMock) {
	t.Helper()
	connection := &rabbitMQAMQP10ConnectionMock{}
	connection.On("NewConsumer", ctx, mock.Anything, mock.Anything).Return(consumer, nil).Once()
	bd := &BrokerDetails{
		ctx:              ctx,
		ClientIdentifier: "subscription-test",
		Connection:       connection,
		ActiveMessages:   util.NewConcurrentMap(),
		subscriptions:    util.NewConcurrentMap(),
	}
	t.Cleanup(func() { connection.AssertExpectations(t) })
	return &Subscription{ctx: ctx, source: source, bd: bd, messageCh: messageCh}, connection
}

func Test_SubscriptionRunningCancellationStopsReceiveWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	receiveStarted := make(chan struct{})
	consumer := &subscriptionConsumerStub{
		closeCalled: make(chan struct{}),
		receive: func(ctx context.Context) (ramqp10.IDeliveryContext, error) {
			close(receiveStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	source := &pb.Source{Name: "orders", Type: pb.Source_QUEUE, Address: &pb.Address{Name: "orders", Type: pb.Address_TOPIC}}
	subscription, _ := newTestSubscription(t, ctx, source, consumer, nil)
	result := make(chan subscribeState, 1)
	go func() {
		state, _ := subscription.running()
		result <- state
	}()

	select {
	case <-receiveStarted:
	case <-time.After(time.Second):
		t.Fatal("receive worker did not start")
	}
	cancel()
	select {
	case state := <-result:
		assert.Equal(t, subscribeStateStopping, state)
	case <-time.After(time.Second):
		t.Fatal("running did not stop after context cancellation")
	}
	select {
	case <-consumer.closeCalled:
	case <-time.After(time.Second):
		t.Fatal("consumer was not closed")
	}
}

func Test_SubscriptionRunningCancellationUnblocksMessageForwarding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	delivery := &subscriptionDeliveryStub{message: &amqp.Message{
		Data:       [][]byte{[]byte("payload")},
		Header:     &amqp.MessageHeader{},
		Properties: &amqp.MessageProperties{MessageID: int64(1)},
	}}
	var receiveOnce sync.Once
	consumer := &subscriptionConsumerStub{
		closeCalled: make(chan struct{}),
		receive: func(ctx context.Context) (ramqp10.IDeliveryContext, error) {
			first := false
			receiveOnce.Do(func() { first = true })
			if first {
				return delivery, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	source := &pb.Source{Name: "orders", Type: pb.Source_QUEUE, Address: &pb.Address{Name: "orders", Type: pb.Address_TOPIC}}
	subscription, _ := newTestSubscription(t, ctx, source, consumer, make(chan *pb.Message))
	otherSubscription := &Subscription{}
	otherDelivery := &activeDelivery{subscription: otherSubscription, delivery: delivery}
	subscription.bd.ActiveMessages.Add("other-subscription-message", otherDelivery)
	result := make(chan subscribeState, 1)
	go func() {
		state, _ := subscription.running()
		result <- state
	}()

	assert.Eventually(t, func() bool { return subscription.bd.ActiveMessages.Length() == 1 }, time.Second, time.Millisecond)
	cancel()
	select {
	case state := <-result:
		assert.Equal(t, subscribeStateStopping, state)
	case <-time.After(time.Second):
		t.Fatal("message forwarding remained blocked after cancellation")
	}
	assert.Equal(t, 1, subscription.bd.ActiveMessages.Length())
	got, ok := subscription.bd.ActiveMessages.Get("other-subscription-message")
	assert.True(t, ok)
	assert.Same(t, otherDelivery, got)
}

func Test_SubscriptionRunningReceiveErrorUnblocksMessageForwarding(t *testing.T) {
	ctx := context.Background()
	delivery := &subscriptionDeliveryStub{message: &amqp.Message{
		Data:       [][]byte{[]byte("payload")},
		Header:     &amqp.MessageHeader{},
		Properties: &amqp.MessageProperties{MessageID: int64(1)},
	}}
	secondReceive := make(chan struct{})
	failReceive := make(chan struct{})
	var receiveOnce sync.Once
	consumer := &subscriptionConsumerStub{
		closeCalled: make(chan struct{}),
		receive: func(context.Context) (ramqp10.IDeliveryContext, error) {
			first := false
			receiveOnce.Do(func() { first = true })
			if first {
				return delivery, nil
			}
			close(secondReceive)
			<-failReceive
			return nil, errors.New("receiver failed")
		},
	}
	source := &pb.Source{Name: "orders", Type: pb.Source_QUEUE, Address: &pb.Address{Name: "orders", Type: pb.Address_TOPIC}}
	subscription, _ := newTestSubscription(t, ctx, source, consumer, make(chan *pb.Message))
	result := make(chan subscribeState, 1)
	go func() {
		state, _ := subscription.running()
		result <- state
	}()

	select {
	case <-secondReceive:
	case <-time.After(time.Second):
		t.Fatal("receive worker did not request the second message")
	}
	require.Eventually(t, func() bool { return subscription.bd.ActiveMessages.Length() == 1 }, time.Second, time.Millisecond)
	close(failReceive)
	select {
	case state := <-result:
		assert.Equal(t, subscribeStateRecovering, state)
	case <-time.After(time.Second):
		t.Fatal("receive error did not unblock forwarding")
	}
	assert.Equal(t, 0, subscription.bd.ActiveMessages.Length())
	require.NotNil(t, subscription.lastRecoveryError)
	assert.EqualError(t, subscription.lastRecoveryError.err, "receiver failed")
	select {
	case <-consumer.closeCalled:
	case <-time.After(time.Second):
		t.Fatal("consumer was not closed after receive failure")
	}
}

func Test_SubscriptionRunningReceiveErrorCleansOwnedDeliveries(t *testing.T) {
	ctx := context.Background()
	delivery := &subscriptionDeliveryStub{message: &amqp.Message{
		Data:       [][]byte{[]byte("payload")},
		Header:     &amqp.MessageHeader{},
		Properties: &amqp.MessageProperties{MessageID: int64(1)},
	}}
	receiveCount := 0
	consumer := &subscriptionConsumerStub{
		closeCalled: make(chan struct{}),
		receive: func(context.Context) (ramqp10.IDeliveryContext, error) {
			receiveCount++
			if receiveCount == 1 {
				return delivery, nil
			}
			return nil, errors.New("receiver failed")
		},
	}
	source := &pb.Source{Name: "orders", Type: pb.Source_QUEUE, Address: &pb.Address{Name: "orders", Type: pb.Address_TOPIC}}
	subscription, _ := newTestSubscription(t, ctx, source, consumer, make(chan *pb.Message, 1))

	state, err := subscription.running()

	assert.Equal(t, subscribeStateRecovering, state)
	assert.Nil(t, err)
	require.NotNil(t, subscription.lastRecoveryError)
	assert.EqualError(t, subscription.lastRecoveryError.err, "receiver failed")
	assert.Equal(t, 0, subscription.bd.ActiveMessages.Length())
	select {
	case <-consumer.closeCalled:
	case <-time.After(time.Second):
		t.Fatal("consumer was not closed after receive failure")
	}
	subscription.bd.provider = waitForConnectProviderStub{wait: func(context.Context) bool { return true }}
	state, err = subscription.recovering()
	assert.Equal(t, subscribeStateRunning, state)
	assert.Nil(t, err)
}

func Test_ConsumerRecoveryBackoff(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: 100 * time.Millisecond},
		{attempt: 2, want: 200 * time.Millisecond},
		{attempt: 3, want: 400 * time.Millisecond},
		{attempt: 4, want: 800 * time.Millisecond},
		{attempt: 5, want: 1600 * time.Millisecond},
		{attempt: 6, want: 2 * time.Second},
	}
	for _, test := range tests {
		assert.Equal(t, test.want, consumerRecoveryBackoff(test.attempt))
	}
}

func Test_SubscriptionRecoveringStopsAfterRetryLimit(t *testing.T) {
	waitCalled := false
	subscription := &Subscription{
		ctx:    context.Background(),
		source: &pb.Source{Name: "orders"},
		bd: &BrokerDetails{
			ClientIdentifier: "subscription-test",
			provider: waitForConnectProviderStub{wait: func(context.Context) bool {
				waitCalled = true
				return true
			}},
		},
		recoveryAttempts: maxConsumerRecoveryAttempts - 1,
		lastRecoveryError: &subscribeStateTransitionError{
			err: errors.New("permission denied"),
			msg: "consumer creation failed",
		},
	}

	state, err := subscription.recovering()

	assert.Equal(t, subscribeStateStopping, state)
	assert.Same(t, subscription.lastRecoveryError, err)
	assert.False(t, waitCalled)
}

func Test_SubscriptionRecoveringWaitsForConnection(t *testing.T) {
	ctx := context.Background()
	waitCalled := false
	bd := &BrokerDetails{
		ClientIdentifier: "subscription-test",
		provider: waitForConnectProviderStub{wait: func(got context.Context) bool {
			waitCalled = true
			assert.Equal(t, ctx, got)
			return true
		}},
	}
	subscription := &Subscription{ctx: ctx, source: &pb.Source{Name: "orders"}, bd: bd}

	state, err := subscription.recovering()

	assert.Nil(t, err)
	assert.Equal(t, subscribeStateRunning, state)
	assert.True(t, waitCalled)
}

func Test_SubscriptionRecoveringStopsWhenConnectionCannotRecover(t *testing.T) {
	ctx := context.Background()
	bd := &BrokerDetails{
		ClientIdentifier: "subscription-test",
		provider:         waitForConnectProviderStub{wait: func(context.Context) bool { return false }},
	}
	subscription := &Subscription{ctx: ctx, source: &pb.Source{Name: "orders"}, bd: bd}

	state, err := subscription.recovering()

	assert.Equal(t, subscribeStateStopping, state)
	require.Error(t, err)
	assert.EqualError(t, err.err, "broker connection is unavailable")
}

func Test_SubscriptionRecoveringStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	waitCalled := false
	bd := &BrokerDetails{
		ClientIdentifier: "subscription-test",
		provider: waitForConnectProviderStub{wait: func(context.Context) bool {
			waitCalled = true
			return true
		}},
	}
	subscription := &Subscription{ctx: ctx, source: &pb.Source{Name: "orders"}, bd: bd}

	state, err := subscription.recovering()

	assert.Equal(t, subscribeStateStopping, state)
	assert.Nil(t, err)
	assert.False(t, waitCalled)
}

func Test_SubscriptionRunningRejectsNilConsumer(t *testing.T) {
	ctx := context.Background()
	source := &pb.Source{Name: "orders", Type: pb.Source_QUEUE, Address: &pb.Address{Name: "orders", Type: pb.Address_TOPIC}}
	subscription, _ := newTestSubscription(t, ctx, source, nil, nil)

	state, err := subscription.running()

	assert.Equal(t, subscribeStateStopping, state)
	require.Error(t, err)
	assert.ErrorContains(t, err.err, "nil consumer")
}

func Test_SubscriptionRunningStopsWhenConnectionIsGone(t *testing.T) {
	source := &pb.Source{Name: "orders", Type: pb.Source_QUEUE, Address: &pb.Address{Name: "orders", Type: pb.Address_TOPIC}}
	bd := &BrokerDetails{ctx: context.Background(), ClientIdentifier: "subscription-test", subscriptions: util.NewConcurrentMap()}
	subscription := &Subscription{ctx: context.Background(), source: source, bd: bd}

	state, err := subscription.running()

	assert.Nil(t, err)
	assert.Equal(t, subscribeStateStopping, state)
}

func Test_SubscriptionStoppingKeepsReplacementSubscription(t *testing.T) {
	source := &pb.Source{Name: "orders", Type: pb.Source_QUEUE, Address: &pb.Address{Name: "orders", Type: pb.Address_TOPIC}}
	bd := &BrokerDetails{subscriptions: util.NewConcurrentMap()}
	old := &Subscription{source: source, bd: bd}
	replacement := &Subscription{source: source, bd: bd}
	bd.subscriptions.Add(subscriptionKey(source), replacement)

	state, err := old.stopping()

	assert.Nil(t, err)
	assert.Equal(t, subscribeStateStopped, state)
	got, ok := bd.subscriptions.Get(subscriptionKey(source))
	require.True(t, ok)
	assert.Same(t, replacement, got)

	_, _ = replacement.stopping()
	_, ok = bd.subscriptions.Get(subscriptionKey(source))
	assert.False(t, ok)
}
