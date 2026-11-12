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
	bd.setConnectionState(provider.CONNECTED)
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
			assert.NotNil(t, got)
			return true
		}},
	}
	bd.setConnectionState(provider.CONNECTED)
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

func Test_SubscriptionNotifyKeepsStopping(t *testing.T) {
	subscription := &Subscription{}

	subscription.notify(subscribeStateRecovering, 1)
	subscription.notify(subscribeStateStopping, 0)
	subscription.notify(subscribeStateRecovering, 2)
	subscription.notify(subscribeStateRunning, 0) // not a connection-driven state

	assert.Equal(t, subscribeStateStopping, subscription.takeEvent())
	assert.Equal(t, subscribeStateNone, subscription.takeEvent())
}

func Test_SubscriptionRunningReturnsStateRequestedByEvent(t *testing.T) {
	for _, requested := range []subscribeState{subscribeStateRecovering, subscribeStateStopping} {
		t.Run(requested.String(), func(t *testing.T) {
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
			<-receiveStarted

			subscription.notify(requested, 1)

			select {
			case state := <-result:
				assert.Equal(t, requested, state)
			case <-time.After(time.Second):
				t.Fatal("running did not react to the connection event")
			}
			select {
			case <-consumer.closeCalled:
			case <-time.After(time.Second):
				t.Fatal("consumer was not closed")
			}
		})
	}
}

func Test_SubscriptionRecoveringStopsOnStoppingEvent(t *testing.T) {
	waitCalled := false
	newSub := func() *Subscription {
		return &Subscription{
			ctx:    context.Background(),
			source: &pb.Source{Name: "orders"},
			bd: &BrokerDetails{provider: waitForConnectProviderStub{wait: func(context.Context) bool {
				waitCalled = true
				return true
			}}},
		}
	}

	t.Run("queued before recovering", func(t *testing.T) {
		subscription := newSub()
		subscription.notify(subscribeStateStopping, 0)

		state, err := subscription.recovering()

		assert.Nil(t, err)
		assert.Equal(t, subscribeStateStopping, state)
		assert.False(t, waitCalled)
	})

	t.Run("arrives during backoff", func(t *testing.T) {
		subscription := newSub()
		go func() {
			time.Sleep(10 * time.Millisecond)
			subscription.notify(subscribeStateStopping, 0)
		}()

		state, err := subscription.recovering()

		assert.Nil(t, err)
		assert.Equal(t, subscribeStateStopping, state)
		assert.False(t, waitCalled)
	})
}

func Test_SubscriptionRunningDiscardsStaleRecoveringEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer := &subscriptionConsumerStub{
		closeCalled: make(chan struct{}),
		receive: func(ctx context.Context) (ramqp10.IDeliveryContext, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	source := &pb.Source{Name: "orders", Type: pb.Source_QUEUE, Address: &pb.Address{Name: "orders", Type: pb.Address_TOPIC}}
	subscription, _ := newTestSubscription(t, ctx, source, consumer, nil)
	subscription.consumerGeneration = subscription.bd.setConnectionState(provider.CONNECTING)
	subscription.bd.setConnectionState(provider.CONNECTED)
	subscription.notify(subscribeStateRecovering, subscription.consumerGeneration)
	result := make(chan subscribeState, 1)
	go func() {
		state, _ := subscription.running()
		result <- state
	}()

	select {
	case state := <-result:
		t.Fatalf("running returned %s on a stale event", state.String())
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	assert.Equal(t, subscribeStateStopping, <-result)
}

func Test_SubscriptionRunningDoesNotDiscardNewReconnect(t *testing.T) {
	ctx := context.Background()
	connection := &rabbitMQAMQP10ConnectionMock{}
	connection.On("NewConsumer", ctx, mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	bd := &BrokerDetails{Connection: connection}
	generation := bd.setConnectionState(provider.CONNECTING)
	subscription := &Subscription{ctx: ctx, source: &pb.Source{Name: "orders"}, bd: bd}
	subscription.notify(subscribeStateRecovering, generation)

	state, err := subscription.running()

	assert.Nil(t, err)
	assert.Equal(t, subscribeStateRecovering, state)
	connection.AssertNotCalled(t, "NewConsumer", mock.Anything, mock.Anything, mock.Anything)
}

func Test_SubscriptionRecoverySnapshotRejectsNewReconnect(t *testing.T) {
	ctx := context.Background()
	connection := &rabbitMQAMQP10ConnectionMock{}
	bd := &BrokerDetails{
		Connection: connection,
		provider:   waitForConnectProviderStub{wait: func(context.Context) bool { return true }},
	}
	bd.setConnectionState(provider.CONNECTED)
	subscription := &Subscription{ctx: ctx, source: &pb.Source{Name: "orders"}, bd: bd}
	state, err := subscription.recovering()
	require.Nil(t, err)
	require.Equal(t, subscribeStateRunning, state)

	generation := bd.setConnectionState(provider.CONNECTING)
	subscription.notify(subscribeStateRecovering, generation)
	state, err = subscription.running()

	assert.Nil(t, err)
	assert.Equal(t, subscribeStateRecovering, state)
	connection.AssertNotCalled(t, "NewConsumer", mock.Anything, mock.Anything, mock.Anything)
}

func Test_SubscriptionRecoveryRechecksConnectedSnapshot(t *testing.T) {
	bd := &BrokerDetails{}
	bd.setConnectionState(provider.CONNECTED)
	calls := 0
	bd.provider = waitForConnectProviderStub{wait: func(context.Context) bool {
		calls++
		if calls == 1 {
			bd.setConnectionState(provider.CONNECTING)
		} else {
			bd.setConnectionState(provider.CONNECTED)
		}
		return true
	}}
	subscription := &Subscription{ctx: context.Background(), source: &pb.Source{Name: "orders"}, bd: bd}

	state, err := subscription.recovering()

	assert.Nil(t, err)
	assert.Equal(t, subscribeStateRunning, state)
	assert.Equal(t, 2, calls)
	assert.Equal(t, uint64(1), subscription.consumerGeneration)
}

func Test_SubscriptionStartingDuringReconnect(t *testing.T) {
	connection := &rabbitMQAMQP10ConnectionMock{}
	bd := &BrokerDetails{Connection: connection, subscriptions: util.NewConcurrentMap()}
	bd.setConnectionState(provider.CONNECTING)
	source := &pb.Source{Name: "orders", Address: &pb.Address{Name: "orders", Type: pb.Address_TOPIC}}
	subscription := &Subscription{ctx: context.Background(), source: source, bd: bd}

	state, err := subscription.starting()
	require.Nil(t, err)
	require.Equal(t, subscribeStateRunning, state)
	state, err = subscription.running()

	assert.Nil(t, err)
	assert.Equal(t, subscribeStateRecovering, state)
	connection.AssertNotCalled(t, "NewConsumer", mock.Anything, mock.Anything, mock.Anything)
}

func Test_SubscriptionNotifyRetainsNewestGeneration(t *testing.T) {
	subscription := &Subscription{}
	subscription.notify(subscribeStateRecovering, 3)
	subscription.notify(subscribeStateRecovering, 2)

	assert.Equal(t, subscribeStateRecovering, subscription.takeEventAfter(2))
	subscription.notify(subscribeStateRecovering, 2)
	assert.Equal(t, subscribeStateNone, subscription.takeEventAfter(2))
}

func Test_SubscriptionReconnectDuringConsumerCreation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	creating := make(chan struct{})
	release := make(chan struct{})
	consumer := &subscriptionConsumerStub{
		closeCalled: make(chan struct{}),
		receive: func(ctx context.Context) (ramqp10.IDeliveryContext, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	connection := &rabbitMQAMQP10ConnectionMock{}
	connection.On("NewConsumer", ctx, mock.Anything, mock.Anything).Run(func(mock.Arguments) {
		close(creating)
		<-release
	}).Return(consumer, nil).Once()
	bd := &BrokerDetails{Connection: connection, ActiveMessages: util.NewConcurrentMap()}
	bd.setConnectionState(provider.CONNECTED)
	subscription := &Subscription{ctx: ctx, source: &pb.Source{Name: "orders"}, bd: bd}
	result := make(chan subscribeState, 1)
	go func() {
		state, _ := subscription.running()
		result <- state
	}()
	select {
	case <-creating:
	case <-time.After(time.Second):
		t.Fatal("consumer creation did not start")
	}
	generation := bd.setConnectionState(provider.CONNECTING)
	bd.setConnectionState(provider.CONNECTED)
	subscription.notify(subscribeStateRecovering, generation)
	close(release)

	select {
	case state := <-result:
		assert.Equal(t, subscribeStateRecovering, state)
	case <-time.After(time.Second):
		t.Fatal("running did not handle reconnect during consumer creation")
	}
	select {
	case <-consumer.closeCalled:
	default:
		t.Fatal("consumer was not closed")
	}
	connection.AssertExpectations(t)
}

func Test_ConnectionEventsDriveSubscriptionLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newConsumer := func(started chan struct{}) *subscriptionConsumerStub {
		return &subscriptionConsumerStub{
			closeCalled: make(chan struct{}),
			receive: func(ctx context.Context) (ramqp10.IDeliveryContext, error) {
				started <- struct{}{}
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}
	}
	started := make(chan struct{}, 2)
	first, second := newConsumer(started), newConsumer(started)
	connection := &rabbitMQAMQP10ConnectionMock{}
	connection.On("NewConsumer", ctx, mock.Anything, mock.Anything).Return(first, nil).Once()
	connection.On("NewConsumer", ctx, mock.Anything, mock.Anything).Return(second, nil).Once()
	bd := &BrokerDetails{
		ctx:              ctx,
		ClientIdentifier: "lifecycle-test",
		Connection:       connection,
		ActiveMessages:   util.NewConcurrentMap(),
		subscriptions:    util.NewConcurrentMap(),
		stateChannel:     make(chan *ramqp10.StateChanged, 5),
		shutdownChan:     make(chan struct{}),
		provider:         waitForConnectProviderStub{wait: func(context.Context) bool { return true }},
	}
	bd.setConnectionState(provider.CONNECTED)
	go bd.watchConnection()
	source := &pb.Source{Name: "orders", Type: pb.Source_QUEUE, Address: &pb.Address{Name: "orders", Type: pb.Address_TOPIC}}
	result := make(chan error, 1)
	go func() { result <- bd.subscribe(ctx, source, make(chan *pb.Message)) }()
	waitStarted := func(what string) {
		t.Helper()
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s did not start receiving", what)
		}
	}
	waitStarted("first consumer")

	bd.stateChannel <- &ramqp10.StateChanged{From: &ramqp10.StateOpen{}, To: &ramqp10.StateReconnecting{}}
	select {
	case <-first.closeCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("first consumer was not closed on reconnect")
	}
	bd.stateChannel <- &ramqp10.StateChanged{From: &ramqp10.StateReconnecting{}, To: &ramqp10.StateOpen{}}
	waitStarted("replacement consumer")

	bd.stateChannel <- &ramqp10.StateChanged{From: &ramqp10.StateOpen{}, To: &ramqp10.StateClosed{}}
	select {
	case err := <-result:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("subscription did not stop when the connection closed")
	}
	select {
	case <-second.closeCalled:
	default:
		t.Fatal("replacement consumer was not closed")
	}
	assert.Equal(t, 0, bd.subscriptions.Length())
	connection.AssertExpectations(t)
}

func Test_SubscriptionRecoveryAttemptsReset(t *testing.T) {
	failingConsumer := func() *subscriptionConsumerStub {
		return &subscriptionConsumerStub{
			closeCalled: make(chan struct{}),
			receive: func(context.Context) (ramqp10.IDeliveryContext, error) {
				return nil, errors.New("link detached")
			},
		}
	}
	source := &pb.Source{Name: "orders", Type: pb.Source_QUEUE, Address: &pb.Address{Name: "orders", Type: pb.Address_TOPIC}}

	t.Run("consumer that fails immediately keeps counting", func(t *testing.T) {
		subscription, _ := newTestSubscription(t, context.Background(), source, failingConsumer(), nil)
		subscription.recoveryAttempts = 3

		state, _ := subscription.running()

		assert.Equal(t, subscribeStateRecovering, state)
		assert.Equal(t, 3, subscription.recoveryAttempts)
	})

	t.Run("consumer that was stable starts a fresh count", func(t *testing.T) {
		old := consumerStableDuration
		consumerStableDuration = 0
		t.Cleanup(func() { consumerStableDuration = old })
		subscription, _ := newTestSubscription(t, context.Background(), source, failingConsumer(), nil)
		subscription.recoveryAttempts = 3

		state, _ := subscription.running()

		assert.Equal(t, subscribeStateRecovering, state)
		assert.Equal(t, 0, subscription.recoveryAttempts)
		assert.NotNil(t, subscription.lastRecoveryError)
	})

	t.Run("connection event is not counted as a failure", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		consumer := &subscriptionConsumerStub{
			closeCalled: make(chan struct{}),
			receive: func(ctx context.Context) (ramqp10.IDeliveryContext, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}
		subscription, _ := newTestSubscription(t, ctx, source, consumer, nil)
		subscription.recoveryAttempts = 4
		subscription.lastRecoveryError = &subscribeStateTransitionError{msg: "old"}
		result := make(chan subscribeState, 1)
		go func() {
			state, _ := subscription.running()
			result <- state
		}()
		time.Sleep(20 * time.Millisecond)

		subscription.notify(subscribeStateRecovering, 1)

		assert.Equal(t, subscribeStateRecovering, <-result)
		assert.Equal(t, 0, subscription.recoveryAttempts)
		assert.Nil(t, subscription.lastRecoveryError)
	})
}
