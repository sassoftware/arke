// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp091

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sassoftware/arke/i18n"
	"github.com/sassoftware/arke/internal/util"
	"github.com/stretchr/testify/mock"
)

// amqp091ConnectionShim Shim so we can do unit testing
type amqp091ConnectionShim interface {
	Connect() error
	Close() error
	IsClosed() bool
	NewChannel(bool) (amqp091ChannelShim, error)
	NotifyClose(chan amqp091Error) chan amqp091Error
}

// amqp091ChannelShim Shim so we can do unit testing
type amqp091ChannelShim interface {
	Close() error
	IsClosed() bool
	Publish(string, string, amqp091Message) error
	ExchangeDeclare(string, string, bool) error
	ExchangeBind(string, string, string) error
	SetPrefetch(int) error
	QueueDeclare(string, bool, bool, amqp091Table) error
	QueueBind(string, string, string, amqp091Table) error
	Consume(string, bool, bool) (<-chan amqp091Message, error)
	NotifyClose(chan amqp091Error) chan amqp091Error
	ensureChannel() error
}

// amqp091Connection A connection to the broker
type amqp091Connection struct {
	amqp091ConnectionShim //nolint:unused
	connection            *amqp.Connection
	connStr               string
	tlsCfg                *tls.Config
	clientIdentifier      string
	channelLock           sync.Mutex
}

// amqp091Channel A channel
type amqp091Channel struct {
	amqp091ChannelShim //nolint:unused
	channel            *amqp.Channel
	connection         *amqp091Connection
	channelLock        sync.Mutex
	prefetch           int
	confirm            bool
}

// amqp091Error Error
type amqp091Error struct {
	error amqp.Error
}

// amqp091Message Structure of a message
type amqp091Message struct {
	delivery        interface{}
	Body            []byte
	DeliveryMode    int
	ContentType     string
	ContentEncoding string
	Headers         amqp091Table
	DeliveryTag     uint64
}

// amqp091Table Simple map
type amqp091Table map[string]interface{}

// Connect Connect to the broker
func (ac *amqp091Connection) Connect() error {
	cfg := amqpConfig(ac.clientIdentifier, ac.tlsCfg)

	conn, err := amqp.DialConfig(ac.connStr, cfg)
	if err != nil {
		return err
	}
	ac.connection = conn
	return nil
}

// NewChannel Create a new channel on the connection
func (ac *amqp091Connection) NewChannel(confirm bool) (amqp091ChannelShim, error) {
	ac.channelLock.Lock()
	defer ac.channelLock.Unlock()
	ch, err := ac.connection.Channel()
	if err != nil {
		return nil, err
	}
	if confirm {
		err := ch.Confirm(false)
		if err != nil {
			util.Logger.Debugf("Error setting confirm on channel : %v", err)
			return nil, err
		}
	}
	ach := &amqp091Channel{channel: ch, connection: ac, confirm: confirm}
	return ach, nil
}

// NotifyClose Channel to receive connection close notifications on
func (ac *amqp091Connection) NotifyClose(rec chan amqp091Error) chan amqp091Error {
	amqpErrors := ac.connection.NotifyClose(make(chan *amqp.Error, cap(rec)))

	go func() {
		defer func() {
			if err := recover(); err != nil {
				return
			}
		}()
		for {
			select {
			case amqpErr := <-amqpErrors:
				var err amqp091Error
				if amqpErr != nil {
					err = amqp091Error{*amqpErr}
				}
				util.Logger.Debugf("Received notify for client %v : %v", ac.clientIdentifier, err)
				select {
				case rec <- err:
					return
				default:
					return
				}
			case <-rec:
				util.Logger.Debugf("Received rec notify for client %v", ac.clientIdentifier)
				// this should theoretically happen only if the subscribe function
				// sends a message on the rec channel while we are waiting
				// for actual errors from amqpErrors
				return
			}
		}
	}()
	return rec
}

// Close Close the connection to the broker
func (ac *amqp091Connection) Close() error {
	return ac.connection.Close()
}

// IsClosed Is the connection to the broker still open
func (ac *amqp091Connection) IsClosed() bool {
	return ac.connection.IsClosed()
}

// Close Close the channel
func (ch *amqp091Channel) Close() error {
	return ch.channel.Close()
}

func (ch *amqp091Channel) IsClosed() bool {
	return ch.channel.IsClosed()
}

func (ch *amqp091Channel) ensureChannel() error {
	ch.channelLock.Lock()
	defer ch.channelLock.Unlock()
	if ch.channel.IsClosed() {
		newCh, err := ch.connection.NewChannel(ch.confirm)
		if err != nil {
			util.Logger.Debug(i18n.EnsureChannelError, ch.connection.clientIdentifier, err)
			return err
		}
		ch.channel = newCh.(*amqp091Channel).channel
	}

	if ch.prefetch > 0 {
		return ch.channel.Qos(ch.prefetch, 0, false)
	}

	return nil
}

// ExchangeDeclare Declare a new exchange
func (ch *amqp091Channel) ExchangeDeclare(addressName, exchangeType string, autoDelete bool) error {
	err := ch.ensureChannel()
	if err != nil {
		return err
	}

	// RabbitMQ has issues with transient queues, therefore we always set Durable to true PSGO-649
	return ch.channel.ExchangeDeclare(addressName, exchangeType, true, autoDelete, false, false, nil)
}

// ExchangeBind Bind an exchange to another exchange
func (ch *amqp091Channel) ExchangeBind(addressName, subject, parentName string) error {
	err := ch.ensureChannel()
	if err != nil {
		return err
	}

	return ch.channel.ExchangeBind(addressName, subject, parentName, false, nil)
}

// QueueDeclare Create a queue
func (ch *amqp091Channel) QueueDeclare(name string, autoDelete, exclusive bool, args amqp091Table) error {
	err := ch.ensureChannel()
	if err != nil {
		return err
	}

	// RabbitMQ has issues with transient queues, therefore we always pass Durable=true (PSGO-649)
	_, err = ch.channel.QueueDeclare(name, true, autoDelete, exclusive, false, toAmqpTable(args))
	return err
}

// SetPrefetch Sets quality of service on the channel
func (ch *amqp091Channel) SetPrefetch(prefetchCount int) error {
	ch.prefetch = prefetchCount

	return ch.ensureChannel()
}

// QueueBind Binds an queue to an exchange with subject/arguments
func (ch *amqp091Channel) QueueBind(name, subject, destination string, args amqp091Table) error {
	err := ch.ensureChannel()
	if err != nil {
		return err
	}

	return ch.channel.QueueBind(name, subject, destination, true, toAmqpTable(args))
}

// Consume Consume messages from a queue
func (ch *amqp091Channel) Consume(subject string, autoAck, exclusive bool) (<-chan amqp091Message, error) {
	err := ch.ensureChannel()
	if err != nil {
		return nil, err
	}

	delChan, err := ch.channel.Consume(subject, "", autoAck, exclusive, false, false, nil)

	if err != nil {
		return nil, err
	}

	msgChan := make(chan amqp091Message, 10)

	go func() {
		for del := range delChan {
			msgChan <- fromAmqpMessage(del)
		}
	}()
	return msgChan, nil
}

// Publish Publish a message to an exchange, if msg.Confirm is set we wait for publish confirmation
func (ch *amqp091Channel) Publish(addressName, subject string, msg amqp091Message) error {
	err := ch.ensureChannel()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if !ch.confirm {
		return ch.channel.PublishWithContext(ctx, addressName, subject, false, false, toAmqpMessage(&msg))
	}

	dc, pErr := ch.channel.PublishWithDeferredConfirmWithContext(ctx, addressName, subject, false, false, toAmqpMessage(&msg))
	if pErr != nil {
		return pErr
	}
	if dc == nil {
		util.Logger.Debugf("Requested publish confirmation but was denied %v", ch.connection.clientIdentifier)
		return nil
	}
	// Wait for confirmation from the server
	recv := dc.Wait()
	if !recv {
		util.Logger.Debugf("Publish confirmation failed %v", ch.connection.clientIdentifier)
		return errors.New("Publish confirmation failed")
	}
	return nil
}

// NotifyClose be notified of deleted queues, or channel closures
func (ch *amqp091Channel) NotifyClose(rec chan amqp091Error) chan amqp091Error {
	cancelErrors := ch.channel.NotifyCancel(make(chan string, cap(rec)))
	closeErrors := ch.channel.NotifyClose(make(chan *amqp.Error, cap(rec)))

	go func() {
		defer func() {
			if err := recover(); err != nil {
				return
			}
		}()
		for {
			select {
			case amqpErr := <-cancelErrors:
				var err amqp091Error
				if amqpErr != "" {
					err.error = amqp.Error{Reason: amqpErr}
				}
				util.Logger.Debugf("Received channel cancel notify for client %v : %v", ch.connection.clientIdentifier, err)
				select {
				case rec <- err:
					return
				default:
					return
				}
			case amqpErr := <-closeErrors:
				var err amqp091Error
				if amqpErr != nil {
					err = amqp091Error{*amqpErr}
				}
				util.Logger.Debugf("Received channel close notify for client %v : %v", ch.connection.clientIdentifier, err)
				select {
				case rec <- err:
					return
				default:
					return
				}
			case <-rec:
				util.Logger.Debugf("Received channel rec notify for client %v", ch.connection.clientIdentifier)
				// this should theoretically happen only if the subscribe function
				// sends a message on the rec channel while we are waiting
				return
			}
		}
	}()
	return rec
}

// Ack Ack a message
func (msg *amqp091Message) Ack() error {
	// For unit testing
	switch msg.delivery.(type) { //nolint:gocritic
	case amqp.Delivery:
		return msg.delivery.(amqp.Delivery).Ack(false)
	case *mock.Mock:
		args := msg.delivery.(*mock.Mock).Called()
		return args.Error(0)
	}
	return nil
}

// Nack Nack a message
func (msg *amqp091Message) Nack(requeue bool) error {
	// For unit testing
	switch msg.delivery.(type) { //nolint:gocritic
	case amqp.Delivery:
		return msg.delivery.(amqp.Delivery).Nack(false, requeue)
	case *mock.Mock:
		args := msg.delivery.(*mock.Mock).Called(false, requeue)
		return args.Error(0)
	}
	return nil
}

// Error Error
func (e *amqp091Error) Error() string {
	return e.error.Error()
}

// Code Error code from amqp.Error
func (e *amqp091Error) Code() int {
	return e.error.Code
}

// newAmqp091Error New error
func newAmqp091Error(e string, code int) amqp091Error {
	err := amqp091Error{}
	err.error = amqp.Error{Reason: e, Code: code}
	return err
}

// SetDelivery convenience method for unit tests
func (msg *amqp091Message) SetDelivery(delivery interface{}) {
	msg.delivery = delivery
}
