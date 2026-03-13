// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp091

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type streamConnectionMock struct {
	mock.Mock
	blockConnect time.Duration
}

func (m *streamConnectionMock) Connect() error {
	args := m.Called()
	if m.blockConnect > 0 {
		time.Sleep(m.blockConnect * time.Second)
	}
	return args.Error(0)
}

func (m *streamConnectionMock) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *streamConnectionMock) IsClosed() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *streamConnectionMock) GetPublisher(_ string, _ string, confirm bool) streamPublisherShim {
	args := m.Called(confirm)
	ret := args.Get(0)
	if ret == nil {
		return nil
	}
	return ret.(streamPublisherShim)
}

func (m *streamConnectionMock) PutPublisher(confirm bool, _ streamPublisherShim) {
	m.Called(confirm)
}

func (m *streamConnectionMock) NewConsumer(streamName string, consumerName string, offset string,
	handler stream.MessagesHandler, singleActive bool) (streamConsumerShim, error) {
	args := m.Called(streamName, consumerName, offset, handler, singleActive)
	addr := stockAddress()
	addr.Name = streamName
	cCtx := stockStreamConsumerContext()
	sMsg := stockAmqp10Message(stockMessage(addr))
	handler(cCtx, &sMsg)
	// Wait for handler() to send the message back
	time.Sleep(500 * time.Millisecond)
	ret := args.Get(0)
	if ret == nil {
		return nil, args.Error(1)
	}
	return ret.(*streamConsumerMock), args.Error(1)
}

func (m *streamConnectionMock) DeclareStream(_ string, _ int64) error {
	args := m.Called()
	return args.Error(0)
}

func (m *streamConnectionMock) GetPublisherName() string {
	return ""
}

func (m *streamConnectionMock) GetLastOffset(streamName string, clientName string) (int64, error) {
	args := m.Called(streamName, clientName)
	return int64(args.Int(0)), args.Error(1)
}

func (m *streamConnectionMock) StoreOffset(streamName string, consumerName string, offset int64) error {
	args := m.Called(streamName, consumerName, offset)
	return args.Error(0)
}

type streamPublisherMock struct {
	mock.Mock
}

func (m *streamPublisherMock) Publish(arg1 streamMessage) error {
	args := m.Called(arg1)
	return args.Error(0)
}

func (m *streamPublisherMock) GetPCChannel() chan streamMessageResponseShim {
	args := m.Called()
	ret := args.Get(0)
	if ret == nil {
		return nil
	}
	return ret.(chan streamMessageResponseShim)
}

func (m *streamPublisherMock) GetPublisherName() string {
	return ""
}

func (m *streamPublisherMock) GetStreamName() string {
	args := m.Called()
	return args.Get(0).(string)
}

func (m *streamPublisherMock) Close() error {
	args := m.Called()
	return args.Error(0)
}

type streamConsumerMock struct {
	mock.Mock
}

func (m *streamConsumerMock) Close() error {
	args := m.Called()
	return args.Error(0)
}

type streamMessageResponseShimMock struct {
	confirmed bool
	msg       message.StreamMessage
	err       error
	pubID     int64
}

func (mrs streamMessageResponseShimMock) IsConfirmed() bool {
	return mrs.confirmed
}
func (mrs streamMessageResponseShimMock) GetPublishingId() int64 { //nolint:revive
	return mrs.pubID
}
func (mrs streamMessageResponseShimMock) GetError() error {
	return mrs.err
}
func (mrs streamMessageResponseShimMock) GetMessage() message.StreamMessage {
	return mrs.msg
}

func stockStreamMessage(msg *pb.Message) streamMessage {
	expectedMsg := streamMessage{}
	expectedMsg.Body = msg.GetBody()
	expectedMsg.Headers = make(map[string]string)
	expectedMsg.Headers["Content-Type"] = msg.Headers["Content-Type"]
	expectedMsg.Headers["Content-Encoding"] = msg.Headers["Content-Encoding"]
	expectedMsg.Headers["Additional-Header"] = msg.Headers["Additional-Header"]

	return expectedMsg
}

func stockAmqp10Message(msg *pb.Message) amqp.Message {
	sMsg := amqp.Message{}
	sMsg.Data = [][]byte{msg.GetBody()}
	return sMsg
}

func stockMessageConfirm(orig streamMessage) message.StreamMessage {
	newMsg := toStreamMessage(orig)

	appProps := newMsg.GetApplicationProperties()
	props := newMsg.GetMessageProperties()
	amqpMessage := &amqp.Message{Properties: props, ApplicationProperties: appProps, Data: newMsg.GetData()}
	v := reflect.ValueOf(newMsg).Elem()
	fieldValue := v.FieldByName("message")
	fieldValue = reflect.NewAt(fieldValue.Type(), unsafe.Pointer(fieldValue.UnsafeAddr())).Elem()
	fieldValue.Set(reflect.ValueOf(amqpMessage))

	return newMsg
}

func stockStreamConsumerContext() stream.ConsumerContext {
	con := &stream.Consumer{}
	setFieldValue(con, "currentOffset", int64(5))
	newMux := new(sync.Mutex)
	setFieldValue(con, "mutex", newMux)
	return stream.ConsumerContext{Consumer: con}
}

func setFieldValue(target any, fieldName string, value any) {
	rv := reflect.ValueOf(target)
	for rv.Kind() == reflect.Ptr && !rv.IsNil() {
		rv = rv.Elem()
	}
	if !rv.CanAddr() {
		panic("target must be addressable")
	}
	if rv.Kind() != reflect.Struct {
		panic(fmt.Sprintf(
			"unable to set the '%s' field value of the type %T, target must be a struct",
			fieldName,
			target,
		))
	}
	rf := rv.FieldByName(fieldName)

	reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).Elem().Set(reflect.ValueOf(value))
}

func Test_PublishStream(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	address := stockAddress()
	address.Type = pb.Address_STREAM
	msg := stockMessage(address)
	expectedMsg := stockStreamMessage(msg)

	pmock := &streamPublisherMock{}
	pmock.On("Publish", expectedMsg).Return(nil)
	smock := &streamConnectionMock{}
	smock.On("Connect").Return(nil)
	smock.On("IsClosed").Return(false)
	smock.On("GetPublisher", false).Return(pmock)
	smock.On("PutPublisher", false)

	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewStreamConn = oldNewStreamConn
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	go func() {
		suberr := prov.PublishOne(ctx, msg)
		assert.Nil(t, suberr)
	}()

	time.Sleep(100 * time.Millisecond)

	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
	amock.AssertExpectations(t)
}

func Test_PublishStreamWithConfirm(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	address := stockAddress()
	address.Type = pb.Address_STREAM
	msg := stockMessage(address)
	msg.Confirm = true
	expectedMsg := stockStreamMessage(msg)

	confirmMsg := stockMessageConfirm(expectedMsg)
	confirmMsg.SetPublishingId(500)
	resp := streamMessageResponseShimMock{
		msg:       confirmMsg,
		confirmed: true,
		pubID:     500,
		err:       nil,
	}

	pcChan := make(chan streamMessageResponseShim, 1)
	pcChan <- resp
	pmock := &streamPublisherMock{}
	pmock.On("Publish", expectedMsg).Return(nil)
	pmock.On("GetPCChannel").Return(pcChan)
	smock := &streamConnectionMock{}
	smock.On("Connect").Return(nil)
	smock.On("IsClosed").Return(false)
	smock.On("GetPublisher", true).Return(pmock)
	smock.On("PutPublisher", true)

	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewStreamConn = oldNewStreamConn
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	go func() {
		suberr := prov.PublishOne(ctx, msg)
		assert.Nil(t, suberr)
	}()

	time.Sleep(100 * time.Millisecond)

	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
	amock.AssertExpectations(t)
}

func Test_PublishStreamFailed(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	address := stockAddress()
	address.Type = pb.Address_STREAM
	msg := stockMessage(address)
	expectedMsg := stockStreamMessage(msg)

	pmock := &streamPublisherMock{}
	pmock.On("Publish", expectedMsg).Return(errors.New("failed"))
	smock := &streamConnectionMock{}
	smock.On("Connect").Return(nil)
	smock.On("IsClosed").Return(false)
	smock.On("GetPublisher", false).Return(pmock)
	smock.On("PutPublisher", false).Return(nil)

	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewStreamConn = oldNewStreamConn
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	suberr := prov.PublishOne(ctx, msg)
	assert.Equal(t, "failed", suberr.Message)

	time.Sleep(100 * time.Millisecond)

	amock.AssertExpectations(t)
	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
}

func Test_PublishStreamFailedConn(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	address := stockAddress()
	address.Type = pb.Address_STREAM
	msg := stockMessage(address)

	pmock := &streamPublisherMock{}
	smock := &streamConnectionMock{}
	smock.On("Connect").Return(errors.New("Failed connection"))

	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewStreamConn = oldNewStreamConn
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	suberr := prov.PublishOne(ctx, msg)
	assert.Equal(t, "failed to create stream connection to broker: Failed connection", suberr.Message)

	time.Sleep(100 * time.Millisecond)

	amock.AssertExpectations(t)
	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
}

func Test_PublishStreamNotConn(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	address := stockAddress()
	address.Type = pb.Address_STREAM
	msg := stockMessage(address)

	pmock := &streamPublisherMock{}
	smock := &streamConnectionMock{}
	smock.On("Connect").Return(nil)
	smock.On("IsClosed").Return(true)

	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewStreamConn = oldNewStreamConn
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	suberr := prov.PublishOne(ctx, msg)
	assert.Equal(t, "connection to broker is closed", suberr.Message)

	time.Sleep(100 * time.Millisecond)

	amock.AssertExpectations(t)
	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
}

func Test_SubscribeStream(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}
	smock := &streamConnectionMock{}
	smock.On("Connect").Return(nil)
	smock.On("IsClosed").Return(false)
	smock.On("DeclareStream").Return(nil)

	subjects := make([]string, 0)
	subjects = append(subjects, "subject1")
	address := stockAddress()
	address.Type = pb.Address_STREAM
	src := &pb.Source{Name: "srcname",
		Address:       address,
		Options:       map[string]string{"Offset": "0", "MessageTTL": "500"},
		Type:          pb.Source_STREAM,
		PrefetchCount: 4}

	pmock := &streamConsumerMock{}
	pmock.On("Close").Return(nil)
	smock.On("NewConsumer", src.GetName(), src.GetName(), src.GetOptions()["Offset"], mock.Anything, mock.AnythingOfType("bool")).Return(pmock, nil)
	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	cmock := &amqpChannelMock{}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)
	amock.On("Close").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
		NewStreamConn = oldNewStreamConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)
	var msg *pb.Message

	mc := make(chan *pb.Message)
	defer close(mc)

	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg = <-mc

	err = prov.Ack(ctx, msg.GetUuid())
	assert.NotNil(t, msg)
	assert.Nil(t, err)
	assert.NotNil(t, msg.GetAddress())
	assert.Equal(t, msg.GetAddress(), src.GetAddress())
	assert.Equal(t, msg.GetAddress().GetSubjects(), subjects)

	prov.Disconnect(ctx)
	cancel()
	time.Sleep(1000 * time.Millisecond)

	amock.AssertExpectations(t)
	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
	cmock.AssertExpectations(t)
}

func Test_SubscribeStreamBadOpt(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}
	smock := &streamConnectionMock{}

	pmock := &streamConsumerMock{}
	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	cmock := &amqpChannelMock{}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)
	amock.On("Close").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
		NewStreamConn = oldNewStreamConn
	}()

	address := stockAddress()
	address.Type = pb.Address_STREAM
	src := &pb.Source{Name: "srcname",
		Address:       address,
		Options:       map[string]string{"Offset": "0", "BadOpt": "true"},
		Type:          pb.Source_STREAM,
		PrefetchCount: 4}

	cc := &pb.ConnectionConfiguration{}
	ctx, cancel := context.WithCancel(context.Background())
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	mc := make(chan *pb.Message)
	defer close(mc)

	suberr := prov.Subscribe(ctx, src, mc)
	assert.Equal(t, "streams do not support the following source options: [BadOpt]", suberr.Message)

	prov.Disconnect(ctx)
	cancel()
	amock.AssertExpectations(t)
	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
	cmock.AssertExpectations(t)
}

func Test_streamSubscribe(t *testing.T) {
	prov := NewAMQP091Provider().(*amqp091provider)

	origNewAmqpConn091 := NewAmqpConn091
	origNewStreamConn := NewStreamConn
	origGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	defer func() {
		GetClientIdentifier = origGetClientIdentifier
		NewAmqpConn091 = origNewAmqpConn091
		NewStreamConn = origNewStreamConn
	}()

	address := stockAddress()
	address.Type = pb.Address_STREAM
	source := &pb.Source{Name: "srcname",
		Address:       address,
		Options:       map[string]string{"Offset": "0"},
		Type:          pb.Source_STREAM,
		PrefetchCount: 4}

	var subTests = []struct {
		singleActiveConsumerEnabled bool
		consumerGroupCalled         string
		consumerGroupOpt            string
		returnError                 string
	}{
		{false, source.GetName(), "", ""},
		{true, "myConsumerGroup", "myConsumerGroup", ""},
		{true, "", "", "no ConsumerGroup option set"},
	}
	for _, subt := range subTests {
		src := source
		src.SingleActiveConsumer = subt.singleActiveConsumerEnabled

		if subt.singleActiveConsumerEnabled {
			src.Options["ConsumerGroup"] = subt.consumerGroupOpt
		}

		bd := &BrokerDetails{}
		bd.activeMessages = util.NewConcurrentMap()
		ctx, cancel := context.WithCancel(context.Background())

		mc := make(chan *pb.Message)
		go func() {
			<-mc
		}()
		defer close(mc)

		errs := make(chan amqp091Error)

		amock := &amqpConnectionMock{}
		amock.On("Connect").Return(nil)
		amock.On("IsClosed").Return(false)
		amock.On("Close").Return(nil)
		amock.On("NotifyClose").Return(errs)
		NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
			return amock
		}

		pmock := &streamConsumerMock{}
		pmock.On("Close").Return(nil)

		smock := &streamConnectionMock{}
		smock.On("Connect").Return(nil)
		smock.On("IsClosed").Return(false)
		smock.On("DeclareStream").Return(nil)
		smock.On("NewConsumer", src.GetName(), subt.consumerGroupCalled, "0", mock.Anything, mock.AnythingOfType("bool")).Return(pmock, nil)

		NewStreamConn = func(string, string, *tls.Config) streamConnectionShim {
			return smock
		}

		go func() {
			time.Sleep(1 * time.Second)
			cancel()
		}()

		err := prov.streamSubscribe(ctx, bd, src, mc)

		if subt.returnError == "" {
			assert.Nil(t, err)
		} else {
			assert.NotNil(t, err)
			assert.Contains(t, err.GetMessage(), subt.returnError)
		}
	}
}

func Test_SubscribeStreamAutoDeleteOrExclusive(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}
	smock := &streamConnectionMock{}

	pmock := &streamConsumerMock{}
	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	cmock := &amqpChannelMock{}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)
	amock.On("Close").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
		NewStreamConn = oldNewStreamConn
	}()

	address := stockAddress()
	address.Type = pb.Address_STREAM
	src := &pb.Source{Name: "srcname",
		Address:       address,
		Options:       map[string]string{"Offset": "0"},
		Exclusive:     false,
		AutoDelete:    true,
		Type:          pb.Source_STREAM,
		PrefetchCount: 4}

	cc := &pb.ConnectionConfiguration{}
	ctx, cancel := context.WithCancel(context.Background())
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	mc := make(chan *pb.Message)
	defer close(mc)

	suberr := prov.Subscribe(ctx, src, mc)
	assert.Equal(t, "streams do not support AutoDelete or Exclusive", suberr.Message)

	src.Exclusive = true
	suberr = prov.Subscribe(ctx, src, mc)
	assert.Equal(t, "streams do not support AutoDelete or Exclusive", suberr.Message)

	src.AutoDelete = false
	suberr = prov.Subscribe(ctx, src, mc)
	assert.Equal(t, "streams do not support AutoDelete or Exclusive", suberr.Message)

	prov.Disconnect(ctx)
	cancel()
	amock.AssertExpectations(t)
	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
	cmock.AssertExpectations(t)
}

func Test_SubscribeStreamInvalidTTL(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}
	smock := &streamConnectionMock{}
	smock.On("Connect").Return(nil)
	smock.On("IsClosed").Return(false)

	pmock := &streamConsumerMock{}
	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	cmock := &amqpChannelMock{}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)
	amock.On("Close").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
		NewStreamConn = oldNewStreamConn
	}()

	address := stockAddress()
	address.Type = pb.Address_STREAM
	src := &pb.Source{Name: "srcname",
		Address:       address,
		Options:       map[string]string{"Offset": "0", "MessageTTL": "true"},
		Type:          pb.Source_STREAM,
		PrefetchCount: 4}

	cc := &pb.ConnectionConfiguration{}
	ctx, cancel := context.WithCancel(context.Background())
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	mc := make(chan *pb.Message)
	defer close(mc)

	suberr := prov.Subscribe(ctx, src, mc)
	assert.Equal(t, "value for MessageTTL option must be a quoted integer", suberr.Message)

	prov.Disconnect(ctx)
	cancel()
	amock.AssertExpectations(t)
	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
	cmock.AssertExpectations(t)
}

func Test_SubscribeStreamFailedConn(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}
	smock := &streamConnectionMock{}
	smock.On("Connect").Return(errors.New("Failed Connection"))

	pmock := &streamConsumerMock{}
	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	cmock := &amqpChannelMock{}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)
	amock.On("Close").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
		NewStreamConn = oldNewStreamConn
	}()

	address := stockAddress()
	address.Type = pb.Address_STREAM
	src := &pb.Source{Name: "srcname",
		Address:       address,
		Options:       map[string]string{"Offset": "0"},
		Type:          pb.Source_STREAM,
		PrefetchCount: 4}

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	mc := make(chan *pb.Message)
	defer close(mc)

	suberr := prov.Subscribe(ctx, src, mc)
	assert.Equal(t, "failed to create stream connection to broker: Failed Connection", suberr.Message)

	prov.Disconnect(ctx)
	amock.AssertExpectations(t)
	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
	cmock.AssertExpectations(t)
}

func Test_SubscribeStreamNotConn(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}
	smock := &streamConnectionMock{}
	smock.On("Connect").Return(nil)
	smock.On("IsClosed").Return(true)

	pmock := &streamConsumerMock{}
	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	cmock := &amqpChannelMock{}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)
	amock.On("Close").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
		NewStreamConn = oldNewStreamConn
	}()

	address := stockAddress()
	address.Type = pb.Address_STREAM
	src := &pb.Source{Name: "srcname",
		Address:       address,
		Options:       map[string]string{"Offset": "0"},
		Type:          pb.Source_STREAM,
		PrefetchCount: 4}

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	mc := make(chan *pb.Message)
	defer close(mc)

	suberr := prov.Subscribe(ctx, src, mc)
	assert.Equal(t, "connection to broker is closed", suberr.Message)

	prov.Disconnect(ctx)
	amock.AssertExpectations(t)
	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
	cmock.AssertExpectations(t)
}

func Test_SubscribeStreamFailedDeclare(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}
	smock := &streamConnectionMock{}
	smock.On("Connect").Return(nil)
	smock.On("IsClosed").Return(false)
	smock.On("DeclareStream").Return(errors.New("Failed stream declare"))

	pmock := &streamConsumerMock{}
	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	cmock := &amqpChannelMock{}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)
	amock.On("Close").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
		NewStreamConn = oldNewStreamConn
	}()

	address := stockAddress()
	address.Type = pb.Address_STREAM
	src := &pb.Source{Name: "srcname",
		Address:       address,
		Options:       map[string]string{"Offset": "0", "MessageTTL": "500"},
		Type:          pb.Source_STREAM,
		PrefetchCount: 4}

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	mc := make(chan *pb.Message)
	defer close(mc)

	suberr := prov.Subscribe(ctx, src, mc)
	assert.Equal(t, "failed to declare stream: Failed stream declare", suberr.GetMessage())
	assert.True(t, suberr.GetIsFatal())

	prov.Disconnect(ctx)
	cancel()
	time.Sleep(500 * time.Millisecond)

	amock.AssertExpectations(t)
	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
	cmock.AssertExpectations(t)
}

func Test_StreamRetry(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	address := stockAddress()
	address.Type = pb.Address_STREAM
	src := &pb.Source{Name: "srcname",
		Address:       address,
		Options:       map[string]string{"Offset": "0", "MessageTTL": "500"},
		Type:          pb.Source_STREAM,
		PrefetchCount: 4}

	smock := &streamConnectionMock{}
	smock.On("Connect").Return(nil)
	smock.On("IsClosed").Return(false)
	smock.On("DeclareStream").Return(nil)

	pmock := &streamConsumerMock{}
	pmock.On("Close").Return(nil)
	smock.On("NewConsumer", src.GetName(), src.GetName(), src.GetOptions()["Offset"], mock.Anything, mock.AnythingOfType("bool")).Return(pmock, nil)
	oldNewStreamConn := NewStreamConn
	NewStreamConn = func(string, string, *tls.Config) streamConnectionShim {
		return smock
	}

	cmock := &amqpChannelMock{}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)
	amock.On("Close").Return(nil)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
		NewStreamConn = oldNewStreamConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)
	var msg *pb.Message

	mc := make(chan *pb.Message)
	defer close(mc)

	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg = <-mc

	assert.NotNil(t, msg)
	err = prov.Retry(ctx, src, msg.GetUuid(), 1)
	assert.Nil(t, err)

	prov.Disconnect(ctx)
	cancel()
	time.Sleep(1000 * time.Millisecond)

	amock.AssertExpectations(t)
	pmock.AssertExpectations(t)
	smock.AssertExpectations(t)
	cmock.AssertExpectations(t)
}

func Test_Subscribe_Stream_DeclareOnly(t *testing.T) {
	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	oldNewAmqpConn091 := NewAmqpConn091

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	var declareOnlyTests = []struct {
		declareError error
	}{
		{nil},
		{errors.New("declareError")},
	}

	for _, dot := range declareOnlyTests {
		t.Run(fmt.Sprintf("DeclareOnlyTests declareError %s", dot.declareError),
			func(t *testing.T) {
				prov := NewAMQP091Provider()

				addr := &pb.Address{Subjects: []string{"routingkey"}, Name: "address", Type: pb.Address_STREAM}
				src := &pb.Source{Address: addr, Name: "queue", Type: pb.Source_STREAM, DeclareOnly: true}

				smock := &streamConnectionMock{}
				smock.On("Connect").Return(nil)
				smock.On("IsClosed").Return(false)
				smock.On("DeclareStream").Return(dot.declareError)

				oldNewStreamConn := NewStreamConn
				NewStreamConn = func(string, string, *tls.Config) streamConnectionShim {
					return smock
				}

				amock := &amqpConnectionMock{}
				amock.On("Connect").Return(nil)

				errs := make(chan amqp091Error)
				amock.On("NotifyClose").Return(errs)

				NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
					return amock
				}

				defer func() {
					NewStreamConn = oldNewStreamConn
				}()

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				cc := &pb.ConnectionConfiguration{}
				err := prov.Connect(ctx, cc, false)
				assert.Nil(t, err)

				mc := make(chan *pb.Message)
				defer close(mc)

				err = prov.Subscribe(ctx, src, mc)
				if dot.declareError == nil {
					assert.Nil(t, err)
				} else {
					assert.NotNil(t, err)
				}
				smock.AssertExpectations(t)
				amock.AssertExpectations(t)
			})
	}
}
