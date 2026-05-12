// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp091

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	// "github.com/NeowayLabs/wabbit/amqptest/server"
	amqp "github.com/rabbitmq/amqp091-go"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var ctx context.Context
var cf *pb.ConnectionConfiguration

const testTenant = "tenant"
const testQueueTypeClassic = "classic"
const testQueueTypeQuorum = "quorum"
const testDeadLetterAddress = "dla"
const testContentTypeJSON = "application/json"
const testContentEncodingText = "text"
const testXMatchAny = "any"

func init() {
	// Register the MockProvider with the Provider factory.

	// Setup our tests
	ctx = context.Background()
	cf = &pb.ConnectionConfiguration{}
}

type amqpConnectionMock struct {
	mock.Mock
	amqp091ConnectionShim //nolint:unused
	blockConnect          time.Duration
}

func (m *amqpConnectionMock) Connect() error {
	args := m.Called()
	if m.blockConnect > 0 {
		time.Sleep(m.blockConnect * time.Second)
	}
	return args.Error(0)
}

func (m *amqpConnectionMock) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *amqpConnectionMock) IsClosed() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *amqpConnectionMock) NewChannel(confirm bool) (amqp091ChannelShim, error) {
	args := m.Called(confirm)
	ret := args.Get(0)
	if ret == nil {
		return nil, args.Error(1)
	}
	return ret.(amqp091ChannelShim), args.Error(1)
}

func (m *amqpConnectionMock) NotifyClose(chan amqp091Error) chan amqp091Error {
	args := m.Called()
	return args.Get(0).(chan amqp091Error)
}

type amqpChannelMock struct {
	mock.Mock
	amqp091ChannelShim
}

func (m *amqpChannelMock) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *amqpChannelMock) Publish(arg1 string, arg2 string, arg3 amqp091Message) error {
	args := m.Called(arg1, arg2, arg3)
	return args.Error(0)
}
func (m *amqpChannelMock) ExchangeDeclare(arg1 string, arg2 string, arg4 bool) error {
	args := m.Called(arg1, arg2, arg4)
	return args.Error(0)
}
func (m *amqpChannelMock) ExchangeBind(arg1 string, arg2 string, arg3 string) error {
	args := m.Called(arg1, arg2, arg3)
	return args.Error(0)
}
func (m *amqpChannelMock) SetPrefetch(arg1 int) error {
	args := m.Called(arg1)
	return args.Error(0)
}
func (m *amqpChannelMock) QueueDeclare(arg1 string, arg2 bool, arg3 bool, arg4 amqp091Table) error {
	args := m.Called(arg1, arg2, arg3, arg4)
	return args.Error(0)
}

func (m *amqpChannelMock) QueueBind(arg1 string, arg2 string, arg3 string, arg4 amqp091Table) error {
	args := m.Called(arg1, arg2, arg3, arg4)
	return args.Error(0)
}
func (m *amqpChannelMock) Consume(arg1 string, arg2 bool, arg3 bool) (<-chan amqp091Message, error) {
	args := m.Called(arg1, arg2, arg3)
	mc := args.Get(0).(chan amqp091Message)
	return mc, args.Error(1)
}

func (m *amqpChannelMock) NotifyClose(chan amqp091Error) chan amqp091Error {
	args := m.Called()
	return args.Get(0).(chan amqp091Error)
}

func (m *amqpChannelMock) IsClosed() bool {
	args := m.Called()
	return args.Bool(0)
}

func TestNewAMQP091Provider(t *testing.T) {
	prov := NewAMQP091Provider()
	assert.NotNil(t, prov)
}

func TestConnect(t *testing.T) {
	prov := NewAMQP091Provider()
	assert.NotNil(t, prov)

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
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
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)

	assert.Nil(t, err)

	amock.AssertExpectations(t)
}

func TestConnect_Error(t *testing.T) {
	prov := NewAMQP091Provider()
	assert.NotNil(t, prov)

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(errors.New("error"))
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)

	assert.NotNil(t, err)

	amock.AssertExpectations(t)
}

func Test_Connect_NoClient(t *testing.T) {
	prov := NewAMQP091Provider()
	assert.NotNil(t, prov)

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "", errors.New("noclient")
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)

	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "noclient")
}

func TestConnect_TLS_SkipVerify(t *testing.T) {
	prov := NewAMQP091Provider()
	assert.NotNil(t, prov)

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
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
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	cc.Tls = true
	err := prov.Connect(ctx, cc, true)

	assert.Nil(t, err)

	amock.AssertExpectations(t)
}

func TestConnect_TLS_WithCert(t *testing.T) {
	prov := NewAMQP091Provider()
	assert.NotNil(t, prov)

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
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
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	cc.Tls = true
	cc.CaCertificate = []byte("asdf")
	err := prov.Connect(ctx, cc, false)

	assert.Nil(t, err)

	amock.AssertExpectations(t)
	// TODO: Figure out a good way to get tlsConfig and see if the cert is set
}

func TestConnect_Stats(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
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
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	stats := prov.Stats()
	assert.Equal(t, len(stats.Clients), 1)
	client := stats.Clients[0]
	assert.Equal(t, client.Streams, 0)
	assert.Equal(t, client.ActiveMessages, 0)
	assert.Equal(t, client.Produced, 0)
	assert.Equal(t, client.Consumed, 0)
	assert.Equal(t, client.ID, "1234")

	amock.AssertExpectations(t)
}

func setupProviderWithSimpleConn(t *testing.T) (provider.Provider, context.Context, func()) {
	t.Helper()
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	cleanup := func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
		amock.AssertExpectations(t)
	}
	return prov, ctx, cleanup
}

func Test_Ack_NoMsg(t *testing.T) {
	prov, ctx, cleanup := setupProviderWithSimpleConn(t)
	defer cleanup()

	msg := pb.Message{}
	err := prov.Ack(ctx, msg.GetUuid())
	assert.Contains(t, err.GetMessage(), "No message with uuid")
}

func Test_Ack_AckErr(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan amqp091Message)
	defer close(msgs)
	delMock := mock.Mock{}
	go func(dMock *mock.Mock) {
		mm := amqp091Message{}
		mm.DeliveryTag = 1
		dMock.On("Ack").Return(errors.New("ackErr"))
		mm.SetDelivery(dMock)

		msgs <- mm
	}(&delMock)

	cancels := make(chan amqp091Error)
	cmock.On("NotifyClose").Return(cancels)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(msgs, nil)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel", false).Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	msrv := mockManagementRequestServer()
	defer msrv.Close()
	u, serr := url.Parse(msrv.URL)
	assert.Nil(t, serr)

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	cc.Tenant = testTenant
	cc.Host = u.Hostname()
	i, _ := strconv.Atoi(u.Port())
	cc.AdminPort = int32(i) //nolint:gosec

	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	subjects := make([]string, 0)
	subjects = append(subjects, "#")
	src := &pb.Source{Name: "queue", Address: &pb.Address{Name: "address", Subjects: subjects}}
	mc := make(chan *pb.Message)
	defer close(mc)

	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc

	err = prov.Ack(ctx, msg.GetUuid())
	assert.NotNil(t, msg)
	assert.NotNil(t, err)
	assert.Equal(t, "ackErr", err.GetMessage())

	cancel()
	time.Sleep(100 * time.Millisecond)

	delMock.AssertExpectations(t)
	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
	cmock.AssertNumberOfCalls(t, "ExchangeDeclare", 1)
}

func Test_Nack_NoMsg(t *testing.T) {
	prov, ctx, cleanup := setupProviderWithSimpleConn(t)
	defer cleanup()

	msg := pb.Message{}
	err := prov.Nack(ctx, msg.GetUuid())
	assert.Contains(t, err.GetMessage(), "No message with uuid")
}

func Test_Retry_NoMsg(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
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
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)
	msg := pb.Message{}
	err = prov.Retry(ctx, &pb.Source{}, msg.GetUuid(), 10)
	assert.Contains(t, err.GetMessage(), "No message with uuid")

	amock.AssertExpectations(t)
}

func Test_DeadLetter_NoMsg(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
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
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)
	msg := pb.Message{}
	err = prov.DeadLetter(ctx, &pb.Source{}, msg.GetUuid())
	assert.Contains(t, err.GetMessage(), "message not found in active messages")

	amock.AssertExpectations(t)
}

func Test_Ack(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan amqp091Message)
	defer close(msgs)
	delMock := mock.Mock{}
	go func(dMock *mock.Mock) {
		mm := amqp091Message{}
		mm.DeliveryTag = 1
		dMock.On("Ack").Return(nil)
		mm.SetDelivery(dMock)

		msgs <- mm
	}(&delMock)

	cancels := make(chan amqp091Error)
	cmock.On("NotifyClose").Return(cancels)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(msgs, nil)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel", false).Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()
	msrv := mockManagementRequestServer()
	defer msrv.Close()
	u, serr := url.Parse(msrv.URL)
	assert.Nil(t, serr)

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	cc.Tenant = testTenant
	cc.Host = u.Hostname()
	i, _ := strconv.Atoi(u.Port())
	cc.AdminPort = int32(i) //nolint:gosec

	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	subjects := make([]string, 0)
	subjects = append(subjects, "#")
	src := &pb.Source{Name: "queue", Address: &pb.Address{Name: "address", Subjects: subjects}}
	mc := make(chan *pb.Message)
	defer close(mc)

	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc

	err = prov.Ack(ctx, msg.GetUuid())
	assert.NotNil(t, msg)
	assert.Nil(t, err)

	cancel()
	time.Sleep(100 * time.Millisecond)

	delMock.AssertExpectations(t)
	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
	cmock.AssertNumberOfCalls(t, "ExchangeDeclare", 1)
}

func Test_Nack(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan amqp091Message)
	defer close(msgs)
	delMock := mock.Mock{}
	go func(dMock *mock.Mock) {
		mm := amqp091Message{}
		mm.DeliveryTag = 1
		dMock.On("Nack", false, false).Return(nil)
		mm.SetDelivery(dMock)

		msgs <- mm
	}(&delMock)

	cancels := make(chan amqp091Error)
	cmock.On("NotifyClose").Return(cancels)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(msgs, nil)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel", false).Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()
	msrv := mockManagementRequestServer()
	defer msrv.Close()
	u, serr := url.Parse(msrv.URL)
	assert.Nil(t, serr)

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	cc.Tenant = testTenant
	cc.Host = u.Hostname()
	i, _ := strconv.Atoi(u.Port())
	cc.AdminPort = int32(i) //nolint:gosec

	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	subjects := make([]string, 0)
	subjects = append(subjects, "#")
	src := &pb.Source{Name: "queue", Address: &pb.Address{Name: "address", Subjects: subjects}}
	mc := make(chan *pb.Message)
	defer close(mc)
	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc

	err = prov.Nack(ctx, msg.GetUuid())
	assert.NotNil(t, msg)
	assert.Nil(t, err)

	cancel()
	time.Sleep(100 * time.Millisecond)

	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
	cmock.AssertNumberOfCalls(t, "ExchangeDeclare", 1)
}

func Test_Nack_NackErr(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan amqp091Message)
	defer close(msgs)
	delMock := mock.Mock{}
	go func(dMock *mock.Mock) {
		mm := amqp091Message{}
		mm.DeliveryTag = 1
		dMock.On("Nack", false, false).Return(errors.New("nackErr"))
		mm.SetDelivery(dMock)

		msgs <- mm
	}(&delMock)

	cancels := make(chan amqp091Error)
	cmock.On("NotifyClose").Return(cancels)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(msgs, nil)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel", false).Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	msrv := mockManagementRequestServer()
	defer msrv.Close()
	u, serr := url.Parse(msrv.URL)
	assert.Nil(t, serr)

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	cc.Tenant = testTenant
	cc.Host = u.Hostname()
	i, _ := strconv.Atoi(u.Port())
	cc.AdminPort = int32(i) //nolint:gosec

	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	subjects := make([]string, 0)
	subjects = append(subjects, "#")
	src := &pb.Source{Name: "queue", Address: &pb.Address{Name: "address", Subjects: subjects}}
	mc := make(chan *pb.Message)
	defer close(mc)
	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc

	err = prov.Nack(ctx, msg.GetUuid())
	assert.NotNil(t, msg)
	assert.NotNil(t, err)
	assert.Equal(t, "nackErr", err.GetMessage())

	cancel()
	time.Sleep(100 * time.Millisecond)

	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
	cmock.AssertNumberOfCalls(t, "ExchangeDeclare", 1)
}

func Test_Retry(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan amqp091Message)
	defer close(msgs)
	delMock := mock.Mock{}
	mm := amqp091Message{}
	mm.DeliveryTag = 1
	delMock.On("Ack").Return(nil)
	mm.SetDelivery(&delMock)
	go func() {
		msgs <- mm
	}()

	mm.Headers = amqp091Table{
		retryCountHeaderName: 1,
	}

	cancels := make(chan amqp091Error)
	cmock.On("NotifyClose").Return(cancels)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(msgs, nil)
	cmock.On("Publish", mock.Anything, mock.Anything, mm).Return(nil)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel", false).Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	msrv := mockManagementRequestServer()
	defer msrv.Close()
	u, serr := url.Parse(msrv.URL)
	assert.Nil(t, serr)

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	cc.Tenant = testTenant
	cc.Host = u.Hostname()
	i, _ := strconv.Atoi(u.Port())
	cc.AdminPort = int32(i) //nolint:gosec

	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	src := &pb.Source{Name: "queue", Address: &pb.Address{Name: "address"}}
	mc := make(chan *pb.Message)
	defer close(mc)
	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc
	retErr := prov.Retry(ctx, src, msg.GetUuid(), 1)
	assert.Nil(t, retErr)

	cancel()
	time.Sleep(100 * time.Millisecond)

	delMock.AssertExpectations(t)
	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
	cmock.AssertNumberOfCalls(t, "ExchangeDeclare", 2)
}

func Test_RetryFailure(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan amqp091Message)
	defer close(msgs)
	delMock := mock.Mock{}
	go func(dMock *mock.Mock) {
		mm := amqp091Message{}
		mm.DeliveryTag = 1
		dMock.On("Nack", false, true).Return(nil)
		mm.SetDelivery(dMock)

		msgs <- mm
	}(&delMock)

	cancels := make(chan amqp091Error)
	cmock.On("NotifyClose").Return(cancels)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(msgs, nil)
	cmock.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("puberr"))

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel", false).Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	msrv := mockManagementRequestServer()
	defer msrv.Close()
	u, serr := url.Parse(msrv.URL)
	assert.Nil(t, serr)

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	cc.Tenant = testTenant
	cc.Host = u.Hostname()
	i, _ := strconv.Atoi(u.Port())
	cc.AdminPort = int32(i) //nolint:gosec

	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	src := &pb.Source{Name: "queue", Address: &pb.Address{Name: "address"}}
	mc := make(chan *pb.Message)
	defer close(mc)
	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc
	retErr := prov.Retry(ctx, src, msg.GetUuid(), 1)
	assert.Nil(t, retErr)

	cancel()
	time.Sleep(100 * time.Millisecond)

	delMock.AssertExpectations(t)
	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
	cmock.AssertNumberOfCalls(t, "ExchangeDeclare", 2)
}

func Test_RetryFailure_NoBrokerDetails(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "", errors.New("no client identifier")
	}
	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
	}()

	retErr := prov.Retry(ctx, nil, "", 1)
	assert.NotNil(t, retErr)
	assert.Contains(t, retErr.GetMessage(), "no client identifier")
}
func Test_RetryFailure_DeclareErrorsStillSuccess(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan amqp091Message)
	defer close(msgs)
	delMock := mock.Mock{}
	go func(dMock *mock.Mock) {
		mm := amqp091Message{}
		mm.DeliveryTag = 1
		dMock.On("Ack").Return(nil)
		mm.SetDelivery(dMock)

		msgs <- mm
	}(&delMock)

	cancels := make(chan amqp091Error)
	cmock.On("NotifyClose").Return(cancels)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	cmock.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("err")).Once()
	cmock.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cmock.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(msgs, nil)
	cmock.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel", false).Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	msrv := mockManagementRequestServer()
	defer msrv.Close()
	u, serr := url.Parse(msrv.URL)
	assert.Nil(t, serr)

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	cc.Tenant = testTenant
	cc.Host = u.Hostname()
	i, _ := strconv.Atoi(u.Port())
	cc.AdminPort = int32(i) //nolint:gosec

	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	opts := make(map[string]string)
	opts["junkheader"] = "junkvalue"
	src := &pb.Source{Name: "queue", Address: &pb.Address{Name: "address"}}
	mc := make(chan *pb.Message)
	defer close(mc)
	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc
	src.Options = opts
	retErr := prov.Retry(ctx, src, msg.GetUuid(), 1)
	assert.Nil(t, retErr)

	cancel()
	time.Sleep(100 * time.Millisecond)

	delMock.AssertExpectations(t)
	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
	cmock.AssertNumberOfCalls(t, "ExchangeDeclare", 2)
}

func Test_updateRetryCountHeader(t *testing.T) {
	tests := map[string]struct {
		msg      amqp091Message
		expected int32
	}{
		"empty message": {
			msg:      amqp091Message{},
			expected: 1,
		},
		"no header": {
			msg: amqp091Message{
				Headers: amqp091Table{},
			},
			expected: 1,
		},
		"non-integer header": {
			msg: amqp091Message{
				Headers: amqp091Table{
					retryCountHeaderName: "not-an-int",
				},
			},
			expected: 1,
		},
		"convert int32 header": {
			msg: amqp091Message{
				Headers: amqp091Table{
					retryCountHeaderName: int32(3),
				},
			},
			expected: 4,
		},
		"fail to convert plain int header": {
			msg: amqp091Message{
				Headers: amqp091Table{
					retryCountHeaderName: 3,
				},
			},
			expected: 1,
		},
		"fail to convert string numeric header": {
			msg: amqp091Message{
				Headers: amqp091Table{
					retryCountHeaderName: "3",
				},
			},
			expected: 1,
		},
		"fail to convert uint32 header": {
			msg: amqp091Message{
				Headers: amqp091Table{
					retryCountHeaderName: uint32(3),
				},
			},
			expected: 1,
		},
		"fail to convert convert float64 header": {
			msg: amqp091Message{
				Headers: amqp091Table{
					retryCountHeaderName: 3.0,
				},
			},
			expected: 1,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			updateRetryCountHeader(&tc.msg)
			if val, ok := tc.msg.Headers[retryCountHeaderName]; !ok {
				t.Errorf("Expected header %s to be set", retryCountHeaderName)
			} else {
				if intVal, ok := val.(int32); !ok {
					t.Errorf("Expected header %s to be of type int", retryCountHeaderName)
				} else if intVal != tc.expected {
					t.Errorf("Expected header %s to be %d, got %d", retryCountHeaderName, tc.expected, intVal)
				}
			}
		})
	}
}

func Test_DLQ(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan amqp091Message)
	delMock := mock.Mock{}
	defer close(msgs)
	go func(dMock *mock.Mock) {
		mm := amqp091Message{}
		mm.DeliveryTag = 1
		dMock.On("Nack", false, false).Return(nil)
		mm.SetDelivery(dMock)

		msgs <- mm
	}(&delMock)

	argsDlq := make(amqp091Table)
	argsDlq["x-queue-type"] = testQueueTypeClassic
	args := make(amqp091Table)
	args["x-dead-letter-exchange"] = testDeadLetterAddress
	args["x-queue-type"] = testQueueTypeQuorum
	cancels := make(chan amqp091Error)
	cmock.On("NotifyClose").Return(cancels)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", "address", "topic", false).Return(nil).Once()
	cmock.On("ExchangeDeclare", "dla", "topic", false).Return(nil).Once()
	cmock.On("QueueDeclare", "queue.quorum", false, false, args).Return(nil).Once()
	cmock.On("QueueDeclare", "queue.dlq", false, false, argsDlq).Return(nil).Once()
	cmock.On("QueueBind", "queue.quorum", "routingkey", "address", mock.Anything).Return(nil).Once()
	cmock.On("QueueBind", "queue.dlq", "queue.quorum", "dla", mock.Anything).Return(nil).Once()
	cmock.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(msgs, nil)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel", false).Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	msrv := mockManagementRequestServer()
	defer msrv.Close()
	u, serr := url.Parse(msrv.URL)
	assert.Nil(t, serr)

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	cc.Tenant = testTenant
	cc.Host = u.Hostname()
	i, _ := strconv.Atoi(u.Port())
	cc.AdminPort = int32(i) //nolint:gosec

	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	subjects := make([]string, 0)
	subjects = append(subjects, "routingkey")
	options := map[string]string{"DeadLetterAddress": testDeadLetterAddress}
	src := &pb.Source{Name: "queue", Address: &pb.Address{Name: "address", Subjects: subjects},
		Options: options}
	mc := make(chan *pb.Message)
	defer close(mc)
	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc

	dlErr := prov.DeadLetter(ctx, src, msg.GetUuid())
	assert.Nil(t, dlErr)

	cancel()
	time.Sleep(100 * time.Millisecond)

	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
	delMock.AssertExpectations(t)
}

func Test_Ack_NoConnect(t *testing.T) {
	prov := NewAMQP091Provider()
	ctx := context.Background()
	msg := pb.Message{}
	err := prov.Ack(ctx, msg.GetUuid())
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "could not retrieve client-id from context")
}

func Test_Nack_NoConnect(t *testing.T) {
	prov := NewAMQP091Provider()
	ctx := context.Background()
	msg := pb.Message{}
	err := prov.Nack(ctx, msg.GetUuid())
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "could not retrieve client-id from context")
}
func Test_Publish_NoConnect(t *testing.T) {
	prov := NewAMQP091Provider()
	ctx := context.Background()
	mc := make(chan *pb.Message)
	ec := make(chan *pb.Error)
	err := prov.Publish(ctx, mc, ec)
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "could not retrieve client-id from context")
}

func Test_Subscribe_NoConnect(t *testing.T) {
	prov := NewAMQP091Provider()
	ctx := context.Background()
	address := &pb.Address{Name: "addressName"}
	src := &pb.Source{Address: address}
	mc := make(chan *pb.Message)

	err := prov.Subscribe(ctx, src, mc)
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "could not retrieve client-id from context")
}
func Test_Subscribe_NoAddressName(t *testing.T) {
	prov := NewAMQP091Provider()
	ctx := context.Background()
	address := &pb.Address{}
	src := &pb.Source{Address: address}
	mc := make(chan *pb.Message)

	err := prov.Subscribe(ctx, src, mc)
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "address name not defined")
}

func Test_SourceNameNotQuorum(t *testing.T) {
	address := &pb.Address{}
	src := &pb.Source{Name: "myname", AutoDelete: true, Address: address}
	src.Name = sourceName(src)
	assert.Equal(t, "myname", src.GetName())
}

func Test_SourceNameQuorum(t *testing.T) {
	address := &pb.Address{}
	src := &pb.Source{Name: "myname", Address: address}
	src.Name = sourceName(src)
	assert.Equal(t, "myname.quorum", src.GetName())
}

func Test_SourceNameNotQuorum_AutoDelete(t *testing.T) {
	address := &pb.Address{}
	src := &pb.Source{Name: "myname", Address: address, AutoDelete: true}
	src.Name = sourceName(src)
	assert.Equal(t, "myname", src.GetName())
}

func Test_SourceNameNamedDotQuorum(t *testing.T) {
	address := &pb.Address{}
	src := &pb.Source{Name: "myname.quorum", Address: address}
	src.Name = sourceName(src)
	assert.Equal(t, "myname.quorum", src.GetName())
}

func Test_Subscribe_NoAddress(t *testing.T) {
	prov := NewAMQP091Provider()
	ctx := context.Background()
	src := &pb.Source{}
	mc := make(chan *pb.Message)

	err := prov.Subscribe(ctx, src, mc)
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "address name not defined")
}

func Test_Subscribe_Options(t *testing.T) {
	prov := NewAMQP091Provider()

	options := make(map[string]string)
	options["MessageTTL"] = "100"
	options["Expires"] = "100"
	options["DeadLetterAddress"] = "dla"
	options["DeadLetterSubject"] = "dls"

	expectedQueueArgs := amqp091Table{}
	expectedQueueArgs["x-message-ttl"] = 100
	expectedQueueArgs["x-expires"] = 100
	expectedQueueArgs["x-dead-letter-exchange"] = testDeadLetterAddress
	expectedQueueArgs["x-dead-letter-routing-key"] = "dls"
	expectedQueueArgs["x-queue-type"] = testQueueTypeClassic

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan amqp091Message)
	defer close(msgs)
	delMock := mock.Mock{}
	go func(dMock *mock.Mock) {
		mm := amqp091Message{}
		mm.ContentType = testContentTypeJSON
		mm.ContentEncoding = testContentEncodingText
		mm.Headers = make(amqp091Table)
		mm.Headers["something"] = "somethingelse"
		mm.DeliveryTag = 1
		dMock.On("Ack").Return(nil)
		mm.SetDelivery(dMock)

		msgs <- mm
	}(&delMock)

	subjects := make([]string, 0)
	subjects = append(subjects, "subject1")
	subjects = append(subjects, "subject2")
	parent := &pb.Address{Name: "parent", Type: pb.Address_QUEUE}
	address := &pb.Address{Name: "addressname", Subjects: subjects, ParentAddress: parent, Type: pb.Address_FILTER}
	matches1 := make([]*pb.Match, 0)
	matches1 = append(matches1, &pb.Match{Name: "key1", Value: "value1"})
	matches1 = append(matches1, &pb.Match{Name: "key2", Value: "value2"})
	matches2 := make([]*pb.Match, 0)
	matches2 = append(matches2, &pb.Match{Name: "key3", Value: "value3"})
	matches2 = append(matches2, &pb.Match{Name: "key4", Value: "value4"})
	filters := make([]*pb.Filter, 0)
	filters = append(filters, &pb.Filter{Matches: matches1, Type: pb.Filter_ANY})
	filters = append(filters, &pb.Filter{Matches: matches2, Type: pb.Filter_ANY})

	src := &pb.Source{Name: "srcname",
		Address:       address,
		Options:       options,
		Filters:       filters,
		Exclusive:     true,
		AutoDelete:    true,
		PrefetchCount: 4}

	expectedMatchHeaders1 := amqp091Table{}
	expectedMatchHeaders1["key1"] = "value1"
	expectedMatchHeaders1["key2"] = "value2"
	expectedMatchHeaders1["x-match"] = testXMatchAny

	expectedMatchHeaders2 := amqp091Table{}
	expectedMatchHeaders2["key3"] = "value3"
	expectedMatchHeaders2["key4"] = "value4"
	expectedMatchHeaders2["x-match"] = testXMatchAny

	cmock.On("SetPrefetch", 4).Return(nil)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", address.GetName(), "headers", address.GetAutoDelete()).Return(nil).Once()
	cmock.On("ExchangeDeclare", parent.GetName(), "direct", parent.GetAutoDelete()).Return(nil).Once()
	cmock.On("ExchangeDeclare", "dla", "topic", false).Return(nil).Once()
	cmock.On("ExchangeBind", address.GetName(), subjects[0], parent.GetName()).Return(nil)
	cmock.On("ExchangeBind", address.GetName(), subjects[1], parent.GetName()).Return(nil)
	cmock.On("QueueDeclare", src.GetName(), false, false, expectedQueueArgs).Return(nil)
	cmock.On("QueueDeclare", "srcname.dlq", false, false, amqp091Table{"x-queue-type": testQueueTypeClassic}).Return(nil)
	cmock.On("QueueBind", src.GetName(), "subject1", address.GetName(), expectedMatchHeaders1).Return(nil).Once()
	cmock.On("QueueBind", src.GetName(), "subject1", address.GetName(), expectedMatchHeaders2).Return(nil).Once()
	cmock.On("QueueBind", src.GetName(), "subject2", address.GetName(), expectedMatchHeaders1).Return(nil).Once()
	cmock.On("QueueBind", src.GetName(), "subject2", address.GetName(), expectedMatchHeaders2).Return(nil).Once()
	cmock.On("QueueBind", "srcname.dlq", "dls", "dla", mock.Anything).Return(nil).Once()
	cmock.On("Consume", src.GetName(), false, src.GetExclusive()).Return(msgs, nil)
	cancels := make(chan amqp091Error)
	cmock.On("NotifyClose").Return(cancels)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel", false).Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	msrv := mockManagementRequestServer()
	defer msrv.Close()
	u, serr := url.Parse(msrv.URL)
	assert.Nil(t, serr)

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	cc.Tenant = testTenant
	cc.Host = u.Hostname()
	i, _ := strconv.Atoi(u.Port())
	cc.AdminPort = int32(i) //nolint:gosec

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

	cancel()
	time.Sleep(100 * time.Millisecond)

	delMock.AssertExpectations(t)
	cmock.AssertExpectations(t)
	cmock.AssertNumberOfCalls(t, "ExchangeDeclare", 3)
}

func Test_Subscribe_NoSubjectsNoFilters(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan amqp091Message)
	defer close(msgs)
	delMock := mock.Mock{}
	go func(dMock *mock.Mock) {
		mm := amqp091Message{}
		mm.ContentType = testContentTypeJSON
		mm.ContentEncoding = testContentEncodingText
		mm.Headers = make(amqp091Table)
		mm.Headers["something"] = "somethingelse"
		mm.DeliveryTag = 1
		dMock.On("Ack").Return(nil)
		mm.SetDelivery(dMock)

		msgs <- mm
	}(&delMock)

	subjects := make([]string, 0)
	address := &pb.Address{Name: "addressname", Subjects: subjects, Type: pb.Address_FILTER}
	filters := make([]*pb.Filter, 0)

	src := &pb.Source{Name: "srcname",
		Address:       address,
		Filters:       filters,
		Exclusive:     true,
		AutoDelete:    false,
		PrefetchCount: 4}

	expectedQueueArgs := amqp091Table{}
	expectedQueueArgs["x-queue-type"] = testQueueTypeQuorum
	expectedQueueArgs["x-expires"] = 300000

	cmock.On("SetPrefetch", 4).Return(nil)
	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", address.GetName(), "headers", address.GetAutoDelete()).Return(nil).Once()
	cmock.On("QueueDeclare", src.GetName()+".quorum", false, false, expectedQueueArgs).Return(nil)
	cmock.On("Consume", src.GetName()+".quorum", false, src.GetExclusive()).Return(msgs, nil)
	cancels := make(chan amqp091Error)
	cmock.On("NotifyClose").Return(cancels)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel", false).Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	msrv := mockManagementRequestServer()
	defer msrv.Close()
	u, serr := url.Parse(msrv.URL)
	assert.Nil(t, serr)

	ctx, cancel := context.WithCancel(context.Background())
	cc := &pb.ConnectionConfiguration{}
	cc.Tenant = testTenant
	cc.Host = u.Hostname()
	i, _ := strconv.Atoi(u.Port())
	cc.AdminPort = int32(i) //nolint:gosec

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

	cancel()
	time.Sleep(100 * time.Millisecond)

	delMock.AssertExpectations(t)
	cmock.AssertExpectations(t)
	cmock.AssertNumberOfCalls(t, "ExchangeDeclare", 1)
	// With not subjects and no filters we should NOT
	// call QueueBind
	cmock.AssertNumberOfCalls(t, "QueueBind", 0)
}

func Test_Subscribe_UnsupportedOptions(t *testing.T) {
	prov := NewAMQP091Provider()

	options := make(map[string]string)
	options["unsupported"] = "100"

	expectedOptions := make(map[string]interface{})
	expectedOptions["x-message-ttl"] = 100

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}
	msgs := make(chan amqp091Message)
	defer close(msgs)

	cmock.On("Close").Return(nil)
	cmock.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel", false).Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	src := &pb.Source{Address: &pb.Address{Name: "addressname"}, Options: options}
	mc := make(chan *pb.Message)
	defer close(mc)

	suberr := prov.Subscribe(ctx, src, mc)
	assert.NotNil(t, suberr)
	assert.Contains(t, suberr.GetMessage(), "unsupported is an unsupported source option")

	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
}

// Disconnect does not return anything so there isn't much to test
func Test_Disconnect(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
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
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)
	prov.Disconnect(ctx)

	amock.AssertExpectations(t)
}

func Test_SupportedSourceOptions(t *testing.T) {
	prov := NewAMQP091Provider()
	opts := prov.SupportedSourceOptions()
	assert.NotNil(t, opts)
	expected := make(map[string]bool)
	expected["MessageTTL"] = true
	expected["DeadLetterAddress"] = true
	expected["DeadLetterSubject"] = true
	expected["Expires"] = true
	expected["Offset"] = true
	expected["ConsumerGroup"] = true

	assert.Equal(t, opts, expected)
}

func Test_WaitForConnect(t *testing.T) {
	prov := NewAMQP091Provider()
	ctx := context.Background()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &amqpConnectionMock{blockConnect: 1}
	amock.On("Connect").Return(nil)
	amock.On("Connect").Return(nil)
	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)
	errs <- newAmqp091Error("chanerr", 1) // simulate an error
	time.Sleep(500 * time.Millisecond)
	connected := prov.WaitForConnect(ctx)
	assert.True(t, connected)

	amock.AssertExpectations(t)
}

func Test_Publish(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	address := stockAddress()
	msg := stockMessage(address)
	expectedMsg := stockAmqpMessage(msg)

	cmock := &amqpChannelMock{}
	cmock.On("Publish", address.GetName(), address.GetSubjects()[0], expectedMsg).Return(nil)
	cmock.On("ExchangeDeclare", address.GetName(), "headers", address.GetAutoDelete()).Return(nil).Once()
	chanerrs := make(chan amqp091Error)
	cmock.On("NotifyClose").Return(chanerrs)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel", false).Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	mc := make(chan *pb.Message)
	errchan := make(chan *pb.Error)

	go func() {
		mc <- msg
	}()

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
		// close(mc)
		// close(errchan)
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	go func() {
		suberr := prov.Publish(ctx, mc, errchan)
		assert.Nil(t, suberr)
	}()

	time.Sleep(100 * time.Millisecond)

	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
}

func Test_Publish_Error(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	address := stockAddress()

	cmock := &amqpChannelMock{}
	cmock.On("Publish", address.GetName(), address.GetSubjects()[0], mock.Anything).Return(errors.New("puberr"))
	cmock.On("ExchangeDeclare", address.GetName(), "headers", address.GetAutoDelete()).Return(nil).Once()
	chanerrs := make(chan amqp091Error)
	cmock.On("NotifyClose").Return(chanerrs)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel", false).Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	mc := make(chan *pb.Message)
	errchan := make(chan *pb.Error)

	msg := stockMessage(address)

	go func() {
		mc <- msg
	}()

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	go func() {
		prov.Publish(ctx, mc, errchan)
	}()

	time.Sleep(100 * time.Millisecond)

	err = <-errchan
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "puberr")

	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
}

func Test_PublishOne(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
	}()

	var publishOneTests = []struct {
		confirm bool
	}{
		{false},
		{true},
	}

	for _, pot := range publishOneTests {
		address := stockAddress()
		msg := stockMessage(address)
		msg.Confirm = pot.confirm
		expectedMsg := stockAmqpMessage(msg)

		cmock := &amqpChannelMock{}
		cmock.On("Publish", address.GetName(), address.GetSubjects()[0], expectedMsg).Return(nil)
		cmock.On("ExchangeDeclare", address.GetName(), "headers", address.GetAutoDelete()).Return(nil).Once()
		cmock.On("IsClosed").Return(false)
		amock := &amqpConnectionMock{}
		amock.On("Connect").Return(nil)
		amock.On("IsClosed").Return(false)

		errs := make(chan amqp091Error)
		amock.On("NotifyClose").Return(errs)
		amock.On("NewChannel", pot.confirm).Return(cmock, nil)
		oldNewAmqpConn091 := NewAmqpConn091
		NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
			return amock
		}

		ctx := context.Background()
		cc := &pb.ConnectionConfiguration{}
		err := prov.Connect(ctx, cc, false)
		assert.Nil(t, err)

		suberr := prov.PublishOne(ctx, msg)

		NewAmqpConn091 = oldNewAmqpConn091

		assert.Nil(t, suberr)

		time.Sleep(100 * time.Millisecond)

		cmock.AssertExpectations(t)
		amock.AssertExpectations(t)
	}
}

func Test_PublishOneFailed(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	address := stockAddress()
	msg := stockMessage(address)
	expectedMsg := stockAmqpMessage(msg)

	cmock := &amqpChannelMock{}
	cmock.On("Publish", address.GetName(), address.GetSubjects()[0], expectedMsg).Return(errors.New("puberr"))
	cmock.On("ExchangeDeclare", address.GetName(), "headers", address.GetAutoDelete()).Return(nil).Once()
	cmock.On("IsClosed").Return(false)
	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel", false).Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	suberr := prov.PublishOne(ctx, msg)
	assert.NotNil(t, suberr)
	assert.Equal(t, "puberr", suberr.GetMessage())

	time.Sleep(100 * time.Millisecond)

	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
}

func Test_PublishOneFailedNewChannel(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	address := stockAddress()
	msg := stockMessage(address)

	cmock := &amqpChannelMock{}
	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel", false).Return(nil, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	suberr := prov.PublishOne(ctx, msg)
	assert.NotNil(t, suberr)
	assert.Equal(t, "failed to get channel from pool", suberr.GetMessage())

	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
}

func Test_PublishOneFailedConnClosed(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	address := stockAddress()
	msg := stockMessage(address)

	cmock := &amqpChannelMock{}
	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(true)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	suberr := prov.PublishOne(ctx, msg)
	assert.NotNil(t, suberr)
	assert.Equal(t, "connection to broker is closed", suberr.GetMessage())

	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
}

func Test_PublishOneFailedNotConnected(t *testing.T) {
	prov := NewAMQP091Provider()

	address := stockAddress()
	msg := stockMessage(address)

	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		assert.Fail(t, "Call to NewAmqpConn091 not expected")
		return nil
	}

	defer func() {
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	suberr := prov.PublishOne(ctx, msg)
	assert.NotNil(t, suberr)
	assert.Equal(t, "could not retrieve client-id from context", suberr.GetMessage())
}

func Test_Publish_ErrorDeclareExchange(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	address := stockAddress()
	msg := stockMessage(address)
	expectedMsg := stockAmqpMessage(msg)

	cmock := &amqpChannelMock{}
	cmock.On("Publish", address.GetName(), address.GetSubjects()[0], expectedMsg).Return(nil)
	cmock.On("ExchangeDeclare", address.GetName(), "headers", address.GetAutoDelete()).Return(errors.New("declareerr")).Once()
	chanerrs := make(chan amqp091Error)
	cmock.On("NotifyClose").Return(chanerrs)

	amock := &amqpConnectionMock{}
	amock.On("Connect").Return(nil)
	amock.On("IsClosed").Return(false)

	errs := make(chan amqp091Error)
	amock.On("NotifyClose").Return(errs)
	amock.On("NewChannel", false).Return(cmock, nil)
	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return amock
	}

	mc := make(chan *pb.Message)
	errchan := make(chan *pb.Error)

	go func() {
		mc <- msg
	}()

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	go func() {
		prov.Publish(ctx, mc, errchan)
	}()

	time.Sleep(100 * time.Millisecond)

	err = <-errchan
	assert.Nil(t, err)

	cmock.AssertExpectations(t)
	amock.AssertExpectations(t)
}

func Test_newAmqp091Error(t *testing.T) {
	e := "myError"
	code := 123
	aErr := newAmqp091Error(e, code)
	assert.Equal(t, code, aErr.Code())
	assert.Equal(t, e, aErr.error.Reason)
}

func Test_fromAmqpMessage(t *testing.T) {
	del := amqp.Delivery{}
	del.Body = []byte("Hello")
	del.DeliveryMode = uint8(2)
	del.Headers = amqp.Table{"h1": "header1"}
	del.ContentType = testContentEncodingText
	del.ContentEncoding = "plain"
	del.DeliveryTag = 1

	aMsg := fromAmqpMessage(del)
	assert.Equal(t, del.Body, aMsg.Body)
	assert.Equal(t, int(del.DeliveryMode), aMsg.DeliveryMode)
	assert.Equal(t, del.Headers["h1"], aMsg.Headers["h1"])
	assert.Equal(t, del.ContentType, aMsg.ContentType)
	assert.Equal(t, del.ContentEncoding, aMsg.ContentEncoding)
	assert.Equal(t, del.DeliveryTag, aMsg.DeliveryTag)
}

func Test_toAmqpMessage(t *testing.T) {
	aMsg := &amqp091Message{}
	aMsg.Body = []byte("Hello")
	aMsg.DeliveryMode = 2
	aMsg.Headers = amqp091Table{"h1": "header1"}
	aMsg.ContentType = testContentEncodingText
	aMsg.ContentEncoding = "plain"

	del := toAmqpMessage(aMsg)
	assert.Equal(t, aMsg.Body, del.Body)
	assert.Equal(t, aMsg.DeliveryMode, int(del.DeliveryMode))
	assert.Equal(t, aMsg.Headers["h1"], del.Headers["h1"])
	assert.Equal(t, aMsg.ContentType, del.ContentType)
	assert.Equal(t, aMsg.ContentEncoding, del.ContentEncoding)
}

func Test_NewAmqp091Connection(t *testing.T) {
	c := NewAmqp091Connection("connStr", "identifier", nil).(*amqp091Connection)
	assert.Equal(t, "identifier", c.clientIdentifier)
	assert.Equal(t, "connStr", c.connStr)
	assert.Nil(t, c.tlsCfg)
}

func Test_amqpConfig(t *testing.T) {
	cfg := amqpConfig("connName", nil)
	assert.Equal(t, "connName", cfg.Properties["connection_name"])
	assert.Equal(t, 10*time.Second, cfg.Heartbeat)
	assert.Equal(t, "en_US", cfg.Locale)
	assert.Nil(t, cfg.TLSClientConfig)
}

func Test_SetDelivery(t *testing.T) {
	m := &amqp091Message{}
	m.SetDelivery(1)
	assert.Equal(t, 1, m.delivery)
}

func Test_ClientExists(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
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
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	exists := prov.ClientExists("1234")
	assert.True(t, exists)
}

func Test_ClientExists_false(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
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
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	exists := prov.ClientExists("4321")
	assert.False(t, exists)
}

func Test_getBrokerDetails_err(t *testing.T) {
	prov := NewAMQP091Provider().(*amqp091provider)

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}
	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
	}()

	ctx := context.Background()
	bd, err := prov.getBrokerDetails(ctx)
	assert.NotNil(t, bd)
	assert.NotNil(t, err)
	assert.Equal(t, "could not retrieve broker details for this connection: 1234", err.Error())
}

func Test_SetupDeadLetter_no_BD(t *testing.T) {
	prov := NewAMQP091Provider().(*amqp091provider)

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "", errors.New("nope")
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
	}()

	ctx := context.Background()
	opts := make(map[string]string)
	opts["DeadLetterAddress"] = testDeadLetterAddress
	src := &pb.Source{Options: opts}
	err := prov.setupDeadLetter(ctx, src)
	assert.NotNil(t, err)
}

func Test_SetupDeadLetter_channel_error(t *testing.T) {
	prov := NewAMQP091Provider().(*amqp091provider)

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	cmock := &amqpChannelMock{}

	amock := &amqpConnectionMock{}
	amock.On("NewChannel", false).Return(cmock, errors.New("chanerr"))

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
	}()

	bd := BrokerDetails{}
	bd.Connection = amock
	prov.connections.Add("1234", &bd)

	ctx := context.Background()
	opts := make(map[string]string)
	opts["DeadLetterAddress"] = testDeadLetterAddress
	src := &pb.Source{Options: opts}
	err := prov.setupDeadLetter(ctx, src)
	assert.NotNil(t, err)
	assert.Equal(t, err.GetMessage(), "chanerr")
	amock.AssertExpectations(t)
	cmock.AssertExpectations(t)
}

func Test_connect_clientDisconnect(t *testing.T) {
	bd := BrokerDetails{}
	bd.clientDisconnect = true
	ok, err := bd.connect()
	assert.False(t, ok)
	assert.Nil(t, err)
}

func Test_connect_connecting_connected(t *testing.T) {
	bd := BrokerDetails{}
	bd.state = provider.CONNECTING
	bd.clientDisconnect = false
	go func() {
		time.Sleep(1 * time.Second)
		bd.state = provider.CONNECTED
	}()
	ok, err := bd.connect()
	assert.True(t, ok)
	assert.Nil(t, err)
}

func Test_connect_connecting_closed(t *testing.T) {
	bd := BrokerDetails{}
	bd.state = provider.CONNECTING
	bd.clientDisconnect = false
	go func() {
		time.Sleep(1 * time.Second)
		bd.state = provider.CLOSED
	}()
	ok, err := bd.connect()
	assert.False(t, ok)
	assert.Nil(t, err)
}

func Test_connect_connecting_disconnected(t *testing.T) {
	bd := BrokerDetails{}
	bd.state = provider.CONNECTING
	bd.clientDisconnect = false

	msrv := mockManagementRequestServer()
	defer msrv.Close()
	u, err := url.Parse(msrv.URL)
	assert.Nil(t, err)
	i, _ := strconv.Atoi(u.Port())
	bd.connectionConfig = &pb.ConnectionConfiguration{
		Host:      u.Hostname(),
		Tenant:    testTenant,
		AdminPort: int32(i), //nolint:gosec
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
		NewAmqpConn091 = oldNewAmqpConn091
	}()

	go func() {
		time.Sleep(1 * time.Second)
		bd.state = provider.DISCONNECTED
	}()
	ok, err := bd.connect()
	assert.True(t, ok)
	assert.Nil(t, err)
	amock.AssertExpectations(t)
}

func mockManagementRequestServer() *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body []byte
		var status int
		bindingBody := []byte(`[{"source":"arke.test","vhost":"tenant","destination":"queue","destination_type":"queue","routing_key":"routingkey","arguments":{},"properties_key":"routingkey"}]`)
		switch r.Method {
		case "GET":
			body = bindingBody
			status = http.StatusOK
			// handle special cases here
			switch r.URL.Path {
			case "/api/queues/tenant/sourceQueue.quorum":
				status = http.StatusOK
				body = []byte(`{
					"messages": 10,
					"consumers": 5,
					"type": "quorum",
					"message_stats": {
						"publish_details": { "rate": 1.5 },
						"deliver_details": { "rate": 2.0 }
					}
			}`)
			case "/api/queues/tenant/sourceStream":
				status = http.StatusOK
				body = []byte(`{"messages": 9, "consumers": 4, "type": "stream"}`)
			case "/api/queues/tenant/sourceStream2":
				status = http.StatusOK
				body = []byte(`{
					"messages": 11,
					"consumers": 6,
					"type": "stream",
					"message_stats": {
						"publish_details": { "rate": 5.0 },
						"deliver_details": { "rate": 6.4 }
					}
				}`)
			case "/api/exchanges/%2f":
				status = http.StatusOK
				body = []byte(`[]`)
			}
		case "DELETE":
			status = http.StatusNoContent
		}
		// we must call WriteHeader before w.Write otherwise we get a log warning message
		// note this was previously suppressed with zlog
		w.WriteHeader(status)
		if body != nil {
			w.Write(body) //nolint:errcheck
		}
	}))
	return server
}

func setupCleanupBindingsTest(t *testing.T) (*BrokerDetails, *pb.Source) {
	t.Helper()
	bd := &BrokerDetails{}
	addr := &pb.Address{Subjects: []string{"routingkey"}, Name: "address"}
	src := &pb.Source{Address: addr, Name: "queue"}
	creds := &pb.Credentials{Username: "user", Password: "password"}
	bd.connectionConfig = &pb.ConnectionConfiguration{Credentials: creds}

	msrv := mockManagementRequestServer()
	t.Cleanup(msrv.Close)
	u, err := url.Parse(msrv.URL)
	assert.Nil(t, err)
	bd.connectionConfig.Host = u.Hostname()
	bd.connectionConfig.Tenant = testTenant
	i, _ := strconv.Atoi(u.Port())
	bd.connectionConfig.AdminPort = int32(i) //nolint:gosec
	return bd, src
}

func Test_cleanupBindings(t *testing.T) {
	bd, src := setupCleanupBindingsTest(t)
	removed := bd.cleanupBindings(src, []string{"routingkey2"})
	assert.Len(t, removed, 1)
}

func Test_cleanupBindings_none(t *testing.T) {
	bd, src := setupCleanupBindingsTest(t)
	removed := bd.cleanupBindings(src, []string{"routingkey"})
	assert.Len(t, removed, 0)
}

func Test_declareQueueAutoDelete(t *testing.T) {
	var autoDeleteTests = []struct {
		autoDelete bool
		exclusive  bool
		expires    int
	}{
		{true, false, 0},
		{true, false, int((5 * time.Minute).Milliseconds())},
		{false, true, 0},
	}

	for _, adt := range autoDeleteTests {
		t.Run(fmt.Sprintf("AutoDeleteTest autoDelete:%t, exclusive: %t, expires:%d",
			adt.autoDelete, adt.exclusive, adt.expires), func(t *testing.T) {
			bd := &BrokerDetails{
				knownQueues: util.NewConcurrentMap(),
			}
			addr := &pb.Address{Subjects: []string{"routingkey"}, Name: "address"}
			src := &pb.Source{Address: addr, Name: "queue", AutoDelete: adt.autoDelete, Exclusive: adt.exclusive}

			expectedArgs := make(amqp091Table)
			if adt.expires > 0 {
				expectedArgs["x-expires"] = adt.expires
			} else {
				expectedArgs["x-expires"] = int((5 * time.Minute).Milliseconds())
			}
			if adt.autoDelete {
				expectedArgs["x-queue-type"] = testQueueTypeClassic
			} else {
				expectedArgs["x-queue-type"] = testQueueTypeQuorum
			}

			cmock := &amqpChannelMock{}
			cmock.On("QueueDeclare", src.GetName(), false, false, expectedArgs).Return(nil)

			prov := NewAMQP091Provider().(*amqp091provider)
			err := prov.declareQueue(src, bd, cmock, false)
			assert.Nil(t, err)
			cmock.AssertExpectations(t)
		})
	}
}

func Test_singleActiveConsumer(t *testing.T) {
	var sacTests = []struct {
		singleActiveConsumer bool
	}{
		{true},
		{false},
	}

	for _, sac := range sacTests {
		t.Run(fmt.Sprintf("SingleActiveConsumerTest singleActiveConsumer:%t",
			sac.singleActiveConsumer), func(t *testing.T) {
			bd := &BrokerDetails{
				knownQueues: util.NewConcurrentMap(),
			}
			addr := &pb.Address{Subjects: []string{"routingkey"}, Name: "address"}
			src := &pb.Source{Address: addr, Name: "queue", SingleActiveConsumer: sac.singleActiveConsumer}

			expectedArgs := make(amqp091Table)
			expectedArgs["x-queue-type"] = testQueueTypeQuorum

			if sac.singleActiveConsumer {
				expectedArgs["x-single-active-consumer"] = true
			}

			cmock := &amqpChannelMock{}
			cmock.On("QueueDeclare", src.GetName(), false, false, expectedArgs).Return(nil)

			prov := NewAMQP091Provider().(*amqp091provider)
			err := prov.declareQueue(src, bd, cmock, false)
			assert.Nil(t, err)
			cmock.AssertExpectations(t)
		})
	}
}

func Test_Subscribe_Queue_DeclareOnly(t *testing.T) {
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
		channelError error
	}{
		{nil},
		{errors.New("channelError")},
	}

	for _, dot := range declareOnlyTests {
		t.Run(fmt.Sprintf("DeclareOnlyTests channelError %s", dot.channelError),
			func(t *testing.T) {
				prov := NewAMQP091Provider()

				addr := &pb.Address{Subjects: []string{"routingkey"}, Name: "address"}
				src := &pb.Source{Address: addr, Name: "queue", Type: pb.Source_QUEUE, DeclareOnly: true}

				cmock := &amqpChannelMock{}
				amock := &amqpConnectionMock{}

				if dot.channelError == nil {
					cmock.On("Close").Return(nil)
					cmock.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything).Return(nil)
					cmock.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
					cmock.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
				}

				amock.On("Connect").Return(nil)

				NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
					return amock
				}

				errs := make(chan amqp091Error)
				amock.On("NotifyClose").Return(errs)
				amock.On("NewChannel", false).Return(cmock, dot.channelError)
				amock.On("IsClosed").Return(false)

				msrv := mockManagementRequestServer()
				defer msrv.Close()
				u, serr := url.Parse(msrv.URL)
				assert.Nil(t, serr)

				ctx, cancel := context.WithCancel(context.Background())
				cc := &pb.ConnectionConfiguration{}
				cc.Tenant = testTenant
				cc.Host = u.Hostname()
				i, _ := strconv.Atoi(u.Port())
				cc.AdminPort = int32(i) //nolint:gosec

				defer cancel()

				err := prov.Connect(ctx, cc, false)
				assert.Nil(t, err)

				mc := make(chan *pb.Message)
				defer close(mc)

				err = prov.Subscribe(ctx, src, mc)
				if dot.channelError == nil {
					assert.Nil(t, err)
				} else {
					assert.NotNil(t, err)
				}
				cmock.AssertExpectations(t)
				amock.AssertExpectations(t)
			})
	}
}

func Test_SourceStats(t *testing.T) {
	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	oldNewAmqpConn091 := NewAmqpConn091
	oldNewStreamConn := NewStreamConn

	prov := NewAMQP091Provider().(*amqp091provider)

	bd := &BrokerDetails{}
	// bd.Connection = amock
	prov.connections.Add("1234", bd)

	msrv := mockManagementRequestServer()
	defer msrv.Close()
	u, err := url.Parse(msrv.URL)
	assert.Nil(t, err)
	creds := &pb.Credentials{Username: "user", Password: "password"}
	bd.connectionConfig = &pb.ConnectionConfiguration{Credentials: creds}
	bd.connectionConfig.Host = u.Hostname()
	bd.connectionConfig.Tenant = testTenant
	i, _ := strconv.Atoi(u.Port())
	bd.connectionConfig.AdminPort = int32(i) //nolint:gosec

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAmqpConn091 = oldNewAmqpConn091
		NewStreamConn = oldNewStreamConn
	}()

	var tests = []struct {
		addressType        pb.Address_TargetType
		sourceType         pb.Source_TargetType
		consumerCnt        int32
		messageCnt         int64 // should be fakeConsLastOffset+1
		lastOffset         int64
		addressName        string
		sourceName         string
		singleActive       bool
		fakeConsLastOffset int64
		consLastOffset     int64
		publishRate        float32
		deliverRate        float32
	}{
		{
			addressType:        pb.Address_QUEUE,
			sourceType:         pb.Source_QUEUE,
			consumerCnt:        int32(5),
			messageCnt:         int64(10),
			lastOffset:         int64(0),
			addressName:        "addressQueue",
			sourceName:         "sourceQueue",
			singleActive:       false,
			fakeConsLastOffset: int64(0),
			consLastOffset:     int64(0),
			publishRate:        float32(1.5),
			deliverRate:        float32(2.0),
		},
		{
			addressType:        pb.Address_STREAM,
			sourceType:         pb.Source_STREAM,
			consumerCnt:        int32(4),
			messageCnt:         int64(0),
			lastOffset:         int64(5),
			addressName:        "addressStream",
			sourceName:         "sourceStream",
			singleActive:       false,
			fakeConsLastOffset: int64(5),
			consLastOffset:     int64(5),
			publishRate:        float32(0), // should be missing in source stats, so zero
		},
		{
			addressType:        pb.Address_STREAM,
			sourceType:         pb.Source_STREAM,
			consumerCnt:        int32(6),
			messageCnt:         int64(0),
			lastOffset:         int64(5),
			addressName:        "addressStream2",
			sourceName:         "sourceStream2",
			singleActive:       true,
			fakeConsLastOffset: int64(5),
			consLastOffset:     int64(5),
			publishRate:        float32(5.00),
			deliverRate:        float32(6.4),
		},
	}

	for _, test := range tests {
		testName := fmt.Sprintf("%s_%s", test.addressName, test.sourceName)
		t.Run(testName, func(t *testing.T) {
			bd.StreamConnection = nil // make sure we call NewStreamConn again

			addr := &pb.Address{
				Subjects: []string{"routingkey"},
				Name:     test.addressName,
				Type:     test.addressType,
			}
			src := &pb.Source{
				Address:              addr,
				Name:                 test.sourceName,
				Type:                 test.sourceType,
				Options:              map[string]string{"ConsumerGroup": "GroupName"},
				SingleActiveConsumer: test.singleActive,
			}

			smock := &streamConnectionMock{}
			pmock := &streamConsumerMock{}

			if test.sourceType == pb.Source_STREAM {
				smock.ExpectedCalls = nil
				pmock.On("Close").Return(nil).Once()
				smock.On("Connect").Return(nil).Once()

				smock.On("NewConsumer", src.GetName(), "arkeSourceStatsConsumer", "last", mock.Anything, mock.AnythingOfType("bool")).Return(pmock, nil).Once()
				smock.On("GetLastOffset", src.GetName(), "arkeSourceStatsConsumer").Return(int(test.fakeConsLastOffset), nil).Once()
				if test.singleActive {
					smock.On("GetLastOffset", src.GetName(), "GroupName").Return(int(test.consLastOffset), nil).Once()
				} else {
					smock.On("GetLastOffset", src.GetName(), test.sourceName).Return(int(test.consLastOffset), nil).Once()
				}
				smock.On("StoreOffset", src.GetName(), "arkeSourceStatsConsumer", int64(5)).Return(nil)

				NewStreamConn = func(string, string, *tls.Config) streamConnectionShim {
					return smock
				}
			}

			stats := prov.SourceStats(ctx, src)
			assert.NotNil(t, stats)
			assert.Nil(t, stats.GetError(), "Get SourceStats error should not be nil")
			assert.Equal(t, test.consumerCnt, stats.ConsumerCount, "Consumer count should match")
			assert.Equal(t, test.messageCnt, stats.MessageCount, "Message count should match")
			assert.Equal(t, test.lastOffset, stats.LastOffset, "Last offset should match")
			assert.Equal(t, test.publishRate, stats.PublishRate, "Publish rate should match")
			pmock.AssertExpectations(t)
			smock.AssertExpectations(t)
		})
	}
}

func Test_SourceStats_errors(t *testing.T) {
	prov := NewAMQP091Provider()
	ctx := context.Background()
	src := &pb.Source{}
	stats := prov.SourceStats(ctx, src)
	assert.NotNil(t, stats)
	assert.Equal(t, "address name not defined", stats.GetError().GetMessage())

	// no client id so prov.getBrokerDetails fails
	src.Address = &pb.Address{Name: "myname"}
	stats = prov.SourceStats(ctx, src)
	assert.NotNil(t, stats)
	assert.Equal(t, "could not retrieve client-id from context", stats.GetError().GetMessage())
}

func stockMessage(address *pb.Address) *pb.Message {
	msg := &pb.Message{Address: address, Body: []byte("thebody")}
	msg.Headers = make(map[string]string)
	msg.Headers["Content-Type"] = testContentTypeJSON
	msg.Headers["Content-Encoding"] = "utf8"
	msg.Headers["Additional-Header"] = "HeaderValue"
	msg.Persistent = true

	return msg
}

func stockAddress() *pb.Address {
	subjects := make([]string, 0)
	subjects = append(subjects, "subject1")
	address := &pb.Address{Name: "addressname", Subjects: subjects, Type: pb.Address_FILTER}

	return address
}

func stockAmqpMessage(msg *pb.Message) amqp091Message {
	expectedMsg := amqp091Message{}
	expectedMsg.Body = msg.GetBody()
	expectedMsg.DeliveryMode = 2 // persistent
	expectedMsg.ContentType = msg.Headers["Content-Type"]
	expectedMsg.ContentEncoding = msg.Headers["Content-Encoding"]
	expectedMsg.Headers = amqp091Table{}
	expectedMsg.Headers["Content-Type"] = msg.Headers["Content-Type"]
	expectedMsg.Headers["Content-Encoding"] = msg.Headers["Content-Encoding"]
	expectedMsg.Headers["Additional-Header"] = msg.Headers["Additional-Header"]

	return expectedMsg
}
func TestCopyHeaderToTimestamp(t *testing.T) {
	address := stockAddress()
	msg := stockMessage(address)
	msg.Headers = make(map[string]string)
	msg.Headers[rabbitReceivedTimeHeaderName] = "test-timestamp-value"
	addTimeStampHeader(msg.Headers)
	// Assert that the new header is set
	assert.Equal(t, msg.Headers[rabbitReceivedTimeHeaderName], msg.Headers[timeStampInMSHeaderName])
}

// ---------------------------------------------------------------------------
// Helpers for commit 92c8502 unit tests
// ---------------------------------------------------------------------------

// notifyCapturingConnMock wraps amqpConnectionMock and records every channel
// argument passed to NotifyClose so tests can verify its capacity.
// The other methods (Connect, Close, IsClosed, NewChannel) are promoted from
// amqpConnectionMock and continue to use testify expectations.
type notifyCapturingConnMock struct {
	amqpConnectionMock
	mu       sync.Mutex
	captures []chan amqp091Error
}

func (m *notifyCapturingConnMock) NotifyClose(ch chan amqp091Error) chan amqp091Error {
	m.mu.Lock()
	m.captures = append(m.captures, ch)
	m.mu.Unlock()
	return ch // return the same channel so the provider uses the one we inspect
}

// notifyCapturingChanMock wraps amqpChannelMock and records every channel
// argument passed to NotifyClose.  notified is closed on the first call,
// allowing tests to synchronise on the moment Publish enters its select loop.
type notifyCapturingChanMock struct {
	amqpChannelMock
	mu       sync.Mutex
	captures []chan amqp091Error
	notified chan struct{}
}

func newNotifyCapturingChanMock() *notifyCapturingChanMock {
	return &notifyCapturingChanMock{notified: make(chan struct{})}
}

func (m *notifyCapturingChanMock) NotifyClose(ch chan amqp091Error) chan amqp091Error {
	m.mu.Lock()
	m.captures = append(m.captures, ch)
	m.mu.Unlock()
	select {
	case <-m.notified: // already closed
	default:
		close(m.notified)
	}
	return ch
}

// Test_Publish_ContextCancellation_ExitsPromptly verifies the ctx.Done()
//
// The test calls prov.Publish directly (bypassing the gRPC server goroutine)
// with a messageChannel that is never closed.  Without the ctx.Done() branch
// there is no exit path once the context is cancelled: the select cannot
// receive from messageChannel (empty), connErrChan, or cancelChan (both idle
// mocks), so Publish blocks indefinitely.  With the fix, ctx.Done() fires
// immediately and Publish returns nil.
func Test_Publish_ContextCancellation_ExitsPromptly(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "test-ctx-cancel-92c8502", nil
	}
	defer func() { GetClientIdentifier = oldGetClientIdentifier }()

	chanMock := &amqpChannelMock{}
	// Buffered so the deferred send in prov.Publish can drain without blocking.
	chanErrChan := make(chan amqp091Error, 1)
	chanMock.On("NotifyClose").Return(chanErrChan)
	chanMock.On("Close").Return(nil)

	connMock := &amqpConnectionMock{}
	connMock.On("Connect").Return(nil)
	connMock.On("IsClosed").Return(false)
	// Buffered so the deferred send in prov.Publish can drain without blocking.
	connErrChan := make(chan amqp091Error, 1)
	connMock.On("NotifyClose").Return(connErrChan)
	connMock.On("NewChannel", false).Return(chanMock, nil)

	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return connMock
	}
	defer func() { NewAmqpConn091 = oldNewAmqpConn091 }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cc := &pb.ConnectionConfiguration{}
	assert.Nil(t, prov.Connect(ctx, cc, false))

	// messageChannel is never closed and never has messages.  The only exit
	// from the Publish select loop is ctx.Done() (post-fix) or an AMQP error
	// notification.  Neither connErrChan nor chanErrChan will fire here.
	messageChannel := make(chan *pb.Message)
	errChan := make(chan *pb.Error, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		prov.Publish(ctx, messageChannel, errChan)
	}()

	// Give Publish time to enter the select loop before cancelling.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Publish exited promptly after context cancellation.
	case <-time.After(2 * time.Second):
		t.Fatal("prov.Publish did not exit within 2 s after context cancellation; " +
			"the ctx.Done() case in the Publish select loop may be missing (commit 92c8502)")
	}
}

// Test_Publish_NotifyCloseChannelsAreBuffered verifies that the channels
// passed to NotifyClose inside the amqp091 Publish function have capacity >= 1
//
// The AMQP library (amqp091-go) documents: "It is recommended that callers of
// NotifyClose use a buffered channel.  The library will drop sends to full or
// closed channels."  A capacity-0 (unbuffered) channel means a close
// notification can be silently dropped if the Publish goroutine is not
// blocking in the select at the exact moment the broker closes the connection.
//
// This test captures the channel arguments via a custom mock and asserts
// cap >= 1.  It fails on the pre-fix code (make(chan amqp091Error), cap=0)
// and passes on the post-fix code (make(chan amqp091Error, 1), cap=1).
func Test_Publish_NotifyCloseChannelsAreBuffered(t *testing.T) {
	prov := NewAMQP091Provider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "test-buffered", nil
	}
	defer func() { GetClientIdentifier = oldGetClientIdentifier }()

	chanCapture := newNotifyCapturingChanMock()
	chanCapture.On("Close").Return(nil)

	connCapture := &notifyCapturingConnMock{}
	connCapture.On("Connect").Return(nil)
	connCapture.On("IsClosed").Return(false)
	connCapture.On("NewChannel", false).Return(chanCapture, nil)

	oldNewAmqpConn091 := NewAmqpConn091
	NewAmqpConn091 = func(string, string, *tls.Config) amqp091ConnectionShim {
		return connCapture
	}
	defer func() { NewAmqpConn091 = oldNewAmqpConn091 }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cc := &pb.ConnectionConfiguration{}
	assert.Nil(t, prov.Connect(ctx, cc, false))

	messageChannel := make(chan *pb.Message)
	errChan := make(chan *pb.Error, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		prov.Publish(ctx, messageChannel, errChan)
	}()

	// Wait until prov.Publish has called amqpChannel.NotifyClose, which is the
	// last NotifyClose call before the select loop starts.  At this point both
	// connection and channel NotifyClose have been called by prov.Publish.
	select {
	case <-chanCapture.notified:
	case <-time.After(2 * time.Second):
		t.Fatal("prov.Publish did not call amqpChannel.NotifyClose within 2 s")
	}

	// Close messageChannel to exit Publish via the nil-message path.  This
	// works regardless of whether ctx.Done() is present in the select loop,
	// keeping this test focused solely on channel capacity.
	close(messageChannel)
	<-done

	chanCapture.mu.Lock()
	chanCaps := append([]chan amqp091Error(nil), chanCapture.captures...)
	chanCapture.mu.Unlock()

	connCapture.mu.Lock()
	connCaps := append([]chan amqp091Error(nil), connCapture.captures...)
	connCapture.mu.Unlock()

	// amqpChannel.NotifyClose is called exactly once by Publish (for cancelChan).
	if assert.Len(t, chanCaps, 1, "Publish should call amqpChannel.NotifyClose exactly once") {
		assert.GreaterOrEqual(t, cap(chanCaps[0]), 1,
			"cancelChan passed to amqpChannel.NotifyClose must have capacity >= 1 "+
				"(AMQP library drops notifications on full or unbuffered channels;)")
	}

	// bd.Connection.NotifyClose is called once by bd.connect() (unbuffered, always)
	// and once by prov.Publish (must be buffered after the fix).
	if assert.GreaterOrEqual(t, len(connCaps), 2,
		"bd.Connection.NotifyClose should be called at least twice: once during connect, once during Publish") {
		lastCap := connCaps[len(connCaps)-1]
		assert.GreaterOrEqual(t, cap(lastCap), 1,
			"connErrChan passed to bd.Connection.NotifyClose in Publish must have capacity >= 1 "+
				"(AMQP library drops notifications on full or unbuffered channels;)")
	}
}
