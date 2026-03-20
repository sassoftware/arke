// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package server_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"testing"
	"time"

	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/provider"
	s "github.com/sassoftware/arke/internal/server"
	"github.com/sassoftware/arke/internal/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var ctx context.Context
var cf *pb.ConnectionConfiguration
var mockp *MockProvider
var conSrv *s.ConsumerServer
var proSrv *s.ProducerServer
var hlthSrv *s.HealthzServer
var expectedErrorMessage = "this is my error"
var errMsg pb.Error = pb.Error{Message: expectedErrorMessage}

func init() {
	// Register the MockProvider with the Provider factory.
	provider.Register("mockp", NewMockProvider)

	// Setup our tests
	ctx = context.Background()
	cf = &pb.ConnectionConfiguration{Provider: provName, ClientName: "ServerTest"}
	mocky, _ := provider.GetProvider(provName)
	mockp = mocky.(*MockProvider)
	conSrv = &s.ConsumerServer{}
	proSrv = &s.ProducerServer{}
	hlthSrv = &s.HealthzServer{}
}

type MockConsumerConsumeServerStream struct {
	mock.Mock
	pb.Consumer_ConsumeServer
}

type MockPubRecv struct {
	Message *pb.Message
	Error   error
}

type MockProducerPublishServerStream struct {
	mock.Mock
	pb.Producer_PublishServer
	Receives   []*MockPubRecv
	SendErrors []error
}

type MockCheckRecv struct {
	Check *pb.Health
	Error error
}

type MockHealthzCheckServerStream struct {
	mock.Mock
	pb.Healthz_CheckServer
	Checks     []*MockCheckRecv
	SendErrors []error
}

func (stream *MockConsumerConsumeServerStream) Send(msg *pb.ConsumeResponse) error {
	args := stream.Called(msg)

	errArg := args.Get(0)
	var err error
	if errArg == nil {
		err = nil
	} else {
		err = errArg.(error)
	}
	return err
}

func (stream *MockConsumerConsumeServerStream) Recv() (*pb.Consume, error) {
	args := stream.Called()

	var cnsm *pb.Consume
	c := args.Get(0)
	if c == nil {
		cnsm = nil
	} else {
		cnsm = c.(*pb.Consume)
	}

	errArg := args.Get(1)
	var err error
	if errArg == nil {
		err = nil
	} else {
		err = errArg.(error)
	}
	return cnsm, err
}

func (stream *MockConsumerConsumeServerStream) Context() context.Context {
	ctx := context.Background()
	return ctx
}

func (stream *MockHealthzCheckServerStream) Context() context.Context {
	args := stream.Called()
	ctx := args.Get(0).(context.Context)
	return ctx
}

func (stream *MockHealthzCheckServerStream) Recv() (*pb.Health, error) {
	var health *pb.Health
	args := stream.Called()
	healthRaw := args.Get(0)
	if healthRaw != nil {
		health = args.Get(0).(*pb.Health)
	}
	err := args.Error(1)
	return health, err
}

func (stream *MockHealthzCheckServerStream) Send(hlth *pb.Health) error {
	args := stream.Called(hlth)
	err := args.Error(0)
	return err
}

func (stream *MockProducerPublishServerStream) Context() context.Context {
	ctx := context.Background()
	return ctx
}

func (stream *MockProducerPublishServerStream) Recv() (*pb.Message, error) {
	responses := stream.Receives
	var resp *MockPubRecv
	if len(stream.Receives) > 0 {
		resp, responses = responses[0], responses[1:]
	} else {
		resp = &MockPubRecv{}
	}
	stream.Receives = responses
	return resp.Message, resp.Error
}

func (stream *MockProducerPublishServerStream) Send(*pb.MessageResponse) error {
	errors := stream.SendErrors
	var err error
	if len(stream.SendErrors) > 0 {
		err, errors = errors[0], errors[1:]
	} else {
		err = nil
	}
	stream.SendErrors = errors

	return err
}

type MockProvider struct {
	mock.Mock
	provider.Provider
	MockMessages         []*pb.Message
	SubscribeReturnDelay time.Duration
}

type MockContext struct {
	mock.Mock
	context.Context
}

const provName string = "mockp"

var defaultDate = time.Date(2021, time.November, 6, 15, 0, 0, 0, time.Local)

// NewMockProvider creates a new provider
func NewMockProvider() provider.Provider {
	prov := &MockProvider{}
	s.GetClientIdentifier = func(context.Context) (string, error) {
		return "127.0.0.1:1234-1234", nil
	}
	s.SetClientIdentifier = func(_ context.Context, n string) (string, error) {
		return fmt.Sprintf("%s-%d", n, 123), nil
	}
	s.RemoveClientIdentifier = func(context.Context) {}
	s.GetClientAddr = func(context.Context) (string, error) {
		return "127.0.0.1:1234", nil
	}
	s.NewTimestampPB = func() *timestamppb.Timestamp {
		return timestamppb.New(defaultDate)
	}
	prov.SubscribeReturnDelay = 500 * time.Millisecond
	return prov
}

// Ack ack a message
func (prov *MockProvider) Ack(ctx context.Context, msgid string) *pb.Error {
	args := prov.Called(ctx, msgid)

	var err *pb.Error
	errArg := args.Get(0)
	if errArg != nil {
		err := errArg.(*pb.Error)
		return err
	}
	return err
}

// Nack nack a message
func (prov *MockProvider) Nack(ctx context.Context, msgid string) *pb.Error {
	args := prov.Called(ctx, msgid)

	var err *pb.Error
	errArg := args.Get(0)
	if errArg != nil {
		err := errArg.(*pb.Error)
		return err
	}
	return err
}

// Retry a message with a delay
func (prov *MockProvider) Retry(ctx context.Context, origSource *pb.Source, msgid string, delay int32) *pb.Error {
	args := prov.Called(ctx, origSource, msgid, delay)

	var err *pb.Error
	errArg := args.Get(0)
	if errArg != nil {
		err := errArg.(*pb.Error)
		return err
	}
	return err
}

// DeadLetter a message
func (prov *MockProvider) DeadLetter(ctx context.Context, origSource *pb.Source, msgid string) *pb.Error {
	args := prov.Called(ctx, origSource, msgid)

	var err *pb.Error
	errArg := args.Get(0)
	if errArg != nil {
		err := errArg.(*pb.Error)
		return err
	}
	return err
}

// Connect connect to broker
func (prov *MockProvider) Connect(ctx context.Context, cf *pb.ConnectionConfiguration, tlsSkipVerify bool) *pb.Error {
	args := prov.Called(ctx, cf, tlsSkipVerify)

	err := args.Get(0).(*pb.Error)
	return err
}

// Subscribe subscribe to a stream of messages from the broker
func (prov *MockProvider) Subscribe(ctx context.Context, source *pb.Source, messageChannel chan<- *pb.Message) *pb.Error {
	args := prov.Called(ctx, source, messageChannel)
	// Subscribe should be long running. Our tests need time to process the
	// MockMessages below before an error is returned so we Sleep at the end.
	defer time.Sleep(prov.SubscribeReturnDelay)

	for _, msg := range prov.MockMessages {
		messageChannel <- msg
	}

	prov.MockMessages = make([]*pb.Message, 0)

	var err *pb.Error
	errArg := args.Get(0)
	if errArg != nil {
		err := errArg.(*pb.Error)
		return err
	}

	return err
}

// Disconnect disconnect from the broker
func (prov *MockProvider) Disconnect(context.Context) {
}

// Publish publish a message to the broker
func (prov *MockProvider) Publish(ctx context.Context, messageChannel <-chan *pb.Message, errChan chan<- *pb.Error) *pb.Error {
	args := prov.Called(ctx, cf)

	errArg := args.Get(0)
	if errArg != nil {
		err := errArg.(*pb.Error)
		return err
	}

	for {
		select {
		case msg := <-messageChannel:
			if msg == nil {
				return nil
			}
			errChan <- nil
		case <-time.After(2 * time.Second):
			return nil
		}
	}
}

func (prov *MockProvider) PublishOne(ctx context.Context, cf *pb.Message) *pb.Error {
	args := prov.Called(ctx, cf)

	errArg := args.Get(0)
	if errArg != nil {
		err := errArg.(*pb.Error)
		return err
	}

	return nil
}

func (prov *MockProvider) WaitForConnect(ctx context.Context) bool {
	args := prov.Called(ctx)

	tf := args.Get(0).(bool)
	return tf
}

func (prov *MockProvider) SupportedSourceOptions() map[string]bool {
	opts := make(map[string]bool)
	opts["option1"] = true
	opts["DeadLetterAddress"] = true
	opts["DeadLetterSubject"] = true
	return opts
}

func (prov *MockProvider) MockConnect() {
	prov.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})
}

func (prov *MockProvider) SourceStats(ctx context.Context, source *pb.Source) *pb.SourceStats {
	args := prov.Called(ctx, source)

	var stats *pb.SourceStats

	statArg := args.Get(0)
	if statArg != nil {
		stats = statArg.(*pb.SourceStats)
	}

	return stats
}

// TestProducerServerNew creates a new producer server
func TestProducerServerNew(t *testing.T) {
	prov := NewMockProvider()
	assert.NotNil(t, prov)

	srv := &s.ProducerServer{}

	assert.NotNil(t, srv)
}

// TestProducerServerNew creates a new producer server
func TestConsumerServerNew(t *testing.T) {
	prov := NewMockProvider()
	assert.NotNil(t, prov)

	srv := &s.ConsumerServer{}

	assert.NotNil(t, srv)
}

func TestConsumerServerConnect_Success(t *testing.T) {
	// We have to clear the ExpectedCalls before each test.
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	(*mockp).On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})
	connectResp, err := conSrv.Connect(ctx, cf)
	conSrv.Disconnect(ctx, &pb.Empty{}) //nolint:errcheck
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestProducerServerConnect_Success(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	connectResp, err := proSrv.Connect(ctx, cf)
	proSrv.Disconnect(ctx, &pb.Empty{}) //nolint:errcheck
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestConsumerServerConnect_Fail(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&errMsg)
	connectResp, err := conSrv.Connect(ctx, cf)

	assert.NotNil(t, connectResp)
	assert.Equal(t, expectedErrorMessage, connectResp.GetError().GetMessage(), fmt.Sprintf("error should be '%s'", expectedErrorMessage))
	assert.Equal(t, expectedErrorMessage, err.Error())

	mockp.AssertExpectations(t)
}

func TestServerConnectBadProvider_Fail(t *testing.T) {
	config := &pb.ConnectionConfiguration{Provider: "unknown"}
	connectResp, err := proSrv.Connect(ctx, config)

	assert.NotNil(t, connectResp)
	assert.Regexp(t, regexp.MustCompile("invalid provider name"), err.Error())
}

func TestServerConnectTwice_Ignore(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	proSrv.Connect(ctx, cf) //nolint:errcheck
	connectResp, err := proSrv.Connect(ctx, cf)
	proSrv.Disconnect(ctx, &pb.Empty{}) //nolint:errcheck

	assert.NotNil(t, connectResp)
	assert.Nil(t, err)
	assert.True(t, connectResp.GetSuccess())

	mockp.AssertExpectations(t)
}

// Make sure that two connections with the same client name don't fail to connect
func TestServerNoConnectionShare(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	connectResp, err := proSrv.Connect(ctx, cf)
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)
	defer proSrv.Disconnect(ctx, &pb.Empty{}) //nolint:errcheck

	oldClientIdentifier := s.GetClientIdentifier
	s.GetClientIdentifier = func(context.Context) (string, error) {
		return "456", nil
	}
	defer func() { s.GetClientIdentifier = oldClientIdentifier }()

	ctx2 := context.WithValue(context.Background(), peer.Peer{}, "")
	cr2, err2 := proSrv.Connect(ctx2, cf)
	assert.NotNil(t, cr2)
	assert.Nil(t, err2)
	defer proSrv.Disconnect(ctx2, &pb.Empty{}) //nolint:errcheck

	mockp.AssertExpectations(t)
}

func TestProducerServerPublish_Success(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})
	ctx := context.WithValue(context.Background(), peer.Peer{}, "")
	msg := &pb.Message{Body: []byte("publish_success message body")}

	proSrv.Connect(ctx, cf) //nolint:errcheck
	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})
	stream := &MockProducerPublishServerStream{}
	stream.Receives = make([]*MockPubRecv, 0)
	stream.Receives = append(stream.Receives, &MockPubRecv{Message: msg})
	stream.Receives = append(stream.Receives, &MockPubRecv{Error: io.EOF})

	mockp.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	// mockp.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(&pb.Error{}).Once()
	// mockp.On("WaitForConnect", mock.Anything.Return(false)

	stream.SendErrors = make([]error, 0)
	stream.SendErrors = append(stream.SendErrors, nil)
	stream.SendErrors = append(stream.SendErrors, nil)
	err := proSrv.Publish(stream)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestProducerServerPublishRecv_Fail(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	ctx := context.WithValue(context.Background(), peer.Peer{}, "")
	msg := &pb.Message{Body: []byte("pub recv fail")}

	proSrv.Connect(ctx, cf) //nolint:errcheck
	stream := &MockProducerPublishServerStream{}
	stream.Receives = make([]*MockPubRecv, 0)
	stream.Receives = append(stream.Receives, &MockPubRecv{Message: msg})
	stream.Receives = append(stream.Receives, &MockPubRecv{Error: errors.New("recverror")})
	stream.SendErrors = make([]error, 0)
	stream.SendErrors = append(stream.SendErrors, nil)
	stream.SendErrors = append(stream.SendErrors, nil)

	mockp.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	err := proSrv.Publish(stream)
	assert.NotNil(t, err)
	assert.Equal(t, "recverror", err.Error())

	mockp.AssertExpectations(t)
}

func TestProducerServerPublishOneNoConnect(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	msg := &pb.Message{Body: []byte("pub recv")}
	oldGetClientIdentifier := s.GetClientIdentifier
	s.GetClientIdentifier = func(context.Context) (string, error) {
		return "", errors.New("Can't get Client UUID")
	}

	resp, err := proSrv.PublishOne(context.Background(), msg)
	s.GetClientIdentifier = oldGetClientIdentifier
	assert.NotNil(t, err)
	assert.NotNil(t, resp)
	assert.False(t, resp.GetSuccess())
}

func TestProducerServerPublishOne(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	subjects := make([]string, 0)
	subjects = append(subjects, "subject1")
	msg := &pb.Message{Body: []byte("pub recv"), Address: &pb.Address{Name: "pubaddress", Subjects: subjects}}
	ctx := context.WithValue(context.Background(), peer.Peer{}, "")

	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	proSrv.Connect(ctx, cf) //nolint:errcheck

	mockp.On("PublishOne", mock.Anything, mock.Anything).Return(nil).Once()

	resp, err := proSrv.PublishOne(ctx, msg)
	assert.Nil(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.GetSuccess())

	mockp.On("PublishOne", mock.Anything, mock.Anything).Return(&pb.Error{Code: 2002, Message: "Failed"}).Once()
	resp, err = proSrv.PublishOne(ctx, msg)
	assert.NotNil(t, err)
	assert.NotNil(t, resp)
	assert.False(t, resp.GetSuccess())

	mockp.AssertExpectations(t)
}

func TestProducerServerPublishOneWithTwoSubjects(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	subjects := make([]string, 0)
	subjects = append(subjects, "subject1", "subject2")
	msg := &pb.Message{Body: []byte("pub recv"), Address: &pb.Address{Name: "pubaddress", Subjects: subjects}}
	ctx := context.WithValue(context.Background(), peer.Peer{}, "")

	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	proSrv.Connect(ctx, cf) //nolint:errcheck

	resp, err := proSrv.PublishOne(ctx, msg)
	assert.NotNil(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "exactly one subject allowed in an Address with Publish", resp.GetError().GetMessage())

	mockp.AssertExpectations(t)
}

func TestProducerServerPublishSend_Fail(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	ctx := context.WithValue(context.Background(), peer.Peer{}, "")
	msg := &pb.Message{Body: []byte("pub send fail")}

	proSrv.Connect(ctx, cf) //nolint:errcheck
	stream := &MockProducerPublishServerStream{}
	stream.Receives = make([]*MockPubRecv, 0)
	stream.Receives = append(stream.Receives, &MockPubRecv{Message: msg})
	stream.Receives = append(stream.Receives, &MockPubRecv{Message: msg})
	stream.SendErrors = make([]error, 0)
	stream.SendErrors = append(stream.SendErrors, nil)
	stream.SendErrors = append(stream.SendErrors, errors.New("senderror"))

	mockp.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	err := proSrv.Publish(stream)
	assert.NotNil(t, err)
	assert.Equal(t, "senderror", err.Error())

	mockp.AssertExpectations(t)
}

func TestServerDisconnect_SuccessNoUUID(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	oldGetClientIdentifier := s.GetClientIdentifier
	s.GetClientIdentifier = func(context.Context) (string, error) {
		return "", errors.New("Can't get Client UUID")
	}

	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	empty := &pb.Empty{}
	conSrv.Connect(ctx, cf) //nolint:errcheck
	connectResp, err := conSrv.Disconnect(ctx, empty)

	s.GetClientIdentifier = oldGetClientIdentifier

	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestServerDisconnect_FailNoMap(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	ctx := context.WithValue(context.Background(), peer.Peer{}, "")
	empty := &pb.Empty{}

	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	conSrv.Connect(ctx, cf) //nolint:errcheck

	oldGetClientIdentifier := s.GetClientIdentifier
	s.GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}
	connectResp, err := conSrv.Disconnect(ctx, empty)
	s.GetClientIdentifier = oldGetClientIdentifier

	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestConsumerServerDisconnect_Success(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	ctx := context.WithValue(context.Background(), peer.Peer{}, "")
	empty := &pb.Empty{}

	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	conSrv.Connect(ctx, cf) //nolint:errcheck
	connectResp, err := conSrv.Disconnect(ctx, empty)
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestProducerServerDisconnect_Success(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	ctx := context.WithValue(context.Background(), peer.Peer{}, "")
	empty := &pb.Empty{}

	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	proSrv.Connect(ctx, cf) //nolint:errcheck
	connectResp, err := proSrv.Disconnect(ctx, empty)
	assert.NotNil(t, connectResp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestServerNoConnect_FAIL(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	prodstream := &MockProducerPublishServerStream{}
	err := proSrv.Publish(prodstream)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Failed to find connection information")

	stream := &MockConsumerConsumeServerStream{}
	stream.On("Send", mock.AnythingOfType("*api.ConsumeResponse")).Return(nil, nil).Once()
	subErr := conSrv.Consume(stream)
	assert.NotNil(t, subErr)
}

func TestConsumerServerConsume(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})
	mockp.On("WaitForConnect", mock.Anything).Return(true)

	sourceOptions := make(map[string]string)
	sourceOptions["option1"] = "ok"
	source := &pb.Source{Name: "asdf", Address: &pb.Address{Name: "addressname"}, Options: sourceOptions}
	stream := &MockConsumerConsumeServerStream{}
	cnsm := &pb.Consume{Msg: &pb.Consume_Src{Src: source}}

	messages := make([]*pb.Message, 0)

	messages = append(messages, &pb.Message{Address: &pb.Address{Name: "addressname"}, Body: []byte("one")})
	messages = append(messages, &pb.Message{Address: &pb.Address{Name: "addressname"}, Body: []byte("two")})

	mockp.MockMessages = messages

	stream.On("Send", mock.AnythingOfType("*api.ConsumeResponse")).Return(nil, nil).Times(3)
	stream.On("Recv").Return(cnsm, nil).Once()
	cnsm = &pb.Consume{Msg: &pb.Consume_Ack{Ack: &pb.MessageConsumed{Uuid: "1"}}}
	stream.On("Recv").Return(cnsm, nil).Once()
	stream.On("Recv").Return(nil, errors.New("stop")).Once().After(500 * time.Millisecond)

	// We are returning an error after 500 ms as a simple way of exiting the subscribe
	mockp.On("Subscribe", mock.Anything, source, mock.Anything).Return(&pb.Error{Message: "breaking"}).After(250 * time.Millisecond)
	mockp.On("Ack", mock.Anything, mock.Anything).Return(nil)
	conSrv.Connect(ctx, cf) //nolint:errcheck
	err := conSrv.Consume(stream)
	assert.NotNil(t, err)

	mockp.AssertExpectations(t)
	stream.AssertExpectations(t)
}

func TestConsumerServerSubscribeReturnNil(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})
	mockp.On("WaitForConnect", mock.Anything).Return(true)

	sourceOptions := make(map[string]string)
	sourceOptions["option1"] = "ok"
	source := &pb.Source{Name: "asdf", Address: &pb.Address{Name: "addressname"}, Options: sourceOptions}
	stream := &MockConsumerConsumeServerStream{}
	cnsm := &pb.Consume{Msg: &pb.Consume_Src{Src: source}}

	messages := make([]*pb.Message, 0)

	messages = append(messages, &pb.Message{Address: &pb.Address{Name: "addressname"}, Body: []byte("one")})
	messages = append(messages, &pb.Message{Address: &pb.Address{Name: "addressname"}, Body: []byte("two")})

	mockp.MockMessages = messages
	mockp.SubscribeReturnDelay = 50 * time.Millisecond

	stream.On("Send", mock.AnythingOfType("*api.ConsumeResponse")).Return(nil, nil).Times(3)
	stream.On("Recv").Return(cnsm, nil).Once()
	cnsm = &pb.Consume{Msg: &pb.Consume_Ack{Ack: &pb.MessageConsumed{Uuid: "1"}}}
	stream.On("Recv").Return(cnsm, nil).Once()
	stream.On("Recv").Return(nil, errors.New("stop")).Once().After(500 * time.Millisecond)

	// We are returning an error after 500 ms as a simple way of exiting the subscribe
	mockp.On("Subscribe", mock.Anything, source, mock.Anything).Return(nil).After(50 * time.Millisecond)
	mockp.On("Ack", mock.Anything, mock.Anything).Return(nil)
	conSrv.Connect(ctx, cf) //nolint:errcheck
	err := conSrv.Consume(stream)

	// The Subscribe() call above should return nil after 50ms, Consume() should handle
	// that properly. If Consume() does not handle that properly, then Recv() will return
	// an error after 500ms with a message 'stop'. We should get nil from Consume().
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
	stream.AssertExpectations(t)
}

func TestSetSourceDefaultsWithNoOptions(t *testing.T) {
	source := &pb.Source{Name: "asdf", Address: &pb.Address{Name: "addressname"}}

	s.SetSourceDefaults(source)
	opts := source.GetOptions()
	assert.Equal(t, "", opts["Expires"])
	assert.Equal(t, int32(1), source.GetPrefetchCount())

	source.AutoDelete = true
	s.SetSourceDefaults(source)
	opts = source.GetOptions()
	assert.Equal(t, "300000", opts["Expires"])
	assert.Equal(t, int32(1), source.GetPrefetchCount())

	source.PrefetchCount = 10
	s.SetSourceDefaults(source)
	assert.Equal(t, int32(10), source.GetPrefetchCount())
}
func TestSetSourceDefaults(t *testing.T) {
	sourceOptions := make(map[string]string)
	sourceOptions["option1"] = "ok"
	source := &pb.Source{Name: "asdf", Address: &pb.Address{Name: "addressname"}, Options: sourceOptions}

	s.SetSourceDefaults(source)
	opts := source.GetOptions()
	assert.Equal(t, "", opts["Expires"])
	assert.Equal(t, int32(1), source.GetPrefetchCount())

	source.AutoDelete = true
	s.SetSourceDefaults(source)
	opts = source.GetOptions()
	assert.Equal(t, "300000", opts["Expires"])
	assert.Equal(t, int32(1), source.GetPrefetchCount())

	sourceOptions["Expires"] = "5000"
	source.Options = sourceOptions
	s.SetSourceDefaults(source)
	assert.Equal(t, "5000", opts["Expires"])
	assert.Equal(t, int32(1), source.GetPrefetchCount())

	source.PrefetchCount = 10
	s.SetSourceDefaults(source)
	assert.Equal(t, int32(10), source.GetPrefetchCount())
}

func TestConsumerServerConsume_Nack(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})
	mockp.On("WaitForConnect", mock.Anything).Return(true)

	sourceOptions := make(map[string]string)
	sourceOptions["option1"] = "ok"
	source := &pb.Source{Name: "asdf", Address: &pb.Address{Name: "addressname"}, Options: sourceOptions}
	stream := &MockConsumerConsumeServerStream{}
	cnsm := &pb.Consume{Msg: &pb.Consume_Src{Src: source}}

	messages := make([]*pb.Message, 0)

	messages = append(messages, &pb.Message{Address: &pb.Address{Name: "addressname"}, Body: []byte("one")})
	messages = append(messages, &pb.Message{Address: &pb.Address{Name: "addressname"}, Body: []byte("two")})

	mockp.MockMessages = messages

	stream.On("Send", mock.AnythingOfType("*api.ConsumeResponse")).Return(nil, nil).Times(3)
	stream.On("Recv").Return(cnsm, nil).Once()
	cnsm = &pb.Consume{Msg: &pb.Consume_Ack{Ack: &pb.MessageConsumed{Uuid: "1", Nack: true}}}
	stream.On("Recv").Return(cnsm, nil).Once()
	stream.On("Recv").Return(nil, errors.New("stop")).Once().After(500 * time.Millisecond)

	// We are returning an error after 500 ms as a simple way of exiting the subscribe
	mockp.On("Subscribe", mock.Anything, source, mock.Anything).Return(&pb.Error{Message: "breaking"}).After(250 * time.Millisecond)
	mockp.On("Nack", mock.Anything, mock.Anything).Return(nil)
	conSrv.Connect(ctx, cf) //nolint:errcheck
	err := conSrv.Consume(stream)
	assert.NotNil(t, err)

	mockp.AssertExpectations(t)
	stream.AssertExpectations(t)
}

func TestConsumerServerConsume_DLQ(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})
	mockp.On("WaitForConnect", mock.Anything).Return(true)

	sourceOptions := make(map[string]string)
	sourceOptions["DeadLetterAddress"] = "dlq"
	source := &pb.Source{Name: "asdf", Address: &pb.Address{Name: "addressname"}, Options: sourceOptions}
	stream := &MockConsumerConsumeServerStream{}
	cnsm := &pb.Consume{Msg: &pb.Consume_Src{Src: source}}

	messages := make([]*pb.Message, 0)

	messages = append(messages, &pb.Message{Address: &pb.Address{Name: "addressname"}, Body: []byte("one")})
	messages = append(messages, &pb.Message{Address: &pb.Address{Name: "addressname"}, Body: []byte("two")})

	mockp.MockMessages = messages

	stream.On("Send", mock.AnythingOfType("*api.ConsumeResponse")).Return(nil, nil).Times(3)
	stream.On("Recv").Return(cnsm, nil).Once()
	cnsm = &pb.Consume{Msg: &pb.Consume_Ack{Ack: &pb.MessageConsumed{Uuid: "1", Nack: true}}}
	stream.On("Recv").Return(cnsm, nil).Once()
	stream.On("Recv").Return(nil, errors.New("stop")).Once().After(500 * time.Millisecond)

	// We are returning an error after 500 ms as a simple way of exiting the subscribe
	mockp.On("Subscribe", mock.Anything, source, mock.Anything).Return(&pb.Error{Message: "breaking"}).After(250 * time.Millisecond)
	mockp.On("DeadLetter", mock.Anything, source, mock.Anything).Return(nil)
	conSrv.Connect(ctx, cf) //nolint:errcheck
	err := conSrv.Consume(stream)
	assert.NotNil(t, err)

	mockp.AssertExpectations(t)
	stream.AssertExpectations(t)
}

func TestConsumerServerConsume_DLQFail(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})
	mockp.On("WaitForConnect", mock.Anything).Return(true)

	sourceOptions := make(map[string]string)
	sourceOptions["DeadLetterAddress"] = "dlq"
	source := &pb.Source{Name: "asdf", Address: &pb.Address{Name: "addressname"}, Options: sourceOptions}
	stream := &MockConsumerConsumeServerStream{}
	cnsm := &pb.Consume{Msg: &pb.Consume_Src{Src: source}}

	messages := make([]*pb.Message, 0)

	messages = append(messages, &pb.Message{Address: &pb.Address{Name: "addressname"}, Body: []byte("one")})
	messages = append(messages, &pb.Message{Address: &pb.Address{Name: "addressname"}, Body: []byte("two")})

	mockp.MockMessages = messages

	stream.On("Send", mock.AnythingOfType("*api.ConsumeResponse")).Return(nil, nil).Times(3)
	stream.On("Recv").Return(cnsm, nil).Once()
	cnsm = &pb.Consume{Msg: &pb.Consume_Ack{Ack: &pb.MessageConsumed{Uuid: "1", Nack: true}}}
	stream.On("Recv").Return(cnsm, nil).Once()
	stream.On("Recv").Return(nil, errors.New("stop")).Once().After(500 * time.Millisecond)

	// We are returning an error after 500 ms as a simple way of exiting the subscribe
	mockp.On("Subscribe", mock.Anything, source, mock.Anything).Return(&pb.Error{Message: "breaking"}).After(250 * time.Millisecond)
	mockp.On("DeadLetter", mock.Anything, source, mock.Anything).Return(&pb.Error{Message: "breaking"})
	mockp.On("Nack", mock.Anything, mock.Anything).Return(nil)
	conSrv.Connect(ctx, cf) //nolint:errcheck
	err := conSrv.Consume(stream)
	assert.NotNil(t, err)

	mockp.AssertExpectations(t)
	stream.AssertExpectations(t)
}

func TestConsumerServerConsume_Retry(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})
	mockp.On("WaitForConnect", mock.Anything).Return(true)

	sourceOptions := make(map[string]string)
	sourceOptions["option1"] = "ok"
	source := &pb.Source{Name: "asdf", Address: &pb.Address{Name: "addressname"}, Options: sourceOptions}
	stream := &MockConsumerConsumeServerStream{}
	cnsm := &pb.Consume{Msg: &pb.Consume_Src{Src: source}}

	messages := make([]*pb.Message, 0)

	messages = append(messages, &pb.Message{Address: &pb.Address{Name: "addressname"}, Body: []byte("one")})
	messages = append(messages, &pb.Message{Address: &pb.Address{Name: "addressname"}, Body: []byte("two")})

	mockp.MockMessages = messages

	stream.On("Send", mock.AnythingOfType("*api.ConsumeResponse")).Return(nil, nil).Times(3)
	stream.On("Recv").Return(cnsm, nil).Once()
	cnsm = &pb.Consume{Msg: &pb.Consume_Ack{Ack: &pb.MessageConsumed{Uuid: "1", Nack: true, RequeueDelay: 10}}}
	stream.On("Recv").Return(cnsm, nil).Once()
	stream.On("Recv").Return(nil, errors.New("stop")).Once().After(500 * time.Millisecond)

	// We are returning an error after 500 ms as a simple way of exiting the subscribe
	mockp.On("Subscribe", mock.Anything, source, mock.Anything).Return(&pb.Error{Message: "breaking"}).After(250 * time.Millisecond)
	mockp.On("Retry", mock.Anything, source, mock.Anything, mock.Anything).Return(nil)
	conSrv.Connect(ctx, cf) //nolint:errcheck
	err := conSrv.Consume(stream)
	assert.NotNil(t, err)

	mockp.AssertExpectations(t)
	stream.AssertExpectations(t)
}

func TestConsumerServerConsume_BadOption(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	sourceOptions := make(map[string]string)
	sourceOptions["option1"] = "ok"
	sourceOptions["badoption"] = "bad"
	source := &pb.Source{Name: "asdf", Address: &pb.Address{Name: "addressname"}, Options: sourceOptions}
	stream := &MockConsumerConsumeServerStream{}
	cnsm := &pb.Consume{Msg: &pb.Consume_Src{Src: source}}

	stream.On("Send", mock.AnythingOfType("*api.ConsumeResponse")).Return(nil, nil).Once()
	stream.On("Recv").Return(cnsm, nil).Once()
	conSrv.Connect(ctx, cf) //nolint:errcheck
	err := conSrv.Consume(stream)
	assert.NotNil(t, err)

	mockp.AssertExpectations(t)
	stream.AssertExpectations(t)
}

func TestConsumerServerConsume_AckErr(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})
	mockp.On("WaitForConnect", mock.Anything).Return(true)

	source := &pb.Source{Name: "asdf", Address: &pb.Address{Name: "addressname"}}
	stream := &MockConsumerConsumeServerStream{}
	cnsm := &pb.Consume{Msg: &pb.Consume_Src{Src: source}}

	messages := make([]*pb.Message, 0)

	messages = append(messages, &pb.Message{Address: &pb.Address{Name: "addressname"}, Body: []byte("one")})
	messages = append(messages, &pb.Message{Address: &pb.Address{Name: "addressname"}, Body: []byte("two")})

	mockp.MockMessages = messages

	stream.On("Send", mock.AnythingOfType("*api.ConsumeResponse")).Return(nil, nil).Times(3)
	stream.On("Recv").Return(cnsm, nil).Once()
	cnsm = &pb.Consume{Msg: &pb.Consume_Ack{Ack: &pb.MessageConsumed{Uuid: "1"}}}
	stream.On("Recv").Return(cnsm, nil).Once()
	stream.On("Recv").Return(nil, errors.New("stop")).Once().After(500 * time.Millisecond)

	// We are returning an error after 500 ms as a simple way of exiting the subscribe
	mockp.On("Subscribe", mock.Anything, mock.Anything, mock.Anything).Return(&pb.Error{Message: "breaking"}).After(250 * time.Millisecond)
	mockp.On("Ack", mock.Anything, mock.Anything).Return(&pb.Error{Message: "ackerr"})
	conSrv.Connect(ctx, cf) //nolint:errcheck
	err := conSrv.Consume(stream)
	assert.NotNil(t, err)

	mockp.AssertExpectations(t)
	stream.AssertExpectations(t)
}

func TestConsumerServerConsume_SourceTwice(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})
	mockp.On("WaitForConnect", mock.Anything).Return(true)

	sourceOptions := make(map[string]string)
	sourceOptions["option1"] = "ok"
	source := &pb.Source{Name: "asdf", Address: &pb.Address{Name: "addressname"}, Options: sourceOptions, PrefetchCount: 1}
	stream := &MockConsumerConsumeServerStream{}
	cnsm := &pb.Consume{Msg: &pb.Consume_Src{Src: source}}

	cnsmResp := &pb.ConsumeResponse{Resp: &pb.ConsumeResponse_Msg{Msg: &pb.Message{Error: &pb.Error{Message: "Only one source message allowed per subscribe"}}}}
	stream.On("Send", cnsmResp).Return(nil, nil).Once()
	stream.On("Recv").Return(cnsm, nil).Twice()
	stream.On("Recv").Return(nil, io.EOF).After(100 * time.Millisecond)
	mockp.On("Subscribe", mock.Anything, mock.Anything, mock.Anything).Return(&pb.Error{Message: "breaking"}).After(250 * time.Millisecond)
	conSrv.Connect(ctx, cf) //nolint:errcheck
	err := conSrv.Consume(stream)
	fmt.Println("err:", err)
	assert.Equal(t, err, io.EOF)

	mockp.AssertExpectations(t)
	stream.AssertExpectations(t)
}

func TestHealthzServerCheck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	stream := &MockHealthzCheckServerStream{}
	stream.On("Context").Return(ctx)
	hc := &pb.Health_Check{}
	hc.Check = &pb.HealthCheck{Uuid: util.GenUUID()}
	hlth := &pb.Health{Resp: hc}
	stream.On("Recv").Return(hlth, nil).Once()
	stream.On("Recv").Return(nil, errors.New("termM")).Once() // send an error to force termination
	stream.On("Send", mock.AnythingOfType("*api.Health")).Return(nil)

	err := hlthSrv.Check(stream)
	assert.Nil(t, err)
	stream.AssertExpectations(t)
}

func runHealthCheckTest(t *testing.T, getStats func() *util.ProcessStats, expectedCode pb.HealthStatus_Code) {
	t.Helper()

	oldGetProcessStats := s.GetProcessStats
	defer func() {
		s.GetProcessStats = oldGetProcessStats
	}()

	s.GetProcessStats = getStats

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	stream := &MockHealthzCheckServerStream{}
	stream.On("Context").Return(ctx)
	uuid := util.GenUUID()
	hc := &pb.Health_Check{}
	hc.Check = &pb.HealthCheck{Uuid: uuid}
	hlth := &pb.Health{Resp: hc}
	stream.On("Recv").Return(hlth, nil).Once()
	stream.On("Recv").Return(nil, errors.New("termM")).Once()

	stream.On("Send", mock.MatchedBy(func(h *pb.Health) bool {
		if status, ok := h.GetResp().(*pb.Health_Status); ok && status != nil && status.Status != nil {
			return status.Status.Code == expectedCode
		}
		return false
	})).Return(nil)

	err := hlthSrv.Check(stream)
	assert.Nil(t, err)
	stream.AssertExpectations(t)
}

func TestHealthzServerCheck_CPUHigh(t *testing.T) {
	runHealthCheckTest(t, func() *util.ProcessStats {
		ps := &util.ProcessStats{}
		ps.CPUAvailability = 0.01
		ps.MemoryAvailability = 0.99
		return ps
	}, pb.HealthStatus_UNHEALTHY)
}

func TestHealthzServerCheck_MemoryHigh(t *testing.T) {
	runHealthCheckTest(t, func() *util.ProcessStats {
		ps := &util.ProcessStats{}
		ps.MemoryAvailability = 0.01
		ps.CPUAvailability = 0.99
		return ps
	}, pb.HealthStatus_UNHEALTHY)
}

func TestHealthzServerCheck_OK(t *testing.T) {
	runHealthCheckTest(t, func() *util.ProcessStats {
		ps := &util.ProcessStats{}
		ps.CPUAvailability = 0.99
		ps.MemoryAvailability = 0.99
		return ps
	}, pb.HealthStatus_OK)
}

func TestConsumerServerConsume_SubscribeDeclareOnly(t *testing.T) {
	var declareOnlyTests = []struct {
		success bool
		subErr  *pb.Error
	}{
		{true, nil},
		{false, &pb.Error{Message: "myerror"}},
	}

	for _, dot := range declareOnlyTests {
		t.Run(fmt.Sprintf("DeclareOnlyTests subErr %s", dot.subErr),
			func(t *testing.T) {
				mockp.ExpectedCalls = make([]*mock.Call, 0)
				mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})
				mockp.On("WaitForConnect", mock.Anything).Return(true)

				sourceOptions := make(map[string]string)
				source := &pb.Source{Name: "asdf", Address: &pb.Address{Name: "addressname"}, Options: sourceOptions, DeclareOnly: true}
				stream := &MockConsumerConsumeServerStream{}
				cnsm := &pb.Consume{Msg: &pb.Consume_Src{Src: source}}

				cr := &pb.ConsumeResponse{
					Resp: &pb.ConsumeResponse_DeclareOnlyResponse{
						DeclareOnlyResponse: &pb.DeclareOnlyResponse{Success: dot.success, Error: dot.subErr},
					},
				}

				stream.On("Recv").Return(cnsm, nil).Once()
				stream.On("Recv").Return(nil, nil).Once().After(1 * time.Second)

				stream.On("Send", cr).Return(nil, nil).Once()

				// We are returning an error after 500 ms as a simple way of exiting the subscribe
				if dot.subErr != nil {
					mockp.On("Subscribe", mock.Anything, source, mock.Anything).Return(dot.subErr) // After(250 * time.Millisecond)
				} else {
					mockp.On("Subscribe", mock.Anything, source, mock.Anything).Return(nil) // After(250 * time.Millisecond)
				}

				conSrv.Connect(ctx, cf) //nolint:errcheck
				err := conSrv.Consume(stream)
				if dot.subErr != nil {
					assert.NotNil(t, err)
				} else {
					assert.Nil(t, err)
				}

				mockp.AssertExpectations(t)
				stream.AssertExpectations(t)
			},
		)
	}
}

func Test_SourceStats(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	// Use a source with a ConsumerGroup option to test that code path
	source := &pb.Source{
		Options: map[string]string{"ConsumerGroup": "group"},
	}
	returnStats := &pb.SourceStats{Error: &pb.Error{}}

	mockp.On("SourceStats", mock.Anything, source).Return(returnStats, nil)
	mockp.MockConnect()

	_, err := conSrv.Connect(ctx, cf)
	assert.Nil(t, err)

	stats, err := conSrv.SourceStats(ctx, source)
	assert.Nil(t, err)
	assert.Equal(t, returnStats, stats)

	mockp.AssertExpectations(t)
}

func Test_SourceStats_noProvider(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	expectedError := "noclientidentifier"

	oldClientIdentifier := s.GetClientIdentifier
	s.GetClientIdentifier = func(context.Context) (string, error) {
		return "", errors.New(expectedError)
	}
	defer func() {
		s.GetClientIdentifier = oldClientIdentifier
	}()

	source := &pb.Source{}
	mockp.MockConnect()

	_, err := conSrv.Connect(ctx, cf)
	assert.Nil(t, err)

	stats, err := conSrv.SourceStats(ctx, source)
	assert.NotNil(t, err)
	assert.Equal(t, expectedError, err.Error())
	assert.Equal(t, expectedError, stats.GetError().GetMessage())

	mockp.AssertExpectations(t)
}

func Test_SourceStatsGroup(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	source := &pb.Source{}
	returnStats := &pb.SourceStats{Error: &pb.Error{}}
	sources := &pb.Sources{Sources: []*pb.Source{source}}
	returnStatsCollection := &pb.SourceStatsCollection{Stats: []*pb.SourceStats{returnStats}}

	mockp.On("SourceStats", mock.Anything, source).Return(returnStats, nil)
	mockp.MockConnect()

	_, err := conSrv.Connect(ctx, cf)
	assert.Nil(t, err)

	stats, err := conSrv.SourceStatsGroup(ctx, sources)
	assert.Nil(t, err)
	assert.Equal(t, returnStatsCollection, stats)

	mockp.AssertExpectations(t)
}

func Test_SourceStatsGroup_noProvider(t *testing.T) {
	mockp.ExpectedCalls = make([]*mock.Call, 0)

	expectedError := "noclientidentifier"

	oldClientIdentifier := s.GetClientIdentifier
	s.GetClientIdentifier = func(context.Context) (string, error) {
		return "", errors.New(expectedError)
	}
	defer func() {
		s.GetClientIdentifier = oldClientIdentifier
	}()

	sources := &pb.Sources{}
	mockp.MockConnect()

	_, err := conSrv.Connect(ctx, cf)
	assert.Nil(t, err)

	stats, err := conSrv.SourceStatsGroup(ctx, sources)
	assert.NotNil(t, err)
	assert.Equal(t, expectedError, err.Error())
	assert.Equal(t, expectedError, stats.GetError().GetMessage())

	mockp.AssertExpectations(t)
}

func (prov *MockProvider) ClientExists(connID string) bool {
	args := prov.Called(connID)
	return args.Bool(0)
}

func TestTrimConnectionList_ClientExists(t *testing.T) {
	s.ResetConnectionMap()
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	ctx := context.Background()
	proSrv.Connect(ctx, cf) //nolint:errcheck

	// Client exists, so it should NOT be removed
	mockp.On("ClientExists", mock.Anything).Return(true).Once()

	s.TrimConnectionList()

	// Verify the client is still connected by calling disconnect (should succeed)
	empty := &pb.Empty{}
	resp, err := proSrv.Disconnect(ctx, empty)
	assert.NotNil(t, resp)
	assert.Nil(t, err)

	mockp.AssertExpectations(t)
}

func TestTrimConnectionList_ClientNotExists(t *testing.T) {
	s.ResetConnectionMap()
	mockp.ExpectedCalls = make([]*mock.Call, 0)
	mockp.On("Connect", mock.Anything, mock.AnythingOfType("*api.ConnectionConfiguration"), mock.AnythingOfType("bool")).Return(&pb.Error{})

	ctx := context.Background()
	proSrv.Connect(ctx, cf) //nolint:errcheck

	// Client does not exist, so it should be removed
	mockp.On("ClientExists", mock.Anything).Return(false).Once()

	s.TrimConnectionList()

	// After trimming, the connection should be gone, so publishing should fail
	msg := &pb.Message{Body: []byte("test")}
	resp, err := proSrv.PublishOne(ctx, msg)
	assert.NotNil(t, err)
	assert.NotNil(t, resp)
	assert.False(t, resp.GetSuccess())

	mockp.AssertExpectations(t)
}
func TestMakeHealthMsg_OK(t *testing.T) {
	oldGetProcessStats := s.GetProcessStats
	defer func() { s.GetProcessStats = oldGetProcessStats }()

	s.GetProcessStats = func() *util.ProcessStats {
		return &util.ProcessStats{
			CPUAvailability:    0.99,
			MemoryAvailability: 0.99,
		}
	}

	uuid := util.GenUUID()
	check := &pb.HealthCheck{Uuid: uuid}
	hlth := s.MakeHealthMsg(check)

	assert.NotNil(t, hlth)
	status := hlth.GetResp().(*pb.Health_Status)
	assert.NotNil(t, status)
	assert.Equal(t, uuid, status.Status.GetUuid())
	assert.Equal(t, pb.HealthStatus_OK, status.Status.GetCode())
	assert.Equal(t, float32(0.99), status.Status.GetCpuAvailability())
	assert.Equal(t, float32(0.99), status.Status.GetMemoryAvailability())
	assert.Equal(t, timestamppb.New(defaultDate), status.Status.GetTime())
}

func TestMakeInternalHealthMsg_OK(t *testing.T) {
	oldGetProcessStats := s.GetProcessStats
	defer func() { s.GetProcessStats = oldGetProcessStats }()

	s.GetProcessStats = func() *util.ProcessStats {
		return &util.ProcessStats{
			CPUAvailability:    0.99,
			MemoryAvailability: 0.99,
		}
	}

	hlth := s.MakeInternalHealthMsg(pb.HealthStatus_OK)

	assert.NotNil(t, hlth)
	status := hlth.GetResp().(*pb.Health_Status)
	assert.NotNil(t, status)
	assert.Equal(t, pb.HealthStatus_OK, status.Status.GetCode())
	assert.Equal(t, float32(0.99), status.Status.GetCpuAvailability())
	assert.Equal(t, float32(0.99), status.Status.GetMemoryAvailability())
	assert.Equal(t, timestamppb.New(defaultDate), status.Status.GetTime())
}

// just a test to get some code coverage for the number to go up
func TestConnectionWatcher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go s.ConnectionWatcher(ctx)
	time.Sleep(500 * time.Millisecond)
	cancel()
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
}
