// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/i18n"
	"github.com/sassoftware/arke/internal/provider"

	// Import the connectors so their init functions are executed
	_ "github.com/sassoftware/arke/internal/provider/connectors"
	"github.com/sassoftware/arke/internal/util"
	"github.com/sassoftware/arke/internal/util/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	EnvMaxConnectRetries = "MAX_RECONNECT_RETRIES"
	EnvMaxConnectDelay   = "MAX_RECONNECT_RETRIES"
)

var GetClientAddr = util.GetClientAddr
var GetClientIdentifier = util.GetClientIdentifier
var SetClientIdentifier = util.SetClientIdentifier
var RemoveClientIdentifier = util.RemoveClientIdentifier
var GetProcessStats = util.GetProcessStats

var NewTimestampPB = util.NewTimestampPB

// Map that tracks our connection information
var connectionMap = util.NewConcurrentMap()
var healthNotifiers = util.NewConcurrentMap()

func init() {
	if !strings.HasSuffix(os.Args[0], ".test") {
		// ctx, _ := context.WithCancel(context.Background())
		util.Logger.Debugf("Starting server connection watcher")
		go connectionWatcher()
		// go util.MonitorProcessStats(ctx)
		go util.MonitorProcessStats()
		util.Logger.Debug("Monitoring Horizontal Pod Autoscaler")
	}
}

func connectionWatcher() {
	// watch connection map
	ticker := time.NewTicker(30 * time.Second)
	for {
		<-ticker.C
		for _, connID := range connectionMap.GetList() {
			if connConf, ok := connectionMap.Get(connID); ok {
				providerType := connConf.(*pb.ConnectionConfiguration).GetProvider()
				if prov, err := provider.GetProvider(providerType); err == nil {
					// if the provider says the client doesn't exists, clean up this dead client
					if !prov.ClientExists(connID) {
						util.Logger.Debugf("Provider says client %s does not exist. Cleaning up dead client.", connID)
						connectionMap.Delete(connID)
					}
				}
			} else {
				// We had it in the list but then couldn't retrieve it, delete it.
				connectionMap.Delete(connID)
			}
		}
	}
}

type consumeRecv struct {
	err error
	msg *pb.Consume
}

// ProducerServer producer server struct
type ProducerServer struct {
	pb.UnimplementedProducerServer
	TLSSkipVerify bool
}

// ConsumerServer consumer server struct
type ConsumerServer struct {
	pb.UnimplementedConsumerServer
	TLSSkipVerify bool
}

type HealthzServer struct {
	pb.UnimplementedHealthzServer
}

// Connect to the message broker and track the connection. If a previous connection exists the client,
// it will not connect again.
func (s *ProducerServer) Connect(ctx context.Context, cf *pb.ConnectionConfiguration) (*pb.ConnectResponse, error) {
	resp, err := brokerConnect(ctx, cf, s.TLSSkipVerify)
	return resp, err
}

// Connect see (ProducerServer).Connect
func (s *ConsumerServer) Connect(ctx context.Context, cf *pb.ConnectionConfiguration) (*pb.ConnectResponse, error) {
	resp, err := brokerConnect(ctx, cf, s.TLSSkipVerify)
	return resp, err
}

type streamSender struct {
	sync.Mutex
	stream pb.Consumer_ConsumeServer
}

func newStreamSender(stream pb.Consumer_ConsumeServer) *streamSender {
	snd := &streamSender{stream: stream}
	return snd
}

func (snd *streamSender) Send(cr *pb.ConsumeResponse) error {
	snd.Lock()
	defer snd.Unlock()
	err := snd.stream.Send(cr)
	return err
}

// Consume Receives a stream of messages (source, ack, nack) and returns a message (message, ackresponse, nackresponse)
func (s *ConsumerServer) Consume(stream pb.Consumer_ConsumeServer) error { //nolint:gocognit,gocyclo
	sender := newStreamSender(stream)

	ctx := stream.Context()
	prov, findErr := findProvider(ctx)
	if prov == nil {
		ftlError := errors.New(findErr.Message)
		cnsmResp := &pb.ConsumeResponse{Resp: &pb.ConsumeResponse_Error{Error: findErr}}
		_ = sender.Send(cnsmResp)
		util.Logger.Debug(i18n.SubscribeError, findErr.Message)
		return ftlError
	}

	clientIdentifier, err := GetClientIdentifier(ctx)
	if err != nil {
		ciErr := &pb.Error{Message: err.Error(), IsFatal: true}
		cnsmResp := &pb.ConsumeResponse{Resp: &pb.ConsumeResponse_Error{Error: ciErr}}
		_ = sender.Send(cnsmResp)
		util.Logger.Debug(i18n.SubscribeError, ciErr.Message)
		return err
	}

	var returnError error
	isSubscribing := false

	// stopForLoop is used for errors that require the exiting of Consume,
	// essentially if a long running goroutine needs to exit, it  needs to
	// use stopForLoop as a killswitch for the entire Consume() because
	// we don't want leaks.
	stopForLoop := make(chan bool)
	defer func() {
		close(stopForLoop)
		stopForLoop = nil
	}()

	// messageChannel is used for sending messages from the provider back to Consume
	// for sending back to the client
	messageChannel := make(chan *pb.Message, 10)
	defer close(messageChannel)

	var source *pb.Source

	// lCtx is our loop context, we use it to shutdown our long running goroutines
	loopCtx, loopCancel := context.WithCancel(ctx)
	// lCancel will signal to all long running goroutines to shutdown
	defer loopCancel()

consumeLoop:
	for {
		// recvChan is the channel used to send messages received from the client
		// to the main processor below
		recvChan := make(chan consumeRecv)

		// stream.Recv in a goroutine so we can send
		// received messages back on a channel and we can
		// use select on that channel and the context.Done()
		// in case of an error
		go func(lCtx context.Context, strm pb.Consumer_ConsumeServer, rchan chan consumeRecv) {
			msg, errer := strm.Recv()
			cnsmRecv := consumeRecv{err: errer, msg: msg}
			select {
			case rchan <- cnsmRecv:
				return
			case <-lCtx.Done():
				return
			}
		}(loopCtx, stream, recvChan)

		select {
		case <-ctx.Done():
			util.Logger.Debugf("Client %v went away.", clientIdentifier)
			break consumeLoop
		case cnsmRecv := <-recvChan:

			if cnsmRecv.err != nil {
				if cnsmRecv.err == io.EOF {
					util.Logger.Debug(i18n.ConsumeRecvChanError, clientIdentifier, cnsmRecv.err.Error())
				} else {
					util.Logger.Warn(i18n.ConsumeRecvChanError, clientIdentifier, cnsmRecv.err.Error())
				}
				returnError = cnsmRecv.err
				break consumeLoop
			}

			if cnsmRecv.msg == nil {
				continue
			}

			if cnsmRecv.msg.GetSrc() != nil {
				if isSubscribing {
					errMsg := "Only one source message allowed per subscribe"
					_ = sender.Send(&pb.ConsumeResponse{Resp: &pb.ConsumeResponse_Msg{Msg: &pb.Message{Error: &pb.Error{Message: errMsg}}}})
					continue
				}

				source = cnsmRecv.msg.GetSrc()

				SetSourceDefaults(source)

				// verify source.SourceOptions
				validOptions := prov.SupportedSourceOptions()
				unsupported := make([]string, 0)
				options := source.GetOptions()
				for option := range options {
					if _, ok := validOptions[option]; !ok {
						util.Logger.Info(i18n.UnsupportedSourceOption, option)
						unsupported = append(unsupported, option)
					}
				}

				if len(unsupported) > 0 {
					errMsg := fmt.Sprintf("provider does not support the following source options: %s", unsupported)
					_ = sender.Send(&pb.ConsumeResponse{Resp: &pb.ConsumeResponse_Msg{Msg: &pb.Message{Error: &pb.Error{Message: errMsg}}}})
					return errors.New(errMsg)
				}

				isSubscribing = true

				// this goroutine receives messages from the provider and sends them to the client for processing
				go func(mc <-chan *pb.Message, cont context.Context, stopFor *chan bool, returnErr *error) {
					defer util.RecoverPanic()
					for {
						select {
						case <-cont.Done():
							return
						case message, ok := <-mc:
							if !ok {
								return
							}

							if message.GetAddress() == nil {
								message.Address = source.GetAddress()
							}

							_, span := tracing.SpanFromHeaders(cont, message.GetHeaders(), message.GetAddress().GetName()+" delivery to client", trace.SpanKindInternal)
							span.AddEvent("sending message from server to consumer client")
							resp := &pb.ConsumeResponse{Resp: &pb.ConsumeResponse_Msg{Msg: message}}
							err := sender.Send(resp)
							if err != nil {
								util.Logger.Warn(i18n.StreamSendError, err.Error(), clientIdentifier)
								span.RecordError(err)
								*returnErr = err
								if *stopFor != nil {
									*stopFor <- true
								}
								span.End()
								return
							}
							span.End()
						}
					}
				}(messageChannel, loopCtx, &stopForLoop, &returnError)

				// call provider.Subscribe and use messageChannel to pass messages from the provider to the receiver func above
				go func(mc chan<- *pb.Message, prov provider.Provider, cont context.Context, stopFor *chan bool, returnErr *error) {
					defer util.RecoverPanic()
					connected := prov.WaitForConnect(cont)
					if connected {
						err := prov.Subscribe(cont, source, mc)
						if err != nil {
							util.Logger.Warn(i18n.SubscribeError, err.Message)
							*returnErr = errors.New(err.GetMessage())
						}

						if source.GetDeclareOnly() {
							dor := &pb.DeclareOnlyResponse{Success: true}
							dor.Error = err
							if err != nil {
								dor.Success = false
							}

							cr := &pb.ConsumeResponse{
								Resp: &pb.ConsumeResponse_DeclareOnlyResponse{
									DeclareOnlyResponse: dor,
								},
							}
							_ = sender.Send(cr)
						}

						if *stopFor != nil {
							*stopFor <- true
						}
					} else {
						util.Logger.Warn(i18n.BrokerConnectError, "could not connect to broker")
						*returnErr = errors.New("could not connect to broker")
						if *stopFor != nil {
							*stopFor <- true
						}
					}
				}(messageChannel, prov, loopCtx, &stopForLoop, &returnError)
			} else if cnsmRecv.msg.GetAck() != nil { // Ack or Nack the message
				go func() {
					ackmsg := cnsmRecv.msg.GetAck()
					mcr := &pb.MessageConsumedResponse{Success: true, Uuid: ackmsg.GetUuid()}

					var ackerr *pb.Error

					if ackmsg.GetUuid() == "" { //nolint:gocritic
						ackerr = &pb.Error{Message: "Uuid not set when acking/nacking"}
					} else if ackmsg.GetNack() && ackmsg.GetRequeueDelay() > 0 { // delayed retry
						ackerr = prov.Retry(ctx, source, ackmsg.GetUuid(), ackmsg.GetRequeueDelay())
					} else if ackmsg.GetNack() { // Nack
						// dead letter if enabled, else Nack
						opts := source.GetOptions()
						if _, ok := opts["DeadLetterAddress"]; ok {
							ackerr = prov.DeadLetter(ctx, source, ackmsg.GetUuid())
						}
						if _, ok := opts["DeadLetterAddress"]; !ok || ackerr != nil {
							ackerr = prov.Nack(ctx, ackmsg.GetUuid())
						}
					} else { // Ack
						ackerr = prov.Ack(ctx, ackmsg.GetUuid())
					}

					if ackerr != nil {
						mcr.Success = false
						mcr.Error = ackerr
					}

					_ = sender.Send(&pb.ConsumeResponse{Resp: &pb.ConsumeResponse_ConsumedResponse{ConsumedResponse: mcr}})
				}()
			}
		case <-stopForLoop:
			break consumeLoop
		}
	}

	return returnError
}

// PublishOne send a single message to the server
func (s *ProducerServer) PublishOne(ctx context.Context, msg *pb.Message) (*pb.MessageResponse, error) {
	prov, findErr := findProvider(ctx)
	if prov == nil {
		ftlError := errors.New(findErr.GetMessage())
		msgResp := &pb.MessageResponse{Success: false, Error: findErr}
		return msgResp, ftlError
	}

	if len(msg.GetAddress().GetSubjects()) != 1 {
		errMsg := &pb.Error{
			Message: "exactly one subject allowed in an Address with Publish",
			IsFatal: false,
		}
		subjectError := errors.New(errMsg.GetMessage())
		return &pb.MessageResponse{Success: false, Error: errMsg}, subjectError
	}

	resp := &pb.MessageResponse{Success: true}
	var respErr error
	pubErr := prov.PublishOne(ctx, msg)
	if pubErr != nil {
		resp.Success = false
		resp.Error = pubErr
		respErr = errors.New(pubErr.GetMessage())
	}

	return resp, respErr
}

// Publish sends message to the server
func (s *ProducerServer) Publish(stream pb.Producer_PublishServer) error { //nolint:gocognit
	ctx := stream.Context()
	prov, findErr := findProvider(ctx)
	if prov == nil {
		ftlError := errors.New(findErr.Message)
		msgResp := &pb.MessageResponse{Success: false, Error: findErr}
		stream.Send(msgResp) //nolint:errcheck
		return ftlError
	}

	var err error
	var msg *pb.Message

	clientIdentifier, err := GetClientIdentifier(ctx)
	if err != nil {
		ciError := &pb.Error{Message: err.Error(), IsFatal: true}
		msgResp := &pb.MessageResponse{Success: false, Error: ciError}
		stream.Send(msgResp) //nolint:errcheck
		return err
	}

	var returnError error
	// stopPublish should only be set to true if:
	// - the client disconnects
	// - we determine the client to be dead
	// - the client stops the stream
	stopPublish := false

	// loop until either the client no longer exists or the stream is dead
	for {
		if !clientExists(ctx) {
			util.Logger.Debugf("client %v no longer exists, stopping publish", clientIdentifier)
			break
		}
		messageChannel := make(chan *pb.Message)
		errChan := make(chan *pb.Error)
		go func(mc chan<- *pb.Message, ec <-chan *pb.Error) {
			// close the channel so the prov.Publish knows to stop
			defer close(mc)
			stopPubFunc := false

			for {
				if stopPubFunc {
					return
				}
				msg, err = stream.Recv()
				if err == io.EOF {
					returnError = nil
					stopPublish = true
					return
				}
				if err != nil {
					util.Logger.Debugf("Error on producer stream for client %s: %v", clientIdentifier, err)
					returnError = err
					stopPublish = true
					return
				}

				var resp *pb.MessageResponse
				loopCtx, loopCtxCancel := context.WithCancel(ctx)

				_, span := tracing.SpanFromHeaders(loopCtx, msg.GetHeaders(), "arke-server-publish", trace.SpanKindProducer)
				span.AddEvent("received message from client")

				endSpan := func() {
					span.End()
					loopCtxCancel()
				}

				if len(msg.GetAddress().GetSubjects()) != 1 {
					errMsg := &pb.Error{
						Message: "exactly one subject allowed in an Address with Publish",
						IsFatal: false,
					}

					resp = &pb.MessageResponse{Success: false, Error: errMsg}
					span.RecordError(errors.New(errMsg.GetMessage()))
				} else {
					span.SetName(msg.GetAddress().GetName() + " publish")
					span.SetAttributes(attribute.KeyValue{
						Key:   "clientIdentifier",
						Value: attribute.StringValue(clientIdentifier),
					})

					timer := time.NewTimer(30 * time.Second)
					select {
					case errChanErr := <-ec:
						resp = &pb.MessageResponse{Success: false, Error: errChanErr}
						span.RecordError(errors.New(errChanErr.GetMessage()))
						stopPubFunc = true
					case mc <- msg:
						span.AddEvent("sent message to provider")
						pubErr := <-ec
						if pubErr != nil {
							resp = &pb.MessageResponse{Success: false, Error: pubErr}
							span.RecordError(errors.New(pubErr.GetMessage()))
						} else {
							resp = &pb.MessageResponse{Success: true}
							span.AddEvent("message published in provider")
						}
					case <-timer.C:
						errMsg := &pb.Error{Message: "failed to send message to provider for publishing", IsFatal: false}
						resp = &pb.MessageResponse{Success: false, Error: errMsg}
						stopPubFunc = true
						span.RecordError(errors.New(errMsg.GetMessage()))
					}
					timer.Stop()
				}

				err = stream.Send(resp)
				span.AddEvent("sent response to client")
				if err == io.EOF {
					stopPublish = true
					span.RecordError(err)
					endSpan()
					return
				}

				if err != nil {
					util.Logger.Warn(i18n.StreamSendError, err.Error(), clientIdentifier)
					returnError = err
					stopPublish = true
					span.RecordError(err)
					endSpan()
					return
				}

				if stopPubFunc {
					endSpan()
					return
				}
				endSpan()
			}
		}(messageChannel, errChan)

		err := prov.Publish(ctx, messageChannel, errChan)
		if err != nil {
			errChan <- err
			if clientExists(ctx) {
				connected := prov.WaitForConnect(ctx)
				if connected {
					continue
				}
				util.Logger.Warn(i18n.BrokerConnectError, err.Message)
			} else {
				util.Logger.Debugf("Client no longer exists. Stopping publish.")
				stopPublish = true
			}
		}
		if stopPublish {
			break
		}
	}

	// We must try to send a response to the client or the stream will stay open
	// with no messages being consumed
	if returnError != nil {
		errMsg := &pb.Error{Message: returnError.Error()}

		resp := &pb.MessageResponse{Success: false, Error: errMsg}

		// we don't care if the send fails here
		stream.Send(resp) //nolint:errcheck
	}

	return returnError
}

// Disconnect disconnects from the consumer server
func (s *ConsumerServer) Disconnect(ctx context.Context, empty *pb.Empty) (*pb.Empty, error) {
	brokerDisconnect(ctx, empty)
	return &pb.Empty{}, nil
}

// Disconnect disconnects from the producer server
func (s *ProducerServer) Disconnect(ctx context.Context, empty *pb.Empty) (*pb.Empty, error) {
	brokerDisconnect(ctx, empty)
	return &pb.Empty{}, nil
}

// SourceStats get stats for a specific source
func (s *ConsumerServer) SourceStats(ctx context.Context, source *pb.Source) (*pb.SourceStats, error) {
	// Find the provider for this client
	prov, findErr := findProvider(ctx)
	if prov == nil {
		ftlError := errors.New(findErr.GetMessage())
		stats := &pb.SourceStats{Error: findErr}
		return stats, ftlError
	}
	// Get stats for the individual source
	return getSourceStats(ctx, source, prov), nil
}

// SourceStatsGroup get stats for a group of sources
func (s *ConsumerServer) SourceStatsGroup(ctx context.Context, sources *pb.Sources) (*pb.SourceStatsCollection, error) {
	// Initialize an empty list to hold the stats
	var sourceStatsList []*pb.SourceStats
	// Find the provider for this client
	prov, findErr := findProvider(ctx)
	if prov == nil {
		ftlError := errors.New(findErr.GetMessage())
		statsCollection := &pb.SourceStatsCollection{Error: findErr}
		return statsCollection, ftlError
	}
	// Iterate over each source and get its stats
	for _, source := range sources.GetSources() {
		// Get stats for the individual source and append to list
		stats := getSourceStats(ctx, source, prov)
		sourceStatsList = append(sourceStatsList, stats)
	}
	// Return the collection of source stats
	return &pb.SourceStatsCollection{Stats: sourceStatsList}, nil
}

func getSourceStats(ctx context.Context, source *pb.Source, prov provider.Provider) *pb.SourceStats {
	stats := prov.SourceStats(ctx, source)
	consumerGroup := ""
	// if the consumer group option is set, format it for logging
	if consumerGroupOption, ok := source.GetOptions()["ConsumerGroup"]; ok && consumerGroupOption != "" {
		consumerGroup = fmt.Sprintf(" (%s)", consumerGroupOption)
	}
	util.Logger.Debugf("SourceStats for %s%s: %+v", source.GetAddress().GetName(), consumerGroup, stats)
	return stats
}

func SetSourceDefaults(source *pb.Source) {
	// Prefetch can not be less than 1 or it will cause a flood
	// of messages
	if source.GetPrefetchCount() < 1 {
		source.PrefetchCount = 1
	}

	// Force auto-delete queues to expire after 5 minutes, unless
	// the client has already set an Expires (PSGO-471)
	if source.AutoDelete {
		opts := source.GetOptions()
		if opts == nil {
			opts = make(map[string]string)
		}
		if _, ok := opts["Expires"]; !ok {
			opts["Expires"] = "300000" // 5 minutes
			source.Options = opts
		}
	}
}

func brokerConnect(ctx context.Context, cf *pb.ConnectionConfiguration, tlsSkipVerify bool) (*pb.ConnectResponse, error) {
	prov, er := provider.GetProvider(cf.GetProvider())
	if er != nil {
		errMsg := &pb.Error{
			Message: er.Error(),
			IsFatal: true,
		}
		return &pb.ConnectResponse{Success: false, Error: errMsg}, er
	}

	// Check for a client address
	clientAddr, clientAddrErr := GetClientAddr(ctx)
	if clientAddrErr != nil {
		err := errors.New(clientAddrErr.Error())
		return &pb.ConnectResponse{Success: false, Error: nil}, err
	}

	clientName := cf.GetClientName()
	// If we have a client identifier at this point then we've likely already connected
	clientIdentifier, clientErr := GetClientIdentifier(ctx)
	if clientErr != nil {
		if clientName == "" {
			clientName = clientAddr
		}
		clientIdentifier, clientErr = SetClientIdentifier(ctx, clientName)
		if clientErr != nil {
			err := errors.New(clientErr.Error())
			return &pb.ConnectResponse{Success: false, Error: nil}, err
		}
	}
	// If we find a clientIdentifier, then we are already connected
	// and don't need to connect twice.
	_, exists := connectionMap.Get(clientIdentifier)
	if exists {
		util.Logger.Debugf("Client %s called Connect more than once.", clientIdentifier)
		return &pb.ConnectResponse{Success: true, Error: nil}, nil
	}
	var errMsg *pb.Error
	var err error
	maxRetries := util.GetConfig(EnvMaxConnectRetries, 5)
	maxSleep := util.GetConfig(EnvMaxConnectDelay, 5000) // Default 5s
	for connectTry := 1; connectTry <= maxRetries.(int); connectTry++ {
		errMsg = prov.Connect(ctx, cf, tlsSkipVerify)
		if errMsg == nil || errMsg.GetMessage() == "" {
			break
		}
		util.SleepRandom(1000, maxSleep.(int))
	}
	success := true
	if errMsg != nil && errMsg.GetMessage() != "" {
		success = false
		err = errors.New(errMsg.GetMessage())
	}
	if success {
		connectionMap.Add(clientIdentifier, cf)
	}

	return &pb.ConnectResponse{Success: success, Error: errMsg}, err
}

func brokerDisconnect(ctx context.Context, _ *pb.Empty) {
	clientIdentifier, err := GetClientIdentifier(ctx)
	if err != nil {
		return
	}
	cf, found := connectionMap.Get(clientIdentifier)
	if found {
		providerType := cf.(*pb.ConnectionConfiguration).GetProvider()
		prov, _ := provider.GetProvider(providerType)
		prov.Disconnect(ctx)
		connectionMap.Delete(clientIdentifier)
		util.Logger.Info(i18n.ClientDisconnect, clientIdentifier)
		return
	}

	util.Logger.Debugf("Disconnect called for client %s, but no connection information found.", clientIdentifier)
}

func findProvider(ctx context.Context) (provider.Provider, *pb.Error) {
	clientIdentifier, err := GetClientIdentifier(ctx)
	if err != nil {
		errMsg := &pb.Error{
			Message: err.Error(),
			IsFatal: true,
		}
		util.Logger.Warn(i18n.ClientFailedIdentifierError, clientIdentifier, err.Error())
		return nil, errMsg
	}

	cf, found := connectionMap.Get(clientIdentifier)
	if !found {
		errMsg := &pb.Error{
			Message: "Failed to find connection information.",
			IsFatal: true,
		}
		util.Logger.Warn(i18n.ClientNoProviderError, clientIdentifier)
		return nil, errMsg
	}

	providerType := cf.(*pb.ConnectionConfiguration).GetProvider()
	prov, _ := provider.GetProvider(providerType)
	return prov, nil
}

func clientExists(ctx context.Context) bool {
	// We don't allow a client to call Connect more than once
	clientIdentifier, clientErr := GetClientIdentifier(ctx)
	if clientErr != nil {
		return false
	}
	_, exists := connectionMap.Get(clientIdentifier)

	return exists
}

func notifyHealth(clientAddr string, receiver chan pb.HealthStatus_Code) {
	// only allow one notifier per client
	if recInt, ok := healthNotifiers.Get(clientAddr); ok {
		rec := recInt.(chan pb.HealthStatus_Code)
		close(rec)
		healthNotifiers.Delete(clientAddr)
	}
	healthNotifiers.Add(clientAddr, receiver)
}

// MonitorHealthChan monitors the health of the server and sends notifications to clients
func MonitorHealthChan(receiver chan pb.HealthStatus_Code) {
	for code := range receiver {
		for _, clientAddr := range healthNotifiers.GetList() {
			if notifierInt, ok := healthNotifiers.Get(clientAddr); ok {
				notifier := notifierInt.(chan pb.HealthStatus_Code)
				notifier <- code
			}
		}
	}
}

func (s *HealthzServer) Check(stream pb.Healthz_CheckServer) error {
	ctx := stream.Context()

	clientAddr, err := GetClientAddr(ctx)
	if err != nil {
		return err
	}

	notifyHealthChan := make(chan pb.HealthStatus_Code)
	notifyHealth(clientAddr, notifyHealthChan)
	defer func() {
		healthNotifiers.Delete(clientAddr)
		close(notifyHealthChan)
	}()

	// Send initial health status with availability when client connects
	processStats := GetProcessStats()
	initialHealth := &pb.Health{}
	initialStatus := &pb.Health_Status{}
	initialStatus.Status = &pb.HealthStatus{
		Uuid:               util.GenUUID(),
		Time:               NewTimestampPB(),
		Code:               pb.HealthStatus_OK,
		CpuAvailability:    processStats.CPUAvailability,
		MemoryAvailability: processStats.MemoryAvailability,
	}

	// Determine health status based on resource usage
	cpuAndMemoryHealthSet(processStats, initialStatus)

	initialHealth.Resp = initialStatus
	// Send initial health message
	if err := stream.Send(initialHealth); err != nil {
		util.Logger.Debugf("Failed to send initial health status to %s: %v", clientAddr, err)
		return err
	}

	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				break
			}

			if check := msg.GetCheck(); check != nil {
				// client asked for a health check
				util.Logger.Tracef("healthz check requested for %s with uuid %s", clientAddr, check.GetUuid())
				hlth := &pb.Health{}
				hs := &pb.Health_Status{}
				hs.Status = &pb.HealthStatus{}
				hs.Status.Uuid = check.GetUuid()
				hs.Status.Code = pb.HealthStatus_OK
				processStats := GetProcessStats()
				hs.Status.CpuAvailability = processStats.CPUAvailability
				hs.Status.MemoryAvailability = processStats.MemoryAvailability
				hs.Status.Code = pb.HealthStatus_OK

				// if mem usage > 90% or cpu usage has been high for an extended period then report unhealthy
				cpuAndMemoryHealthSet(processStats, hs)

				// set the time right before sending the response
				hs.Status.Time = NewTimestampPB()
				hlth.Resp = hs
				// We dont' care if this send fails
				stream.Send(hlth) //nolint:errcheck
			} else if status := msg.GetStatus(); status != nil {
				// TODO: we are going to do nothing for now, but we need to determine
				// if there are any actual scenarios for us caring about a status response
				// from a client. Possible arke->arke status messages in the future?
				util.Logger.Debugf("client %s status is %v", clientAddr, status.GetCode())
			}
		}
	}()

	for {
		done := false

		select {
		case code := <-notifyHealthChan:
			util.Logger.Debugf("Internal health notification received. Sending %s to %s", code.String(), clientAddr)

			// Get current process stats for availability
			processStats := GetProcessStats()

			hlth := &pb.Health{}
			hs := &pb.Health_Status{}
			hs.Status = &pb.HealthStatus{
				Code:               code,
				Time:               NewTimestampPB(),
				CpuAvailability:    processStats.CPUAvailability,
				MemoryAvailability: processStats.MemoryAvailability,
			}
			hlth.Resp = hs
			// We don't care if this send fails
			stream.Send(hlth) //nolint:errcheck
		case <-ctx.Done():
			done = true
		}

		if done {
			break
		}
	}
	return nil
}

func cpuAndMemoryHealthSet(processStats *util.ProcessStats, initialStatus *pb.Health_Status) {
	if processStats.IsUnhealthyUsage() {
		initialStatus.Status.Code = pb.HealthStatus_UNHEALTHY
	}
}
