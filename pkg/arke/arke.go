// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package arke

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/ratelimit"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/i18n"
	metrics "github.com/sassoftware/arke/internal/metrics/prometheus"
	_ "github.com/sassoftware/arke/internal/provider/connectors" // initializes providers
	"github.com/sassoftware/arke/internal/server"
	"github.com/sassoftware/arke/internal/server/prometheus"
	"github.com/sassoftware/arke/internal/server/ratelimiter"
	"github.com/sassoftware/arke/internal/util"
	"github.com/sassoftware/arke/internal/util/tracing"
	"github.com/soheilhy/cmux"
	"go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	channelzservice "google.golang.org/grpc/channelz/service"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

const (
	EnvPort = "ARKE_PORT"
)

type Arke struct {
	port           int
	certFile       string
	certKey        string
	hpaName        string
	server         *grpc.Server
	tlsSkipVerify  bool
	serverOptions  []grpc.ServerOption
	tracerProvider *trace.TracerProvider
	mux            cmux.CMux
	ratelimiter    *ratelimiter.ClientLimitManager
	interceptors   struct {
		chainUnary  []grpc.UnaryServerInterceptor
		chainStream []grpc.StreamServerInterceptor
	}
}

// Build must be called before Serve. The grpc.Chain*Interceptors can only be added once.
// Would it be better to move these three statements to the Serve method? Is there any
// reason to call anything else before Serve that should go in Build?
func (a *Arke) Build() *Arke {
	a.serverOptions = append(a.serverOptions, grpc.ChainUnaryInterceptor(a.interceptors.chainUnary...))
	a.serverOptions = append(a.serverOptions, grpc.ChainStreamInterceptor(a.interceptors.chainStream...))
	a.server = grpc.NewServer(a.serverOptions...)
	channelzservice.RegisterChannelzServiceToServer(a.server)
	return a
}

func (a *Arke) WithPrometheus() *Arke {
	a.interceptors.chainUnary = append(a.interceptors.chainUnary, prometheus.UnaryInterceptor)
	a.interceptors.chainStream = append(a.interceptors.chainStream, prometheus.StreamInterceptor)
	return a
}

func (a *Arke) WithRateLimit(rlp *RateLimitParameters) *Arke {
	if rlp == nil {
		util.Logger.Warn(i18n.InvalidRateParameters)
		return a
	}
	if rlp.BucketSize <= 0 || rlp.RefillInterval <= time.Duration(0) || rlp.MaxAgeStaleClient <= time.Duration(0) {
		util.Logger.Warn(i18n.InvalidRateParameters)
		return a
	}

	rl, err := ratelimiter.NewClientLimitManager(rlp.BucketSize, rlp.RefillInterval, rlp.MaxAgeStaleClient, rlp.Enforced)
	if err != nil {
		util.Logger.Warn(i18n.CouldNotCreateRateLimiter, err.Error())
		return a
	}
	util.Logger.Info(i18n.RateLimiterInitialized2, rlp.BucketSize, rlp.RefillInterval, rlp.MaxAgeStaleClient)
	a.ratelimiter = rl

	a.interceptors.chainUnary = append(
		a.interceptors.chainUnary,
		selector.UnaryServerInterceptor(
			ratelimit.UnaryServerInterceptor(a.ratelimiter), selector.MatchFunc(ratelimiter.LimitMethods),
		),
	)
	a.interceptors.chainStream = append(
		a.interceptors.chainStream,
		selector.StreamServerInterceptor(
			ratelimit.StreamServerInterceptor(a.ratelimiter), selector.MatchFunc(ratelimiter.LimitMethods),
		),
	)

	return a
}

func (a *Arke) WithTLSSkipVerify(tlsSkipVerify bool) *Arke {
	a.tlsSkipVerify = tlsSkipVerify
	return a
}

func (a *Arke) WithPort(port int) *Arke {
	a.port = port
	return a
}

func (a *Arke) WithCertFilePath(path string) *Arke {
	a.certFile = path
	return a
}

func (a *Arke) WithCertKeyPath(path string) *Arke {
	a.certKey = path
	return a
}

func (a *Arke) WithHpaName(name string) *Arke {
	a.hpaName = name
	return a
}

type RateLimitParameters struct {
	BucketSize        int
	RefillInterval    time.Duration
	MaxAgeStaleClient time.Duration
	Enforced          bool
}

// GetRateLimitParameters parses the rate limit parameters from the
// string values. It is expected that this is called using environment
// variable values, which will always be strings. If the parsing fails,
// an error is returned.
func GetRateLimitParameters(bucketSize string, refillIntervalSec string, maxAgeStaleClientSec string, enforced string) (*RateLimitParameters, error) {
	p := RateLimitParameters{
		Enforced: enforced == "true",
	}
	settingsOK := true

	bsEnv, err := strconv.Atoi(bucketSize)
	if err == nil {
		p.BucketSize = bsEnv
	} else {
		util.Logger.Warn(i18n.InvalidBucketSize, bucketSize)
		settingsOK = false
	}

	maxAgeDuration, err := strconv.Atoi(maxAgeStaleClientSec)
	if err == nil {
		p.MaxAgeStaleClient = time.Duration(maxAgeDuration) * time.Second
	} else {
		util.Logger.Warn(i18n.InvalidMaxAge, maxAgeStaleClientSec)
		settingsOK = false
	}

	refillDuration, err := strconv.Atoi(refillIntervalSec)
	if err == nil {
		p.RefillInterval = time.Duration(refillDuration) * time.Second
	} else {
		util.Logger.Warn(i18n.InvalidRefillInterval, refillIntervalSec)
		settingsOK = false
	}

	if settingsOK {
		return &p, nil
	}
	return nil, fmt.Errorf(i18n.InvalidRateParameters)
}

func defaultKeepAliveParams() keepalive.ServerParameters {
	return keepalive.ServerParameters{
		Time:    20 * time.Second,
		Timeout: 60 * time.Second,
		// Disconnect clients that have been idle for 5 minutes.
		// Idleness on bidirectional streams only kicks in when there are
		// no open streams.
		MaxConnectionIdle: 5 * time.Minute,
	}
}

func defaultKeepAliveEnforcementPolicy() keepalive.EnforcementPolicy {
	return keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}
}

func DefaultArkeServer() *Arke {
	a := &Arke{
		port:    50051,
		hpaName: "arke",
	}

	tp, err := tracing.InitTracerProvider()
	if err != nil {
		util.Logger.Fatal(i18n.OTELInitError, err)
	}
	a.tracerProvider = tp

	setGoMemLimit()

	portEnv := os.Getenv(EnvPort)
	if portEnv != "" {
		var err error
		a.port, err = strconv.Atoi(portEnv)
		if err != nil {
			util.Logger.Fatal(i18n.PortParsingError, err)
		}
	}

	kp := defaultKeepAliveParams()

	kaep := defaultKeepAliveEnforcementPolicy()

	a.serverOptions = append(a.serverOptions, grpc.KeepaliveEnforcementPolicy(kaep))
	a.serverOptions = append(a.serverOptions, grpc.KeepaliveParams(kp))

	return a
}

func setGoMemLimit() {
	_, _ = memlimit.SetGoMemLimitWithOpts(
		memlimit.WithRatio(0.9),
		memlimit.WithProvider(
			memlimit.ApplyFallback(
				memlimit.FromCgroup,
				memlimit.FromSystem,
			),
		),
	)
}

// listener returns a TLS-enabled listener if the certFile and certKey are set,
// otherwise a plain TCP listener.
func (a Arke) listener() (net.Listener, error) {
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", fmt.Sprintf(":%d", a.port))

	if err != nil {
		return nil, err
	}

	tlsCfg, err := a.tlsConfig()
	if tlsCfg != nil && err == nil {
		lis = tls.NewListener(lis, tlsCfg)
	}

	return lis, nil
}

// tlsConfig returns an arke-suitable TLS config if the certFile and certKey are set.
func (a Arke) tlsConfig() (*tls.Config, error) {
	if a.certFile != "" && a.certKey != "" {
		certificate, err := tls.LoadX509KeyPair(a.certFile, a.certKey)
		if err != nil {
			return nil, err
		}

		config := &tls.Config{
			Certificates: []tls.Certificate{certificate},
			Rand:         rand.Reader,
			NextProtos: []string{
				"h2", "http/1.1",
			},
		}
		return config, nil
	}
	return nil, nil
}

// Serve starts the Arke server and blocks until the server is stopped or an error
// if it fails to start. OTEL is initialized, endpoints are registered, the server
// begins listening for incoming gRPC and https connections. Server will also
// startup routines to monitor the number of Arke pods running in K8S.
func (a *Arke) Serve(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			a.server.Stop()
		}
	}()

	if a.tracerProvider != nil {
		defer func() {
			if err := a.tracerProvider.Shutdown(context.Background()); err != nil {
				util.Logger.Error(i18n.OTELShutdownError, err)
			}
		}()
	}

	lis, err := a.listener()

	if err != nil {
		util.Logger.Error(i18n.NetListenError, err.Error())
		return err
	}

	util.Logger.Debug("Registering producer and consumer services")
	pb.RegisterProducerServer(a.server, &server.ProducerServer{TLSSkipVerify: a.tlsSkipVerify})
	pb.RegisterConsumerServer(a.server, &server.ConsumerServer{TLSSkipVerify: a.tlsSkipVerify})
	pb.RegisterHealthzServer(a.server, &server.HealthzServer{})

	util.Logger.Debug("Registering health check service")
	server.RegisterHealthServer(a.server)

	healthChan := make(chan pb.HealthStatus_Code)
	go util.MonitorHPA(healthChan, a.hpaName)
	go server.MonitorHealthChan(healthChan)

	util.Logger.Debug("Registering reflection service")
	reflection.Register(a.server)

	util.Logger.Info(i18n.Starting, a.port)

	a.mux = cmux.New(lis)
	httpListener := a.mux.Match(cmux.HTTP1Fast())
	// Matching on application/grpc Content-Type (as suggested) does not seem to work
	// so if we're not HTTP/1, assume gRPC.
	grpcListener := a.mux.Match(cmux.Any())

	go a.server.Serve(grpcListener) //nolint:errcheck
	// To emit prometeus metrics for arke
	go metrics.Serve(ctx, &httpListener)
	if a.ratelimiter != nil {
		go a.ratelimiter.StartClientCull(ctx)
	}
	serveErrChan := make(chan error)
	go func(as *Arke) {
		if err := as.mux.Serve(); err != nil {
			switch err.(type) { //nolint:gocritic
			case *net.OpError:
				serveErrChan <- nil
				return
			}
			serveErrChan <- err
		}
		serveErrChan <- nil
	}(a)

	var serveErr error
	select {
	case <-ctx.Done():
		a.mux.Close()
	case serveErr = <-serveErrChan:
		if serveErr != nil {
			util.Logger.Error(i18n.FailedServeError, serveErr.Error())
		}
	}
	return serveErr
}
