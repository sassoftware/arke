// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package arke

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/sassoftware/arke/api"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// writeTestCert generates a self-signed cert/key pair and writes them to the
// provided directory. Returns the cert and key file paths.
func writeTestCert(t *testing.T, dir string) (certPath, keyPath string) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	assert.Nil(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	assert.Nil(t, err)

	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")

	certOut, err := os.Create(certPath)
	assert.Nil(t, err)
	assert.Nil(t, pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der}))
	assert.Nil(t, certOut.Close())

	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	assert.Nil(t, err)
	keyOut, err := os.Create(keyPath)
	assert.Nil(t, err)
	assert.Nil(t, pem.Encode(keyOut, &pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	assert.Nil(t, keyOut.Close())

	return certPath, keyPath
}

func Test_DefaultArkeServer(t *testing.T) {
	a := DefaultArkeServer().WithCertFilePath("/cert").WithCertKeyPath("/key").WithTLSSkipVerify(true)

	assert.True(t, a.tlsSkipVerify)
	assert.Equal(t, 50051, a.port)
	assert.Equal(t, "/key", a.certKey)
	assert.Equal(t, "/cert", a.certFile)
}

func Test_defaultKAP(t *testing.T) {
	kp := defaultKeepAliveParams()
	assert.Equal(t, 5*time.Minute, kp.MaxConnectionIdle)
	assert.Equal(t, 20*time.Second, kp.Time)
	assert.Equal(t, 60*time.Second, kp.Timeout)
}

func Test_defaultKAEP(t *testing.T) {
	kaep := defaultKeepAliveEnforcementPolicy()
	assert.Equal(t, 5*time.Second, kaep.MinTime)
	assert.Equal(t, true, kaep.PermitWithoutStream)
}

func Test_DefaultArkeServerWithPortEnv(t *testing.T) {
	os.Setenv("ARKE_PORT", "1234")
	a := DefaultArkeServer()
	assert.Equal(t, 1234, a.port)
}

func Test_DefaultArkeServerWithPortEnvWithPortCall(t *testing.T) {
	os.Setenv("ARKE_PORT", "1234")
	a := DefaultArkeServer().WithPort(1235)
	assert.Equal(t, 1235, a.port)
}

func Test_WithTLSSkipVerify(t *testing.T) {
	a := &Arke{}
	assert.False(t, a.tlsSkipVerify)
	a = a.WithTLSSkipVerify(true)
	assert.True(t, a.tlsSkipVerify)
}

func Test_WithPort(t *testing.T) {
	a := &Arke{port: 10000}
	assert.Equal(t, 10000, a.port)
	a = a.WithPort(50051)
	assert.Equal(t, 50051, a.port)
}

func Test_WithCertKeyPath(t *testing.T) {
	a := &Arke{}
	assert.Equal(t, "", a.certKey)
	a = a.WithCertKeyPath("/my/path")
	assert.Equal(t, "/my/path", a.certKey)
}

func Test_WithCertFilePath(t *testing.T) {
	a := &Arke{}
	assert.Equal(t, "", a.certFile)
	a = a.WithCertFilePath("/my/path")
	assert.Equal(t, "/my/path", a.certFile)
}

func Test_WithHpaName(t *testing.T) {
	a := &Arke{}
	assert.Equal(t, "", a.hpaName)
	a = a.WithHpaName("myHpaName")
	assert.Equal(t, "myHpaName", a.hpaName)
}

func Test_tlsConfig_none(t *testing.T) {
	a := Arke{}
	cfg, err := a.tlsConfig()
	assert.Nil(t, cfg)
	assert.Nil(t, err)
}

func Test_tlsConfig_noCert(t *testing.T) {
	a := Arke{certKey: "/key"}
	cfg, err := a.tlsConfig()
	assert.Nil(t, cfg)
	assert.Nil(t, err)
}

func Test_tlsConfig_noKey(t *testing.T) {
	a := Arke{certFile: "/cert"}
	cfg, err := a.tlsConfig()
	assert.Nil(t, cfg)
	assert.Nil(t, err)
}

func Test_tlsConfig_fail_noload(t *testing.T) {
	a := DefaultArkeServer().WithCertFilePath("/cert").WithCertKeyPath("/key")
	cfg, err := a.tlsConfig()
	assert.Nil(t, cfg)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "no such file")
}

func Test_listener(t *testing.T) {
	a := Arke{port: 1234}
	lis, err := a.listener()
	assert.Nil(t, err)
	assert.NotNil(t, lis)

	assert.Equal(t, "[::]:1234", lis.Addr().String())
}

func testHealth(port int) error {
	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewHealthzClient(conn)
	ctx := context.Background()
	_, err = c.Check(ctx)
	return err
}

func Test_Serve_cancelCtxNoErr(t *testing.T) {
	a := DefaultArkeServer().WithPort(50059).Build()
	ctx, cancel := context.WithCancel(context.Background())
	// sleep half a second and cancel, assert no error
	go func() {
		time.Sleep(500 * time.Millisecond)
		err := testHealth(50059)
		assert.Nil(t, err)
		cancel()
	}()
	err := a.Serve(ctx)
	assert.Nil(t, err)
}

func Test_Serve_muxClose(t *testing.T) {
	a := DefaultArkeServer().WithPort(50059).Build()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// sleep half a second and close the mux, assert no error
	go func(as *Arke) {
		time.Sleep(500 * time.Millisecond)
		err := testHealth(50059)
		assert.Nil(t, err)
		as.mux.Close()
	}(a)
	err := a.Serve(ctx)
	assert.Nil(t, err)
}
func Test_WithPrometheus(t *testing.T) {
	a := &Arke{}
	assert.Empty(t, a.interceptors.chainUnary)
	assert.Empty(t, a.interceptors.chainStream)
	a = a.WithPrometheus()
	assert.Len(t, a.interceptors.chainUnary, 1)
	assert.Len(t, a.interceptors.chainStream, 1)
}

func Test_WithRateLimit_nil(t *testing.T) {
	a := &Arke{}
	a = a.WithRateLimit(nil)
	assert.Nil(t, a.ratelimiter)
	assert.Empty(t, a.interceptors.chainUnary)
	assert.Empty(t, a.interceptors.chainStream)
}

func Test_WithRateLimit_invalidParams(t *testing.T) {
	tests := []struct {
		name string
		rlp  *RateLimitParameters
	}{
		{
			name: "zero bucket size",
			rlp:  &RateLimitParameters{BucketSize: 0, RefillInterval: time.Second, MaxAgeStaleClient: time.Second},
		},
		{
			name: "zero refill interval",
			rlp:  &RateLimitParameters{BucketSize: 1, RefillInterval: 0, MaxAgeStaleClient: time.Second},
		},
		{
			name: "zero max age",
			rlp:  &RateLimitParameters{BucketSize: 1, RefillInterval: time.Second, MaxAgeStaleClient: 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := (&Arke{}).WithRateLimit(tt.rlp)
			assert.Nil(t, a.ratelimiter)
			assert.Empty(t, a.interceptors.chainUnary)
			assert.Empty(t, a.interceptors.chainStream)
		})
	}
}

func Test_WithRateLimit_valid(t *testing.T) {
	rlp := &RateLimitParameters{
		BucketSize:        5,
		RefillInterval:    time.Second,
		MaxAgeStaleClient: time.Minute,
		Enforced:          true,
	}
	a := (&Arke{}).WithRateLimit(rlp)
	assert.NotNil(t, a.ratelimiter)
	assert.Len(t, a.interceptors.chainUnary, 1)
	assert.Len(t, a.interceptors.chainStream, 1)
}

func Test_tlsConfig_success(t *testing.T) {
	certPath, keyPath := writeTestCert(t, t.TempDir())
	a := Arke{certFile: certPath, certKey: keyPath}
	cfg, err := a.tlsConfig()
	assert.Nil(t, err)
	assert.NotNil(t, cfg)
	assert.Len(t, cfg.Certificates, 1)
	assert.Contains(t, cfg.NextProtos, "h2")
	assert.Contains(t, cfg.NextProtos, "http/1.1")
}

func Test_listener_TLS(t *testing.T) {
	certPath, keyPath := writeTestCert(t, t.TempDir())
	a := Arke{port: 0, certFile: certPath, certKey: keyPath}
	lis, err := a.listener()
	assert.Nil(t, err)
	assert.NotNil(t, lis)
	assert.Nil(t, lis.Close())
}

func Test_listener_tlsConfigError(t *testing.T) {
	a := Arke{port: 0, certFile: "/nonexistent/cert", certKey: "/nonexistent/key"}
	lis, err := a.listener()
	assert.Nil(t, lis)
	assert.NotNil(t, err)
}

func Test_Serve_withRateLimiter(t *testing.T) {
	rlp := &RateLimitParameters{
		BucketSize:        5,
		RefillInterval:    time.Second,
		MaxAgeStaleClient: time.Minute,
	}
	a := DefaultArkeServer().WithPort(50062).WithRateLimit(rlp).Build()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(500 * time.Millisecond)
		err := testHealth(50062)
		assert.Nil(t, err)
		cancel()
	}()
	err := a.Serve(ctx)
	assert.Nil(t, err)
}

func Test_Serve_listenerError(t *testing.T) {
	a := DefaultArkeServer().
		WithPort(50063).
		WithCertFilePath("/nonexistent/cert").
		WithCertKeyPath("/nonexistent/key").
		Build()
	err := a.Serve(context.Background())
	assert.NotNil(t, err)
}

func Test_GetRateLimitParameters(t *testing.T) {
	tests := []struct {
		name                 string
		bucketSize           string
		refillIntervalSec    string
		maxAgeStaleClientSec string
		enforced             string
		expectedParams       *RateLimitParameters
		expectError          bool
	}{
		{
			name:                 "Valid parameters",
			bucketSize:           "10",
			refillIntervalSec:    "30",
			maxAgeStaleClientSec: "60",
			enforced:             "true",
			expectedParams: &RateLimitParameters{
				BucketSize:        10,
				RefillInterval:    30 * time.Second,
				MaxAgeStaleClient: 60 * time.Second,
				Enforced:          true,
			},
			expectError: false,
		},
		{
			name:                 "Invalid bucket size",
			bucketSize:           "invalid",
			refillIntervalSec:    "30",
			maxAgeStaleClientSec: "60",
			enforced:             "true",
			expectedParams:       nil,
			expectError:          true,
		},
		{
			name:                 "Invalid refill interval",
			bucketSize:           "10",
			refillIntervalSec:    "invalid",
			maxAgeStaleClientSec: "60",
			enforced:             "true",
			expectedParams:       nil,
			expectError:          true,
		},
		{
			name:                 "Invalid max age stale client",
			bucketSize:           "10",
			refillIntervalSec:    "30",
			maxAgeStaleClientSec: "invalid",
			enforced:             "true",
			expectedParams:       nil,
			expectError:          true,
		},
		{
			name:                 "Enforced false",
			bucketSize:           "10",
			refillIntervalSec:    "30",
			maxAgeStaleClientSec: "60",
			enforced:             "false",
			expectedParams: &RateLimitParameters{
				BucketSize:        10,
				RefillInterval:    30 * time.Second,
				MaxAgeStaleClient: 60 * time.Second,
				Enforced:          false,
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params, err := GetRateLimitParameters(tt.bucketSize, tt.refillIntervalSec, tt.maxAgeStaleClientSec, tt.enforced)
			if tt.expectError {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, tt.expectedParams, params)
			}
		})
	}
}
