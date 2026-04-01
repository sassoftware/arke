// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build integration || failover

package integration

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	pb "github.com/sassoftware/arke/api"
	cfg "github.com/sassoftware/arke/test/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func arkeAddress() string {
	var arkeHost string
	var arkePort string
	if value, ok := os.LookupEnv("ARKE_INTEGRATION_HOSTNAME"); ok {
		arkeHost = value
	} else {
		arkeHost = "localhost"
	}
	if value, ok := os.LookupEnv("ARKE_INTEGRATION_PORT"); ok {
		arkePort = value
	} else {
		arkePort = "50051"
	}
	return fmt.Sprintf("%s:%s", arkeHost, arkePort)
}

func connectConfig(clientName string) *pb.ConnectionConfiguration {
	connConfig := cfg.ConnectionConfigurationFromEnv()

	providerTLS := strings.ToLower(os.Getenv("ARKE_PROVIDER_TLS"))

	if providerTLS == "sendca" {
		cacert, err := os.ReadFile("certs/testca/ca_certificate.pem")
		if err != nil {
			log.Fatalf("Error reading provider CA cert: %v", err)
		}
		connConfig.Tls = true
		connConfig.CaCertificate = cacert
	} else if providerTLS == "true" {
		connConfig.Tls = true
	}

	connConfig.ClientName = clientName

	return &connConfig
}

func connect() *grpc.ClientConn {
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()

	var conn *grpc.ClientConn
	var err error
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Attempt a non-TLS connection to arke first
	conn, err = grpc.NewClient(arkeAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}

	c := healthpb.NewHealthClient(conn)
	resp, err := c.Check(ctx, &healthpb.HealthCheckRequest{Service: "arke"})

	// If the health check failed, try with TLS
	if err != nil && (resp == nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING) {
		b, rErr := os.ReadFile("certs/testca/ca_certificate.pem")
		if rErr != nil {
			log.Fatal(err)
		}
		cp := x509.NewCertPool()
		if !cp.AppendCertsFromPEM(b) {
			log.Fatalf("client did not connect: %v", "credentials: failed to append certificates")
		}
		tlsConfig := &tls.Config{RootCAs: cp}

		conn, _ = grpc.NewClient(arkeAddress(), grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))) // , grpc.WithInsecure()

		c := healthpb.NewHealthClient(conn)
		resp, err = c.Check(ctx, &healthpb.HealthCheckRequest{Service: "arke"})
	}

	if err != nil && resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		log.Fatalf("client did not connect: %v", err)
	}
	return conn
}
