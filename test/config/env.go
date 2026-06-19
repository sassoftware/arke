// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"log"
	"os"
	"strconv"

	pb "github.com/sassoftware/arke/api"
)

const brokerP = "ARKE_BROKER_PASSWORD"

func getenv(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return defaultValue
}

// ConnectionConfigurationFromEnv read environment vars and return a ConnectionConfiguration
func ConnectionConfigurationFromEnv() pb.ConnectionConfiguration {
	// needed: ARKE_BROKER_HOSTNAME, ARKE_BROKER_PORT, ARKE_BROKER_USERNAME, ARKE_BROKER_PASSWORD
	// ARKE_BROKER_TYPE
	hostname := getenv("ARKE_BROKER_HOSTNAME", "rabbitmq")
	rawport := getenv("ARKE_BROKER_PORT", "5672")
	port, err := strconv.ParseInt(rawport, 10, 32)
	if err != nil {
		log.Fatalf("Could not convert '%s' to int", rawport)
	}

	rawAdminPort := getenv("ARKE_BROKER_ADMIN_PORT", "0")
	adminPort, err := strconv.ParseInt(rawAdminPort, 10, 32)
	if err != nil {
		log.Fatalf("Could not convert '%s' to int", rawAdminPort)
	}
	username := getenv("ARKE_BROKER_USERNAME", "guest")
	password := getenv(brokerP, "guest")
	brokerType := getenv("ARKE_BROKER_TYPE", "amqp091")
	tenant := getenv("ARKE_BROKER_TENANT", "/")

	creds := &pb.Credentials{
		Username: username,
		Password: password,
	}

	connConf := pb.ConnectionConfiguration{
		Host:        hostname,
		Port:        int32(port),
		Credentials: creds,
		Provider:    brokerType,
		Tenant:      tenant,
		AdminPort:   int32(adminPort),
	}
	return connConf //nolint
}
