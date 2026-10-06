// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"

	pb "github.com/sassoftware/arke/api"
)

const (
	defaultAdminPort = 15672
	EnvAdminPortName = "ARKE_BROKER_ADMIN_PORT"
)

var (
	envAdminPortOnce  = sync.Once{}
	envAdminPortValue int32
)

// getAdminPort returns the admin port for the AMQP connection. The order of
// precedence is: connection configuration, environment variable, default value.
func getAdminPort(cf *pb.ConnectionConfiguration) int32 {
	// Prefer the admin port specified in the connection configuration first.
	if cf.GetAdminPort() != 0 {
		return cf.GetAdminPort()
	}

	// Next, check if the admin port is specified in the environment variable.
	envAdminPortOnce.Do(func() {
		if val, err := strconv.ParseInt(strings.TrimSpace(os.Getenv(EnvAdminPortName)), 10, 32); err == nil {
			envAdminPortValue = int32(val)
		}
	})
	if envAdminPortValue != 0 {
		return envAdminPortValue
	}

	// Fallback to the default admin port if neither the connection
	// configuration nor the environment variable specifies it.
	return defaultAdminPort
}

func GetConnURL(cf *pb.ConnectionConfiguration) string {
	username := cf.GetCredentials().GetUsername()
	password := cf.GetCredentials().GetPassword()
	host := cf.GetHost()
	port := cf.GetPort()
	tenant := cf.GetTenant()
	if tenant == "" {
		tenant = "/"
	}
	scheme := "amqp"
	if cf.GetTls() {
		scheme = "amqps"
	}
	return (&url.URL{
		Scheme: scheme,
		User:   url.UserPassword(username, password),
		Host:   net.JoinHostPort(host, strconv.Itoa(int(port))),
		Path:   tenant,
	}).String()
}

func GetVhost(cf *pb.ConnectionConfiguration) string {
	tenant := cf.GetTenant()
	if tenant == "" {
		tenant = "/"
	}
	return tenant
}

// GetMgmtEndpoint returns the management endpoint URL with port for the AMQP
// connection, using the appropriate scheme (http or https) and the admin port.
func GetMgmtEndpoint(cf *pb.ConnectionConfiguration) string {
	endpoint := "http://"
	if cf.GetTls() {
		endpoint = "https://"
	}
	host := cf.GetHost()
	port := getAdminPort(cf)
	endpoint += fmt.Sprintf("%s:%d", host, port)
	return endpoint
}

func GetUsernamePassword(cf *pb.ConnectionConfiguration) (string, string) {
	return cf.GetCredentials().GetUsername(), cf.GetCredentials().GetPassword()
}

func SourceName(source *pb.Source) string {
	if IsQuorum(source) && !strings.HasSuffix(source.GetName(), ".quorum") {
		return source.GetName() + ".quorum"
	}
	return source.GetName()
}

func IsQuorum(source *pb.Source) bool {
	if source.GetAutoDelete() {
		return false
	}
	return source.GetType() == pb.Source_QUEUE
}
