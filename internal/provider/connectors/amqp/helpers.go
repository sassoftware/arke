// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp

import (
	"net"
	"net/url"
	"strconv"

	pb "github.com/sassoftware/arke/api"
)

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
