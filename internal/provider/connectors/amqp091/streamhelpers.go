// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp091

import (
	"fmt"
	"net/url"
)

const streamPort = 5552

type CtxKey struct {
	name string
}

// getStreamConnectionString constructs the provider-specific Stream URI.
func getStreamConnectionString(bd *BrokerDetails) string {
	cfg := bd.ConnectionConfig
	tenant := cfg.GetTenant()
	if tenant == "" {
		tenant = "/"
	}
	scheme := "rabbitmq-stream"
	if bd.tlsEnabled {
		scheme += "+tls"
	}
	return fmt.Sprintf("%s://%s:%s@%s:%d/%s", scheme,
		url.QueryEscape(cfg.GetCredentials().GetUsername()),
		url.QueryEscape(cfg.GetCredentials().GetPassword()),
		cfg.GetHost(), streamPort, url.QueryEscape(tenant))
}
