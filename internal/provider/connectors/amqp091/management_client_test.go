// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp091

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	pb "github.com/sassoftware/arke/api"
	iamqp "github.com/sassoftware/arke/internal/provider/connectors/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newManagementTestBrokerDetails(t *testing.T, endpoint string) *BrokerDetails {
	t.Helper()

	parsedEndpoint, err := url.Parse(endpoint)
	require.NoError(t, err)
	adminPort, err := strconv.Atoi(parsedEndpoint.Port())
	require.NoError(t, err)

	return &BrokerDetails{
		CommonBrokerDetails: iamqp.CommonBrokerDetails{
			ConnectionConfig: &pb.ConnectionConfiguration{
				Host:      parsedEndpoint.Hostname(),
				AdminPort: int32(adminPort), //nolint:gosec
				Credentials: &pb.Credentials{
					Username: "user",
					Password: "password",
				},
			},
		},
	}
}

func Test_doManagementRequestWithoutMarshalUsesSharedClient(t *testing.T) {
	tests := []struct {
		name            string
		statusCode      int
		wantStatusError bool
	}{
		{name: "successful request", statusCode: http.StatusOK},
		{
			name:            "non-2xx keeps provider error format",
			statusCode:      http.StatusServiceUnavailable,
			wantStatusError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			type requestInfo struct {
				method   string
				path     string
				username string
				password string
				hasAuth  bool
			}
			requests := make(chan requestInfo, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				username, password, hasAuth := req.BasicAuth()
				requests <- requestInfo{
					method:   req.Method,
					path:     req.URL.Path,
					username: username,
					password: password,
					hasAuth:  hasAuth,
				}
				w.WriteHeader(tt.statusCode)
				_, _ = w.Write([]byte("management response"))
			}))
			defer server.Close()

			bd := newManagementTestBrokerDetails(t, server.URL)
			body, err := bd.doManagementRequestWithoutMarshal(http.MethodGet, "/api/test")

			assert.Equal(t, []byte("management response"), body)
			request := <-requests
			assert.Equal(t, http.MethodGet, request.method)
			assert.Equal(t, "/api/test", request.path)
			assert.True(t, request.hasAuth)
			assert.Equal(t, "user", request.username)
			assert.Equal(t, "password", request.password)

			if !tt.wantStatusError {
				assert.NoError(t, err)
				return
			}

			require.Error(t, err)
			assert.Equal(t, fmt.Sprintf(
				"Error GET on management request %s/api/test: request returned a %d",
				server.URL, tt.statusCode,
			), err.Error())
		})
	}
}

func Test_doManagementRequestWithoutMarshalUsesBrokerPortFallback(t *testing.T) {
	requestHosts := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requestHosts <- req.Host
		_, _ = w.Write([]byte("management response"))
	}))
	defer server.Close()

	parsedEndpoint, err := url.Parse(server.URL)
	require.NoError(t, err)
	adminPort, err := strconv.Atoi(parsedEndpoint.Port())
	require.NoError(t, err)
	require.Greater(t, adminPort, 10000, "fallback broker port must be positive")

	bd := newManagementTestBrokerDetails(t, server.URL)
	bd.ConnectionConfig.AdminPort = 0
	bd.ConnectionConfig.Port = int32(adminPort - 10000) //nolint:gosec

	body, err := bd.doManagementRequestWithoutMarshal(http.MethodGet, "/api/test")

	require.NoError(t, err)
	assert.Equal(t, []byte("management response"), body)
	assert.Equal(t, parsedEndpoint.Host, <-requestHosts)
}

func Test_doManagementRequestWithoutMarshalWrapsRequestErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	endpoint := server.URL
	server.Close()

	bd := newManagementTestBrokerDetails(t, endpoint)
	_, err := bd.doManagementRequestWithoutMarshal(http.MethodGet, "/api/test")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "Error retrieving bindings:")
}

func Test_getBindingKeysForSourcePreservesEscapedPath(t *testing.T) {
	paths := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		paths <- req.URL.EscapedPath()
		_, _ = w.Write([]byte("[]"))
	}))
	defer server.Close()

	bd := newManagementTestBrokerDetails(t, server.URL)
	bd.ConnectionConfig.Tenant = "tenant /one"
	source := &pb.Source{
		Name: "queue /one",
		Address: &pb.Address{
			Name: "exchange &one",
		},
	}

	assert.Empty(t, bd.getBindingKeysForSource(source))
	assert.Equal(t, "/api/bindings/tenant+%2Fone/e/exchange+%26one/q/queue+%2Fone/", <-paths)
}

func Test_doManagementRequestWithoutMarshalUsesTLSConfig(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("secure response"))
	}))
	defer server.Close()

	bd := newManagementTestBrokerDetails(t, server.URL)
	bd.ConnectionConfig.Tls = true
	bd.tlsEnabled = true
	transport, ok := server.Client().Transport.(*http.Transport)
	require.True(t, ok)
	bd.tlsConfig = transport.TLSClientConfig

	body, err := bd.doManagementRequestWithoutMarshal(http.MethodGet, "/api/test")

	require.NoError(t, err)
	assert.Equal(t, []byte("secure response"), body)
	client, err := bd.getManagementClient()
	require.NoError(t, err)
	assert.NotNil(t, client)
}
