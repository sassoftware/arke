package amqp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	pb "github.com/sassoftware/arke/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type ManagementClientMock struct {
	*mock.Mock
}

func (m *ManagementClientMock) Do(req *http.Request) ([]byte, int, error) {
	args := m.Called(req)
	return args.Get(0).([]byte), args.Int(1), args.Error(2)
}

func (m *ManagementClientMock) SourceStats(vhost, queueName string) (*pb.SourceStats, error) {
	args := m.Called(vhost, queueName)
	return args.Get(0).(*pb.SourceStats), args.Error(1)
}

func Test_makeRequest(t *testing.T) {
	t.Run("sets JSON headers and basic authentication", func(t *testing.T) {
		req, err := makeRequest(context.Background(), http.MethodGet, "http://example.com/api", "user", "pass")

		require.NoError(t, err)
		username, password, ok := req.BasicAuth()
		assert.True(t, ok)
		assert.Equal(t, "user", username)
		assert.Equal(t, "pass", password)
		assert.Equal(t, "application/json", req.Header.Get("Accept"))
		assert.Equal(t, "application/json", req.Header.Get("Content-Type"))
	})

	t.Run("omits basic authentication when either credential is empty", func(t *testing.T) {
		req, err := makeRequest(context.Background(), http.MethodGet, "http://example.com/api", "user", "")

		require.NoError(t, err)
		assert.Empty(t, req.Header.Get("Authorization"))
	})

	t.Run("returns invalid request errors", func(t *testing.T) {
		_, err := makeRequest(context.Background(), "bad method", "http://example.com/api", "", "")

		assert.Error(t, err)
	})
}

func Test_AMQPManagementClient_Do(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("response body"))
	}))
	defer server.Close()

	client := &AMQPManagementClient{
		Ctx:    context.Background(),
		client: server.Client(),
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
	require.NoError(t, err)

	body, status, err := client.Do(req)

	require.NoError(t, err)
	assert.Equal(t, http.StatusAccepted, status)
	assert.Equal(t, "response body", string(body))
}

func Test_AMQPManagementClient_SourceStats(t *testing.T) {
	tests := []struct {
		name             string
		typeName         string
		vhost            string
		queueName        string
		expectedPath     string
		expectedMsgCount int64
	}{
		{
			name:             "parses quorum stats and escapes path segments",
			typeName:         "quorum",
			vhost:            "tenant /one",
			queueName:        "orders & one",
			expectedPath:     "/api/queues/tenant+%2Fone/orders+%26+one",
			expectedMsgCount: 19,
		},
		{
			name:         "defaults empty vhost and does not set stream message count",
			typeName:     "stream",
			queueName:    "events",
			expectedPath: "/api/queues/%2F/events",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			type requestInfo struct {
				path          string
				authorization string
			}
			requests := make(chan requestInfo, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				requests <- requestInfo{path: req.URL.EscapedPath(), authorization: req.Header.Get("Authorization")}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"consumers":4,"messages":19,"type":"` + tt.typeName + `","message_stats":{"publish_details":{"rate":1.25},"deliver_details":{"rate":2.5}}}`))
			}))
			defer server.Close()

			client, err := NewManagementClient(context.Background(), server.URL, "user", "pass", nil)
			require.NoError(t, err)

			stats := client.SourceStats(tt.vhost, tt.queueName)

			require.NotNil(t, stats)
			assert.Nil(t, stats.Error)
			assert.Equal(t, int32(4), stats.ConsumerCount)
			assert.Equal(t, float32(1.25), stats.PublishRate)
			assert.Equal(t, float32(2.5), stats.DeliverRate)
			assert.Equal(t, tt.expectedMsgCount, stats.MessageCount)
			request := <-requests
			assert.Equal(t, tt.expectedPath, request.path)
			assert.Equal(t, "Basic dXNlcjpwYXNz", request.authorization)
		})
	}
}

func Test_AMQPManagementClient_SourceStatsInvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not JSON"))
	}))
	defer server.Close()

	client, err := NewManagementClient(context.Background(), server.URL, "user", "pass", nil)
	require.NoError(t, err)

	stats := client.SourceStats("/", "queue")

	require.NotNil(t, stats)
	require.NotNil(t, stats.Error)
	assert.Contains(t, stats.Error.Message, "invalid character")
}
