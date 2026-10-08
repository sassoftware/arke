package amqp

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/util"
)

const (
	queueTypeQuorum = "quorum"
	queueTypeStream = "stream"
)

type queueStats struct {
	MessageStats struct {
		PublishDetails struct {
			Rate float64 `json:"rate"`
		} `json:"publish_details"`
		DeliverDetails struct {
			Rate float64 `json:"rate"`
		} `json:"deliver_details"`
	} `json:"message_stats"`
	Consumers float64 `json:"consumers"`
	Messages  float64 `json:"messages"`
	Type      string  `json:"type"` // one of classic, quorum, stream
}

type ManagementClientShim interface {
	Do(req *http.Request) ([]byte, int, error)
	SourceStats(vhost, queueName string) *pb.SourceStats
}

type AMQPManagementClient struct {
	Ctx      context.Context
	client   *http.Client
	endpoint string
	username string
	password string
}

// NewManagementClient creates a new instance of AMQPManagementClient with the
// given parameters. If not port is specified in the endpoint, it will be
// looked up with amqp.GetAdminPort.
func NewManagementClient(ctx context.Context, endpoint, username, password string, tlsConfig *tls.Config) (*AMQPManagementClient, error) {
	if !strings.HasPrefix(endpoint, "http") {
		return nil, fmt.Errorf("endpoint must start with http or https: %s", endpoint)
	}
	c := AMQPManagementClient{
		Ctx:      ctx,
		client:   &http.Client{Timeout: 5 * time.Second},
		username: username,
		password: password,
		endpoint: endpoint,
	}
	if tlsConfig != nil {
		c.client.Transport = &http.Transport{
			TLSClientConfig: tlsConfig,
		}
	}
	return &c, nil
}

func makeRequest(ctx context.Context, method, url, username, password string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return nil, err
	}
	if username != "" && password != "" {
		req.SetBasicAuth(username, password)
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")
	return req, nil
}

// Do performs an HTTP request to the AMQP management endpoint and returns the
// response body, status code, and any error encountered.
func (c *AMQPManagementClient) Do(req *http.Request) ([]byte, int, error) {
	resp, err := c.client.Do(req) //nolint:gosec
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return data, resp.StatusCode, nil
}

// Request sends an authenticated request to a path on the management API.
func (c *AMQPManagementClient) Request(ctx context.Context, method, path string) ([]byte, int, error) {
	requestURL := strings.TrimRight(c.endpoint, "/") + "/" + strings.TrimLeft(path, "/")
	req, err := makeRequest(ctx, method, requestURL, c.username, c.password)
	if err != nil {
		return nil, 0, err
	}
	return c.Do(req)
}

// SourceStats retrieves statistics for a specific queue in the given virtual host.
// Any error encountered will be in the Error field of the returned SourceStats.
func (c *AMQPManagementClient) SourceStats(vhost, queueName string) *pb.SourceStats {
	stats := &pb.SourceStats{}

	queue := url.QueryEscape(queueName)
	if vhost == "" {
		vhost = "/"
	}
	vhost = url.QueryEscape(vhost)

	urn := fmt.Sprintf("/api/queues/%s/%s", vhost, queue)
	body, _, err := c.Request(c.Ctx, http.MethodGet, urn)
	if err != nil {
		stats.Error = &pb.Error{Message: err.Error()}
		return stats
	}
	var qStats queueStats
	if err := json.Unmarshal(body, &qStats); err != nil {
		util.Logger.Debugf("Failed to unmarshal management API response into queueStats struct: %s", err.Error())
		stats.Error = &pb.Error{Message: err.Error()}
		return stats
	} else {
		stats.PublishRate = float32(qStats.MessageStats.PublishDetails.Rate)
		stats.DeliverRate = float32(qStats.MessageStats.DeliverDetails.Rate)
		stats.ConsumerCount = int32(qStats.Consumers)
	}
	switch qStats.Type {
	case queueTypeQuorum:
		stats.MessageCount = int64(qStats.Messages)
	case queueTypeStream:
		// TODO - Issue 220/221
	}
	return stats
}
