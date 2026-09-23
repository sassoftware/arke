package amqp10

import (
	"net"
	"net/url"
	"strconv"

	pb "github.com/sassoftware/arke/api"
)

var clientIdentifierCtxKey = CtxKey{name: "clientIdentifier"}

type CtxKey struct {
	name string
}

func getConnURL(cf *pb.ConnectionConfiguration) string {
	username := cf.GetCredentials().GetUsername()
	password := cf.GetCredentials().GetPassword()
	host := cf.GetHost()
	port := cf.GetPort()
	scheme := "amqp"
	if cf.GetTls() {
		scheme = "amqps"
	}
	return (&url.URL{
		Scheme: scheme,
		User:   url.UserPassword(username, password),
		Host:   net.JoinHostPort(host, strconv.Itoa(int(port))),
	}).String()
}
