package amqp10

var clientIdentifierCtxKey = CtxKey{name: "clientIdentifier"}

type CtxKey struct {
	name string
}
