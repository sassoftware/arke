package amqp10

// amqp10Table Simple map
type amqp10Table map[string]interface{}

// amqp10Message Structure of a message
type amqp10Message struct {
	delivery        interface{}
	Body            []byte
	DeliveryMode    int
	ContentType     string
	ContentEncoding string
	Headers         amqp10Table
	DeliveryTag     uint64
}

// SetDelivery convenience method for unit tests
func (msg *amqp10Message) SetDelivery(delivery interface{}) {
	msg.delivery = delivery
}
