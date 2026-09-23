package amqp10

// rabbitmqAmqp10Table Simple map
type rabbitmqAmqp10Table map[string]interface{}

// rabbitmqAmqp10Message Structure of a message
type rabbitmqAmqp10Message struct {
	delivery        interface{}
	Body            []byte
	DeliveryMode    int
	ContentType     string
	ContentEncoding string
	Headers         rabbitmqAmqp10Table
	DeliveryTag     uint64
}

// SetDelivery convenience method for unit tests
func (msg *rabbitmqAmqp10Message) SetDelivery(delivery interface{}) {
	msg.delivery = delivery
}
