package amqp10

// rabbitMQAMQP10Table Simple map
type rabbitMQAMQP10Table map[string]interface{}

// rabbitMQAMQP10Message Structure of a message
type rabbitMQAMQP10Message struct {
	delivery        interface{}
	Body            []byte
	DeliveryMode    int
	ContentType     string
	ContentEncoding string
	Headers         rabbitMQAMQP10Table
	DeliveryTag     uint64
}

// SetDelivery convenience method for unit tests
func (msg *rabbitMQAMQP10Message) SetDelivery(delivery interface{}) {
	msg.delivery = delivery
}
