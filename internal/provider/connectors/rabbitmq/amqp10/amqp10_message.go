package amqp10

// rabbitMQAMQP10Table Simple map
type rabbitMQAMQP10Table map[string]any

// rabbitMQAMQP10Message Structure of a message
type rabbitMQAMQP10Message struct {
	delivery        any
	Body            []byte
	DeliveryMode    int
	ContentType     string
	ContentEncoding string
	Headers         rabbitMQAMQP10Table
	DeliveryTag     uint64
}

// SetDelivery convenience method for unit tests
func (msg *rabbitMQAMQP10Message) SetDelivery(delivery any) {
	msg.delivery = delivery
}
