package amqp10

import "github.com/Azure/go-amqp"

// rabbitMQAMQP10Error Error
type rabbitMQAMQP10Error struct {
	error amqp.Error
}

// Error Error
func (e *rabbitMQAMQP10Error) Error() string {
	return e.error.Error()
}

// Code Error condition from amqp.Error
// https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#type-amqp-error
func (e *rabbitMQAMQP10Error) Code() amqp.ErrCond {
	return e.error.Condition
}

// newRabbitAMQP10Error New error
func newRabbitAMQP10Error(e string, code amqp.ErrCond) rabbitMQAMQP10Error {
	err := rabbitMQAMQP10Error{}
	err.error = amqp.Error{
		Condition:   code,
		Description: e,
		Info:        map[string]any{},
	}
	return err
}
