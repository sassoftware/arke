package amqp10

import "github.com/Azure/go-amqp"

// rabbitmqAmqp10Error Error
type rabbitmqAmqp10Error struct {
	error amqp.Error
}

// Error Error
func (e *rabbitmqAmqp10Error) Error() string {
	return e.error.Error()
}

// Code Error condition from amqp.Error
// https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#type-amqp-error
func (e *rabbitmqAmqp10Error) Code() amqp.ErrCond {
	return e.error.Condition
}

// newAmqp10Error New error
func newAmqp10Error(e string, code amqp.ErrCond) rabbitmqAmqp10Error {
	err := rabbitmqAmqp10Error{}
	err.error = amqp.Error{
		Condition:   code,
		Description: e,
		Info:        map[string]any{},
	}
	return err
}
