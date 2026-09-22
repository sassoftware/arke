package amqp10

import "github.com/Azure/go-amqp"

// amqp10Error Error
type amqp10Error struct {
	error amqp.Error
}

// Error Error
func (e *amqp10Error) Error() string {
	return e.error.Error()
}

// Code Error condition from amqp.Error
// https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#type-amqp-error
func (e *amqp10Error) Code() amqp.ErrCond {
	return e.error.Condition
}

// newAmqp10Error New error
func newAmqp10Error(e string, code amqp.ErrCond) amqp10Error {
	err := amqp10Error{}
	err.error = amqp.Error{
		Condition:   code,
		Description: e,
		Info:        map[string]any{},
	}
	return err
}
