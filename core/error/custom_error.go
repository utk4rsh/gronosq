package error

import "errors"

type CustomError struct {
	msg string
}

func NewCustomError(msg string) error {
	return errors.New(msg)
}

func (error *CustomError) Error() string {
	return error.msg
}
