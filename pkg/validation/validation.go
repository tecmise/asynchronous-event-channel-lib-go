package validation

import (
	"errors"
	"github.com/go-playground/validator/v10"
)

type (
	Validatable interface {
		ValidateBeforeEmit(functions ...ValidatorField) error
	}

	ValidatorField struct {
		Name   string
		Method func(fl validator.FieldLevel) bool
	}
)

func ValidateObject(req Validatable, validations ...ValidatorField) error {
	if req == nil {
		return errors.New("the request cannot be nil")
	}
	if err := req.ValidateBeforeEmit(validations...); err != nil {
		return err
	}
	return nil
}
