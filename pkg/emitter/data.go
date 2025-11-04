package emitter

import (
	"fmt"
	"github.com/tecmise/connector-lib/pkg/adapters/outbound/shared_kernel"
)

type Emitable[T any] interface {
	GetFifoProperties() *shared_kernel.FifoProperties
	Metadada() EmitableMetadata
	GetAsyncEmitterData() (*T, error)
}

type EmitableMetadata struct {
	Publisher string
	Name      string
}

type DTOEmitted[T any] struct {
	Data      T         `validate:"required"`
	Operation Operation `validate:"required"`
}

type Operation string

const (
	OperationCreate Operation = "CREATE"
	OperationUpdate Operation = "UPDATE"
	OperationDelete Operation = "DELETE"
)

// IsValid verifica se a operação é válida.
func (o Operation) IsValid() bool {
	switch o {
	case OperationCreate, OperationUpdate, OperationDelete:
		return true
	default:
		return false
	}
}

func ParseOperation(s string) (Operation, error) {
	op := Operation(s)
	if !op.IsValid() {
		return "", fmt.Errorf("operação inválida: %s", s)
	}
	return op, nil
}

func (o Operation) String() string {
	return string(o)
}
