package emitter

import (
	"fmt"
	"github.com/tecmise/connector-lib/pkg/adapters/outbound/shared_kernel"
	"github.com/tecmise/connector-lib/pkg/ports/output/request"
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

type DTOEmitted[T request.Validatable] struct {
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

// ParseOperation converte string para Operation ou retorna erro.
func ParseOperation(s string) (Operation, error) {
	op := Operation(s)
	if !op.IsValid() {
		return "", fmt.Errorf("operação inválida: %s", s)
	}
	return op, nil
}
