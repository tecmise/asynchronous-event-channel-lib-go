package emitter

import (
	"github.com/tecmise/connector-lib/pkg/adapters/outbound/shared_kernel"
)

type Emitable[T any] interface {
	GetFifoProperties() *shared_kernel.FifoProperties
	Metadada() EmitableMetadata
}

type EmitableMetadata struct {
	Publisher string
	Name      string
}
