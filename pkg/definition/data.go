package definition

import "github.com/tecmise/asynchronous-event-channel-lib-go/pkg/properties"

type Emitable[T any] interface {
	GetFifoProperties() *properties.FifoProperties
	Metadada() EmitableMetadata
	GetAsyncEmitterData() (*T, error)
}

type EmitableMetadata struct {
	Publisher string
	Name      string
}

type DTOEmitted[T any] struct {
	Data      T      `validate:"required"`
	Operation string `validate:"required"`
}

type StructMapper[Entity any, Proto any] interface {
	ToProto(Entity) Proto
	Validate(Proto) error
}
