package emitter

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/tecmise/asynchronous-event-channel-lib-go/pkg/definition"
	"github.com/tecmise/asynchronous-event-channel-lib-go/pkg/properties"
	"github.com/tecmise/asynchronous-event-channel-lib-go/pkg/publisher"
)

func NewSnsEmitter[R any](client *sns.Client, serviceName string) Channel[R] {
	return &publisherData[R]{
		publisher:   publisher.NewPublisher(client, serviceName),
		serviceName: serviceName,
	}
}

type Channel[R any] interface {
	OnUpdate(ctx context.Context, req R, metadata definition.EmitableMetadata, properties properties.FifoProperties) (*publisher.SnsTriggerResponse, error)
	OnDelete(ctx context.Context, req R, metadata definition.EmitableMetadata, properties properties.FifoProperties) (*publisher.SnsTriggerResponse, error)
	OnCreate(ctx context.Context, req R, metadata definition.EmitableMetadata, properties properties.FifoProperties) (*publisher.SnsTriggerResponse, error)
}

type publisherData[R any] struct {
	publisher   publisher.EmitterEntityEvent
	serviceName string
}

func (p publisherData[R]) OnUpdate(ctx context.Context, r R, metadata definition.EmitableMetadata, properties properties.FifoProperties) (*publisher.SnsTriggerResponse, error) {
	return p.publisher.Publish(ctx, definition.DTOEmitted[any]{
		Data:      r,
		Operation: "UPDATE",
	}, metadata.Publisher, fmt.Sprintf("OnUpdate %s", metadata.Name), &properties)
}

func (p publisherData[R]) OnCreate(ctx context.Context, r R, metadata definition.EmitableMetadata, properties properties.FifoProperties) (*publisher.SnsTriggerResponse, error) {
	return p.publisher.Publish(ctx, definition.DTOEmitted[any]{
		Data:      r,
		Operation: "INSERT",
	}, metadata.Publisher, fmt.Sprintf("OnCreate %s", metadata.Name), &properties)
}

func (p publisherData[R]) OnDelete(ctx context.Context, r R, metadata definition.EmitableMetadata, properties properties.FifoProperties) (*publisher.SnsTriggerResponse, error) {
	return p.publisher.Publish(ctx, definition.DTOEmitted[any]{
		Data:      r,
		Operation: "DELETE",
	}, metadata.Publisher, fmt.Sprintf("OnDelete %s", metadata.Name), &properties)
}
