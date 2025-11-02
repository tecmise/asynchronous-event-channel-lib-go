package emitter

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/tecmise/connector-lib/pkg/adapters/outbound/client_sns"
	"github.com/tecmise/connector-lib/pkg/ports/output/assync"
	"github.com/tecmise/connector-lib/pkg/ports/output/request"
)

func NewSnsEmitter[T request.Validatable](client *sns.Client, serviceName string) Channel[T] {
	return &publisherData[T]{
		publisher:   client_sns.NewPublisher(client, serviceName),
		serviceName: serviceName,
	}
}

type Channel[T any] interface {
	OnDelete(ctx context.Context, req T, emit Emitable[T]) (*assync.SnsTriggerResponse, error)
	OnUpdate(ctx context.Context, req T, emit Emitable[T]) (*assync.SnsTriggerResponse, error)
	OnCreate(ctx context.Context, req T, emit Emitable[T]) (*assync.SnsTriggerResponse, error)
}

type publisherData[T request.Validatable] struct {
	publisher   client_sns.AssyncPublisherSns
	serviceName string
}

func (p publisherData[T]) OnUpdate(ctx context.Context, req T, emit Emitable[T]) (*assync.SnsTriggerResponse, error) {
	if err := request.ValidateObject(req); err != nil {
		return nil, err
	}
	metadata := emit.Metadada()
	return p.publisher.Publish(ctx, req, metadata.Publisher, fmt.Sprintf("OnUpdate %s", metadata.Name), emit.GetFifoProperties(), map[string]string{
		"service":   p.serviceName,
		"operation": "update",
	})
}

func (p publisherData[T]) OnCreate(ctx context.Context, req T, emit Emitable[T]) (*assync.SnsTriggerResponse, error) {
	if err := request.ValidateObject(req); err != nil {
		return nil, err
	}
	metadata := emit.Metadada()
	return p.publisher.Publish(ctx, req, metadata.Publisher, fmt.Sprintf("OnCreate %s", metadata.Name), emit.GetFifoProperties(), map[string]string{
		"service":   p.serviceName,
		"operation": "insert",
	})
}

func (p publisherData[T]) OnDelete(ctx context.Context, req T, emit Emitable[T]) (*assync.SnsTriggerResponse, error) {
	if err := request.ValidateObject(req); err != nil {
		return nil, err
	}
	metadata := emit.Metadada()
	return p.publisher.Publish(ctx, req, metadata.Publisher, fmt.Sprintf("OnDelete %s", metadata.Name), emit.GetFifoProperties(), map[string]string{
		"service":   p.serviceName,
		"operation": "delete",
	})
}
