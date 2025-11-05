package emitter

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/tecmise/connector-lib/pkg/adapters/outbound/client_sns"
	"github.com/tecmise/connector-lib/pkg/adapters/outbound/shared_kernel"
	"github.com/tecmise/connector-lib/pkg/ports/output/assync"
	"github.com/tecmise/connector-lib/pkg/ports/output/request"
)

func NewSnsEmitter[R any](client *sns.Client, serviceName string) Channel[R] {
	return &publisherData[R]{
		publisher:   client_sns.NewPublisher(client, serviceName),
		serviceName: serviceName,
	}
}

type Channel[R any] interface {
	OnUpdate(ctx context.Context, req R, metadata EmitableMetadata, properties shared_kernel.FifoProperties) (*assync.SnsTriggerResponse, error)
	OnDelete(ctx context.Context, req R, metadata EmitableMetadata, properties shared_kernel.FifoProperties) (*assync.SnsTriggerResponse, error)
	OnCreate(ctx context.Context, req R, metadata EmitableMetadata, properties shared_kernel.FifoProperties) (*assync.SnsTriggerResponse, error)
}

type publisherData[R any] struct {
	publisher   client_sns.AssyncPublisherSns
	serviceName string
}

func (p publisherData[R]) asValidatable(r R) (request.Validatable, bool) {
	if v, ok := any(r).(request.Validatable); ok {
		return v, true
	}
	if v, ok := any(&r).(request.Validatable); ok {
		return v, true
	}
	return nil, false
}

func (p publisherData[R]) OnUpdate(ctx context.Context, r R, metadata EmitableMetadata, properties shared_kernel.FifoProperties) (*assync.SnsTriggerResponse, error) {
	req, ok := p.asValidatable(r)
	if !ok {
		return nil, fmt.Errorf("request does not implement request.Validatable (type %T)", r)
	}
	return p.publisher.Publish(ctx, DTOEmitted[request.Validatable]{
		Data:      req,
		Operation: "UPDATE",
	}, metadata.Publisher, fmt.Sprintf("OnUpdate %s", metadata.Name), &properties, nil)
}

func (p publisherData[R]) OnCreate(ctx context.Context, r R, metadata EmitableMetadata, properties shared_kernel.FifoProperties) (*assync.SnsTriggerResponse, error) {
	req, ok := p.asValidatable(r)
	if !ok {
		return nil, fmt.Errorf("request does not implement request.Validatable (type %T)", r)
	}
	return p.publisher.Publish(ctx, DTOEmitted[request.Validatable]{
		Data:      req,
		Operation: "INSERT",
	}, metadata.Publisher, fmt.Sprintf("OnCreate %s", metadata.Name), &properties, nil)
}

func (p publisherData[R]) OnDelete(ctx context.Context, r R, metadata EmitableMetadata, properties shared_kernel.FifoProperties) (*assync.SnsTriggerResponse, error) {
	req, ok := p.asValidatable(r)
	if !ok {
		return nil, fmt.Errorf("request does not implement request.Validatable (type %T)", r)
	}
	return p.publisher.Publish(ctx, DTOEmitted[request.Validatable]{
		Data:      req,
		Operation: "DELETE",
	}, metadata.Publisher, fmt.Sprintf("OnDelete %s", metadata.Name), &properties, nil)
}
