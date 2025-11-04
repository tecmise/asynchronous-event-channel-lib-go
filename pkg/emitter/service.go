package emitter

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/tecmise/connector-lib/pkg/adapters/outbound/client_sns"
	"github.com/tecmise/connector-lib/pkg/adapters/outbound/shared_kernel"
	"github.com/tecmise/connector-lib/pkg/ports/output/assync"
)

func NewSnsEmitter[E any, R any](client *sns.Client, serviceName string) Channel[E, R] {
	return &publisherData[E, R]{
		publisher:   client_sns.NewPublisher(client, serviceName),
		serviceName: serviceName,
	}
}

type Channel[E any, R any] interface {
	OnUpdate(ctx context.Context, req R, metadata EmitableMetadata, properties shared_kernel.FifoProperties) (*assync.SnsTriggerResponse, error)
	OnDelete(ctx context.Context, req R, metadata EmitableMetadata, properties shared_kernel.FifoProperties) (*assync.SnsTriggerResponse, error)
	OnCreate(ctx context.Context, req R, metadata EmitableMetadata, properties shared_kernel.FifoProperties) (*assync.SnsTriggerResponse, error)
}

type publisherData[E any, R any] struct {
	publisher   client_sns.AssyncPublisherSns
	serviceName string
}

func (p publisherData[E, R]) OnUpdate(ctx context.Context, req R, metadata EmitableMetadata, properties shared_kernel.FifoProperties) (*assync.SnsTriggerResponse, error) {
	//if err := request.ValidateObject(req); err != nil {
	//	return nil, err
	//}
	//return p.publisher.Publish(ctx, req, metadata.Publisher, fmt.Sprintf("OnUpdate %s", metadata.Name), &properties, map[string]string{
	//	"service":   p.serviceName,
	//	"operation": "update",
	//})
	return nil, nil
}

func (p publisherData[E, R]) OnCreate(ctx context.Context, req R, metadata EmitableMetadata, properties shared_kernel.FifoProperties) (*assync.SnsTriggerResponse, error) {
	//if err := request.ValidateObject(req); err != nil {
	//	return nil, err
	//}
	//return p.publisher.Publish(ctx, req, metadata.Publisher, fmt.Sprintf("OnCreate %s", metadata.Name), &properties, map[string]string{
	//	"service":   p.serviceName,
	//	"operation": "insert",
	//})
	return nil, nil
}

func (p publisherData[E, R]) OnDelete(ctx context.Context, req R, metadata EmitableMetadata, properties shared_kernel.FifoProperties) (*assync.SnsTriggerResponse, error) {
	//if err := request.ValidateObject(req); err != nil {
	//	return nil, err
	//}
	//return p.publisher.Publish(ctx, req, metadata.Publisher, fmt.Sprintf("OnDelete %s", metadata.Name), &properties, map[string]string{
	//	"service":   p.serviceName,
	//	"operation": "delete",
	//})
	return nil, nil
}
