package emitter

import (
	"context"
	"github.com/tecmise/asynchronous-event-channel-lib-go/pkg/response"
)

type EventData[T any] interface {
	OnUpdate(ctx context.Context, req *T, subject string) (*response.Message, error)
	OnCreate(ctx context.Context, req *T, subject string) (*response.Message, error)
	OnDelete(ctx context.Context, req *T, subject string) (*response.Message, error)
}
