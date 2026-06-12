package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/sirupsen/logrus"
	"github.com/tecmise/asynchronous-event-channel-lib-go/pkg/definition"
	"github.com/tecmise/asynchronous-event-channel-lib-go/pkg/keys"
	"github.com/tecmise/asynchronous-event-channel-lib-go/pkg/properties"
)

type (
	EmitterEntityEvent interface {
		Publish(ctx context.Context, req definition.DTOEmitted[any], topicArn, subject string, fifoData *properties.FifoProperties) (*SnsTriggerResponse, error)
	}

	emitterEntityEvent struct {
		client     *sns.Client
		identifier string
	}
)

func NewPublisher(client *sns.Client, identifier string) EmitterEntityEvent {
	return &emitterEntityEvent{
		client:     client,
		identifier: identifier,
	}
}

func (a emitterEntityEvent) Publish(ctx context.Context, req definition.DTOEmitted[any], topicArn, subject string, fifoData *properties.FifoProperties) (*SnsTriggerResponse, error) {
	content, err := json.Marshal(req)
	if err != nil {
		logrus.Error("error marshaling request:", err)
		return nil, err
	}

	if topicArn == "" {
		return nil, fmt.Errorf("topic ARN cannot be empty verify into Metadada()")
	}

	isFifo := strings.HasSuffix(topicArn, ".fifo")

	if fifoData != nil && !isFifo {
		return nil, fmt.Errorf("fifo data provided but queue URL is not a FIFO queue (missing .fifo suffix)")
	}
	if fifoData == nil && isFifo {
		return nil, fmt.Errorf("queue URL is FIFO but no fifo data provided")
	}

	authenticatedUser := ctx.Value(keys.AuthenticatedUser)

	if authenticatedUser == nil || authenticatedUser.(string) == "" {
		logrus.Error("authenticated user not found in context")
		return nil, fmt.Errorf("authenticated user not found in context")
	}

	input := &sns.PublishInput{
		TopicArn: aws.String(topicArn),
		Subject:  aws.String(subject),
		MessageAttributes: map[string]types.MessageAttributeValue{
			keys.AuthenticatedUser: {
				DataType:    aws.String("String"),
				StringValue: aws.String(authenticatedUser.(string)),
			},
		},
		Message: aws.String(string(content)),
	}

	// IP do usuário é opcional (não vem em fluxos sem requisição HTTP, ex: jobs/system).
	// Quando presente no contexto, propaga como atributo pro consumer auditar a origem.
	if ip, ok := ctx.Value(keys.UserIP).(string); ok && ip != "" {
		input.MessageAttributes[keys.UserIP] = types.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(ip),
		}
	}

	if isFifo {
		input.MessageGroupId = aws.String(fifoData.MessageGroupId)
		if fifoData.MessageDeduplicationId != "" {
			input.MessageDeduplicationId = aws.String(fifoData.MessageDeduplicationId)
		}
	}

	message, err := a.client.Publish(ctx, input)
	if err != nil {
		logrus.Error("error sending message:", err)
		return nil, err
	}

	return &SnsTriggerResponse{
		MessageId:      aws.ToString(message.MessageId),
		SequenceNumber: aws.ToString(message.SequenceNumber),
	}, nil
}
