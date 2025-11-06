package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/sirupsen/logrus"
	"github.com/tecmise/asynchronous-event-channel-lib-go/pkg/definition"
	"github.com/tecmise/asynchronous-event-channel-lib-go/pkg/properties"
	"os"
	"strings"
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

func (a emitterEntityEvent) Publish(ctx context.Context, req definition.DTOEmitted[any], topicKeyVariable, subject string, fifoData *properties.FifoProperties) (*SnsTriggerResponse, error) {
	content, err := json.Marshal(req)
	if err != nil {
		logrus.Error("error marshaling request:", err)
		return nil, err
	}

	if topicKeyVariable == "" {
		return nil, fmt.Errorf("topic ARN cannot be empty")
	}

	topicArn := os.Getenv(topicKeyVariable)

	if topicArn == "" {
		return nil, fmt.Errorf("environment variable %s for topic ARN is not set or empty", topicKeyVariable)
	}

	isFifo := strings.HasSuffix(topicArn, ".fifo")

	if fifoData != nil && !isFifo {
		return nil, fmt.Errorf("fifo data provided but queue URL is not a FIFO queue (missing .fifo suffix)")
	}
	if fifoData == nil && isFifo {
		return nil, fmt.Errorf("queue URL is FIFO but no fifo data provided")
	}

	input := &sns.PublishInput{
		TopicArn: aws.String(topicArn),
		Subject:  aws.String(subject),
		Message:  aws.String(string(content)),
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
