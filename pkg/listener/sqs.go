package listener

import (
	"context"
	"log"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/sirupsen/logrus"
)

type (
	sqsConsumer struct {
		client              *sqs.Client
		queueURL            string
		maxNumberOfMessages int32
		waitTimeSeconds     int32
	}
)

func (s *sqsConsumer) DeleteMessage(receiptHandle *string) error {
	_, err := s.client.DeleteMessage(context.TODO(), &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(s.queueURL),
		ReceiptHandle: receiptHandle,
	})
	return err
}

func (s *sqsConsumer) GetKey() string {
	return s.queueURL
}

func (s *sqsConsumer) GetMessages(ctx context.Context) []*consumedMessage {
	_queue := aws.String(s.queueURL)
	output, err := s.client.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
		QueueUrl:            _queue,
		MaxNumberOfMessages: s.maxNumberOfMessages,
		WaitTimeSeconds:     s.waitTimeSeconds,
	})

	if err != nil {
		logrus.WithError(err).Fatal("erro ao receber mensagens")
	}

	if len(output.Messages) == 0 {
		return []*consumedMessage{}
	}
	result := make([]*consumedMessage, len(output.Messages))

	for index, message := range output.Messages {
		var receiveCount int
		if val, ok := message.Attributes["ApproximateReceiveCount"]; ok {
			if parsed, err := strconv.Atoi(val); err == nil {
				receiveCount = parsed
			}
		}
		result[index] = &consumedMessage{
			Id:                      message.MessageId,
			Body:                    message.Body,
			ReceiptHandle:           message.ReceiptHandle, // Preenche o ReceiptHandle
			ApproximateReceiveCount: receiveCount,
		}
	}

	return result
}

func NewSqsConsumer(ctx context.Context, queueURL string, maxNumberOfMessages, waitTimeSeconds int32) QueueConsumer {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("não foi possível carregar a configuração do SDK, %v", err)
	}
	return &sqsConsumer{
		client:              sqs.NewFromConfig(cfg),
		queueURL:            queueURL,
		maxNumberOfMessages: maxNumberOfMessages,
		waitTimeSeconds:     waitTimeSeconds,
	}
}
