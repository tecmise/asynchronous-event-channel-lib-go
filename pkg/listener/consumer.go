package listener

import (
	"context"
	"encoding/json"

	"github.com/sirupsen/logrus"
)

type (
	QueueConsumer interface {
		GetKey() string
		GetMessages(ctx context.Context) []*consumedMessage
		DeleteMessage(receiptHandle *string) error
	}

	consumedMessage struct {
		Id                      *string `json:"id"`
		Body                    *string `json:"body"`
		ReceiptHandle           *string `json:"-"` // Adicionado para exclusão
		ApproximateReceiveCount int     `json:"-"`
	}
)

func Start[T any](ctx context.Context, consumer QueueConsumer, run func(ctx context.Context, value *T) error) {
	messages := consumer.GetMessages(ctx)
	if len(messages) == 0 {
		logrus.WithField("queue", consumer.GetKey()).Info("Nenhuma mensagem na fila.")
	}

	fields := logrus.Fields{
		"queue": consumer.GetKey(),
	}
	for _, message := range messages {
		if message == nil {
			logrus.WithFields(fields).Warn("Mensagem nula recebida, ignorando...")
			continue
		}
		if message.Body == nil || *message.Body == "" {
			logrus.WithFields(fields).Warn("Mensagem com corpo vazio ou nulo recebida, ignorando...")
			continue
		}

		logrus.WithFields(logrus.Fields{
			"queue": consumer.GetKey(),
			"id":    *message.Id,
			"count": message.ApproximateReceiveCount,
		}).Debugf("Mensagem recebida: %s", *message.Body)

		var converted T
		err := json.Unmarshal([]byte(*message.Body), &converted)
		if err != nil {
			logrus.WithFields(fields).WithError(err).Error("Erro ao converter mensagem")
			continue
		}

		errRun := run(ctx, &converted)

		if errRun != nil {
			logrus.Errorf("Error to execute ")
		} else {
			errorToExclude := consumer.DeleteMessage(message.ReceiptHandle)
			if errorToExclude != nil {
				logrus.Errorf("Erro ao excluir mensagem: %v", errorToExclude)
			}
		}

	}
	Start[T](ctx, consumer, run)
}
