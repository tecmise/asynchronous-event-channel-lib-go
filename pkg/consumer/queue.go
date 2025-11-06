package consumer

import (
	"github.com/tecmise/asynchronous-event-channel-lib-go/pkg/emitter"
	"time"
)

type SNSNotification[T any] struct {
	Type              string                `json:"Type"`
	MessageID         string                `json:"MessageId"`
	SequenceNumber    string                `json:"SequenceNumber"`
	TopicARN          string                `json:"TopicArn"`
	Subject           string                `json:"Subject"`
	Message           emitter.DTOEmitted[T] `json:"Message"`
	Timestamp         time.Time             `json:"Timestamp"`
	UnsubscribeURL    string                `json:"UnsubscribeURL"`
	MessageAttributes map[string]string     `json:"MessageAttributes"`
}
