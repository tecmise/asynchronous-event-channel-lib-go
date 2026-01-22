package consumer

import (
	"time"
)

type SNSNotification struct {
	Type              string                 `json:"Type"`
	MessageID         string                 `json:"MessageId"`
	SequenceNumber    string                 `json:"SequenceNumber"`
	TopicARN          string                 `json:"TopicArn"`
	Subject           string                 `json:"Subject"`
	Message           string                 `json:"Message"`
	Timestamp         time.Time              `json:"Timestamp"`
	UnsubscribeURL    string                 `json:"UnsubscribeURL"`
	MessageAttributes map[string]interface{} `json:"messageAttributes"`
}
