package events

import (
	"time"
)

type SnsEvent struct {
	Records []SnsMessage `json:"Records"`
}

type SQSMessageAttribute struct {
	StringValue      *string  `json:"stringValue,omitempty"`
	BinaryValue      []byte   `json:"binaryValue,omitempty"`
	StringListValues []string `json:"stringListValues"`
	BinaryListValues [][]byte `json:"binaryListValues"`
	DataType         string   `json:"dataType"`
}

type SnsMessage struct {
	MessageId              string `json:"messageId"`
	ReceiptHandle          string `json:"receiptHandle"`
	Body                   `json:"body"`
	Md5OfBody              string                       `json:"md5OfBody"`
	Md5OfMessageAttributes string                       `json:"md5OfMessageAttributes"`
	Attributes             Attributes                   `json:"attributes"`
	MessageAttributes      map[string]MessageAttributes `json:"messageAttributes"`
	EventSourceARN         string                       `json:"eventSourceARN"`
	EventSource            string                       `json:"eventSource"`
	AwsRegion              string                       `json:"awsRegion"`
}

type Body struct {
	Type              string                       `json:"Type"`
	MessageId         string                       `json:"MessageId"`
	SequenceNumber    string                       `json:"SequenceNumber"`
	TopicArn          string                       `json:"TopicArn"`
	Subject           string                       `json:"Subject"`
	Message           string                       `json:"Message"`
	Timestamp         time.Time                    `json:"Timestamp"`
	UnsubscribeURL    string                       `json:"UnsubscribeURL"`
	MessageAttributes map[string]MessageAttributes `json:"MessageAttributes"`
}

type MessageAttributes struct {
	Type  string `json:"Type"`
	Value string `json:"Value"`
}

type Attributes struct {
	ApproximateFirstReceiveTimestamp string `json:"ApproximateFirstReceiveTimestamp"`
	ApproximateReceiveCount          string `json:"ApproximateReceiveCount"`
	MessageDeduplicationId           string `json:"MessageDeduplicationId"`
	MessageGroupId                   string `json:"MessageGroupId"`
	SenderId                         string `json:"SenderId"`
	SentTimestamp                    string `json:"SentTimestamp"`
	SequenceNumber                   string `json:"SequenceNumber"`
}
