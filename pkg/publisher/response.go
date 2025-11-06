package publisher

type SnsTriggerResponse struct {
	MessageId      string `json:"message_id,omitempty"`
	SequenceNumber string `json:"sequence_number,omitempty"`
}
