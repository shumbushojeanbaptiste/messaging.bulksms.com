package queue

import (
	"time"

	"github.com/segmentio/kafka-go"
)

func NewWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		BatchTimeout: 250 * time.Millisecond,
		MaxAttempts:  3,
	}
}

func NewReader(
	brokers []string,
	topic string,
	groupID string,
) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10 * 1024 * 1024,
		MaxWait:        500 * time.Millisecond,
		CommitInterval: 0,
	})
}
