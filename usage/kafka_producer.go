package usage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const kafkaBatchTimeout = 10 * time.Millisecond

type messageWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

// KafkaProducer serializes usage records and writes them to Kafka.
type KafkaProducer struct {
	writer messageWriter
}

// NewKafkaProducer constructs a synchronous, best-effort Kafka producer.
func NewKafkaProducer(brokers []string, topic string) (*KafkaProducer, error) {
	if len(brokers) == 0 {
		return nil, errors.New("usage: at least one Kafka broker is required")
	}
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return nil, errors.New("usage: Kafka topic cannot be blank")
	}

	cleaned := make([]string, 0, len(brokers))
	for _, broker := range brokers {
		broker = strings.TrimSpace(broker)
		if broker == "" {
			return nil, errors.New("usage: Kafka broker cannot be blank")
		}
		cleaned = append(cleaned, broker)
	}

	return &KafkaProducer{writer: &kafka.Writer{
		Addr:                   kafka.TCP(cleaned...),
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		MaxAttempts:            3,
		BatchTimeout:           kafkaBatchTimeout,
		WriteTimeout:           flushTimeout,
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: false,
	}}, nil
}

func (p *KafkaProducer) Write(ctx context.Context, records []Record) error {
	if len(records) == 0 {
		return nil
	}

	messages := make([]kafka.Message, 0, len(records))
	for _, record := range records {
		value, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("marshal usage record: %w", err)
		}
		messages = append(messages, kafka.Message{
			Key:   []byte(record.ClientID),
			Value: value,
		})
	}
	return p.writer.WriteMessages(ctx, messages...)
}

func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}
