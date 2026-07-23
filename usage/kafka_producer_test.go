package usage

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

type fakeMessageWriter struct {
	messages []kafka.Message
	closed   bool
}

func (w *fakeMessageWriter) WriteMessages(_ context.Context, messages ...kafka.Message) error {
	w.messages = append(w.messages, messages...)
	return nil
}

func (w *fakeMessageWriter) Close() error {
	w.closed = true
	return nil
}

func TestKafkaProducerSerializesRecords(t *testing.T) {
	writer := &fakeMessageWriter{}
	producer := &KafkaProducer{writer: writer}
	record := Record{
		ID:           "5f0e8c2a-9b3d-4c71-a6e4-1d2f3a4b5c6d",
		ClientID:     "instance:019f45e26c307c86bd45ab350bb52ca8",
		Service:      ServiceLeafage,
		ResourceType: ResourceTypeRead,
		Usage:        420,
		Timestamp:    1783568373000,
	}

	require.NoError(t, producer.Write(context.Background(), []Record{record}))
	require.Len(t, writer.messages, 1)
	require.Equal(t, record.ClientID, string(writer.messages[0].Key))
	require.JSONEq(t, `{
		"id":"5f0e8c2a-9b3d-4c71-a6e4-1d2f3a4b5c6d",
		"client_id":"instance:019f45e26c307c86bd45ab350bb52ca8",
		"service":"leafage",
		"resource_type":"read",
		"usage":420,
		"timestamp":1783568373000
	}`, string(writer.messages[0].Value))
}

func TestKafkaProducerConfiguration(t *testing.T) {
	producer, err := NewKafkaProducer(
		[]string{" kafka-1:9092 ", "kafka-2:9092"},
		" custom-usage-topic ",
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = producer.Close() })

	writer, ok := producer.writer.(*kafka.Writer)
	require.True(t, ok)
	require.Equal(t, "custom-usage-topic", writer.Topic)
	require.Equal(t, kafka.RequireAll, writer.RequiredAcks)
	require.False(t, writer.Async)
	require.False(t, writer.AllowAutoTopicCreation)
	require.Equal(t, 3, writer.MaxAttempts)
	_, ok = writer.Balancer.(*kafka.Hash)
	require.True(t, ok)
}

func TestNewKafkaProducerRejectsInvalidBrokers(t *testing.T) {
	_, err := NewKafkaProducer(nil, "topic")
	require.Error(t, err)
	_, err = NewKafkaProducer([]string{" "}, "topic")
	require.Error(t, err)
	_, err = NewKafkaProducer([]string{"kafka:9092"}, " ")
	require.Error(t, err)
}
