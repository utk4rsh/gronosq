package kafka_sink

import (
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"gronosq/core/entry"
)

type SimpleKafkaMessage struct {
}

func NewSimpleKafkaMessage() *SimpleKafkaMessage {
	return &SimpleKafkaMessage{}
}

func (s *SimpleKafkaMessage) GetKeyedMessage(topic string, schedulerEntry entry.SchedulerEntry) kafka.Message {
	return kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(schedulerEntry.Key()),
		Value:          []byte(schedulerEntry.Payload())}
}
