package kafka_sink

import (
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"gronos/core/entry"
)

type KafkaMessage interface {
	GetKeyedMessage(topic string, schedulerEntry entry.SchedulerEntry) kafka.Message
}
