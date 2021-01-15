package kafka_sink

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"gronos/core/entry"
)

type KafkaSchedulerSink struct {
	producer kafka.Producer
	topic    string
}

func (k *KafkaSchedulerSink) GiveExpiredForProcessing(schedulerEntry entry.SchedulerEntry) {
	_ = k.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &k.topic, Partition: kafka.PartitionAny},
		Key:            []byte(schedulerEntry.Key()),
		Value:          []byte(schedulerEntry.Payload()),
	}, nil)
}

func (k *KafkaSchedulerSink) GiveExpiredListForProcessing(schedulerEntries []entry.SchedulerEntry) {
	for _, schedulerEntry := range schedulerEntries {
		_ = k.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &k.topic, Partition: kafka.PartitionAny},
			Key:            []byte(schedulerEntry.Key()),
			Value:          []byte(schedulerEntry.Payload()),
		}, nil)
	}
}

func (k *KafkaSchedulerSink) delivery() {
	go func() {
		for e := range k.producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()
}
