package kafka_sink

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"gronosq/core/entry"
)

type KafkaSchedulerSink struct {
	producer     *kafka.Producer
	topic        string
	kafkaMessage KafkaMessage
}

func NewKafkaSchedulerSink(producer *kafka.Producer, topic string, kafkaMessage KafkaMessage) *KafkaSchedulerSink {
	return &KafkaSchedulerSink{producer: producer, topic: topic, kafkaMessage: kafkaMessage}
}

func (k *KafkaSchedulerSink) GiveExpiredForProcessing(schedulerEntry entry.SchedulerEntry) chan kafka.Event {
	message := k.kafkaMessage.GetKeyedMessage(k.topic, schedulerEntry)
	channel := make(chan kafka.Event)
	k.producer.ProduceChannel() <- &message
	k.delivery(channel)
	return channel
}

func (k *KafkaSchedulerSink) logDelivery(m *kafka.Message) {
	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}
}

func (k *KafkaSchedulerSink) GiveExpiredListForProcessing(schedulerEntries []entry.SchedulerEntry) []chan kafka.Event {
	channels := make([]chan kafka.Event, 10, 100)
	for idx, schedulerEntry := range schedulerEntries {
		channels[idx] = make(chan kafka.Event)
		message := k.kafkaMessage.GetKeyedMessage(k.topic, schedulerEntry)
		k.producer.ProduceChannel() <- &message
		k.delivery(channels[idx])
	}
	return channels
}

func (k *KafkaSchedulerSink) delivery(doneChan chan kafka.Event) {
	go func() {
		defer close(doneChan)
		for e := range k.producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
				return
			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

}
