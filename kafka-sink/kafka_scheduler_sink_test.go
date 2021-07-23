package kafka_sink

import (
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"gronosq/core/entry"
	"strconv"
	"testing"
)

func TestKafkaSchedulerSink_GiveExpiredForProcessing(t *testing.T) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}
	topic := "quickstart-events"
	type fields struct {
		producer     *kafka.Producer
		topic        string
		kafkaMessage KafkaMessage
	}
	schedulerEntry := entry.NewDefaultSchedulerEntry("key", "payload")
	kafkaMessage := NewSimpleKafkaMessage()
	f := fields{producer: producer, topic: topic, kafkaMessage: kafkaMessage}
	type args struct {
		schedulerEntry entry.SchedulerEntry
	}
	a := args{schedulerEntry: schedulerEntry}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"GiveExpiredForProcessing", f, a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &KafkaSchedulerSink{
				producer:     tt.fields.producer,
				topic:        tt.fields.topic,
				kafkaMessage: tt.fields.kafkaMessage,
			}
			doneChan := k.GiveExpiredForProcessing(tt.args.schedulerEntry)
			_ = <-doneChan
		})
	}
	producer.Close()
}

func TestKafkaSchedulerSink_GiveExpiredListForProcessing(t *testing.T) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}
	topic := "quickstart-events"
	type fields struct {
		producer     *kafka.Producer
		topic        string
		kafkaMessage KafkaMessage
	}
	schedulerEntries := make([]entry.SchedulerEntry, 10)
	for i := 0; i < 10; i++ {
		schedulerEntries[i] = entry.NewDefaultSchedulerEntry("key_"+strconv.FormatInt(int64(i), 10), "payload"+strconv.FormatInt(int64(i), 10))
	}
	kafkaMessage := NewSimpleKafkaMessage()
	f := fields{producer: producer, topic: topic, kafkaMessage: kafkaMessage}
	type args struct {
		schedulerEntries []entry.SchedulerEntry
	}
	a := args{schedulerEntries: schedulerEntries}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"GiveExpiredListForProcessing", f, a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &KafkaSchedulerSink{
				producer:     tt.fields.producer,
				topic:        tt.fields.topic,
				kafkaMessage: tt.fields.kafkaMessage,
			}
			doneChannels := k.GiveExpiredListForProcessing(tt.args.schedulerEntries)
			for _, channel := range doneChannels {
				_ = <-channel
			}
		})
	}
	producer.Close()
}
