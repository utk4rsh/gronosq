package consumer

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"gronosq/config"
)

func main() {
	go consume()
	select {} // block forever
}

func consume() {
	configPath := "config.yaml"
	configuration, err := config.NewReader().Read(configPath)
	if err != nil {
		fmt.Println("Could not read configuration file from path : ", configPath)
		panic(err)
	}
	consumer, _ := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  configuration.KafkaConfig.Brokers,
		"group.id":           configuration.KafkaConfig.Group,
		"session.timeout.ms": 6000,
		"auto.offset.reset":  "earliest"})
	kafkaConsumer := NewKafkaConsumer(consumer)
	topics := []string{configuration.KafkaConfig.Topic}
	kafkaConsumer.Subscribe(topics)
}
