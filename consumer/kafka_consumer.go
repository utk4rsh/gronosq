package consumer

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"os"
	"os/signal"
	"syscall"
)

type KafkaConsumer struct {
	consumer *kafka.Consumer
}

func NewKafkaConsumer(consumer *kafka.Consumer) *KafkaConsumer {
	return &KafkaConsumer{consumer: consumer}
}

func (k *KafkaConsumer) Subscribe(topics []string) {
	fmt.Println("Subscribing ...", topics)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	err := k.consumer.SubscribeTopics(topics, nil)
	if err != nil {
		fmt.Println("Panic during subscribe", topics)
		panic(err)
	}
	fmt.Println("Subscribed", topics)
	run := true
	for run == true {
		select {
		case sig := <-sigChan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := k.consumer.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("%% Message on %s: %s, %s\n", e.TopicPartition, string(e.Key), string(e.Value))
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}
	fmt.Printf("Closing consumer\n")
}
