package main

import (
	"fmt"
	"github.com/google/uuid"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"gronos/client"
	"gronos/core/bucket"
	"gronos/core/checkpoint"
	"gronos/core/entry"
	"gronos/core/partition"
	"gronos/core/redis"
	kafkaSink "gronos/kafka-sink"
	redisStore "gronos/redis-store"
	"gronos/worker"
	"time"
)

func startTask() {
	redisClient := redis.RedisClient{}
	producer, _ := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	prefix := "pr_"
	checkPointer := checkpoint.NewRedisCheckPointer(prefix, redisClient)
	schedulerStore := redisStore.NewRedisSchedulerStore(prefix, redisClient)
	timeBucket := bucket.NewSecondGroupedTimeBucket(20)
	kafkaMessage := kafkaSink.NewSimpleKafkaMessage()
	topic := "topic"
	schedulerSink := kafkaSink.NewKafkaSchedulerSink(producer, topic, kafkaMessage)
	batchSize := 100
	partitionNum := 1
	taskContext := worker.NewTaskContext(checkPointer, schedulerStore, timeBucket, schedulerSink, int64(batchSize), int64(partitionNum), false)
	task := worker.NewGTask(*taskContext)
	task.Start()
}

func main() {
	go startTask()
	clientInsert()
	select {} // block forever
}

func clientInsert() {
	redisClient := redis.RedisClient{}
	prefix := "pr_"
	schedulerStore := redisStore.NewRedisSchedulerStore(prefix, redisClient)
	timeBucket := bucket.NewSecondGroupedTimeBucket(20)
	partitioner := partition.NewRandomPartitioner(1)
	schedulerClient := client.NewSchedulerClient(schedulerStore, timeBucket, partitioner)
	for {
		u, _ := uuid.NewUUID()
		schedulerEntry := entry.NewDefaultSchedulerEntry(u.String(), u.String())
		nano := time.Now().Unix() + 2*1000
		fmt.Println(nano)
		add, err := schedulerClient.Add(schedulerEntry, nano)
		if err != nil {
			fmt.Println("Error Adding : ", add, err)
		} else {
			fmt.Println("Added : ", u.String())
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)
	}
}
