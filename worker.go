package main

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"gronos/config"
	"gronos/core/bucket"
	"gronos/core/checkpoint"
	"gronos/core/rdb"
	"gronos/core/zk"
	haworker "gronos/ha-worker"
	kafkaSink "gronos/kafka-sink"
	redisStore "gronos/redis-store"
	"gronos/worker"
	"os"
)

func startTask() {
	configPath := "config.yaml"
	configuration, err := config.NewReader().Read(configPath)
	if err != nil {
		fmt.Println("Could not read configuration file from path : ", configPath)
		panic(err)
	}
	r := rdb.Client{}
	redisClient := r.Get(configuration)
	zkClient := zk.Client{}
	producer, _ := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	prefix := configuration.CommonConfig.Prefix
	checkPointer := checkpoint.NewRedisCheckPointer(prefix, redisClient)
	schedulerStore := redisStore.NewRedisSchedulerStore(prefix, redisClient)
	timeBucket := bucket.NewSecondGroupedTimeBucket(1)
	kafkaMessage := kafkaSink.NewSimpleKafkaMessage()
	topic := configuration.KafkaConfig.Topic
	zkPrefix := configuration.ZooKeeperConfig.ZkPath
	schedulerSink := kafkaSink.NewKafkaSchedulerSink(producer, topic, kafkaMessage)
	batchSize := configuration.CommonConfig.BatchSize
	taskContext := worker.NewTaskContext(checkPointer, schedulerStore, timeBucket, schedulerSink, int64(batchSize), false)
	zkDiscovery := haworker.NewZKDiscovery(zkClient)
	name, _ := os.Hostname()
	taskDistributor := haworker.NewZkTaskDistributor(zkPrefix, 2, name, zkDiscovery)
	workerManager := haworker.NewWorkerManager(taskDistributor, worker.TaskFactory{}, taskContext)
	workerManager.Start()
}

func main() {
	go startTask()
	select {} // block forever
}
