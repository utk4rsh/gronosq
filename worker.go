package main

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"gronosq/config"
	"gronosq/core/bucket"
	"gronosq/core/checkpoint"
	"gronosq/core/rdb"
	"gronosq/core/sink/kafka-sink"
	"gronosq/core/store/redis-store"
	worker2 "gronosq/core/worker"
	"gronosq/core/worker/ha-worker"
	"gronosq/core/zk"
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
	schedulerStore := redis_store.NewRedisSchedulerStore(prefix, redisClient)
	timeBucket := bucket.NewSecondGroupedTimeBucket(1)
	kafkaMessage := kafka_sink.NewSimpleKafkaMessage()
	topic := configuration.KafkaConfig.Topic
	zkPrefix := configuration.ZooKeeperConfig.ZkPath
	schedulerSink := kafka_sink.NewKafkaSchedulerSink(producer, topic, kafkaMessage)
	batchSize := configuration.CommonConfig.BatchSize
	taskContext := worker2.NewTaskContext(checkPointer, schedulerStore, timeBucket, schedulerSink, int64(batchSize), false)
	zkDiscovery := ha_worker.NewZKDiscovery(zkClient)
	name, _ := os.Hostname()
	taskDistributor := ha_worker.NewZkTaskDistributor(zkPrefix, 2, name+"-11", zkDiscovery)
	workerManager := ha_worker.NewWorkerManager(taskDistributor, worker2.TaskFactory{}, taskContext)
	workerManager.Start()
}

func main() {
	startTask()
	select {} // block forever
}
