package main

import (
	"fmt"
	"github.com/google/uuid"
	"gronos/client"
	"gronos/config"
	"gronos/core/bucket"
	"gronos/core/entry"
	"gronos/core/partition"
	"gronos/core/rdb"
	redisStore "gronos/redis-store"
	"time"
)

func main() {
	go produce()
	select {} // block forever
}

func produce() {
	configPath := "config.yaml"
	configuration, err := config.NewReader().Read(configPath)
	if err != nil {
		fmt.Println("Could not read configuration file from path : ", configPath)
		panic(err)
	}
	r := rdb.Client{}
	redisClient := r.Get(configuration)
	prefix := configuration.CommonConfig.Prefix
	schedulerStore := redisStore.NewRedisSchedulerStore(prefix, redisClient)
	timeBucket := bucket.NewSecondGroupedTimeBucket(1)
	partitioner := partition.NewRandomPartitioner(1)
	schedulerClient := client.NewSchedulerClient(schedulerStore, timeBucket, partitioner)
	for {
		u, _ := uuid.NewUUID()
		millis := getScheduledTime()
		schedulerEntry := entry.NewDefaultSchedulerEntry(u.String(), getPayload(u, millis))
		add, err := schedulerClient.Add(schedulerEntry, millis)
		if err != nil {
			fmt.Println("Error Adding : ", add, err)
		} else {
			fmt.Println("Added : ", schedulerEntry)
		}
		time.Sleep(time.Duration(60000) * time.Millisecond)
	}
}

func getScheduledTime() int64 {
	future := int64(20 * 1000)
	millis := time.Now().UnixNano()/int64(time.Millisecond) + future
	return millis
}

func getPayload(u uuid.UUID, millis int64) string {
	t := time.Unix(0, millis*int64(time.Millisecond))
	return "Payload for " + u.String() + "scheduled at " + t.String()
}
