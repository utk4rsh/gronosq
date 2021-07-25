package pb

import (
	"context"
	"gronosq/config"
	"gronosq/core/bucket"
	"gronosq/core/entry"
	"gronosq/core/partition"
	"gronosq/core/rdb"
	"gronosq/core/scheduler"
	redisStore "gronosq/core/store/redis-store"
	"strconv"
)

type SchedulerServerInstance struct {
	scheduler *scheduler.Scheduler
}

func NewSchedulerServerInstance(configuration *config.Configuration) *SchedulerServerInstance {
	r := rdb.Client{}
	redisClient := r.Get(configuration)
	schedulerStore := redisStore.NewRedisSchedulerStore(configuration.CommonConfig.Prefix, redisClient)
	timeBucket := bucket.NewSecondGroupedTimeBucket(configuration.SecondsForABucket)
	partitioner := partition.NewRandomPartitioner(configuration.Partitions)
	return &SchedulerServerInstance{scheduler: scheduler.NewScheduler(schedulerStore, timeBucket, partitioner)}
}

func (s *SchedulerServerInstance) Add(ctx context.Context, request *SchedulerEntryRequest) (*SchedulerResponse, error) {
	result, err := s.scheduler.Add(entry.NewDefaultSchedulerEntry(request.GetKey(), request.GetPayload()), request.GetScheduledTimeEpoch())
	if err != nil {
		panic(err)
	}
	return &SchedulerResponse{result}, nil
}

func (s *SchedulerServerInstance) Remove(ctx context.Context, request *SchedulerEntryRequest) (*SchedulerResponse, error) {
	result, err := s.scheduler.Remove(entry.NewDefaultSchedulerEntry(request.GetKey(), request.GetPayload()), request.GetScheduledTimeEpoch())
	if err != nil {
		panic(err)
	}
	return &SchedulerResponse{strconv.FormatBool(result)}, nil
}
