package client

import (
	"gronosq/core/bucket"
	"gronosq/core/entry"
	"gronosq/core/partition"
	"gronosq/core/store"
)

const DatastoreNoOperation = 0

type SchedulerClient struct {
	store       store.SchedulerStore
	timeBucket  bucket.TimeBucket
	partitioner partition.Partitioner
}

func NewSchedulerClient(store store.SchedulerStore, timeBucket bucket.TimeBucket, partitioner partition.Partitioner) *SchedulerClient {
	return &SchedulerClient{store: store, timeBucket: timeBucket, partitioner: partitioner}
}

func (s *SchedulerClient) Remove(entry entry.SchedulerEntry, time int64) (bool, error) {
	key := entry.Key()
	partitionNumber := s.partitioner.Partition(key)
	storeResult, err := s.store.Remove(entry, s.timeBucket.ToBucket(time), partitionNumber)
	return storeResult, err
}

func (s *SchedulerClient) Add(entry entry.SchedulerEntry, time int64) (string, error) {
	key := entry.Key()
	partitionNumber := s.partitioner.Partition(key)
	storeResult, err := s.store.Add(entry, s.timeBucket.ToBucket(time), partitionNumber)
	return storeResult, err
}

func (s *SchedulerClient) Update(entry entry.SchedulerEntry, oldTime int64, newTime int64) (bool, error) {
	key := entry.Key()
	partitionNumber := s.partitioner.Partition(key)
	storeResult, err := s.store.Update(entry, s.timeBucket.ToBucket(oldTime), s.timeBucket.ToBucket(newTime), partitionNumber)
	if err != nil {
		return false, err
	} else {
		return storeResult, nil
	}
}
