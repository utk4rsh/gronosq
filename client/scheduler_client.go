package client

import (
	"gronos/core/bucket"
	"gronos/core/entry"
	"gronos/core/partitioner"
	"gronos/core/store"
)

const DatastoreNoOperation = 0

type SchedulerClient struct {
	store       store.SchedulerStore
	timeBucket  bucket.TimeBucket
	partitioner partitioner.Partitioner
}

func NewSchedulerClient(store store.SchedulerStore, timeBucket bucket.TimeBucket, partitioner partitioner.Partitioner) *SchedulerClient {
	return &SchedulerClient{store: store, timeBucket: timeBucket, partitioner: partitioner}
}

func (s *SchedulerClient) Remove(entry entry.SchedulerEntry, time int64) (bool, error) {
	key := entry.Key()
	partitionNumber := s.partitioner.Partition(key)
	storeResult, err := s.store.Remove(key, s.timeBucket.ToBucket(time), partitionNumber)
	if err != nil {
		return false, err
	} else {
		if storeResult != DatastoreNoOperation {
			return true, nil
		} else {
			return false, nil
		}
	}
}

func (s *SchedulerClient) Add(entry entry.SchedulerEntry, time int64) {
	key := entry.Key()
	partitionNumber := s.partitioner.Partition(key)
	s.store.Add(entry, s.timeBucket.ToBucket(time), partitionNumber)
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
