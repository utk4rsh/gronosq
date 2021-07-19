package store

import (
	"gronosq/core/entry"
)

type SchedulerStore interface {
	Add(entry entry.SchedulerEntry, time int64, partitionNum int64) (string, error)
	Update(entry entry.SchedulerEntry, oldTime int64, newTime int64, partitionNum int64) (bool, error)
	Remove(schedulerEntry entry.SchedulerEntry, time int64, partitionNum int64) (bool, error)
	Get(time int64, partitionNum int64) []entry.SchedulerEntry
	GetNextN(time int64, partitionNum int64, n int64) []entry.SchedulerEntry
	RemoveBulk(schedulerEntries []entry.SchedulerEntry, time int64, partitionNum int64) (bool, error)
}
