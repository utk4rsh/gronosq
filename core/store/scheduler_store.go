package store

import (
	"gronos/core/entry"
)

type SchedulerStore interface {
	add(entry entry.SchedulerEntry, time uint64, partitionNum int64)
	update(entry entry.SchedulerEntry, oldTime uint64, newTime uint64, partitionNum int64) uint64
	remove(value string, time uint64, partitionNum int64) int64
	get(time uint64, partitionNum int64) []entry.SchedulerEntry
	getNextN(time uint64, partitionNum int64, n int64) []entry.SchedulerEntry
	removeBulk(time uint64, partitionNum int64, values []string)
}
