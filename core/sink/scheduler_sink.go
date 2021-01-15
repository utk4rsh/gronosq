package sink

import "gronos/core/entry"

type SchedulerSink interface {
	GiveExpiredForProcessing(schedulerEntry entry.SchedulerEntry)
	GiveExpiredListForProcessing(schedulerEntries []entry.SchedulerEntry)
}
