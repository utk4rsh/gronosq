package sink

import "gronos/core/entry"

type SchedulerSink interface {
	giveExpiredForProcessing(schedulerEntry entry.SchedulerEntry)
	giveExpiredListForProcessing(schedulerEntries []entry.SchedulerEntry)
}
