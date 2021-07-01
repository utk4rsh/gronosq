package sink

import (
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"gronos/core/entry"
)

type SchedulerSink interface {
	GiveExpiredForProcessing(schedulerEntry entry.SchedulerEntry) chan kafka.Event
	GiveExpiredListForProcessing(schedulerEntries []entry.SchedulerEntry) []chan kafka.Event
}
