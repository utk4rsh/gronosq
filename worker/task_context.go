package worker

import (
	"gronosq/core/bucket"
	"gronosq/core/checkpoint"
	"gronosq/core/sink"
	"gronosq/core/store"
)

type TaskContext struct {
	checkPointer   checkpoint.CheckPointer
	schedulerStore store.SchedulerStore
	timeBucket     bucket.TimeBucket
	schedulerSink  sink.SchedulerSink
	batchSize      int64
	interrupt      bool
}

func NewTaskContext(checkPointer checkpoint.CheckPointer, schedulerStore store.SchedulerStore, timeBucket bucket.TimeBucket, schedulerSink sink.SchedulerSink, batchSize int64, interrupt bool) *TaskContext {
	return &TaskContext{checkPointer: checkPointer, schedulerStore: schedulerStore, timeBucket: timeBucket, schedulerSink: schedulerSink, batchSize: batchSize, interrupt: interrupt}
}
