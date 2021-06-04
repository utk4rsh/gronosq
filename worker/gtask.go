package worker

import (
	"gronos/core/bucket"
	"gronos/core/checkpoint"
	"gronos/core/sink"
	"gronos/core/store"
	"strconv"
	"time"
)

type GTask struct {
	checkPointer   checkpoint.CheckPointer
	schedulerStore store.SchedulerStore
	timeBucket     bucket.TimeBucket
	schedulerSink  sink.SchedulerSink
	batchSize      int64
	partitionNum   int64
	interrupt      bool
}

func (w *GTask) Start() {
	go w.worker()
}

func (w *GTask) worker() {
	for !w.isInterrupted() {
		currentTimeInMillis := w.currentTimeInMillis()
		partitionNum := w.partitionNum
		nextIntervalForProcess := w.calculateNextIntervalForProcess(partitionNum)
		for !w.isInterrupted() && nextIntervalForProcess <= currentTimeInMillis {
			schedulerEntries := w.schedulerStore.GetNextN(nextIntervalForProcess, partitionNum, w.batchSize)
			for !w.isInterrupted() && len(schedulerEntries) != 0 {
				w.schedulerSink.GiveExpiredListForProcessing(schedulerEntries)
				_, _ = w.schedulerStore.RemoveBulk(schedulerEntries, nextIntervalForProcess, partitionNum)
				schedulerEntries = w.schedulerStore.GetNextN(nextIntervalForProcess, partitionNum, w.batchSize)
			}
			w.checkPointer.Set(strconv.FormatInt(nextIntervalForProcess, 10), partitionNum)
			nextIntervalForProcess = w.timeBucket.Next(nextIntervalForProcess)
			currentTimeInMillis = w.currentTimeInMillis()
		}
		sleepTime := nextIntervalForProcess - currentTimeInMillis
		time.Sleep(time.Duration(sleepTime))
	}
}

func (w *GTask) currentTimeInMillis() int64 {
	nano := time.Now().Unix()
	return nano
}

func (w *GTask) isInterrupted() bool {
	return w.interrupt
}

func (w *GTask) interruptWorker() {
	w.interrupt = true
}

func (w *GTask) calculateNextIntervalForProcess(partitionNum int64) int64 {
	peek := w.checkPointer.Peek(partitionNum)
	i, _ := strconv.ParseInt(peek, 10, 64)
	return w.timeBucket.ToBucket(i)
}

func (w *GTask) Stop() {
	w.interruptWorker()
}

func (w *GTask) ShutDown() {
	w.interruptWorker()
}
