package worker

import (
	"fmt"
	"strconv"
	"time"
)

type GTask struct {
	taskContext TaskContext
}

func NewGTask(taskContext TaskContext) *GTask {
	return &GTask{taskContext: taskContext}
}

func (w *GTask) Start() {
	go w.worker()
}

func (w *GTask) worker() {
	for !w.isInterrupted() {
		currentTimeInMillis := w.currentTimeInMillis()
		partitionNum := w.taskContext.partitionNum
		batchSize := w.taskContext.batchSize
		nextIntervalForProcess := w.calculateNextIntervalForProcess(partitionNum)
		for !w.isInterrupted() && nextIntervalForProcess <= currentTimeInMillis {
			schedulerEntries := w.taskContext.schedulerStore.GetNextN(nextIntervalForProcess, partitionNum, batchSize)
			for !w.isInterrupted() && len(schedulerEntries) != 0 {
				w.taskContext.schedulerSink.GiveExpiredListForProcessing(schedulerEntries)
				_, _ = w.taskContext.schedulerStore.RemoveBulk(schedulerEntries, nextIntervalForProcess, partitionNum)
				schedulerEntries = w.taskContext.schedulerStore.GetNextN(nextIntervalForProcess, partitionNum, batchSize)
				fmt.Println(schedulerEntries)
			}
			w.taskContext.checkPointer.Set(strconv.FormatInt(nextIntervalForProcess, 10), partitionNum)
			nextIntervalForProcess = w.taskContext.timeBucket.Next(nextIntervalForProcess)
			currentTimeInMillis = w.currentTimeInMillis()
		}
		sleepTime := nextIntervalForProcess - currentTimeInMillis
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}
}

func (w *GTask) currentTimeInMillis() int64 {
	nano := time.Now().Unix()
	return nano
}

func (w *GTask) isInterrupted() bool {
	return w.taskContext.interrupt
}

func (w *GTask) interruptWorker() {
	w.taskContext.interrupt = true
}

func (w *GTask) calculateNextIntervalForProcess(partitionNum int64) int64 {
	peek := w.taskContext.checkPointer.Peek(partitionNum)
	if len(peek) != 0 {
		i, _ := strconv.ParseInt(peek, 10, 64)
		return w.taskContext.timeBucket.ToBucket(i)
	} else {
		currentTime := w.currentTimeInMillis() - 1000*10
		w.taskContext.checkPointer.Set(strconv.FormatInt(currentTime, 10), partitionNum)
		return currentTime
	}
}

func (w *GTask) Stop() {
	w.interruptWorker()
}

func (w *GTask) ShutDown() {
	w.interruptWorker()
}
