package worker

import (
	"fmt"
	"strconv"
	"time"
)

type GTask struct {
	taskContext  TaskContext
	partitionNum int64
}

func NewGTask(taskContext TaskContext, partitionNum int64) *GTask {
	return &GTask{taskContext: taskContext, partitionNum: partitionNum}
}

func (w *GTask) Start() {
	go w.worker()
}

func (w *GTask) worker() {
	for !w.isInterrupted() {
		partitionNum := w.partitionNum
		batchSize := w.taskContext.batchSize
		nextIntervalForProcess := w.calculateNextIntervalForProcess(partitionNum)
		for !w.isInterrupted() && nextIntervalForProcess <= w.currentTimeInMillis() {
			schedulerEntries := w.taskContext.schedulerStore.GetNextN(nextIntervalForProcess, partitionNum, batchSize)
			fmt.Println("schedulerEntries", schedulerEntries)
			for !w.isInterrupted() && len(schedulerEntries) != 0 {
				w.taskContext.schedulerSink.GiveExpiredListForProcessing(schedulerEntries)
				_, _ = w.taskContext.schedulerStore.RemoveBulk(schedulerEntries, nextIntervalForProcess, partitionNum)
				schedulerEntries = w.taskContext.schedulerStore.GetNextN(nextIntervalForProcess, partitionNum, batchSize)
			}
			w.taskContext.checkPointer.Set(strconv.FormatInt(nextIntervalForProcess, 10), partitionNum)
			nextIntervalForProcess = w.taskContext.timeBucket.Next(nextIntervalForProcess)
		}
		sleepTime := nextIntervalForProcess - w.currentTimeInMillis()
		fmt.Println("sleepTime", sleepTime)
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}
}

func (w *GTask) currentTimeInMillis() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
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
