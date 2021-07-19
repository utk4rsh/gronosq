package worker

import (
	"fmt"
	"strconv"
	"time"
)

type GTask struct {
	taskContext  TaskContext
	partitionNum int64
	QuitChan     chan bool
}

func NewGTask(taskContext TaskContext, partitionNum int64) *GTask {
	return &GTask{taskContext: taskContext, partitionNum: partitionNum, QuitChan: make(chan bool)}
}

func (w *GTask) Start() {
	go func() {
		for {
			select {
			case <-w.QuitChan:
				fmt.Println("Stopping GTask for partition", w.partitionNum)
				return
			default:
				fmt.Println("Starting GTask for partition", w.partitionNum)
				w.doWork()
			}
		}
	}()
}

func (w *GTask) Stop() {
	go func() {
		w.QuitChan <- true
	}()
}

func (w *GTask) doWork() {
	fmt.Println("Starting work..")
	partitionNum := w.partitionNum
	batchSize := w.taskContext.batchSize
	nextIntervalForProcess := w.calculateNextIntervalForProcess(partitionNum)
	//fmt.Println("Current epoch ", w.currentTimeInMillis(), ", Next run at ", nextIntervalForProcess)
	for nextIntervalForProcess <= w.currentTimeInMillis() {
		schedulerEntries := w.taskContext.schedulerStore.GetNextN(nextIntervalForProcess, partitionNum, batchSize)
		for len(schedulerEntries) != 0 {
			w.taskContext.schedulerSink.GiveExpiredListForProcessing(schedulerEntries)
			_, _ = w.taskContext.schedulerStore.RemoveBulk(schedulerEntries, nextIntervalForProcess, partitionNum)
			schedulerEntries = w.taskContext.schedulerStore.GetNextN(nextIntervalForProcess, partitionNum, batchSize)
		}
		w.taskContext.checkPointer.Set(strconv.FormatInt(nextIntervalForProcess, 10), partitionNum)
		nextIntervalForProcess = w.taskContext.timeBucket.Next(nextIntervalForProcess)
	}
	sleepTime := nextIntervalForProcess - w.currentTimeInMillis()
	//fmt.Println("Current epoch ", w.currentTimeInMillis(), ", Next run at ", nextIntervalForProcess, "sleeping for ", sleepTime)
	time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	fmt.Println("Completed work...")
}

func (w *GTask) currentTimeInMillis() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
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

func (w *GTask) ShutDown() {
	w.Stop()
}
