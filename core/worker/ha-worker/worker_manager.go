package ha_worker

import (
	"fmt"
	worker2 "gronosq/core/worker"
	"sync"
	"time"
)

type WorkerManager struct {
	taskDistributor TaskDistributor
	taskFactory     worker2.TaskFactory
	taskContext     *worker2.TaskContext
	managedTasks    []worker2.Task
	mutex           sync.Mutex
}

func NewWorkerManager(taskDistributor TaskDistributor, taskFactory worker2.TaskFactory, taskContext *worker2.TaskContext) *WorkerManager {
	w := &WorkerManager{taskDistributor: taskDistributor, taskFactory: taskFactory, taskContext: taskContext}
	w.taskDistributor.SetRestartAble(w)
	return w
}

func (w *WorkerManager) Start() {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.taskDistributor.Init()
	tasks := w.taskDistributor.GetTasks()
	fmt.Println("Task ids ", tasks)
	if len(tasks) > 0 {
		w.managedTasks = []worker2.Task{}
		for _, task := range tasks {
			partitionNum := task
			task := w.taskFactory.GetTask(*w.taskContext, int64(partitionNum))
			w.managedTasks = append(w.managedTasks, task)
			task.Start()
		}
	}
}

func (w *WorkerManager) Stop() {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	for _, task := range w.managedTasks {
		fmt.Println("Stopping task... for task", task)
		task.Stop()
	}
}

func (w *WorkerManager) Restart() {
	fmt.Println("Stopping worker...")
	w.Stop()
	fmt.Println("Sleeping for 2 sec...")
	time.Sleep(time.Duration(2000) * time.Millisecond)
	w.Start()
	fmt.Println("Starting worker...")
}
