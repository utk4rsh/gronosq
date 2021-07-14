package ha_worker

import (
	"fmt"
	"gronos/worker"
	"sync"
	"time"
)

type WorkerManager struct {
	taskDistributor TaskDistributor
	taskFactory     worker.TaskFactory
	taskContext     *worker.TaskContext
	managedTasks    []worker.Task
	mutex           sync.Mutex
}

func NewWorkerManager(taskDistributor TaskDistributor, taskFactory worker.TaskFactory, taskContext *worker.TaskContext) *WorkerManager {
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
		w.managedTasks = []worker.Task{}
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
