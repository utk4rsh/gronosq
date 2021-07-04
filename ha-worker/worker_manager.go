package ha_worker

import (
	"gronos/worker"
)

type WorkerManager struct {
	taskDistributor TaskDistributor
	taskFactory     worker.TaskFactory
	taskContext     *worker.TaskContext
	managedTasks    []worker.Task
	quit            chan bool
}

func NewWorkerManager(taskDistributor TaskDistributor, taskFactory worker.TaskFactory, taskContext *worker.TaskContext) *WorkerManager {
	return &WorkerManager{taskDistributor: taskDistributor, taskFactory: taskFactory, taskContext: taskContext}
}

func (w *WorkerManager) Start() {
	go func() {
		select {
		case <-w.quit:
			return
		default:
			w.taskDistributor.Init()
			tasks := w.taskDistributor.GetTasks()
			if len(tasks) > 0 {
				for _, task := range tasks {
					partitionNum := task
					task := w.taskFactory.GetTask(*w.taskContext, int64(partitionNum))
					w.managedTasks = append(w.managedTasks, task)
					task.Start()
				}
			}
		}
	}()
}

func (w *WorkerManager) stop() {
	w.quit <- true
	for _, task := range w.managedTasks {
		task.Stop()
	}
	w.quit <- false
}
