package ha_worker

type TaskDistributor interface {
	Init()
	GetTasks() []int
	SetRestartAble(manager *WorkerManager)
}
