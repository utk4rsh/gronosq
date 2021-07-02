package ha_worker

type TaskDistributor interface {
	Init()
	GetTasks() []string
}
