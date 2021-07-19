package worker

type Task interface {
	Stop()
	ShutDown()
	Start()
}
