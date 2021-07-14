package ha_worker

type Restartable interface {
	Stop()
	Start()
	Restart()
}
