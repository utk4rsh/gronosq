package entry

type SchedulerEntry interface {
	Key() string
	Payload() string
}
