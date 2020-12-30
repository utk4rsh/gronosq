package entry

type DefaultSchedulerEntry struct {
	key     string
	payload string
}

func NewDefaultSchedulerEntry(key string, payload string) *DefaultSchedulerEntry {
	return &DefaultSchedulerEntry{key: key, payload: payload}
}

func (s *DefaultSchedulerEntry) Key() string {
	return s.key
}

func (s *DefaultSchedulerEntry) Payload() string {
	return s.payload
}
