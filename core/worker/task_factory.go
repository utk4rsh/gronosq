package worker

type TaskFactory struct {
}

func (t *TaskFactory) GetTask(ctx TaskContext, partitionNum int64) Task {
	return NewGTask(ctx, partitionNum)
}
