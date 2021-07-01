package partition

type Partitioner interface {
	GetNumberOfPartitions() int64
	Partition(key string) int64
}
