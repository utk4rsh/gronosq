package partitioner

type Partitioner interface {
	getNumberOfPartitions() int64
	partition(entry string) int64
}
