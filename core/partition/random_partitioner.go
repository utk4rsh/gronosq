package partition

import (
	"time"
)

type RandomPartitioner struct {
	numOfPartitions int64
}

func NewRandomPartitioner(numOfPartitions int64) *RandomPartitioner {
	return &RandomPartitioner{numOfPartitions: numOfPartitions}
}

func (r *RandomPartitioner) NumOfPartitions() int64 {
	return r.numOfPartitions
}

func (r *RandomPartitioner) GetNumberOfPartitions() int64 {
	return r.numOfPartitions
}

func (r *RandomPartitioner) Partition(entry string) int64 {
	nano := time.Now().UnixNano()
	partition := nano % r.numOfPartitions
	return partition
}
