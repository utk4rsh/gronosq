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

func (randomPartitioner RandomPartitioner) NumOfPartitions() int64 {
	return randomPartitioner.numOfPartitions
}

func (randomPartitioner *RandomPartitioner) GetNumberOfPartitions() int64 {
	return randomPartitioner.numOfPartitions
}

func (randomPartitioner *RandomPartitioner) Partition(entry string) int64 {
	nano := time.Now().Unix()
	partition := nano % randomPartitioner.numOfPartitions
	return partition
}
