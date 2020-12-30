package partitioner

import (
	"time"
)

type RandomPartitioner struct {
	numOfPartitions int64
}

func (randomPartitioner RandomPartitioner) NumOfPartitions() int64 {
	return randomPartitioner.numOfPartitions
}

func (randomPartitioner *RandomPartitioner) getNumberOfPartitions() int64 {
	return randomPartitioner.numOfPartitions
}

func (randomPartitioner *RandomPartitioner) partition(entry string) int64 {
	nano := time.Now().Unix()
	partition := nano % randomPartitioner.numOfPartitions
	return partition
}
