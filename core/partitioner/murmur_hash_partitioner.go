package partitioner

import (
	"hash/fnv"
	"math"
)

type MurMurHashPartitioner struct {
	numOfPartitions int64
}

func (m *MurMurHashPartitioner) NumOfPartitions() int64 {
	return m.numOfPartitions
}

func (m *MurMurHashPartitioner) GetNumberOfPartitions() int64 {
	return m.numOfPartitions
}

func (m *MurMurHashPartitioner) Partition(entry string) int64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(entry))
	hashCode := float64(h.Sum32())
	partition := int64(math.Abs(hashCode)) % m.numOfPartitions
	return partition
}
