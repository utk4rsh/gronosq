package checkpoint

type CheckPointer interface {
	peek(partitionNum int64) string
	set(value string, partitionNum int64)
}
