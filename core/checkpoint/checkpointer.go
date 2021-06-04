package checkpoint

type CheckPointer interface {
	Peek(partitionNum int64) string
	Set(value string, partitionNum int64)
}
