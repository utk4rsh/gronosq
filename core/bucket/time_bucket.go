package bucket

type TimeBucket interface {
	toBucket(epochTimestamp int64) int64
	next(epochTimestamp int64) int64
	previous(epochTimestamp int64) int64
}
