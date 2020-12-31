package bucket

type TimeBucket interface {
	ToBucket(epochTimestamp int64) int64
	Next(epochTimestamp int64) int64
	Previous(epochTimestamp int64) int64
}
