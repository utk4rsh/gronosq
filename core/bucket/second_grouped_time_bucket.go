package bucket

type SecondGroupedTimeBucket struct {
	numOfSecsForABucket int64
}

func NewSecondGroupedTimeBucket(numOfSecsForABucket int64) *SecondGroupedTimeBucket {
	return &SecondGroupedTimeBucket{numOfSecsForABucket: numOfSecsForABucket}
}

func (s SecondGroupedTimeBucket) NumOfSecsForABucket() int64 {
	return s.numOfSecsForABucket
}

func (s SecondGroupedTimeBucket) ToBucket(epochTimestamp int64) int64 {
	result := epochTimestamp - (epochTimestamp % (s.numOfSecsForABucket * 1000))
	return result
}

func (s SecondGroupedTimeBucket) Next(epochTimestamp int64) int64 {
	result := s.ToBucket(epochTimestamp) + (s.numOfSecsForABucket * 1000)
	return result
}

func (s SecondGroupedTimeBucket) Previous(epochTimestamp int64) int64 {
	result := s.ToBucket(epochTimestamp) - (s.numOfSecsForABucket * 1000)
	return result
}
