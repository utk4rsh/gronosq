package bucket

type SecondGroupedTimeBucket struct {
	numOfSecsForABucket int64
}

func (s SecondGroupedTimeBucket) NumOfSecsForABucket() int64 {
	return s.numOfSecsForABucket
}

func NewSecondGroupedTimeBucket(numOfSecsForABucket int64) *SecondGroupedTimeBucket {
	return &SecondGroupedTimeBucket{numOfSecsForABucket: numOfSecsForABucket}
}

func (s SecondGroupedTimeBucket) toBucket(epochTimestamp int64) int64 {
	result := epochTimestamp - (epochTimestamp % (s.numOfSecsForABucket * 1000))
	return result
}

func (s SecondGroupedTimeBucket) next(epochTimestamp int64) int64 {
	result := s.toBucket(epochTimestamp) + (s.numOfSecsForABucket * 1000)
	return result
}

func (s SecondGroupedTimeBucket) previous(epochTimestamp int64) int64 {
	result := s.toBucket(epochTimestamp) - (s.numOfSecsForABucket * 1000)
	return result
}
