package bucket

import (
	"reflect"
	"testing"
)

func TestNewSecondGroupedTimeBucket(t *testing.T) {
	type args struct {
		numOfSecsForABucket int64
	}
	tests := []struct {
		name string
		args args
		want *SecondGroupedTimeBucket
	}{
		{"constructor_test", args{numOfSecsForABucket: 1}, NewSecondGroupedTimeBucket(1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewSecondGroupedTimeBucket(tt.args.numOfSecsForABucket); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewSecondGroupedTimeBucket() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSecondGroupedTimeBucket_NumOfSecsForABucket(t *testing.T) {
	type fields struct {
		numOfSecsForABucket int64
	}
	tests := []struct {
		name   string
		fields fields
		want   int64
	}{
		{"get_num_test", fields{numOfSecsForABucket: 10}, 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := SecondGroupedTimeBucket{
				numOfSecsForABucket: tt.fields.numOfSecsForABucket,
			}
			if got := s.NumOfSecsForABucket(); got != tt.want {
				t.Errorf("NumOfSecsForABucket() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSecondGroupedTimeBucket_next(t *testing.T) {
	type fields struct {
		numOfSecsForABucket int64
	}
	type args struct {
		epochTimestamp int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int64
	}{
		{"next_test", fields{numOfSecsForABucket: 1}, args{epochTimestamp: 19012143232}, 19012144000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := SecondGroupedTimeBucket{
				numOfSecsForABucket: tt.fields.numOfSecsForABucket,
			}
			if got := s.next(tt.args.epochTimestamp); got != tt.want {
				t.Errorf("next() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSecondGroupedTimeBucket_previous(t *testing.T) {
	type fields struct {
		numOfSecsForABucket int64
	}
	type args struct {
		epochTimestamp int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int64
	}{
		{"next_test", fields{numOfSecsForABucket: 1}, args{epochTimestamp: 19012143232}, 19012142000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := SecondGroupedTimeBucket{
				numOfSecsForABucket: tt.fields.numOfSecsForABucket,
			}
			if got := s.previous(tt.args.epochTimestamp); got != tt.want {
				t.Errorf("previous() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSecondGroupedTimeBucket_toBucket(t *testing.T) {
	type fields struct {
		numOfSecsForABucket int64
	}
	type args struct {
		epochTimestamp int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int64
	}{
		{"toBucket_test", fields{numOfSecsForABucket: 1}, args{epochTimestamp: 19012143232}, 19012143000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := SecondGroupedTimeBucket{
				numOfSecsForABucket: tt.fields.numOfSecsForABucket,
			}
			if got := s.toBucket(tt.args.epochTimestamp); got != tt.want {
				t.Errorf("toBucket() = %v, want %v", got, tt.want)
			}
		})
	}
}
