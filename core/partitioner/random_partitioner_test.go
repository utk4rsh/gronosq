package partitioner

import "testing"

func TestRandomPartitioner_partition(t *testing.T) {
	type fields struct {
		numOfPartitions int64
	}
	type args struct {
		entry string
	}
	f := fields{numOfPartitions: 16}
	a := args{entry: "random"}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int64
	}{
		{"test_return", f, a, 1},
		{"test_return_1", f, a, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			randomPartitioner := &RandomPartitioner{
				numOfPartitions: tt.fields.numOfPartitions,
			}
			if got := randomPartitioner.Partition(tt.args.entry); got >= f.numOfPartitions {
				t.Errorf("Partition() = %v, want %v", got, tt.want)
			}
		})
	}
}
