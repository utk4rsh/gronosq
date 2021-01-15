package redis_store

import (
	"gronos/core/entry"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestRedisSchedulerStore_Add(t *testing.T) {
	type fields struct {
		keyPrefix string
		redis     RedisClient
	}
	type args struct {
		schedulerEntry entry.SchedulerEntry
		time           int64
		partitionNum   int64
	}
	f := fields{keyPrefix: "test_prefix", redis: RedisClient{}}
	schedulerEntry := entry.NewDefaultSchedulerEntry("key", "payload")
	uTime := time.Now().Unix()
	a := args{schedulerEntry: schedulerEntry, time: uTime, partitionNum: 1}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   entry.SchedulerEntry
	}{
		{"test_add", f, a, schedulerEntry},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RedisSchedulerStore{
				keyPrefix: tt.fields.keyPrefix,
				redis:     tt.fields.redis,
			}
			result, err := r.Add(tt.args.schedulerEntry, tt.args.time, tt.args.partitionNum)
			if err != nil {
				t.Errorf("error %v", err)
			}
			if result != "OK" {
				t.Errorf("get() = %v, want %v", "OK", result)
			}
			var got = r.get(tt.args.time, tt.args.partitionNum)
			entries := []entry.SchedulerEntry{tt.want}
			for idx, e := range got {
				if !reflect.DeepEqual(e.Key(), entries[idx].Key()) && !reflect.DeepEqual(e.Payload(), entries[idx].Payload()) {
					t.Errorf("get() = %v, %v, want %v, %v", e.Key(), e.Payload(), entries[idx].Key(), entries[idx].Payload())
				}
			}
		})
	}
}

func TestRedisSchedulerStore_KeyPrefix(t *testing.T) {
	type fields struct {
		keyPrefix string
		redis     RedisClient
	}
	wantedKeyPrefix := "keyPrefix"
	f := fields{keyPrefix: wantedKeyPrefix, redis: RedisClient{}}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{"test_KeyPrefix", f, wantedKeyPrefix},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := RedisSchedulerStore{
				keyPrefix: tt.fields.keyPrefix,
				redis:     tt.fields.redis,
			}
			if got := r.KeyPrefix(); got != tt.want {
				t.Errorf("KeyPrefix() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRedisSchedulerStore_RemoveBulk(t *testing.T) {
	type fields struct {
		keyPrefix string
		redis     RedisClient
	}
	tests := []struct {
		name             string
		fields           fields
		wantTime         int64
		wantPartitionNum int64
		wantValues       []string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RedisSchedulerStore{
				keyPrefix: tt.fields.keyPrefix,
				redis:     tt.fields.redis,
			}
			gotTime, gotPartitionNum, gotValues := r.RemoveBulk()
			if gotTime != tt.wantTime {
				t.Errorf("RemoveBulk() gotTime = %v, want %v", gotTime, tt.wantTime)
			}
			if gotPartitionNum != tt.wantPartitionNum {
				t.Errorf("RemoveBulk() gotPartitionNum = %v, want %v", gotPartitionNum, tt.wantPartitionNum)
			}
			if !reflect.DeepEqual(gotValues, tt.wantValues) {
				t.Errorf("RemoveBulk() gotValues = %v, want %v", gotValues, tt.wantValues)
			}
		})
	}
}

func TestRedisSchedulerStore_Update(t *testing.T) {
	type fields struct {
		keyPrefix string
		redis     RedisClient
	}
	type args struct {
		entry        entry.SchedulerEntry
		oldTime      int64
		newTime      int64
		partitionNum int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RedisSchedulerStore{
				keyPrefix: tt.fields.keyPrefix,
				redis:     tt.fields.redis,
			}
			got, err := r.Update(tt.args.entry, tt.args.oldTime, tt.args.newTime, tt.args.partitionNum)
			if (err != nil) != tt.wantErr {
				t.Errorf("Update() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Update() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRedisSchedulerStore_getKey(t *testing.T) {
	type fields struct {
		keyPrefix string
		redis     RedisClient
	}
	type args struct {
		time         int64
		partitionNum int64
	}
	prefix := "prefix"
	f := fields{keyPrefix: prefix, redis: RedisClient{}}
	uTime := time.Now().Unix()
	var partitionNum int64 = 1
	a := args{time: uTime, partitionNum: partitionNum}
	expectedKey := prefix + DELIMITER + strconv.FormatInt(uTime, 10) + DELIMITER + strconv.FormatInt(partitionNum, 10)
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{"test_getKey", f, a, expectedKey},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RedisSchedulerStore{
				keyPrefix: tt.fields.keyPrefix,
				redis:     tt.fields.redis,
			}
			if got := r.getKey(tt.args.time, tt.args.partitionNum); got != tt.want {
				t.Errorf("getKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRedisSchedulerStore_getNextN(t *testing.T) {
	type fields struct {
		keyPrefix string
		redis     RedisClient
	}
	type args struct {
		time         int64
		partitionNum int64
		n            int64
	}
	f := fields{keyPrefix: "test_prefix", redis: RedisClient{}}
	schedulerEntry := entry.NewDefaultSchedulerEntry("key", "payload")
	uTime := time.Now().Unix()
	a := args{time: uTime, partitionNum: 1, n: 5}
	entries := []entry.SchedulerEntry{schedulerEntry}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []entry.SchedulerEntry
	}{
		{"test_getNextN", f, a, entries},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RedisSchedulerStore{
				keyPrefix: tt.fields.keyPrefix,
				redis:     tt.fields.redis,
			}
			for _, e := range entries {
				_, _ = r.Add(e, tt.args.time, tt.args.partitionNum)
			}
			if got := r.getNextN(tt.args.time, tt.args.partitionNum, tt.args.n); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getNextN() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRedisSchedulerStore_getPayloadKey(t *testing.T) {
	type fields struct {
		keyPrefix string
		redis     RedisClient
	}
	type args struct {
		key string
	}
	prefix := "keyPrefix"
	f := fields{keyPrefix: prefix}
	key := "random_key"
	a := args{key: key}
	expectedKey := prefix + DELIMITER + key
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{"test_getKey", f, a, expectedKey},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RedisSchedulerStore{
				keyPrefix: tt.fields.keyPrefix,
				redis:     tt.fields.redis,
			}
			if got := r.getPayloadKey(tt.args.key); got != tt.want {
				t.Errorf("getPayloadKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRedisSchedulerStore_getSchedulerPayloadValues(t *testing.T) {
	type fields struct {
		keyPrefix string
		redis     RedisClient
	}
	type args struct {
		partitionNum int64
		resultSet    []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []entry.SchedulerEntry
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RedisSchedulerStore{
				keyPrefix: tt.fields.keyPrefix,
				redis:     tt.fields.redis,
			}
			if got := r.getSchedulerPayloadValues(tt.args.resultSet); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getSchedulerPayloadValues() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRedisSchedulerStore_remove(t *testing.T) {
	type fields struct {
		keyPrefix string
		redis     RedisClient
	}
	type args struct {
		value        string
		time         int64
		partitionNum int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int64
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RedisSchedulerStore{
				keyPrefix: tt.fields.keyPrefix,
				redis:     tt.fields.redis,
			}
			if got := r.remove(tt.args.value, tt.args.time, tt.args.partitionNum); got != tt.want {
				t.Errorf("remove() = %v, want %v", got, tt.want)
			}
		})
	}
}
