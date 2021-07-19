package redis_store

import (
	"gronosq/core/entry"
	"gronosq/core/rdb"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestRedisSchedulerStore_Add(t *testing.T) {
	type fields struct {
		keyPrefix string
		redis     rdb.Client
	}
	type args struct {
		schedulerEntry entry.SchedulerEntry
		time           int64
		partitionNum   int64
	}
	f := fields{keyPrefix: "test_prefix", redis: rdb.Client{}}
	schedulerEntry := entry.NewDefaultSchedulerEntry("key", "payload")
	uTime := time.Now().UnixNano() / int64(time.Millisecond)
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
				redis:     tt.fields.redis.Get(),
			}
			result, err := r.Add(tt.args.schedulerEntry, tt.args.time, tt.args.partitionNum)
			if err != nil {
				t.Errorf("error %v", err)
			}
			if result != "OK" {
				t.Errorf("Get() = %v, want %v", "OK", result)
			}
			var got = r.Get(tt.args.time, tt.args.partitionNum)
			entries := []entry.SchedulerEntry{tt.want}
			for idx, e := range got {
				if !reflect.DeepEqual(e.Key(), entries[idx].Key()) && !reflect.DeepEqual(e.Payload(), entries[idx].Payload()) {
					t.Errorf("Get() = %v, %v, want %v, %v", e.Key(), e.Payload(), entries[idx].Key(), entries[idx].Payload())
				}
			}
		})
	}
}

func TestRedisSchedulerStore_KeyPrefix(t *testing.T) {
	type fields struct {
		keyPrefix string
		redis     rdb.Client
	}
	wantedKeyPrefix := "keyPrefix"
	f := fields{keyPrefix: wantedKeyPrefix, redis: rdb.Client{}}
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
		redis     rdb.Client
	}
	type args struct {
		entries      []entry.SchedulerEntry
		time         int64
		partitionNum int64
	}
	f := fields{keyPrefix: "test_prefix", redis: rdb.Client{}}
	schedulerEntry := entry.NewDefaultSchedulerEntry("key", "payload")
	entries := []entry.SchedulerEntry{schedulerEntry}
	uTime := time.Now().UnixNano() / int64(time.Millisecond)
	a := args{entries: entries, time: uTime, partitionNum: 1}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   entry.SchedulerEntry
	}{
		{"test_Remove", f, a, schedulerEntry},
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
			result, err := r.RemoveBulk(tt.args.entries, tt.args.time, tt.args.partitionNum)
			if err != nil {
				t.Errorf("error %v", err)
			}
			if result != true {
				t.Errorf("Get() = %v, want %v", "OK", result)
			}
		})
	}
}

func TestRedisSchedulerStore_Update(t *testing.T) {
	type fields struct {
		keyPrefix string
		redis     rdb.Client
	}
	type args struct {
		entry        entry.SchedulerEntry
		oldTime      int64
		newTime      int64
		partitionNum int64
	}
	f := fields{keyPrefix: "test_prefix", redis: rdb.Client{}}
	schedulerEntry := entry.NewDefaultSchedulerEntry("key", "payload")
	oldTime := time.Now().UnixNano() / int64(time.Millisecond)
	newTime := time.Now().UnixNano() / int64(time.Millisecond)
	a := args{entry: schedulerEntry, oldTime: oldTime, newTime: newTime, partitionNum: 1}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{"test_Update", f, a, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RedisSchedulerStore{
				keyPrefix: tt.fields.keyPrefix,
				redis:     tt.fields.redis,
			}
			got, _ := r.Update(tt.args.entry, tt.args.oldTime, tt.args.newTime, tt.args.partitionNum)
			if got != tt.want {
				t.Errorf("Update() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRedisSchedulerStore_getKey(t *testing.T) {
	type fields struct {
		keyPrefix string
		redis     rdb.Client
	}
	type args struct {
		time         int64
		partitionNum int64
	}
	prefix := "prefix"
	f := fields{keyPrefix: prefix, redis: rdb.Client{}}
	uTime := time.Now().UnixNano() / int64(time.Millisecond)
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
		redis     rdb.Client
	}
	type args struct {
		time         int64
		partitionNum int64
		n            int64
	}
	f := fields{keyPrefix: "test_prefix", redis: rdb.Client{}}
	schedulerEntry := entry.NewDefaultSchedulerEntry("key", "payload")
	uTime := time.Now().UnixNano() / int64(time.Millisecond)
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
			schedulerEntries := r.GetNextN(tt.args.time, tt.args.partitionNum, tt.args.n)
			for idx, e := range schedulerEntries {
				if !reflect.DeepEqual(e.Key(), entries[idx].Key()) && !reflect.DeepEqual(e.Payload(), entries[idx].Payload()) {
					t.Errorf("Get() = %v, %v, want %v, %v", e.Key(), e.Payload(), entries[idx].Key(), entries[idx].Payload())
				}
			}
		})
	}
}

func TestRedisSchedulerStore_getPayloadKey(t *testing.T) {
	type fields struct {
		keyPrefix string
		redis     rdb.Client
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

func TestRedisSchedulerStore_Remove(t *testing.T) {
	type fields struct {
		keyPrefix string
		redis     rdb.Client
	}
	type args struct {
		schedulerEntry entry.SchedulerEntry
		time           int64
		partitionNum   int64
	}
	f := fields{keyPrefix: "test_prefix", redis: rdb.Client{}}
	schedulerEntry := entry.NewDefaultSchedulerEntry("key", "payload")
	entries := []entry.SchedulerEntry{schedulerEntry}
	uTime := time.Now().UnixNano() / int64(time.Millisecond)
	a := args{schedulerEntry: schedulerEntry, time: uTime, partitionNum: 1}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   entry.SchedulerEntry
	}{
		{"test_Remove", f, a, schedulerEntry},
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
			result, err := r.Remove(tt.args.schedulerEntry, tt.args.time, tt.args.partitionNum)
			if err != nil {
				t.Errorf("error %v", err)
			}
			if result != true {
				t.Errorf("Get() = %v, want %v", "OK", result)
			}
		})
	}
}
