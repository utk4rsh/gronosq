package redis_store

import (
	"context"
	"github.com/go-redis/redis/v8"
	"gronos/core/entry"
	"strconv"
)

const DELIMITER = "_"

var ctx = context.Background()

type RedisSchedulerStore struct {
	keyPrefix string
}

func (redisSchedulerStore RedisSchedulerStore) KeyPrefix() string {
	return redisSchedulerStore.keyPrefix
}

func (redisSchedulerStore RedisSchedulerStore) add(schedulerEntry entry.SchedulerEntry, time uint64, partitionNum int64) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	key := redisSchedulerStore.getKey(time, partitionNum)
	rdb.SAdd(ctx, key, schedulerEntry.Key())
	rdb.Set(ctx, redisSchedulerStore.getPayloadKey(schedulerEntry.Key()), schedulerEntry.Payload(), 0)
}

func (redisSchedulerStore RedisSchedulerStore) getPayloadKey(key string) string {
	prefix := redisSchedulerStore.keyPrefix + DELIMITER
	return prefix + key
}

func (redisSchedulerStore RedisSchedulerStore) getKey(time uint64, partitionNum int64) string {
	prefix := redisSchedulerStore.keyPrefix + DELIMITER
	return prefix + strconv.FormatUint(time, 10) + DELIMITER + strconv.FormatInt(partitionNum, 10)
}

func (RedisSchedulerStore) update(entry entry.SchedulerEntry, oldTime uint64, newTime uint64, partitionNum int64) uint64 {
	panic("implement me")
}

func (RedisSchedulerStore) remove(value string, time uint64, partitionNum int64) int64 {
	panic("implement me")
}

func (RedisSchedulerStore) get(time uint64, partitionNum int64) []entry.SchedulerEntry {
	panic("implement me")
}

func (RedisSchedulerStore) getNextN(time uint64, partitionNum int64, n int64) []entry.SchedulerEntry {
	panic("implement me")
}

func (RedisSchedulerStore) removeBulk() (time uint64, partitionNum int64, values []string) {
	panic("implement me")
}
