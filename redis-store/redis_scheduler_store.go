package redis_store

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"gronosq/core/entry"
	"strconv"
)

const DELIMITER = "_"

var ctx = context.Background()

type RedisSchedulerStore struct {
	keyPrefix string
	redis     *redis.Client
}

func NewRedisSchedulerStore(keyPrefix string, redis *redis.Client) *RedisSchedulerStore {
	return &RedisSchedulerStore{keyPrefix: keyPrefix, redis: redis}
}

func (r RedisSchedulerStore) KeyPrefix() string {
	return r.keyPrefix
}

func (r *RedisSchedulerStore) Add(schedulerEntry entry.SchedulerEntry, time int64, partitionNum int64) (string, error) {
	rdb := r.redis
	key := r.getKey(time, partitionNum)
	rdb.SAdd(ctx, key, schedulerEntry.Key())
	result, err := rdb.Set(ctx, r.getPayloadKey(schedulerEntry.Key()), schedulerEntry.Payload(), 0).Result()
	return result, err
}

func (r *RedisSchedulerStore) getPayloadKey(key string) string {
	prefix := r.keyPrefix + DELIMITER
	return prefix + key
}

func (r *RedisSchedulerStore) getKey(time int64, partitionNum int64) string {
	prefix := r.keyPrefix + DELIMITER
	return prefix + strconv.FormatInt(time, 10) + DELIMITER + strconv.FormatInt(partitionNum, 10)
}

func (r *RedisSchedulerStore) Update(entry entry.SchedulerEntry, oldTime int64, newTime int64, partitionNum int64) (bool, error) {
	rdb := r.redis
	oldKey := r.getKey(oldTime, partitionNum)
	newKey := r.getKey(newTime, partitionNum)
	result := rdb.SMove(ctx, oldKey, newKey, entry.Key())
	return result.Val(), nil
}

func (r *RedisSchedulerStore) Remove(schedulerEntry entry.SchedulerEntry, time int64, partitionNum int64) (bool, error) {
	rdb := r.redis
	key := r.getKey(time, partitionNum)
	r1, _ := rdb.SRem(ctx, key, schedulerEntry.Key()).Result()
	r2, _ := rdb.Del(ctx, r.getPayloadKey(schedulerEntry.Key())).Result()
	return r1 == 1 && r2 == 1, nil
}

func (r *RedisSchedulerStore) Get(time int64, partitionNum int64) []entry.SchedulerEntry {
	rdb := r.redis
	key := r.getKey(time, partitionNum)
	resultSet := rdb.SMembers(ctx, key)
	val, _ := resultSet.Result()
	schedulerDataList := r.getSchedulerPayloadValues(val)
	return schedulerDataList
}

func (r *RedisSchedulerStore) getSchedulerPayloadValues(resultSet []string) []entry.SchedulerEntry {
	rdb := r.redis
	keySet := make(map[string]string)
	for _, s := range resultSet {
		keySet[s] = r.getPayloadKey(s)
	}
	var keys = make([]string, len(keySet))
	var i = 0
	for _, v := range keySet {
		keys[i] = v
		i++
	}
	values, _ := rdb.MGet(ctx, keys...).Result()
	var schedulerDataList = make([]entry.SchedulerEntry, len(resultSet))
	var idx = 0
	for k := range keySet {
		value := values[idx].(string)
		schedulerDataList[idx] = entry.NewDefaultSchedulerEntry(k, value)
	}
	return schedulerDataList
}

func (r *RedisSchedulerStore) GetNextN(time int64, partitionNum int64, n int64) []entry.SchedulerEntry {
	rdb := r.redis
	key := r.getKey(time, partitionNum)
	resultSet := rdb.SRandMemberN(ctx, key, n)
	schedulerDataList := r.getSchedulerPayloadValues(resultSet.Val())
	return schedulerDataList
}

func (r *RedisSchedulerStore) RemoveBulk(schedulerEntries []entry.SchedulerEntry, time int64, partitionNum int64) (bool, error) {
	rdb := r.redis
	key := r.getKey(time, partitionNum)
	pipeline := rdb.Pipeline()
	for _, schedulerEntry := range schedulerEntries {
		entryKey := schedulerEntry.Key()
		payloadKey := r.getPayloadKey(schedulerEntry.Key())
		pipeline.SRem(ctx, key, entryKey)
		pipeline.Del(ctx, payloadKey)
		fmt.Println("Removing entry & payload key", entryKey, payloadKey)
	}
	_, err := pipeline.Exec(ctx)
	if err != nil {
		panic(err)
	}
	return true, nil
}
