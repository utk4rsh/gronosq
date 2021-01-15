package redis_store

import (
	"context"
	"fmt"
	"gronos/core/entry"
	"strconv"
)

const DELIMITER = "_"

var ctx = context.Background()

type RedisSchedulerStore struct {
	keyPrefix string
	redis     RedisClient
}

func (r RedisSchedulerStore) KeyPrefix() string {
	return r.keyPrefix
}

func (r *RedisSchedulerStore) Add(schedulerEntry entry.SchedulerEntry, time int64, partitionNum int64) (string, error) {
	rdb := r.redis.Client()
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
	rdb := r.redis.Client()
	oldKey := r.getKey(oldTime, partitionNum)
	newKey := r.getKey(newTime, partitionNum)
	result := rdb.SMove(ctx, oldKey, newKey, entry.Key())
	return result.Val(), nil
}

func (r *RedisSchedulerStore) remove(value string, time int64, partitionNum int64) int64 {
	rdb := r.redis.Client()
	key := r.getKey(time, partitionNum)
	result := rdb.SRem(ctx, key, value)
	rdb.Del(ctx, r.getPayloadKey(value))
	return result.Val()
}

func (r *RedisSchedulerStore) get(time int64, partitionNum int64) []entry.SchedulerEntry {
	rdb := r.redis.Client()
	key := r.getKey(time, partitionNum)
	resultSet := rdb.SMembers(ctx, key)
	val, _ := resultSet.Result()
	schedulerDataList := r.getSchedulerPayloadValues(val)
	return schedulerDataList
}

func (r *RedisSchedulerStore) getSchedulerPayloadValues(resultSet []string) []entry.SchedulerEntry {
	rdb := r.redis.Client()
	keySet := make(map[string]string)
	for _, s := range resultSet {
		keySet[s] = s
	}
	var keys = make([]string, len(keySet))
	var i = 0
	for k := range keySet {
		keys[i] = r.getPayloadKey(k)
		i++
	}
	values, _ := rdb.MGet(ctx, keys...).Result()
	var schedulerDataList = make([]entry.SchedulerEntry, len(resultSet))
	for idx, key := range keys {
		value := values[idx].(string)
		schedulerDataList[idx] = entry.NewDefaultSchedulerEntry(key, value)
	}
	fmt.Printf("val %v \n", schedulerDataList)
	return schedulerDataList
}

func (r *RedisSchedulerStore) getNextN(time int64, partitionNum int64, n int64) []entry.SchedulerEntry {
	rdb := r.redis.Client()
	key := r.getKey(time, partitionNum)
	resultSet := rdb.SRandMemberN(ctx, key, n)
	schedulerDataList := r.getSchedulerPayloadValues(resultSet.Val())
	return schedulerDataList
}

func (r *RedisSchedulerStore) RemoveBulk() (time int64, partitionNum int64, values []string) {
	rdb := r.redis.Client()
	key := r.getKey(time, partitionNum)
	pipeline := rdb.Pipeline()
	for _, value := range values {
		pipeline.SRem(ctx, key, value)
		pipeline.Del(ctx, r.getPayloadKey(value))
	}
	return
}
