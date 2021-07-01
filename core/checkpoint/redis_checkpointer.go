package checkpoint

import (
	"context"
	"gronos/core/redis"
	"strconv"
)

const DELIMITER = "_"

var ctx = context.Background()

type RedisCheckPointer struct {
	keyPrefix string
	redis     redis.RedisClient
}

func NewRedisCheckPointer(keyPrefix string, redis redis.RedisClient) *RedisCheckPointer {
	return &RedisCheckPointer{keyPrefix: keyPrefix, redis: redis}
}

func (r *RedisCheckPointer) Peek(partitionNum int64) string {
	rdb := r.redis.Client()
	key := r.getKey(r.keyPrefix, partitionNum)
	resultSet := rdb.Get(ctx, key)
	val, _ := resultSet.Result()
	return val
}

func (r *RedisCheckPointer) Set(value string, partitionNum int64) {
	rdb := r.redis.Client()
	key := r.getKey(r.keyPrefix, partitionNum)
	rdb.Set(ctx, key, value, 0)
}

func (r *RedisCheckPointer) getKey(timerKeyPrefix string, partitionNum int64) string {
	return timerKeyPrefix + DELIMITER + strconv.FormatInt(partitionNum, 10)
}
