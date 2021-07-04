package checkpoint

import (
	"context"
	"github.com/go-redis/redis/v8"
	"strconv"
	"time"
)

const DELIMITER = "_"

var ctx = context.Background()

type RedisCheckPointer struct {
	keyPrefix string
	redis     *redis.Client
}

func NewRedisCheckPointer(keyPrefix string, redis *redis.Client) *RedisCheckPointer {
	return &RedisCheckPointer{keyPrefix: keyPrefix, redis: redis}
}

func (r *RedisCheckPointer) Peek(partitionNum int64) string {
	rdb := r.redis
	key := r.getKey(r.keyPrefix, partitionNum)
	resultSet := rdb.Get(ctx, key)
	val, _ := resultSet.Result()
	return val
}

func (r *RedisCheckPointer) Set(value string, partitionNum int64) {
	rdb := r.redis
	key := r.getKey(r.keyPrefix, partitionNum)
	rdb.Set(ctx, key, value, time.Duration(60)*time.Millisecond)
}

func (r *RedisCheckPointer) getKey(timerKeyPrefix string, partitionNum int64) string {
	return timerKeyPrefix + DELIMITER + strconv.FormatInt(partitionNum, 10)
}
