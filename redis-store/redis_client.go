package redis_store

import "github.com/go-redis/redis/v8"

type RedisClient struct {
}

func (r *RedisClient) Client() *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	return rdb
}
