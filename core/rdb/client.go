package rdb

import (
	"github.com/go-redis/redis/v8"
	"gronosq/config"
)

type Client struct {
}

func (r *Client) Get(config *config.Configuration) *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     config.RedisConfig.Servers,
		Password: config.RedisConfig.Password,
		DB:       config.RedisConfig.Database, //0 : use default DB,
	})
	return rdb
}
