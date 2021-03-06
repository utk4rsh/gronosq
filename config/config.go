package config

// Configuration exported
type Configuration struct {
	CommonConfig struct {
		Prefix    string `yaml:"prefix"`
		BatchSize int    `yaml:"batchSize"`
	}`yaml:"commonConfig"`
	KafkaConfig struct {
		Brokers string `yaml:"brokers"`
		Topic   string `yaml:"topic"`
		Group   string `yaml:"group"`
	} `yaml:"kafkaConfig"`
	ZooKeeperConfig struct {
		Servers string `yaml:"servers"`
		ZkPath  string `yaml:"zkPath"`
	} `yaml:"zooKeeperConfig"`
	RedisConfig struct {
		Servers  string `yaml:"servers"`
		Password string `yaml:"password"`
		Database int    `yaml:"database"`
	} `yaml:"redisConfig"`
}
