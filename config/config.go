package config

import (
	"log"
	"os"

	"github.com/Chaintable/nodex-proxy/types"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Listen        string        `yaml:"listen"`
	MetricListen  string        `yaml:"metric_listen"`
	EtcdEndpoints []string      `yaml:"etcd_endpoints"`
	ProxyConfig   *types.Config `yaml:"proxy_config"`
	Usage         UsageConfig   `yaml:"usage"`
	LogLevel      string        `yaml:"log_level"` // debug, info, warn, error
}

// UsageConfig controls the optional process-wide usage collector. Usage
// collection is disabled when KafkaBrokers is empty.
type UsageConfig struct {
	KafkaBrokers []string `yaml:"kafka_brokers"`
}

var defaultConfig = Config{
	Listen:       "8663",
	MetricListen: "8664",
}

func LoadConfig(configPath string) Config {
	configFile, err := os.Open(configPath)
	if err != nil {
		log.Fatalf("open config file error: %v\n", err)
	}
	defer configFile.Close()

	var config = defaultConfig
	parser := yaml.NewDecoder(configFile)
	err = parser.Decode(&config)
	if err != nil {
		log.Fatalf("parse config file error: %v\n", err)
	}
	types.FillWithDefaultConfig(config.ProxyConfig)
	return config
}
