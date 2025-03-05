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
}

type ReplicaNotificationSetting struct {
	EtcdEndpoints []string `yaml:"etcd_endpoints"`
}

var defaultConfig = Config{
	Listen:       ":8663",
	MetricListen: ":8664",
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
