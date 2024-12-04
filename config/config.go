package config

import (
	"log"
	"os"

	"github.com/Chaintable/nodex-proxy/types"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Listen                      string                       `yaml:"listen"`
	ReplicaNotificationSettings []ReplicaNotificationSetting `yaml:"replica_notification_settings"`
	ProxyConfig                 *types.Config                `yaml:"proxy_config"`
}

type ReplicaNotificationSetting struct {
	EtcdEndpoints []string `yaml:"etcd_endpoints"`
	Key           string   `yaml:"key"`
	ChainID       string   `yaml:"chain_id"`
}

var defaultConfig = Config{
	Listen: ":8663",
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
