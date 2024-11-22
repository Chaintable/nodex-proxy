package config

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Listen                       string   `yaml:"listen"`
	InnerReplicaBrokers          []string `yaml:"inner_replica_brokers"`
	InnerReplicaStateChangeTopic string   `yaml:"inner_replica_state_change_topic"`
	InnerReplicaGroupID          string   `yaml:"inner_replica_group_id"`
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
	return config
}
