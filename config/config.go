package config

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

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
	KafkaBrokers   []string      `yaml:"kafka_brokers"`
	KafkaTopic     string        `yaml:"kafka_topic"`
	ReportInterval time.Duration `yaml:"report_interval"`
}

var defaultConfig = Config{
	Listen:       "8663",
	MetricListen: "8664",
	Usage: UsageConfig{
		KafkaTopic:     "leafage-usage",
		ReportInterval: 5 * time.Second,
	},
}

func (c UsageConfig) Enabled() bool {
	return len(c.KafkaBrokers) > 0
}

func (c UsageConfig) Validate() error {
	if !c.Enabled() {
		return nil
	}
	if strings.TrimSpace(c.KafkaTopic) == "" {
		return fmt.Errorf("usage.kafka_topic cannot be blank")
	}
	if c.ReportInterval <= 0 {
		return fmt.Errorf("usage.report_interval must be positive")
	}
	return nil
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
