package config

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestExampleConfigLoads(t *testing.T) {
	cfg := LoadConfig("config.example.yaml")
	if cfg.ProxyConfig == nil {
		t.Fatal("expected proxy_config to be set")
	}
	if cfg.Listen == "" {
		t.Fatal("expected listen to be set")
	}
	if cfg.MetricListen == "" {
		t.Fatal("expected metric_listen to be set")
	}
	if len(cfg.EtcdEndpoints) == 0 {
		t.Fatal("expected etcd_endpoints to be set")
	}
}

func TestUsageConfig(t *testing.T) {
	var cfg Config
	require.NoError(t, yaml.Unmarshal([]byte(`
usage:
  kafka_brokers:
    - kafka-1:9092
    - kafka-2:9092
`), &cfg))
	require.Equal(t, []string{"kafka-1:9092", "kafka-2:9092"}, cfg.Usage.KafkaBrokers)
}
