package config

import (
	"testing"
	"time"

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
	require.Equal(t, "leafage-usage", cfg.Usage.KafkaTopic)
	require.Equal(t, 5*time.Second, cfg.Usage.ReportInterval)
}

func TestUsageConfigDefaultsAndOverrides(t *testing.T) {
	cfg := defaultConfig
	require.NoError(t, yaml.Unmarshal([]byte(`
usage:
  kafka_brokers:
    - kafka-1:9092
    - kafka-2:9092
`), &cfg))
	require.Equal(t, []string{"kafka-1:9092", "kafka-2:9092"}, cfg.Usage.KafkaBrokers)
	require.Equal(t, "leafage-usage", cfg.Usage.KafkaTopic)
	require.Equal(t, 5*time.Second, cfg.Usage.ReportInterval)

	cfg = defaultConfig
	require.NoError(t, yaml.Unmarshal([]byte(`
usage:
  kafka_brokers:
    - kafka-1:9092
  kafka_topic: custom-usage
  report_interval: 12s
`), &cfg))
	require.Equal(t, "custom-usage", cfg.Usage.KafkaTopic)
	require.Equal(t, 12*time.Second, cfg.Usage.ReportInterval)
	require.True(t, cfg.Usage.Enabled())
	require.NoError(t, cfg.Usage.Validate())
}

func TestUsageConfigValidation(t *testing.T) {
	require.NoError(t, (UsageConfig{}).Validate(), "disabled usage needs no Kafka settings")
	require.Error(t, (UsageConfig{
		KafkaBrokers:   []string{"kafka:9092"},
		ReportInterval: time.Second,
	}).Validate())
	require.Error(t, (UsageConfig{
		KafkaBrokers: []string{"kafka:9092"},
		KafkaTopic:   "topic",
	}).Validate())
}
