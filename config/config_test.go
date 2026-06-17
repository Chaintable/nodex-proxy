package config

import "testing"

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
