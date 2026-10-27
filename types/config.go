// Copyright (c) 2022 DeBank Inc. <admin@debank.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package types

import (
	"github.com/Chaintable/nodex-proxy/jsonrpc"
)

var DefaultWeight = 100

// defaultConfig default configuration if config file not specified.
var defaultConfig = Config{
	DefaultRPCTimeout:      5000,
	ConnectionPoolSize:     512,
	ConnMaxIdleDuration:    90000,
	ConnMaxWaitTimeout:     1000,
	ConnDialTimeout:        3000,
	RPCMethodTimeoutConfig: map[string]int{},
	ArchiveHashCacheSize:   1024,
	Processor: ProcessorConfig{
		ObservabilityLog: ObservabilityLogProcessorConfig{
			Enable: false,
			SlowThreshold: SlowLogThresholdConfig{
				Default:    500,
				RpcMethods: map[jsonrpc.RPCMethod]int{},
			},
		},
		RequestMirror: RequestMirrorConfig{
			Enable: false,
		},
		MethodDenied: &[]jsonrpc.RPCMethod{
			"eth_newPendingTransactionFilter",
			"txpool_content",
			"txpool_inspect",
			"txpool_contentFrom",
			"txpool_status",
		},
	},
	EtcdPrefix:             "",
	NodeHealthCheckTimeout: 5,   // in seconds
	NodeHealthCheckMaxWait: 300, // in seconds
}

type ObservabilityLogProcessorConfig struct {
	Enable         bool                   `yaml:"enable"`
	SlowThreshold  SlowLogThresholdConfig `yaml:"slow_threshold"`
	EnableErrorLog bool                   `yaml:"enable_error_log"`
}

// SlowLogThresholdConfig configuration of slow log threshold, in millisecond.
type SlowLogThresholdConfig struct {
	Default    int                       `yaml:"default"`
	RpcMethods map[jsonrpc.RPCMethod]int `yaml:"rpc_methods"`
}

type RequestMirrorConfig struct {
	Enable bool `yaml:"enable"`
}

type ProcessorConfig struct {
	ObservabilityLog ObservabilityLogProcessorConfig `yaml:"observability_log"`
	RequestMirror    RequestMirrorConfig             `yaml:"request_mirror"`
	MethodDenied     *[]jsonrpc.RPCMethod            `yaml:"method_denied"`
}

// Config ...
type Config struct {
	DefaultRPCTimeout      int             `yaml:"default_rpc_timeout"`
	RPCMethodTimeoutConfig map[string]int  `yaml:"rpc_method_timeout_config"`
	ConnectionPoolSize     int             `yaml:"connection_pool_size"`   // max upstream connections per backend host
	ConnMaxIdleDuration    int             `yaml:"conn_max_idle_duration"` // in milliseconds; keep below the backend keep-alive idle timeout
	ConnMaxWaitTimeout     int             `yaml:"conn_max_wait_timeout"`  // in milliseconds; how long a request waits for a free connection before failing
	ConnDialTimeout        int             `yaml:"conn_dial_timeout"`      // in milliseconds
	Processor              ProcessorConfig `yaml:"processor"`
	EtcdPrefix             string          `yaml:"etcd_prefix"`
	NodeHealthCheckTimeout int             `yaml:"node_health_check_timeout"`  // in seconds
	NodeHealthCheckMaxWait int             `yaml:"node_health_check_max_wait"` // in seconds
	// ArchiveHashCacheSize is how many block hashes per chain are remembered as
	// archive-only (state returned -39006, archive served it). Requests on a
	// remembered hash go straight to archive. 0 uses the default; negative
	// disables the cache.
	ArchiveHashCacheSize int `yaml:"archive_hash_cache_size"`
}

func FillWithDefaultConfig(cfg *Config) {
	if cfg.DefaultRPCTimeout == 0 {
		cfg.DefaultRPCTimeout = defaultConfig.DefaultRPCTimeout
	}
	if cfg.ConnectionPoolSize == 0 {
		cfg.ConnectionPoolSize = defaultConfig.ConnectionPoolSize
	}
	if cfg.ConnMaxIdleDuration == 0 {
		cfg.ConnMaxIdleDuration = defaultConfig.ConnMaxIdleDuration
	}
	if cfg.ConnMaxWaitTimeout == 0 {
		cfg.ConnMaxWaitTimeout = defaultConfig.ConnMaxWaitTimeout
	}
	if cfg.ConnDialTimeout == 0 {
		cfg.ConnDialTimeout = defaultConfig.ConnDialTimeout
	}
	if cfg.ArchiveHashCacheSize == 0 {
		cfg.ArchiveHashCacheSize = defaultConfig.ArchiveHashCacheSize
	}
	if cfg.RPCMethodTimeoutConfig == nil {
		cfg.RPCMethodTimeoutConfig = map[string]int{}
	}

	slow := &cfg.Processor.ObservabilityLog.SlowThreshold
	if slow.Default == 0 {
		slow.Default = defaultConfig.Processor.ObservabilityLog.SlowThreshold.Default
	}
	if slow.RpcMethods == nil {
		slow.RpcMethods = map[jsonrpc.RPCMethod]int{}
	}

	if cfg.Processor.MethodDenied == nil {
		cfg.Processor.MethodDenied = defaultConfig.Processor.MethodDenied
	}
	cfg.initMetrics()
}

const (
	MirrorRequestHost = "mirror"
)

func DefaultConfig() Config {
	return defaultConfig
}
