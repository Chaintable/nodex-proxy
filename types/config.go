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
	"github.com/Chaintable/nodex-proxy/lib/log"
)

var DefaultWeight = 100

// defaultConfig default configuration if config file not specified.
var defaultConfig = Config{
	ServiceName:            "jrpcx",
	RPCListen:              ":9545",
	InternalAPIListen:      ":9268",
	EnablePProf:            false,
	NativeNodeURL:          "http://127.0.0.1:8545",
	OfficialNodeURL:        "",
	BlockReaderCacheTTL:    1,
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
		RateLimiter: RateLimiterProcessorConfig{
			RpcMethods: map[string]int{},
		},
		BlockRangeQueryLimit: BlockRangeQueryLimitProcessorConfig{
			Enable:          false,
			RecentBlocks:    0,
			RewriteToLatest: false,
		},
		RequestMirror: RequestMirrorConfig{
			Enable: false,
		},
		MethodNameChecker: MethodNameCheckerConfig{
			Enable: false,
			Regexp: "^[a-zA-Z_][a-zA-Z0-9_]*$", // by default only alphanumeric and "_" are allowed, numeric cannot be leading character
		},
		MethodDenied: &[]jsonrpc.RPCMethod{
			"eth_newPendingTransactionFilter",
			"txpool_content",
			"txpool_inspect",
			"txpool_contentFrom",
			"txpool_status",
		},
	},
	Observability: ObservabilityConfig{
		Trace: ObservabilityTraceConfig{
			SamplingRatio: 0.1,
		},
		Log: log.ObservabilityLogConfig{
			Sampling: &log.ObservabilityLogSamplingConfig{
				Enable:     true,
				Initial:    100,
				Thereafter: 100,
			},
		},
	},
	NodeSelectStrategy:     "random",
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

type RateLimiterProcessorConfig struct {
	RpcMethods map[string]int `yaml:"rpc_methods"` // map[method]rps
}

type BlockRangeQueryLimitProcessorConfig struct {
	Enable          bool   `yaml:"enable"`
	HashBlockEnable bool   `yaml:"hash_block_enable"`
	RecentBlocks    uint64 `yaml:"recent_blocks"` // most recently blocks
	RewriteToLatest bool   `yaml:"rewrite_to_latest"`
}

type RequestMirrorConfig struct {
	Enable bool `yaml:"enable"`
}

type MethodNameCheckerConfig struct {
	Enable       bool   `yaml:"enable"`
	Regexp       string `yaml:"regexp"`
	DeniedRegexp string `yaml:"denied_regexp"`
}

type ProcessorConfig struct {
	ObservabilityLog     ObservabilityLogProcessorConfig     `yaml:"observability_log"`
	RateLimiter          RateLimiterProcessorConfig          `yaml:"rate_limiter"`
	BlockRangeQueryLimit BlockRangeQueryLimitProcessorConfig `yaml:"block_range_query_limit"`
	RequestMirror        RequestMirrorConfig                 `yaml:"request_mirror"`
	MethodNameChecker    MethodNameCheckerConfig             `yaml:"method_name_checker"`
	MethodDenied         *[]jsonrpc.RPCMethod                `yaml:"method_denied"`
}

type ObservabilityTraceConfig struct {
	Enable        bool    `yaml:"enable"`
	OTLPEndpoint  string  `yaml:"otlp_endpoint"`
	SamplingRatio float64 `yaml:"sampling_ratio"`
}
type (
	ObservabilityMetricConfig struct{}
	ObservabilityConfig       struct {
		Trace  ObservabilityTraceConfig   `yaml:"trace"`
		Metric ObservabilityMetricConfig  `yaml:"metric"`
		Log    log.ObservabilityLogConfig `yaml:"log"`
		// StaticResource can contain for example information about the application that emits the record or about the infrastructure where the application runs.
		StaticResource map[string]string `yaml:"static_resource"`
	}
)

// Config ...
type Config struct {
	ServiceName            string              `yaml:"service_name"`
	RPCListen              string              `yaml:"rpc_listen"`
	InternalAPIListen      string              `yaml:"internal_api_listen"`
	EnablePProf            bool                `yaml:"enable_pprof"`
	NativeNodeURL          string              `yaml:"native_node_url"`
	ReverseProxy           bool                `yaml:"reverse_proxy"`
	DefaultRPCTimeout      int                 `yaml:"default_rpc_timeout"`
	RPCMethodTimeoutConfig map[string]int      `yaml:"rpc_method_timeout_config"`
	OfficialNodeURL        string              `yaml:"official_node_url"`
	BlockReaderCacheTTL    int                 `yaml:"block_reader_cache_ttl"`
	ChainId                string              `yaml:"chain_id"`
	ConnectionPoolSize     int                 `yaml:"connection_pool_size"`   // max upstream connections per backend host
	ConnMaxIdleDuration    int                 `yaml:"conn_max_idle_duration"` // in milliseconds; keep below the backend keep-alive idle timeout
	ConnMaxWaitTimeout     int                 `yaml:"conn_max_wait_timeout"`  // in milliseconds; how long a request waits for a free connection before failing
	ConnDialTimeout        int                 `yaml:"conn_dial_timeout"`      // in milliseconds
	Processor              ProcessorConfig     `yaml:"processor"`
	Observability          ObservabilityConfig `yaml:"observability"`
	NodeSelectStrategy     string              `yaml:"node_select_strategy"`
	EtcdPrefix             string              `yaml:"etcd_prefix"`
	NodeHealthCheckTimeout int                 `yaml:"node_health_check_timeout"`  // in seconds
	NodeHealthCheckMaxWait int                 `yaml:"node_health_check_max_wait"` // in seconds
	// ArchiveHashCacheSize is how many block hashes per chain are remembered as
	// archive-only (state returned -39006, archive served it). Requests on a
	// remembered hash go straight to archive. 0 uses the default; negative
	// disables the cache.
	ArchiveHashCacheSize int `yaml:"archive_hash_cache_size"`
}

func FillWithDefaultConfig(cfg *Config) {
	if cfg.ServiceName == "" {
		cfg.ServiceName = defaultConfig.ServiceName
	}
	if cfg.RPCListen == "" {
		cfg.RPCListen = defaultConfig.RPCListen
	}
	if cfg.InternalAPIListen == "" {
		cfg.InternalAPIListen = defaultConfig.InternalAPIListen
	}
	if cfg.NativeNodeURL == "" {
		cfg.NativeNodeURL = defaultConfig.NativeNodeURL
	}
	if cfg.BlockReaderCacheTTL == 0 {
		cfg.BlockReaderCacheTTL = defaultConfig.BlockReaderCacheTTL
	}
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
	// processor config

	// Observability Log Processor
	defaultObservabilityLogProcessConfig := defaultConfig.Processor.ObservabilityLog
	observabilityLogProcessConfig := &cfg.Processor.ObservabilityLog
	if observabilityLogProcessConfig.SlowThreshold.Default == 0 {
		observabilityLogProcessConfig.SlowThreshold.Default = defaultObservabilityLogProcessConfig.SlowThreshold.Default
	}
	if observabilityLogProcessConfig.SlowThreshold.RpcMethods == nil { // make sure RpcMethods isn't nil value for avoid panic reason.
		observabilityLogProcessConfig.SlowThreshold.RpcMethods = defaultObservabilityLogProcessConfig.SlowThreshold.RpcMethods
	}

	defaultMethodNameCheckerProcessConfig := defaultConfig.Processor.MethodNameChecker
	methodNameProcessConfig := &cfg.Processor.MethodNameChecker
	if methodNameProcessConfig.Regexp == "" {
		methodNameProcessConfig.Regexp = defaultMethodNameCheckerProcessConfig.Regexp
	}

	if cfg.Processor.MethodDenied == nil {
		cfg.Processor.MethodDenied = defaultConfig.Processor.MethodDenied
	}

	// rate limiter processor config
	defaultRateLimiterProcessConfig := defaultConfig.Processor.RateLimiter
	rateLimiterProcessConfig := &cfg.Processor.RateLimiter
	if rateLimiterProcessConfig.RpcMethods == nil {
		rateLimiterProcessConfig.RpcMethods = defaultRateLimiterProcessConfig.RpcMethods
	}

	// raft processor config
	// trace
	defaultObservabilityTraceConfig := defaultConfig.Observability.Trace
	observabilityTraceConfig := &cfg.Observability.Trace
	if observabilityTraceConfig.SamplingRatio == 0 {
		observabilityTraceConfig.SamplingRatio = defaultObservabilityTraceConfig.SamplingRatio
	}

	// log
	defaultObservabilityLogConfig := defaultConfig.Observability.Log
	observabilityLogConfig := &cfg.Observability.Log
	if observabilityLogConfig.Sampling == nil {
		observabilityLogConfig.Sampling = defaultObservabilityLogConfig.Sampling
	}
	if cfg.NodeSelectStrategy == "" {
		cfg.NodeSelectStrategy = defaultConfig.NodeSelectStrategy
	}
	if cfg.EtcdPrefix == "" {
		cfg.EtcdPrefix = defaultConfig.EtcdPrefix
	}
	cfg.initMetrics()
}

const (
	MirrorRequestHost = "mirror"
)

func DefaultConfig() Config {
	return defaultConfig
}
