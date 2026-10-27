package log

import (
	"time"

	"go.opentelemetry.io/otel/attribute"

	"go.uber.org/zap"
)

var (
	observabilityLogger *zap.Logger
)

// ObservabilityLogSamplingConfig sets a sampling strategy for the logger. Sampling caps the
// global CPU and I/O load that logging puts on your process while attempting
// to preserve a representative subset of your logs.
//
// If specified, the Sampler will invoke the Hook after each decision.
//
// Values configured here are per-second. See zapcore.NewSamplerWithOptions for
// details.
type ObservabilityLogSamplingConfig struct {
	Enable     bool `yaml:"enable"`
	Initial    int  `yaml:"initial"`
	Thereafter int  `yaml:"thereafter"`
}

type ObservabilityLogConfig struct {
	Enable   bool                            `yaml:"enable"`
	Endpoint string                          `yaml:"endpoint"`
	Sampling *ObservabilityLogSamplingConfig `yaml:"sampling"`
}

func init() {
	logger, err := zap.NewProduction(zap.AddCallerSkip(1))
	if err != nil {
		panic("init zap logger error: " + err.Error())
	}
	observabilityLogger = logger
}

// Observe writes a record to the observability logger, a zap production
// logger with its default sampling.
func Observe(msg string, attributes ...attribute.KeyValue) {
	observabilityLogger.Info(msg,
		zap.Any("Attributes", attribute.NewSet(attributes...).MarshalLog()),
	)
}

func EpochNanosTimeAttribute(k string, t time.Time) attribute.KeyValue {
	return attribute.Int64(k, t.UnixNano())
}

func MillisDurationAttribute(k string, d time.Duration) attribute.KeyValue {
	return attribute.Int64(k, d.Nanoseconds()/1e6)
}
