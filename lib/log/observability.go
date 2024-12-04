package log

import (
	"context"
	"os"
	"time"

	"go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/otel/attribute"

	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
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

func ObservabilityLogger() *zap.Logger {
	return observabilityLogger.WithOptions(zap.AddCallerSkip(-1))
}

func BuildObservabilityLogger(logConfig ObservabilityLogConfig, serviceName string, staticAttributes map[string]string) {
	if !logConfig.Enable {
		return
	}
	config := zap.NewProductionConfig()
	if logConfig.Endpoint != "" {
		config.OutputPaths = append(config.OutputPaths, logConfig.Endpoint)
	}
	if logConfig.Sampling != nil && logConfig.Sampling.Enable {
		config.Sampling = &zap.SamplingConfig{
			Initial:    logConfig.Sampling.Initial,
			Thereafter: logConfig.Sampling.Thereafter,
		}
	} else {
		config.Sampling = nil
	}
	BuildObservabilityLoggerWithConfig(config, serviceName, staticAttributes)
}

func BuildObservabilityLoggerWithConfig(config zap.Config, serviceName string, staticResource map[string]string) {
	resource := map[string]string{
		string(semconv.ServiceNameKey): serviceName,
	}

	for k, v := range staticResource {
		resource[k] = v
	}
	// k8s
	// https://opentelemetry.io/docs/reference/specification/resource/semantic_conventions/k8s/
	if envValue := os.Getenv("K8S_CLUSTER_NAME"); envValue != "" {
		resource[string(semconv.K8SClusterNameKey)] = envValue
	}
	if envValue := os.Getenv("K8S_POD_NAMESPACE"); envValue != "" {
		resource[string(semconv.K8SNamespaceNameKey)] = envValue
	}
	if envValue := os.Getenv("K8S_POD_NAME"); envValue != "" {
		resource[string(semconv.K8SPodNameKey)] = envValue
	}
	if envValue := os.Getenv("K8S_POD_UID"); envValue != "" {
		resource[string(semconv.K8SPodUIDKey)] = envValue
	}
	zapLogger, err := config.Build(
		zap.AddCallerSkip(1),
		zap.Fields(Any("Resource", resource)),
	)
	if err != nil {
		panic("init zap logger: " + err.Error())
	}
	observabilityLogger = zapLogger
}

func Observe(msg string, context context.Context, attributes ...attribute.KeyValue) {
	span := trace.SpanFromContext(context)
	traceID := span.SpanContext().TraceID().String()
	observabilityLogger.Info(msg,
		zap.Any("TraceId", traceID),
		zap.Any("Attributes", attribute.NewSet(attributes...).MarshalLog()),
	)
}

func ObserveError(msg string, err error, context context.Context, attributes ...attribute.KeyValue) {
	Observe(msg, context, attribute.String("error", err.Error()))
}

func EpochNanosTimeAttribute(k string, t time.Time) attribute.KeyValue {
	return attribute.Int64(k, t.UnixNano())
}

func MillisDurationAttribute(k string, d time.Duration) attribute.KeyValue {
	return attribute.Int64(k, d.Nanoseconds()/1e6)
}
