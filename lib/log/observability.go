package log

import (
	"time"

	"go.opentelemetry.io/otel/attribute"

	"go.uber.org/zap"
)

var (
	observabilityLogger *zap.Logger
)

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
