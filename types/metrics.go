package types

import (
	"fmt"
	"reflect"

	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/prometheus"
	stdprome "github.com/prometheus/client_golang/prometheus"
)

const (
	ConfigNameLabel  = "config_name"
	ConfigValueLabel = "config_value"
)

var configGauge metrics.Gauge

func init() {
	configGauge = prometheus.NewGaugeFrom(stdprome.GaugeOpts{
		Namespace: "jrpcx",
		Subsystem: "pkg",
		Name:      "configuration",
		Help:      "Current JRPCX configuration",
	}, []string{ConfigNameLabel, ConfigValueLabel})
}

func initMetrics(t reflect.Type, v reflect.Value, prefix string) {
	switch t.Kind() {
	case reflect.Ptr:
		initMetrics(t.Elem(), v.Elem(), prefix)
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			sf := t.Field(i)
			switch v.Kind() {
			case reflect.Invalid:
				initMetrics(sf.Type, v, prefix+"."+sf.Name)
			default:
				initMetrics(sf.Type, v.Field(i), prefix+"."+sf.Name)
			}
		}
	default:
		configGauge.With(
			ConfigNameLabel, prefix,
			ConfigValueLabel, fmt.Sprint(v),
		).Set(1)
	}

}

func (c Config) initMetrics() {
	initMetrics(reflect.TypeOf(c), reflect.ValueOf(c), "")
}
