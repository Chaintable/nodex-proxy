package types

import (
	"net/http"
	"time"

	"github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lib/log"
	nJson "github.com/goccy/go-json"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

type (
	ProcessorTarget string
	ProcessorHost   string
)

// ProcessorData is the accompanying data in the pre-processing and post-processing processes,
// and is used to pass the required data during the processing.
type ProcessorData struct {
	Host   ProcessorHost   `json:"host"`
	Target ProcessorTarget `json:"target"`
	// Request source metadata
	SourceBiz           string `json:"source_biz"`
	SourceHost          string `json:"source_host"`
	SourceIP            string `json:"source_ip"`
	SourceEnv           string `json:"source_env"`
	SourceServerVersion string `json:"source_server_version"`

	UpstreamRelated bool `json:"-"`

	RequestId        string                   `json:"request_id"`
	Start            time.Time                `json:"start"`
	Error            error                    `json:"-"`
	RawRequestBody   []byte                   `json:"-"`
	RequestBody      []*jsonrpc.RequestObject `json:"request_body"`
	Method           jsonrpc.RPCMethod        `json:"method"`
	IsBatch          bool                     `json:"is_batch"`
	ResponseBody     *jsonrpc.ResponseObject  `json:"-"`
	RequestBodySize  int                      `json:"request_body_size"`
	ResponseBodySize int                      `json:"response_body_size"`
	Cached           bool                     `json:"cached"`
}

type (
	// PreProcessorFunc emphasizes the preprocessing of the **request**,
	// and when the request meets certain conditions, the response is pre-built to achieve the purpose of preprocessing.
	PreProcessorFunc func(request *http.Request, response *http.Response, processData *ProcessorData) (*http.Request, *http.Response, *ProcessorData)
	// PostProcessorFunc emphasizes the **observation and recording of the response**, to achieve specific requirements,
	// and **does not modify the response**.
	PostProcessorFunc       func(request *http.Request, response *http.Response, processData *ProcessorData) *ProcessorData
	PreProcessorProcessors  []PreProcessorFunc
	PostProcessorProcessors []PostProcessorFunc
)

func (p PreProcessorProcessors) Call(request *http.Request, response *http.Response, processData *ProcessorData) (*http.Request, *http.Response, *ProcessorData) {
	for _, processFunc := range p {
		if processFunc != nil {
			request, response, processData = processFunc(request, response, processData)
		}
		if response != nil {
			break
		}
	}
	return request, response, processData
}

func (p PostProcessorProcessors) Call(request *http.Request, response *http.Response, processData *ProcessorData) *ProcessorData {
	for _, processFunc := range p {
		if processFunc != nil {
			processData = processFunc(request, response, processData)
		}
	}
	return processData
}

func (p *ProcessorData) LogAttributes() []attribute.KeyValue {
	requestBodyStr, _ := nJson.Marshal(p.RequestBody)
	result := []attribute.KeyValue{
		log.EpochNanosTimeAttribute("Start", p.Start),
		attribute.Key("RequestBody").String(string(requestBodyStr)),
		attribute.Key("Host").String(string(p.Host)),
		attribute.Key("Target").String(string(p.Target)),
		attribute.Key("SourceBiz").String(p.SourceBiz),
		attribute.Key("SourceHost").String(p.SourceHost),
		attribute.Key("SourceIP").String(p.SourceIP),
		attribute.Key("SourceEnv").String(p.SourceEnv),
		attribute.Key("SourceServerVersion").String(p.SourceServerVersion),
		attribute.Key("RequestBodySize").Int(p.RequestBodySize),
		attribute.Key("ResponseBodySize").Int(p.ResponseBodySize),
		attribute.Key("Cached").Bool(p.Cached),
		attribute.Key("Method").String(string(p.Method)),
	}
	return result
}

func (p *ProcessorData) LogField() zap.Field {
	return zap.Any("Attributes", attribute.NewSet(p.LogAttributes()...).MarshalLog())
}
