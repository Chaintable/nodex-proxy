package types

import (
	"context"
	"net/http"
	"reflect"
	"time"

	"github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/bytedance/sonic"
	nJson "github.com/bytedance/sonic"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/ethereum/go-ethereum/rpc"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

func init() {
	_ = sonic.Pretouch(reflect.TypeOf(RequestContext{}))
	_ = sonic.Pretouch(reflect.TypeOf(jsonrpc.RequestObject{}))
	_ = sonic.Pretouch(reflect.TypeOf(jsonrpc.RawResponseObject{}))
}

type (
	ProcessorTarget string
	ProcessorHost   string
)

type BlockContext struct {
	BlockId *rpc.BlockNumberOrHash `json:"block_id"`
	Type    string                 `json:"type"`
}
type jrpcxContextKeyType int

const (
	originHostKey jrpcxContextKeyType = iota

	// DBK HTTP metadata headers
	DBKBiz           = "x-dbk-biz"
	DBKSourceHost    = "x-dbk-source-host"
	DBKSource        = "x-dbk-source"
	DBKEnv           = "x-dbk-env"
	DBKServerVersion = "x-dbk-server-version"
)

// RequestContext is the accompanying data in the pre-processing and post-processing processes,
// and is used to pass the required data during the processing.
type RequestContext struct {
	Context context.Context
	Host    ProcessorHost   `json:"host"`
	Target  ProcessorTarget `json:"target"`
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
	BlockContext     *BlockContext            `json:"block_context"`
	Cached           bool                     `json:"cached"`
	BaseChainId      string                   `json:"base_chain_id"`
	ChainUUID        string                   `json:"chain_uuid"`
	ChainId          string                   `json:"chain_id"`
	TargetNodeAddr   string                   `json:"target_node_addr"`
	Archive          bool                     `json:"archive"`
	Native           bool                     `json:"native"`
}

func (p *RequestContext) SetTargetNode(addr string) {
	if p == nil {
		return
	}
	p.TargetNodeAddr = addr
}

func (p *RequestContext) ClearTargetNode() {
	if p == nil {
		return
	}
	p.TargetNodeAddr = ""
}

type (
	// PreProcessorFunc emphasizes the preprocessing of the **request**,
	// and when the request meets certain conditions, the response is pre-built to achieve the purpose of preprocessing.
	PreProcessorFunc   func(request *http.Request, response *http.Response, processData *RequestContext) (*http.Request, *http.Response, *RequestContext)
	ProcessorFuncHertz func(ctx context.Context, c *app.RequestContext, processData *RequestContext) (context.Context, *app.RequestContext, *RequestContext)
	// PostProcessorFunc emphasizes the **observation and recording of the response**, to achieve specific requirements,
	// and **does not modify the response**.
	PostProcessorFunc func(request *http.Request, response *http.Response, processData *RequestContext) *RequestContext

	PreProcessorProcessors []PreProcessorFunc

	PreProcessorProcessorsHertz  []ProcessorFuncHertz
	PostProcessorProcessors      []PostProcessorFunc
	PostProcessorProcessorsHertz []ProcessorFuncHertz
)

func (p PreProcessorProcessors) Call(request *http.Request, response *http.Response, processData *RequestContext) (*http.Request, *http.Response, *RequestContext) {
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

func (p PreProcessorProcessorsHertz) Call(ctx context.Context, c *app.RequestContext, processData *RequestContext) (context.Context, *app.RequestContext, *RequestContext) {
	for _, processFunc := range p {
		if processFunc != nil {
			ctx, c, processData = processFunc(ctx, c, processData)
		}
		if len(c.Response.Body()) != 0 {
			break
		}
	}
	return ctx, c, processData
}

func (p PostProcessorProcessors) Call(request *http.Request, response *http.Response, processData *RequestContext) *RequestContext {
	for _, processFunc := range p {
		if processFunc != nil {
			processData = processFunc(request, response, processData)
		}
	}
	return processData
}

func (p PostProcessorProcessorsHertz) Call(ctx context.Context, c *app.RequestContext, processData *RequestContext) (context.Context, *app.RequestContext, *RequestContext) {
	for _, processFunc := range p {
		if processFunc != nil {
			ctx, c, processData = processFunc(ctx, c, processData)
		}
	}
	return ctx, c, processData
}

func (p *RequestContext) LogAttributes() []attribute.KeyValue {
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

func (p *RequestContext) LogField() zap.Field {
	return zap.Any("Attributes", attribute.NewSet(p.LogAttributes()...).MarshalLog())
}
