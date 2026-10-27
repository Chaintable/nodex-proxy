package types

import (
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

// RequestContext is the accompanying data in the pre-processing and post-processing processes,
// and is used to pass the required data during the processing.
type RequestContext struct {
	Host   ProcessorHost   `json:"host"`
	Target ProcessorTarget `json:"target"`
	// Request source metadata
	SourceBiz           string `json:"source_biz"`
	SourceHost          string `json:"source_host"`
	SourceIP            string `json:"source_ip"`
	SourceEnv           string `json:"source_env"`
	SourceServerVersion string `json:"source_server_version"`

	UpstreamRelated bool `json:"-"`

	Start          time.Time `json:"start"`
	Error          error     `json:"-"`
	RawRequestBody []byte    `json:"-"`
	// RequestBody borrows the hertz request body (see jsonrpc.ParseRequest):
	// copy what must outlive the request, and call jsonrpc.OwnRequests before
	// rewriting the body.
	RequestBody      []*jsonrpc.RequestObject `json:"request_body"`
	Method           jsonrpc.RPCMethod        `json:"method"`
	IsBatch          bool                     `json:"is_batch"`
	ResponseBody     *jsonrpc.ResponseObject  `json:"-"`
	RequestBodySize  int                      `json:"request_body_size"`
	ResponseBodySize int                      `json:"response_body_size"`
	BlockContext     *BlockContext            `json:"block_context"`
	blockContextFn   func() *BlockContext
	blockContextDone bool
	BaseChainId      string `json:"base_chain_id"`
	ChainUUID        string `json:"chain_uuid"`
	ChainId          string `json:"chain_id"`
	TargetNodeAddr   string `json:"target_node_addr"`
	Archive          bool   `json:"archive"`
	Native           bool   `json:"native"`

	responseErrorKeyKnown bool
	responseHasErrorKey   bool
}

// CachedResponseContainsErrorKey returns the cached conservative pre-filter for
// the current attempt, computing it from body only on the first call. Only the
// request goroutine may access it. Attempts and response resets invalidate it.
// A positive result still needs unmarshalling to find a top-level error.
func (p *RequestContext) CachedResponseContainsErrorKey(body []byte) bool {
	if p == nil {
		return jsonrpc.ContainsErrorKey(body)
	}
	if !p.responseErrorKeyKnown {
		p.responseHasErrorKey = jsonrpc.ContainsErrorKey(body)
		p.responseErrorKeyKnown = true
	}
	return p.responseHasErrorKey
}

func (p *RequestContext) ResetResponseErrorKey() {
	if p == nil {
		return
	}
	p.responseErrorKeyKnown = false
	p.responseHasErrorKey = false
}

// SetBlockContextFn installs a lazy block-context parser. Parsing the params
// is only needed when a node selector actually consults the block context, so
// the work is deferred until GetBlockContext and memoized.
func (p *RequestContext) SetBlockContextFn(fn func() *BlockContext) {
	if p == nil {
		return
	}
	p.blockContextFn = fn
}

// GetBlockContext returns the block context, invoking the lazy parser at most
// once. A directly assigned BlockContext takes precedence. Not safe for
// concurrent use; callers run on the request goroutine.
func (p *RequestContext) GetBlockContext() *BlockContext {
	if p == nil {
		return nil
	}
	if p.BlockContext == nil && !p.blockContextDone && p.blockContextFn != nil {
		p.BlockContext = p.blockContextFn()
		p.blockContextDone = true
	}
	return p.BlockContext
}

// WriteError writes a proxy-generated JSON-RPC error response. A single
// request gets its own id back, as JSON-RPC requires; otherwise (unparsable
// body, batch) resp keeps the id it was built with.
func (p *RequestContext) WriteError(c *app.RequestContext, status int, resp *jsonrpc.ResponseObject) {
	if p != nil && len(p.RequestBody) == 1 && p.RequestBody[0] != nil && p.RequestBody[0].ID != nil {
		resp.ID = p.RequestBody[0].ID
	}
	c.JSON(status, resp)
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
	// ProcessorFuncHertz runs before or after the upstream call. A
	// pre-processor that writes a response body ends the request there.
	ProcessorFuncHertz func(c *app.RequestContext, processData *RequestContext)

	PreProcessorProcessorsHertz  []ProcessorFuncHertz
	PostProcessorProcessorsHertz []ProcessorFuncHertz
)

func (p PreProcessorProcessorsHertz) Call(c *app.RequestContext, processData *RequestContext) {
	for _, processFunc := range p {
		if processFunc != nil {
			processFunc(c, processData)
		}
		if len(c.Response.Body()) != 0 {
			break
		}
	}
}

func (p PostProcessorProcessorsHertz) Call(c *app.RequestContext, processData *RequestContext) {
	for _, processFunc := range p {
		if processFunc != nil {
			processFunc(c, processData)
		}
	}
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
		attribute.Key("Method").String(string(p.Method)),
	}
	return result
}

func (p *RequestContext) LogField() zap.Field {
	return zap.Any("Attributes", attribute.NewSet(p.LogAttributes()...).MarshalLog())
}
