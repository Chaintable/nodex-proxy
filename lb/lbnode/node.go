package lbnode

import (
	"fmt"
	"sync"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/cloudwego/hertz/pkg/app"
	rconfig "github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/hertz-contrib/reverseproxy"
)

type Node struct {
	key    string
	ip     string
	port   int
	weight int

	conns         int64
	currentWeight int
	stateType     int
	source        string

	NodeType discovery.NodeType

	lock         sync.RWMutex
	ReverseProxy *reverseproxy.ReverseProxy
}
type Option func(*Node)

func WithSource(source string) Option {
	return func(node *Node) {
		if source == "" {
			source = "official"
		}
		node.source = source
	}
}

func WithReverseProxyMaxConnsPerHost(maxConnsPerHost int) func(o *rconfig.ClientOptions) {
	return func(o *rconfig.ClientOptions) {
		o.MaxConnsPerHost = maxConnsPerHost
	}
}

func New(key, ip string, port, weight int, nodeType discovery.NodeType, opts ...Option) (*Node, error) {
	if weight <= 0 {
		weight = 1
	}
	node := &Node{
		key:           key,
		ip:            ip,
		port:          port,
		weight:        weight,
		currentWeight: weight,
		NodeType:      nodeType,
	}
	for _, opt := range opts {
		opt(node)
	}

	proxy, err := reverseproxy.NewSingleHostReverseProxy(fmt.Sprintf("http://%s", node.Addr()), rconfig.ClientOption{F: WithReverseProxyMaxConnsPerHost(2048)})
	if err != nil {
		return node, err
	}
	proxy.SetErrorHandler(func(c *app.RequestContext, err error) {
		log.Error("reverse proxy transport error", err,
			log.Any("node_addr", node.Addr()),
			log.Any("node_key", node.key),
			log.Any("node_type", node.NodeType),
			log.Any("request_method", string(c.Request.Method())),
			log.Any("request_uri", string(c.Request.URI().FullURI())),
			log.Any("request_host", string(c.Request.Host())),
			log.Any("accept_encoding", c.Request.Header.Get("Accept-Encoding")),
		)
		c.Response.Header.SetStatusCode(consts.StatusBadGateway)
	})
	node.ReverseProxy = proxy
	return node, nil
}

func (node *Node) Key() string {
	return node.key
}

func (node *Node) Weight() int {
	node.lock.RLock()
	defer node.lock.RUnlock()

	return node.weight
}

// EffectWeight returns effect weight
func (node *Node) EffectWeight() int {
	node.lock.RLock()
	defer node.lock.RUnlock()

	return node.effectWeight()
}

func (node *Node) effectWeight() int {
	return int(int64(node.weight))
}

// SetWeight set weight
func (node *Node) SetWeight(weight int) {
	node.lock.Lock()
	defer node.lock.Unlock()

	node.weight = weight
}

func (node *Node) SetState(state int) {
	node.lock.Lock()
	defer node.lock.Unlock()

	node.stateType = state
}

// CurrentWeight returns currentWeight
func (node *Node) CurrentWeight() int {
	node.lock.RLock()
	defer node.lock.RUnlock()

	return node.currentWeight
}

// IncrCurrentWeight increment currentWeight
func (node *Node) IncrCurrentWeight(n int) int {
	node.lock.Lock()
	defer node.lock.Unlock()

	node.currentWeight += n
	return node.currentWeight
}

// IP returns ip
func (node *Node) IP() string {
	return node.ip
}

// Port returns port
func (node *Node) Port() int {
	return node.port
}

// Addr returns <ip>:<port>
func (node *Node) Addr() string {
	return fmt.Sprintf("%s:%d", node.IP(), node.Port())
}

func (node *Node) Source() string {
	return node.source
}

// Conns returns conns
func (node *Node) Conns() int64 {
	node.lock.RLock()
	defer node.lock.RUnlock()

	return node.conns
}

// IncrConns increment connection number
func (node *Node) IncrConns(n int64) int64 {
	node.lock.Lock()
	defer node.lock.Unlock()

	node.conns += n
	if node.conns < 0 {
		node.conns = 0
	}
	return node.conns
}

func (node *Node) State() int {
	node.lock.RLock()
	defer node.lock.RUnlock()

	return node.stateType
}

func (node *Node) Available() bool {
	node.lock.RLock()
	defer node.lock.RUnlock()

	return node.stateType == 1
}

func (node *Node) Clone() *Node {
	node.lock.RLock()
	defer node.lock.RUnlock()

	n := Node{
		key:           node.key,
		ip:            node.ip,
		port:          node.port,
		weight:        node.weight,
		source:        node.source,
		conns:         node.conns,
		currentWeight: node.currentWeight,
		lock:          sync.RWMutex{},
		stateType:     node.stateType,
		NodeType:      node.NodeType,
		ReverseProxy:  node.ReverseProxy,
	}
	return &n
}
