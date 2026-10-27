package lbnode

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/cloudwego/hertz/pkg/app/client"
	rconfig "github.com/cloudwego/hertz/pkg/common/config"
	"github.com/hertz-contrib/reverseproxy"
)

// All node reverse proxies share one hertz client so that upstream connection
// pools live per host (inside the client) instead of per Node object. This
// keeps warm connections across node upserts/rebuilds and gives a single
// place to bound connections towards each RPC backend.
var (
	sharedClientLock sync.Mutex
	sharedClient     *client.Client
)

// DefaultClientOptions returns the connection pool options used when
// InitSharedClient was not called (e.g. in tests).
func DefaultClientOptions() []rconfig.ClientOption {
	return []rconfig.ClientOption{
		client.WithMaxConnsPerHost(2048),
		client.WithMaxIdleConnDuration(90 * time.Second),
		client.WithMaxConnWaitTimeout(time.Second),
		client.WithDialTimeout(3 * time.Second),
		client.WithKeepAlive(true),
	}
}

// InitSharedClient builds the hertz client shared by every node's reverse
// proxy. Nodes created before the call keep the client they were built with,
// so call it once at startup before nodes are discovered.
func InitSharedClient(opts ...rconfig.ClientOption) error {
	c, err := client.NewClient(opts...)
	if err != nil {
		return err
	}
	sharedClientLock.Lock()
	sharedClient = c
	sharedClientLock.Unlock()
	return nil
}

func getSharedClient() (*client.Client, error) {
	sharedClientLock.Lock()
	defer sharedClientLock.Unlock()
	if sharedClient == nil {
		c, err := client.NewClient(DefaultClientOptions()...)
		if err != nil {
			return nil, err
		}
		sharedClient = c
	}
	return sharedClient, nil
}

// Node is shared by every request routed to it. Identity fields (key, address,
// source, NodeType, ReverseProxy) are immutable after construction; the
// mutable load-balancing state is held in atomics so the per-request
// availability scan does not bounce a lock's cache line between cores.
type Node struct {
	key    string
	ip     string
	port   int
	addr   string
	source string

	weight        atomic.Int64
	currentWeight atomic.Int64
	stateType     atomic.Int64

	NodeType discovery.NodeType

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

func New(key, ip string, port, weight int, nodeType discovery.NodeType, opts ...Option) (*Node, error) {
	if weight <= 0 {
		weight = 1
	}
	node := &Node{
		key:      key,
		ip:       ip,
		port:     port,
		addr:     ip + ":" + strconv.Itoa(port),
		NodeType: nodeType,
	}
	node.weight.Store(int64(weight))
	node.currentWeight.Store(int64(weight))
	for _, opt := range opts {
		opt(node)
	}

	proxy, err := reverseproxy.NewSingleHostReverseProxy("http://" + node.Addr())
	if err != nil {
		return node, err
	}
	cli, err := getSharedClient()
	if err != nil {
		return node, err
	}
	proxy.SetClient(cli)
	proxy.SetErrorHandler(upstreamErrorHandler)
	node.ReverseProxy = proxy
	return node, nil
}

func (node *Node) Key() string {
	return node.key
}

func (node *Node) Weight() int {
	return int(node.weight.Load())
}

// EffectWeight returns effect weight
func (node *Node) EffectWeight() int {
	return int(node.weight.Load())
}

func (node *Node) SetState(state int) {
	node.stateType.Store(int64(state))
}

// CurrentWeight returns currentWeight
func (node *Node) CurrentWeight() int {
	return int(node.currentWeight.Load())
}

// IncrCurrentWeight increment currentWeight
func (node *Node) IncrCurrentWeight(n int) int {
	return int(node.currentWeight.Add(int64(n)))
}

// IP returns ip
func (node *Node) IP() string {
	return node.ip
}

// Port returns port
func (node *Node) Port() int {
	return node.port
}

// Addr returns <ip>:<port>, computed once at construction.
func (node *Node) Addr() string {
	return node.addr
}

func (node *Node) Source() string {
	return node.source
}

func (node *Node) State() int {
	return int(node.stateType.Load())
}

// StateAvailable is the discovery state of a node that serves traffic
// (1=latest; 2=delay and 3=offline do not).
const StateAvailable = 1

func (node *Node) Available() bool {
	return node.stateType.Load() == StateAvailable
}

// UpdateFrom refreshes the mutable attributes of node from other, keeping the
// existing ReverseProxy and load-balancing state so that warm upstream
// connections are not discarded. source and NodeType are immutable after a
// node is published; nodes whose identity changed must be replaced instead.
func (node *Node) UpdateFrom(other *Node) {
	node.weight.Store(other.weight.Load())
	node.stateType.Store(other.stateType.Load())
}

// UpsertInto merges node into nodes. A node with the same identity (key,
// address, source, type) is updated in place so its ReverseProxy and warm
// connection pool survive; a changed identity replaces the node, and a new
// key is appended.
func UpsertInto(nodes []*Node, node *Node) []*Node {
	for i, existing := range nodes {
		if existing.Key() == node.Key() {
			if existing.Addr() == node.Addr() &&
				existing.Source() == node.Source() &&
				existing.NodeType == node.NodeType {
				existing.UpdateFrom(node)
			} else {
				nodes[i] = node
			}
			return nodes
		}
	}
	return append(nodes, node)
}

func (node *Node) Clone() *Node {
	n := &Node{
		key:          node.key,
		ip:           node.ip,
		port:         node.port,
		addr:         node.addr,
		source:       node.source,
		NodeType:     node.NodeType,
		ReverseProxy: node.ReverseProxy,
	}
	n.weight.Store(node.weight.Load())
	n.currentWeight.Store(node.currentWeight.Load())
	n.stateType.Store(node.stateType.Load())
	return n
}
