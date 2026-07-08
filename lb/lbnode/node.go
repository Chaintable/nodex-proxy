package lbnode

import (
	"fmt"
	"sync"
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

	proxy, err := reverseproxy.NewSingleHostReverseProxy(fmt.Sprintf("http://%s", node.Addr()))
	if err != nil {
		return node, err
	}
	cli, err := getSharedClient()
	if err != nil {
		return node, err
	}
	proxy.SetClient(cli)
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

// UpdateFrom refreshes the mutable attributes of node from other, keeping the
// existing ReverseProxy and load-balancing state so that warm upstream
// connections are not discarded. source and NodeType are immutable after a
// node is published (they are read without holding node.lock); nodes whose
// identity changed must be replaced instead.
func (node *Node) UpdateFrom(other *Node) {
	weight := other.Weight()
	state := other.State()

	node.lock.Lock()
	defer node.lock.Unlock()
	node.weight = weight
	node.stateType = state
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
