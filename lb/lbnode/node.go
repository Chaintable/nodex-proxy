package lbnode

import (
	"fmt"
	"github.com/hertz-contrib/reverseproxy"
	"sync"
)

type Node struct {
	key         string
	ip          string
	port        int
	weight      int
	shardingKey int

	conns           int64
	currentWeight   int
	currentHandling int64
	stateType       int
	lock            sync.RWMutex
	ReverseProxy    *reverseproxy.ReverseProxy
}
type Option func(*Node)

func New(key, ip string, port, weight int, opts ...Option) (*Node, error) {
	if weight <= 0 {
		weight = 1
	}
	node := &Node{
		key:           key,
		ip:            ip,
		port:          port,
		weight:        weight,
		currentWeight: weight,
	}
	for _, opt := range opts {
		opt(node)
	}

	proxy, err := reverseproxy.NewSingleHostReverseProxy(fmt.Sprintf("http://%s", node.Addr()))
	if err != nil {
		return node, err
	}
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

func (node *Node) Available() bool {

	node.lock.RLock()
	defer node.lock.RUnlock()

	return node.stateType == 1
}

func (node *Node) Clone() *Node {
	node.lock.RLock()
	defer node.lock.RUnlock()

	n := Node{
		key:             node.key,
		ip:              node.ip,
		port:            node.port,
		weight:          node.weight,
		shardingKey:     node.shardingKey,
		conns:           node.conns,
		currentWeight:   node.currentWeight,
		lock:            sync.RWMutex{},
		currentHandling: node.currentHandling,
		stateType:       node.stateType,
		ReverseProxy:    node.ReverseProxy,
	}
	return &n
}
