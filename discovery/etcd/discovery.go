package etcd

import (
	"context"
	"regexp"
	"time"

	json "github.com/bytedance/sonic"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/lib/log"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Discover struct {
	etcdClient  *clientv3.Client
	watchCancel context.CancelFunc

	quit chan struct{}

	backends       []string
	nodeChannel    chan *discovery.TargetNode
	heightChan     chan *discovery.ChainHeight
	gatewayChannel chan *discovery.Gateway
	keyPrefix      string
	watchRevision  int64
}

const (
	EVENT_PUT = 0 + iota
	EVENT_DELETE
)

var (
	lastBlockPattern = regexp.MustCompile(`^(?P<chain>.*?)/lastBlockNumber$`)
	nodesPattern     = regexp.MustCompile(`^(?P<chain>.*?)/nodes/(?P<node>.*?)$`)
	gateWayPattern   = regexp.MustCompile(`^(?P<chain>.*?)/gateway$`)
)

func New(ctx context.Context, etcdEndpoints []string, keyPrefix string) (*Discover, error) {
	log.Info("Init Discover etcd endpoints", log.Any("endpoints", etcdEndpoints))
	etcdCli, err := NewEtcdClient(ctx, etcdEndpoints)
	if err != nil {
		log.Fatal("connecting etcd failed", err)
	}
	ctx, cancel := context.WithCancel(context.Background())

	refresher := &Discover{
		etcdClient:  etcdCli,
		watchCancel: cancel,
		quit:        make(chan struct{}),
		keyPrefix:   keyPrefix,
	}

	return refresher, err
}

func NewEtcdClient(ctx context.Context, etcdEndpoints []string) (*clientv3.Client, error) {
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	return etcdCli, err
}

func (r *Discover) Close() error {
	close(r.quit)
	r.watchCancel()
	return r.etcdClient.Close()
}

func (r *Discover) Init(ctx context.Context) (chan *discovery.TargetNode, <-chan *discovery.ChainHeight, <-chan *discovery.Gateway, error) {
	// Initial request to get the current value of the key

	resp, err := r.etcdClient.Get(ctx, r.keyPrefix, clientv3.WithPrefix())
	log.Info("get key resp", log.Any("resp", resp), log.Any("key", r.keyPrefix))
	if err != nil {
		log.Error("failed to get initial value", err)
		return nil, nil, nil, err
	}
	// todo: fix channel potential stuck issue
	nodeChannel := make(chan *discovery.TargetNode, 1000)
	heightChannel := make(chan *discovery.ChainHeight, 1000)
	gatewayChannel := make(chan *discovery.Gateway, 1000)
	r.nodeChannel = nodeChannel
	r.heightChan = heightChannel
	r.gatewayChannel = gatewayChannel

	for _, kv := range resp.Kvs {
		if match := nodesPattern.FindStringSubmatch(string(kv.Key)); match != nil {
			chainId := match[nodesPattern.SubexpIndex("chain")]
			nodeKey := match[nodesPattern.SubexpIndex("node")]
			var node discovery.TargetNode
			err := json.Unmarshal(kv.Value, &node)
			if err != nil {
				log.Error("failed to unmarshal value for key", err, log.Any("key", kv.Key), log.Any("chain_id", chainId))
				continue
			}
			node.NodeKey = nodeKey
			node.ChangeType = EVENT_PUT
			node.ChainId = chainId
			nodeChannel <- &node
			continue
		}
		if match := lastBlockPattern.FindStringSubmatch(string(kv.Key)); match != nil {
			chainId := match[lastBlockPattern.SubexpIndex("chain")]
			var height discovery.ChainHeight
			err := json.Unmarshal(kv.Value, &height)
			if err != nil {
				log.Error("failed to unmarshal value for key", err, log.Any("key", kv.Key), log.Any("chain_id", chainId))
				continue
			}
			height.ChainId = chainId
			heightChannel <- &height
			continue
		}
		if match := gateWayPattern.FindStringSubmatch(string(kv.Key)); match != nil {
			chainId := match[gateWayPattern.SubexpIndex("chain")]
			var gateway discovery.Gateway
			err := json.Unmarshal(kv.Value, &gateway)
			if err != nil {
				log.Error("failed to unmarshal value for key", err, log.Any("key", kv.Key), log.Any("chain_id", chainId))
				continue
			}
			gateway.ChainId = chainId
			gateway.ChangeType = EVENT_PUT
			gatewayChannel <- &gateway
		}
	}
	r.watchRevision = resp.Header.Revision
	go r.watchConfig(ctx)

	return nodeChannel, heightChannel, gatewayChannel, nil
}

func (r *Discover) watchConfig(ctx context.Context) {
	watchChan := r.etcdClient.Watch(ctx, r.keyPrefix, clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithRev(r.watchRevision))
	for {
		select {
		case <-r.quit:
			return
		case watchResp := <-watchChan:
			for _, event := range watchResp.Events {
				if event.Type == clientv3.EventTypePut {
					key := event.Kv.Key
					value := event.Kv.Value
					if match := nodesPattern.FindStringSubmatch(string(key)); match != nil {
						chainId := match[nodesPattern.SubexpIndex("chain")]
						nodeKey := match[nodesPattern.SubexpIndex("node")]
						var node discovery.TargetNode
						err := json.Unmarshal(value, &node)
						if err != nil {
							log.Error("failed to unmarshal value for key", err, log.Any("key", key), log.Any("chain_id", chainId))
							continue
						}
						node.NodeKey = nodeKey
						node.ChangeType = EVENT_PUT
						node.ChainId = chainId
						r.nodeChannel <- &node
						continue
					}
					if match := lastBlockPattern.FindStringSubmatch(string(key)); match != nil {
						chainId := match[lastBlockPattern.SubexpIndex("chain")]
						var height discovery.ChainHeight
						err := json.Unmarshal(value, &height)
						if err != nil {
							log.Error("failed to unmarshal value for key", err, log.Any("key", key), log.Any("chain_id", chainId))
							continue
						}
						height.ChainId = chainId
						r.heightChan <- &height
						continue
					}
					if match := gateWayPattern.FindStringSubmatch(string(key)); match != nil {
						chainId := match[gateWayPattern.SubexpIndex("chain")]
						var gateway discovery.Gateway
						err := json.Unmarshal(value, &gateway)
						if err != nil {
							log.Error("failed to unmarshal value for key", err, log.Any("key", key), log.Any("chain_id", chainId))
							continue
						}
						gateway.ChainId = chainId
						gateway.ChangeType = EVENT_PUT
						r.gatewayChannel <- &gateway
						continue
					}
				} else {
					key := event.Kv.Key
					value := event.PrevKv.Value
					if match := nodesPattern.FindStringSubmatch(string(key)); match != nil {
						chainId := match[nodesPattern.SubexpIndex("chain")]
						nodeKey := match[nodesPattern.SubexpIndex("node")]
						var node discovery.TargetNode
						err := json.Unmarshal(value, &node)
						if err != nil {
							log.Error("failed to unmarshal value for key", err, log.Any("key", key), log.Any("chain_id", chainId))
							continue
						}
						node.NodeKey = nodeKey
						node.ChangeType = EVENT_DELETE
						node.ChainId = chainId
						r.nodeChannel <- &node
						continue
					}
					if match := gateWayPattern.FindStringSubmatch(string(key)); match != nil {
						chainId := match[gateWayPattern.SubexpIndex("chain")]
						var gateway discovery.Gateway
						err := json.Unmarshal(value, &gateway)
						if err != nil {
							log.Error("failed to unmarshal value for key", err, log.Any("key", key), log.Any("chain_id", chainId))
							continue
						}
						gateway.ChainId = chainId
						gateway.ChangeType = EVENT_DELETE
						r.gatewayChannel <- &gateway
						continue
					}
				}
			}
		}
	}
}
