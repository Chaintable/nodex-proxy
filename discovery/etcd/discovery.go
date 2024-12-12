package etcd

import (
	"context"
	"encoding/json"
	"log"
	"regexp"
	"time"

	"github.com/Chaintable/nodex-proxy/discovery"
	"go.etcd.io/etcd/client/v3"
)

type Discover struct {
	etcdClient  *clientv3.Client
	watchCancel context.CancelFunc

	quit chan struct{}

	backends      []string
	nodeChannel   chan *discovery.TargetNode
	heightChan    chan *discovery.ChainHeight
	keyPrefix     string
	watchRevision int64
}

const (
	addNode = 0 + iota
	delNode
)

var (
	lastBlockPattern = regexp.MustCompile(`^(?P<chain>.*?)/lastBlockNumber$`)
	nodesPattern     = regexp.MustCompile(`^(?P<chain>.*?)/nodes/(?P<node>.*?)$`)
)

func New(ctx context.Context, etcdEndpoints []string, keyPrefix string) (*Discover, error) {
	log.Printf("Init Discover etcd endpoints: %v", etcdEndpoints)
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("connectting etcd failed: %v\n", err)
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

func (r *Discover) Close() error {
	close(r.quit)
	r.watchCancel()
	return r.etcdClient.Close()
}

func (r *Discover) Init(ctx context.Context) (<-chan *discovery.TargetNode, <-chan *discovery.ChainHeight, error) {
	// Initial request to get the current value of the key

	resp, err := r.etcdClient.Get(ctx, r.keyPrefix, clientv3.WithPrefix())
	log.Printf("get key resp: %+v, key: %s ", resp, r.keyPrefix)
	if err != nil {
		log.Printf("failed to get initial value for: %+v", err)
		return nil, nil, err
	}
	nodeChannel := make(chan *discovery.TargetNode, 1000)
	heightChannel := make(chan *discovery.ChainHeight, 1000)
	r.nodeChannel = nodeChannel
	r.heightChan = heightChannel
	for _, kv := range resp.Kvs {
		if match := nodesPattern.FindStringSubmatch(string(kv.Key)); match != nil {
			chainId := match[nodesPattern.SubexpIndex("chain")]
			nodeKey := match[nodesPattern.SubexpIndex("node")]
			var node discovery.TargetNode
			err := json.Unmarshal(kv.Value, &node)
			if err != nil {
				log.Printf("failed to unmarshal value for key %s: %+v, chain id: %v", kv.Key, err, chainId)
				continue
			}
			node.NodeKey = nodeKey
			node.ChangeType = addNode
			node.ChainId = chainId
			nodeChannel <- &node
			continue
		}
		if match := lastBlockPattern.FindStringSubmatch(string(kv.Key)); match != nil {
			chainId := match[lastBlockPattern.SubexpIndex("chain")]
			var height discovery.ChainHeight
			err := json.Unmarshal(kv.Value, &height)
			if err != nil {
				log.Printf("failed to unmarshal value for key %s: %+v, chain id: %v", kv.Key, err, chainId)
				continue
			}
			height.ChainId = chainId
			heightChannel <- &height
			continue
		}
	}
	r.watchRevision = resp.Header.Revision
	go r.watchConfig(ctx)

	return nodeChannel, heightChannel, nil

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
							log.Printf("failed to unmarshal value for key %s: %+v, chain id: %v", key, err, chainId)
							continue
						}
						node.NodeKey = nodeKey
						node.ChangeType = addNode
						node.ChainId = chainId
						r.nodeChannel <- &node
						continue
					}
					if match := lastBlockPattern.FindStringSubmatch(string(key)); match != nil {
						chainId := match[lastBlockPattern.SubexpIndex("chain")]
						var height discovery.ChainHeight
						err := json.Unmarshal(value, &height)
						if err != nil {
							log.Printf("failed to unmarshal value for key %s: %+v, chain id: %v", key, err, chainId)
							continue
						}
						height.ChainId = chainId
						r.heightChan <- &height
						continue
					}
				} else if event.Type == clientv3.EventTypeDelete {
					key := event.Kv.Key
					value := event.PrevKv.Value
					if match := nodesPattern.FindStringSubmatch(string(key)); match != nil {
						chainId := match[nodesPattern.SubexpIndex("chain")]
						nodeKey := match[nodesPattern.SubexpIndex("node")]
						var node discovery.TargetNode
						err := json.Unmarshal(value, &node)
						if err != nil {
							log.Printf("failed to unmarshal value for key %s: %+v, chain id: %v", key, err, chainId)
							continue
						}
						node.NodeKey = nodeKey
						node.ChangeType = delNode
						node.ChainId = chainId
						r.nodeChannel <- &node
					}
				}
			}
		}
	}
}
