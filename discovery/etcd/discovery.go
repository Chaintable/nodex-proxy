package etcd

import (
	"context"
	"fmt"
	"regexp"
	"strings"
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

	nodeChannel    chan *discovery.TargetNode
	heightChan     chan *discovery.ChainHeight
	gatewayChannel chan *discovery.Gateway
	mirrorChannel  chan *discovery.MirrorTarget
	versionChannel chan *discovery.ChainVersion
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
	nativeNodesPattern = regexp.MustCompile(`^(?P<chain>.*?)/nativeNodes/(?P<node>.*?)$`)
	gateWayPattern   = regexp.MustCompile(`^(?P<chain>.*?)/gateway$`)
	mirrorPattern    = regexp.MustCompile(`^(?P<chain>.*?)/mirror/(?P<addr>.*?)$`)
	versionPattern   = regexp.MustCompile(`^(?P<chain>.*?)/version$`)
)

func New(ctx context.Context, etcdEndpoints []string, keyPrefix string) (*Discover, error) {
	log.Info("Init Discover etcd endpoints", log.Any("endpoints", etcdEndpoints))
	etcdCli, err := NewEtcdClient(ctx, etcdEndpoints)
	if err != nil {
		log.Fatal("connecting etcd failed", err)
	}
	_, cancel := context.WithCancel(context.Background())

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

func (r *Discover) Init(ctx context.Context) (chan *discovery.TargetNode, <-chan *discovery.ChainHeight, <-chan *discovery.Gateway, <-chan *discovery.MirrorTarget, <-chan *discovery.ChainVersion, error) {
	// Initial request to get the current value of the key

	resp, err := r.etcdClient.Get(ctx, r.keyPrefix, clientv3.WithPrefix())
	log.Info("get key resp", log.Any("resp", resp), log.Any("key", r.keyPrefix))
	if err != nil {
		log.Error("failed to get initial value", err)
		return nil, nil, nil, nil, nil, err
	}
	nodeChannel := make(chan *discovery.TargetNode, 1000)
	heightChannel := make(chan *discovery.ChainHeight, 1000)
	gatewayChannel := make(chan *discovery.Gateway, 1000)
	mirrorChannel := make(chan *discovery.MirrorTarget, 1000)
	versionChannel := make(chan *discovery.ChainVersion, 1000)
	r.nodeChannel = nodeChannel
	r.heightChan = heightChannel
	r.gatewayChannel = gatewayChannel
	r.mirrorChannel = mirrorChannel
	r.versionChannel = versionChannel

	for _, kv := range resp.Kvs {
		if match := nodesPattern.FindStringSubmatch(string(kv.Key)); match != nil {
			chainId := normalizeMultiVersionChainID(match[nodesPattern.SubexpIndex("chain")])
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
		if match := nativeNodesPattern.FindStringSubmatch(string(kv.Key)); match != nil {
			chainId := normalizeMultiVersionChainID(match[nativeNodesPattern.SubexpIndex("chain")])
			nodeKey := match[nativeNodesPattern.SubexpIndex("node")]
			var node discovery.TargetNode
			err := json.Unmarshal(kv.Value, &node)
			if err != nil {
				log.Error("failed to unmarshal value for key", err, log.Any("key", kv.Key), log.Any("chain_id", chainId))
				continue
			}
			node.NodeKey = nodeKey
			node.ChangeType = EVENT_PUT
			node.ChainId = chainId
			node.Source = "native"
			nodeChannel <- &node
			continue
		}
		if match := lastBlockPattern.FindStringSubmatch(string(kv.Key)); match != nil {
			chainId := normalizeMultiVersionChainID(match[lastBlockPattern.SubexpIndex("chain")])
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
			chainId := normalizeMultiVersionChainID(match[gateWayPattern.SubexpIndex("chain")])
			var gateway discovery.Gateway
			err := json.Unmarshal(kv.Value, &gateway)
			if err != nil {
				log.Error("failed to unmarshal value for key", err, log.Any("key", kv.Key), log.Any("chain_id", chainId))
				continue
			}
			gateway.ChainId = chainId
			gateway.ChangeType = EVENT_PUT
			gatewayChannel <- &gateway
			continue
		}
		if match := mirrorPattern.FindStringSubmatch(string(kv.Key)); match != nil {
			chainId := normalizeMultiVersionChainID(match[mirrorPattern.SubexpIndex("chain")])
			addrKey := match[mirrorPattern.SubexpIndex("addr")]
			var mirror discovery.MirrorTarget
			err := json.Unmarshal(kv.Value, &mirror)
			if err != nil {
				log.Error("failed to unmarshal mirror target", err, log.Any("key", kv.Key), log.Any("chain_id", chainId))
				continue
			}
			mirror.ChainId = chainId
			mirror.AddrKey = addrKey
			mirrorChannel <- &mirror
			continue
		}
		if match := versionPattern.FindStringSubmatch(string(kv.Key)); match != nil {
			chainId := normalizeMultiVersionChainID(match[versionPattern.SubexpIndex("chain")])
			versionValue := parseVersionValue(kv.Value)
			versionChannel <- &discovery.ChainVersion{
				ChainId:    chainId,
				Version:    versionValue,
				ChangeType: EVENT_PUT,
			}
			continue
		}
	}
	r.watchRevision = resp.Header.Revision
	go r.watchConfig(ctx)

	return nodeChannel, heightChannel, gatewayChannel, mirrorChannel, versionChannel, nil
}

func (r *Discover) watchConfig(ctx context.Context) {
	for {
		watchChan := r.etcdClient.Watch(ctx, r.keyPrefix, clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithRev(r.watchRevision))
		if err := r.processWatchEvents(ctx, watchChan); err != nil {
			log.Error("watch channel error, will retry", err)
			time.Sleep(time.Second) // 重连前等待1秒
			// 重新获取最新 revision
			resp, err := r.etcdClient.Get(ctx, r.keyPrefix, clientv3.WithPrefix())
			if err != nil {
				log.Error("failed to get latest revision for watch retry", err)
				continue
			}
			r.watchRevision = resp.Header.Revision
			continue
		}
		return // quit 信号触发时退出
	}
}

func (r *Discover) processWatchEvents(ctx context.Context, watchChan clientv3.WatchChan) error {
	for {
		select {
		case <-r.quit:
			return nil
		case watchResp, ok := <-watchChan:
			if !ok {
				return fmt.Errorf("watch channel closed")
			}
			if watchResp.Err() != nil {
				return fmt.Errorf("watch response error: %w", watchResp.Err())
			}
			if watchResp.Canceled {
				return fmt.Errorf("watch canceled")
			}
			r.watchRevision = watchResp.Header.Revision
			for _, event := range watchResp.Events {
				if event.Type == clientv3.EventTypePut {
					key := event.Kv.Key
					value := event.Kv.Value
					if match := nodesPattern.FindStringSubmatch(string(key)); match != nil {
						chainId := normalizeMultiVersionChainID(match[nodesPattern.SubexpIndex("chain")])
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
					if match := nativeNodesPattern.FindStringSubmatch(string(key)); match != nil {
						chainId := normalizeMultiVersionChainID(match[nativeNodesPattern.SubexpIndex("chain")])
						nodeKey := match[nativeNodesPattern.SubexpIndex("node")]
						var node discovery.TargetNode
						err := json.Unmarshal(value, &node)
						if err != nil {
							log.Error("failed to unmarshal value for key", err, log.Any("key", key), log.Any("chain_id", chainId))
							continue
						}
						node.NodeKey = nodeKey
						node.ChangeType = EVENT_PUT
						node.ChainId = chainId
						node.Source = "native"
						r.nodeChannel <- &node
						continue
					}
					if match := lastBlockPattern.FindStringSubmatch(string(key)); match != nil {
						chainId := normalizeMultiVersionChainID(match[lastBlockPattern.SubexpIndex("chain")])
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
						chainId := normalizeMultiVersionChainID(match[gateWayPattern.SubexpIndex("chain")])
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
					if match := mirrorPattern.FindStringSubmatch(string(key)); match != nil {
						chainId := normalizeMultiVersionChainID(match[mirrorPattern.SubexpIndex("chain")])
						addrKey := match[mirrorPattern.SubexpIndex("addr")]
						var mirror discovery.MirrorTarget
						err := json.Unmarshal(value, &mirror)
						if err != nil {
							log.Error("failed to unmarshal mirror target", err, log.Any("key", key), log.Any("chain_id", chainId))
							continue
						}
						mirror.ChainId = chainId
						mirror.AddrKey = addrKey
						r.mirrorChannel <- &mirror
						continue
					}
					if match := versionPattern.FindStringSubmatch(string(key)); match != nil {
						chainId := normalizeMultiVersionChainID(match[versionPattern.SubexpIndex("chain")])
						versionValue := parseVersionValue(value)
						r.versionChannel <- &discovery.ChainVersion{
							ChainId:    chainId,
							Version:    versionValue,
							ChangeType: EVENT_PUT,
						}
						continue
					}
				} else {
					key := event.Kv.Key
					var value []byte
					if event.PrevKv != nil {
						value = event.PrevKv.Value
					}
					if match := nodesPattern.FindStringSubmatch(string(key)); match != nil {
						chainId := normalizeMultiVersionChainID(match[nodesPattern.SubexpIndex("chain")])
						nodeKey := match[nodesPattern.SubexpIndex("node")]
						var node discovery.TargetNode
						if value != nil {
							err := json.Unmarshal(value, &node)
							if err != nil {
								log.Error("failed to unmarshal value for key", err, log.Any("key", key), log.Any("chain_id", chainId))
							}
						}
						node.NodeKey = nodeKey
						node.ChangeType = EVENT_DELETE
						node.ChainId = chainId
						log.Info("node delete event detected", log.Any("key", key), log.Any("chain_id", chainId), log.Any("node_key", nodeKey))
						r.nodeChannel <- &node
						continue
					}
					if match := nativeNodesPattern.FindStringSubmatch(string(key)); match != nil {
						chainId := normalizeMultiVersionChainID(match[nativeNodesPattern.SubexpIndex("chain")])
						nodeKey := match[nativeNodesPattern.SubexpIndex("node")]
						var node discovery.TargetNode
						if value != nil {
							err := json.Unmarshal(value, &node)
							if err != nil {
								log.Error("failed to unmarshal value for key", err, log.Any("key", key), log.Any("chain_id", chainId))
							}
						}
						node.NodeKey = nodeKey
						node.ChangeType = EVENT_DELETE
						node.ChainId = chainId
						node.Source = "native"
						log.Info("native node delete event detected", log.Any("key", key), log.Any("chain_id", chainId), log.Any("node_key", nodeKey))
						r.nodeChannel <- &node
						continue
					}
					if match := gateWayPattern.FindStringSubmatch(string(key)); match != nil {
						chainId := normalizeMultiVersionChainID(match[gateWayPattern.SubexpIndex("chain")])
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
					if match := mirrorPattern.FindStringSubmatch(string(key)); match != nil {
						chainId := normalizeMultiVersionChainID(match[mirrorPattern.SubexpIndex("chain")])
						addrKey := match[mirrorPattern.SubexpIndex("addr")]
						var mirror discovery.MirrorTarget
						if value != nil {
							err := json.Unmarshal(value, &mirror)
							if err != nil {
								log.Error("failed to unmarshal mirror target", err, log.Any("key", key), log.Any("chain_id", chainId))
							}
						}
						mirror.ChainId = chainId
						mirror.AddrKey = addrKey
						mirror.Deleted = true
						r.mirrorChannel <- &mirror
						continue
					}
					if match := versionPattern.FindStringSubmatch(string(key)); match != nil {
						chainId := normalizeMultiVersionChainID(match[versionPattern.SubexpIndex("chain")])
						versionValue := parseVersionValue(value)
						r.versionChannel <- &discovery.ChainVersion{
							ChainId:    chainId,
							Version:    versionValue,
							ChangeType: EVENT_DELETE,
						}
						continue
					}
				}
			}
		}
	}
}

func normalizeMultiVersionChainID(raw string) string {
	raw = strings.Trim(raw, "/")
	if raw == "" {
		return raw
	}
	return strings.ReplaceAll(raw, "/", "-")
}

func parseVersionValue(value []byte) string {
	if len(value) == 0 {
		return ""
	}
	trimmed := strings.TrimSpace(string(value))
	if trimmed == "" {
		return ""
	}
	var obj struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(value, &obj); err == nil {
		if strings.TrimSpace(obj.Version) != "" {
			return strings.TrimSpace(obj.Version)
		}
	}
	var plain string
	if err := json.Unmarshal(value, &plain); err == nil {
		if strings.TrimSpace(plain) != "" {
			return strings.TrimSpace(plain)
		}
	}
	return strings.Trim(trimmed, "\"")
}
