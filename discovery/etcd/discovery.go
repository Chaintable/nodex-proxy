package etcd

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	json "github.com/bytedance/sonic"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
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
	// watchRevision is the revision of the last snapshot or processed watch
	// response; watches resume from watchRevision+1 so no event is skipped
	// or delivered twice.
	watchRevision int64
	// knownKeys maps every delivered key to its last seen value. It lets a
	// resync after compaction synthesize DELETE events for keys that
	// disappeared while the watch was broken, and serves as a PrevKV
	// fallback. Only the Init goroutine and the watch goroutine touch it,
	// strictly one after the other, so no lock is needed.
	knownKeys map[string][]byte
}

const (
	EVENT_PUT = 0 + iota
	EVENT_DELETE
)

var (
	lastBlockPattern   = regexp.MustCompile(`^(?P<chain>.*?)/lastBlockNumber$`)
	nodesPattern       = regexp.MustCompile(`^(?P<chain>.*?)/nodes/(?P<node>.*?)$`)
	nativeNodesPattern = regexp.MustCompile(`^(?P<chain>.*?)/nativeNodes/(?P<node>.*?)$`)
	gateWayPattern     = regexp.MustCompile(`^(?P<chain>.*?)/gateway$`)
	mirrorPattern      = regexp.MustCompile(`^(?P<chain>.*?)/mirror/(?P<addr>.*?)$`)
	versionPattern     = regexp.MustCompile(`^(?P<chain>.*?)/version$`)
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
		knownKeys:   make(map[string][]byte),
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
		r.dispatchPut(string(kv.Key), kv.Value)
	}
	r.watchRevision = resp.Header.Revision
	go r.watchConfig(ctx)

	return nodeChannel, heightChannel, gatewayChannel, mirrorChannel, versionChannel, nil
}

func (r *Discover) watchConfig(ctx context.Context) {
	for {
		// Resume from the revision right after the last one we processed so
		// etcd replays everything missed during a disconnect; skipping ahead
		// to the latest revision would silently drop PUT/DELETE events.
		watchChan := r.etcdClient.Watch(ctx, r.keyPrefix, clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithRev(r.watchRevision+1))
		err := r.processWatchEvents(ctx, watchChan)
		if err == nil {
			return // quit 信号触发时退出
		}
		log.Error("watch channel error, will retry", err)

		select {
		case <-r.quit:
			return
		case <-time.After(time.Second): // 重连前等待1秒
		}

		if errors.Is(err, errWatchCompacted) {
			// The revision we need was compacted away and can no longer be
			// replayed; reconcile against a fresh snapshot instead.
			if err := r.resync(ctx); err != nil {
				log.Error("failed to resync after compaction, will retry", err)
			}
		}
	}
}

// errWatchCompacted marks a watch failure whose missed events cannot be
// replayed, requiring a full snapshot resync.
var errWatchCompacted = errors.New("watch revision compacted")

// resync reconciles local state with a fresh snapshot: it synthesizes
// DELETE events for known keys that disappeared while the watch was broken
// and re-dispatches every live key as a PUT (consumers upsert idempotently).
func (r *Discover) resync(ctx context.Context) error {
	resp, err := r.etcdClient.Get(ctx, r.keyPrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	liveKeys := make(map[string]struct{}, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		liveKeys[string(kv.Key)] = struct{}{}
	}
	for key, lastValue := range r.knownKeys {
		if _, ok := liveKeys[key]; !ok {
			log.Info("synthesizing delete for key removed while watch was broken", log.Any("key", key))
			r.dispatchDelete(key, lastValue)
		}
	}
	for _, kv := range resp.Kvs {
		r.dispatchPut(string(kv.Key), kv.Value)
	}

	r.watchRevision = resp.Header.Revision
	return nil
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
			if watchResp.CompactRevision != 0 {
				return fmt.Errorf("watch revision compacted at %d: %w", watchResp.CompactRevision, errWatchCompacted)
			}
			if err := watchResp.Err(); err != nil {
				if errors.Is(err, rpctypes.ErrCompacted) {
					return fmt.Errorf("watch response error: %v: %w", err, errWatchCompacted)
				}
				return fmt.Errorf("watch response error: %w", err)
			}
			if watchResp.Canceled {
				return fmt.Errorf("watch canceled")
			}
			for _, event := range watchResp.Events {
				if event.Type == clientv3.EventTypePut {
					r.dispatchPut(string(event.Kv.Key), event.Kv.Value)
				} else {
					var prevValue []byte
					if event.PrevKv != nil {
						prevValue = event.PrevKv.Value
					}
					r.dispatchDelete(string(event.Kv.Key), prevValue)
				}
			}
			r.watchRevision = watchResp.Header.Revision
		}
	}
}

// dispatchPut parses a PUT for key and forwards it to the matching channel,
// recording the key/value in knownKeys for resync and PrevKV fallback. Only
// the Init goroutine and the watch goroutine may call it.
func (r *Discover) dispatchPut(key string, value []byte) {
	if match := nodesPattern.FindStringSubmatch(key); match != nil {
		chainId := normalizeMultiVersionChainID(match[nodesPattern.SubexpIndex("chain")])
		nodeKey := match[nodesPattern.SubexpIndex("node")]
		var node discovery.TargetNode
		err := json.Unmarshal(value, &node)
		if err != nil {
			log.Error("failed to unmarshal value for key", err, log.Any("key", key), log.Any("chain_id", chainId))
			return
		}
		node.NodeKey = nodeKey
		node.ChangeType = EVENT_PUT
		node.ChainId = chainId
		r.knownKeys[key] = value
		r.nodeChannel <- &node
		return
	}
	if match := nativeNodesPattern.FindStringSubmatch(key); match != nil {
		chainId := normalizeMultiVersionChainID(match[nativeNodesPattern.SubexpIndex("chain")])
		nodeKey := match[nativeNodesPattern.SubexpIndex("node")]
		var node discovery.TargetNode
		err := json.Unmarshal(value, &node)
		if err != nil {
			log.Error("failed to unmarshal value for key", err, log.Any("key", key), log.Any("chain_id", chainId))
			return
		}
		node.NodeKey = nodeKey
		node.ChangeType = EVENT_PUT
		node.ChainId = chainId
		node.Source = "native"
		r.knownKeys[key] = value
		r.nodeChannel <- &node
		return
	}
	if match := lastBlockPattern.FindStringSubmatch(key); match != nil {
		chainId := normalizeMultiVersionChainID(match[lastBlockPattern.SubexpIndex("chain")])
		var height discovery.ChainHeight
		err := json.Unmarshal(value, &height)
		if err != nil {
			log.Error("failed to unmarshal value for key", err, log.Any("key", key), log.Any("chain_id", chainId))
			return
		}
		height.ChainId = chainId
		// heights have no DELETE semantics, so they are not tracked in knownKeys
		r.heightChan <- &height
		return
	}
	if match := gateWayPattern.FindStringSubmatch(key); match != nil {
		chainId := normalizeMultiVersionChainID(match[gateWayPattern.SubexpIndex("chain")])
		var gateway discovery.Gateway
		err := json.Unmarshal(value, &gateway)
		if err != nil {
			log.Error("failed to unmarshal value for key", err, log.Any("key", key), log.Any("chain_id", chainId))
			return
		}
		gateway.ChainId = chainId
		gateway.ChangeType = EVENT_PUT
		r.knownKeys[key] = value
		r.gatewayChannel <- &gateway
		return
	}
	if match := mirrorPattern.FindStringSubmatch(key); match != nil {
		chainId := normalizeMultiVersionChainID(match[mirrorPattern.SubexpIndex("chain")])
		addrKey := match[mirrorPattern.SubexpIndex("addr")]
		var mirror discovery.MirrorTarget
		err := json.Unmarshal(value, &mirror)
		if err != nil {
			log.Error("failed to unmarshal mirror target", err, log.Any("key", key), log.Any("chain_id", chainId))
			return
		}
		mirror.ChainId = chainId
		mirror.AddrKey = addrKey
		r.knownKeys[key] = value
		r.mirrorChannel <- &mirror
		return
	}
	if match := versionPattern.FindStringSubmatch(key); match != nil {
		chainId := normalizeMultiVersionChainID(match[versionPattern.SubexpIndex("chain")])
		versionValue := parseVersionValue(value)
		r.knownKeys[key] = value
		r.versionChannel <- &discovery.ChainVersion{
			ChainId:    chainId,
			Version:    versionValue,
			ChangeType: EVENT_PUT,
		}
		return
	}
}

// dispatchDelete parses a DELETE for key and forwards it to the matching
// channel. prevValue may be nil (etcd omits PrevKV in some cases); the last
// value recorded in knownKeys is used as a fallback so consumers still get
// node type/source details. Only the watch goroutine may call it.
func (r *Discover) dispatchDelete(key string, prevValue []byte) {
	if prevValue == nil {
		prevValue = r.knownKeys[key]
	}
	delete(r.knownKeys, key)

	if match := nodesPattern.FindStringSubmatch(key); match != nil {
		chainId := normalizeMultiVersionChainID(match[nodesPattern.SubexpIndex("chain")])
		nodeKey := match[nodesPattern.SubexpIndex("node")]
		var node discovery.TargetNode
		if prevValue != nil {
			if err := json.Unmarshal(prevValue, &node); err != nil {
				log.Error("failed to unmarshal value for key", err, log.Any("key", key), log.Any("chain_id", chainId))
			}
		}
		node.NodeKey = nodeKey
		node.ChangeType = EVENT_DELETE
		node.ChainId = chainId
		log.Info("node delete event detected", log.Any("key", key), log.Any("chain_id", chainId), log.Any("node_key", nodeKey))
		r.nodeChannel <- &node
		return
	}
	if match := nativeNodesPattern.FindStringSubmatch(key); match != nil {
		chainId := normalizeMultiVersionChainID(match[nativeNodesPattern.SubexpIndex("chain")])
		nodeKey := match[nativeNodesPattern.SubexpIndex("node")]
		var node discovery.TargetNode
		if prevValue != nil {
			if err := json.Unmarshal(prevValue, &node); err != nil {
				log.Error("failed to unmarshal value for key", err, log.Any("key", key), log.Any("chain_id", chainId))
			}
		}
		node.NodeKey = nodeKey
		node.ChangeType = EVENT_DELETE
		node.ChainId = chainId
		node.Source = "native"
		log.Info("native node delete event detected", log.Any("key", key), log.Any("chain_id", chainId), log.Any("node_key", nodeKey))
		r.nodeChannel <- &node
		return
	}
	if match := gateWayPattern.FindStringSubmatch(key); match != nil {
		chainId := normalizeMultiVersionChainID(match[gateWayPattern.SubexpIndex("chain")])
		var gateway discovery.Gateway
		err := json.Unmarshal(prevValue, &gateway)
		if err != nil {
			log.Error("failed to unmarshal value for key", err, log.Any("key", key), log.Any("chain_id", chainId))
			return
		}
		gateway.ChainId = chainId
		gateway.ChangeType = EVENT_DELETE
		r.gatewayChannel <- &gateway
		return
	}
	if match := mirrorPattern.FindStringSubmatch(key); match != nil {
		chainId := normalizeMultiVersionChainID(match[mirrorPattern.SubexpIndex("chain")])
		addrKey := match[mirrorPattern.SubexpIndex("addr")]
		var mirror discovery.MirrorTarget
		if prevValue != nil {
			if err := json.Unmarshal(prevValue, &mirror); err != nil {
				log.Error("failed to unmarshal mirror target", err, log.Any("key", key), log.Any("chain_id", chainId))
			}
		}
		mirror.ChainId = chainId
		mirror.AddrKey = addrKey
		mirror.Deleted = true
		r.mirrorChannel <- &mirror
		return
	}
	if match := versionPattern.FindStringSubmatch(key); match != nil {
		chainId := normalizeMultiVersionChainID(match[versionPattern.SubexpIndex("chain")])
		versionValue := parseVersionValue(prevValue)
		r.versionChannel <- &discovery.ChainVersion{
			ChainId:    chainId,
			Version:    versionValue,
			ChangeType: EVENT_DELETE,
		}
		return
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
