package node

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	proxyType "github.com/Chaintable/nodex-proxy/types"
	"github.com/Chaintable/pipeline/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"go.etcd.io/etcd/client/v3"
)

type Refresher struct {
	etcdClient  *clientv3.Client
	watchCancel context.CancelFunc
	watchKey    string

	quit chan struct{}

	backends          []string
	mu                sync.RWMutex
	stateBackends     []*lbnode.Node
	archiveBackends   []*lbnode.Node
	LatestBlockNumber *hexutil.Big
	chainID           string
}

func NewRefresher(ctx context.Context, etcdEndpoints []string, configKey string, chainID string) (*Refresher, error) {
	log.Printf("init Refresher etcd endpoints: %v, chain id: %v", etcdEndpoints, chainID)
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("connectting etcd failed: %v\n", err)
	}
	ctx, cancel := context.WithCancel(context.Background())

	refresher := &Refresher{
		etcdClient:  etcdCli,
		watchCancel: cancel,
		quit:        make(chan struct{}),
		chainID:     chainID,
	}
	timeOutCtx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
	defer cancelFunc()
	err = refresher.init(configKey, timeOutCtx)
	if err != nil {
		return nil, err
	}
	go refresher.watchConfig(configKey, ctx)

	return refresher, err
}

func (r *Refresher) Close() error {
	close(r.quit)
	r.watchCancel()
	return r.etcdClient.Close()
}

func (r *Refresher) init(key string, ctx context.Context) error {
	// Initial request to get the current value of the key
	resp, err := r.etcdClient.Get(ctx, key)
	log.Printf("get key resp: %+v, key: %s , chainID: %v", resp, key, r.chainID)
	if err != nil {
		log.Printf("failed to get initial value for key %s: %+v, chain id: %v", key, err, r.chainID)
		return err
	}
	for _, kv := range resp.Kvs {
		replicaNotification := &types.ReplicaStateChangeNotification{}
		err := json.Unmarshal(kv.Value, replicaNotification)
		if err != nil {
			log.Printf("decode initial message error %+v, chain id: %v", err, r.chainID)
			return err
		}
		var (
			stateBackends     []string
			archiveBackends   []string
			latestBlockNumber *hexutil.Big
		)
		for _, replicaState := range replicaNotification.ReplicaStates {
			if replicaState.StateType != 1 {
				continue
			}
			if replicaState.NodeType == 1 {
				stateBackends = append(stateBackends, replicaState.Address)
			} else {
				archiveBackends = append(archiveBackends, replicaState.Address)
			}
			latestBlockNumber = replicaState.LatestBlockNumber
		}
		r.setBackends(stateBackends, archiveBackends, latestBlockNumber)
		log.Printf("initial chain: %+v backends success", r.chainID)
	}
	return nil
}

func (r *Refresher) watchConfig(key string, ctx context.Context) {
	watchChan := r.etcdClient.Watch(ctx, key, clientv3.WithPrefix())

	for {
		select {
		case <-r.quit:
			return
		case watchResp := <-watchChan:
			for _, event := range watchResp.Events {
				if event.Type == clientv3.EventTypePut {
					replicaNotification := &types.ReplicaStateChangeNotification{}
					err := json.Unmarshal(event.Kv.Value, replicaNotification)
					if err != nil {
						log.Printf("decode message error %+v, chain id:%+v, val: %+v", err, r.chainID, event.Kv.Value)
						time.Sleep(1 * time.Second)
						continue
					}

					var (
						stateBackends   []string
						archiveBackends []string
						lastBlockNumber *hexutil.Big
					)
					for _, replicaState := range replicaNotification.ReplicaStates {
						if replicaState.StateType != 1 {
							continue
						}
						if replicaState.NodeType == 1 {
							stateBackends = append(stateBackends, replicaState.Address)
						} else {
							archiveBackends = append(archiveBackends, replicaState.Address)
						}
						lastBlockNumber = replicaState.LatestBlockNumber
					}

					r.setBackends(stateBackends, archiveBackends, lastBlockNumber)
					log.Printf("chain: %+v backends updated, backends: %v, height:%v", r.chainID, stateBackends, r.LatestBlockNumber)
				}
			}
		}
	}
}

// GetBackends ...
func (r *Refresher) GetBackends() ([]*lbnode.Node, []*lbnode.Node, *hexutil.Big) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stateBackends, r.archiveBackends, r.LatestBlockNumber
}

// setBackends ...
func (r *Refresher) setBackends(states []string, archives []string, blkNum *hexutil.Big) {

	var stateBackends []*lbnode.Node
	for _, state := range states {
		temp := strings.Split(state, ":")
		port, _ := strconv.Atoi(temp[1])
		stateBackends = append(stateBackends, lbnode.New(state, temp[0], port, proxyType.DefaultLoadBalancerWeight))
	}
	var archiveBackends []*lbnode.Node
	for _, archive := range archives {
		temp := strings.Split(archive, ":")
		port, _ := strconv.Atoi(temp[1])
		archiveBackends = append(archiveBackends, lbnode.New(archive, temp[0], port, proxyType.DefaultLoadBalancerWeight))
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.stateBackends = stateBackends
	r.archiveBackends = archiveBackends
	r.LatestBlockNumber = blkNum
}
