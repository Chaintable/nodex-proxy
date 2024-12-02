package node

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/Chaintable/pipeline/types"
	"go.etcd.io/etcd/clientv3"
)

type Refresher struct {
	etcdClient  *clientv3.Client
	watchCancel context.CancelFunc
	watchKey    string

	quit chan struct{}

	backends []string
	mu       sync.RWMutex

	chainID string
}

func NewRefresher(etcdEndpoints []string, configKey string, chainID string) *Refresher {
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

	err = refresher.init(configKey, ctx)
	if err != nil {
		return nil
	}
	go refresher.watchConfig(configKey, ctx)

	return refresher
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
		newBackends := make([]string, 0, len(replicaNotification.ReplicaStates))
		for _, replicaState := range replicaNotification.ReplicaStates {
			if replicaState.StateType != 1 {
				continue
			}
			newBackends = append(newBackends, replicaState.Address)
		}
		r.setBackends(newBackends)
		log.Printf("initial chain: %+v backends success", r.chainID)
	}
	return nil
}

func (r *Refresher) watchConfig(key string, ctx context.Context) {
	watchChan := r.etcdClient.Watch(ctx, key)

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

					newBackends := make([]string, 0, len(replicaNotification.ReplicaStates))

					for _, replicaState := range replicaNotification.ReplicaStates {
						if replicaState.StateType != 1 {
							continue
						}
						newBackends = append(newBackends, replicaState.Address)
					}

					r.setBackends(newBackends)

					log.Printf("chain: %+v backends updated, backends: %v", r.chainID, newBackends)
				}
			}
		}
	}
}
func (r *Refresher) GetBackends() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.backends
}

func (r *Refresher) setBackends(backends []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.backends = backends
}
