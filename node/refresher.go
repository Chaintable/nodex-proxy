package node

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/Chaintable/pipeline/types"
	"github.com/Chaintable/pipeline/util"
	"go.etcd.io/etcd/clientv3"
)

type Refresher struct {
	etcdClient  *clientv3.Client
	watchCancel context.CancelFunc
	watchKey    string

	quit chan struct{}

	backends []string
	mu       sync.RWMutex
}

func NewRefresher(etcdEndpoints []string, configKey string) *Refresher {
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("连接 etcd 失败: %v\n", err)
	}
	ctx, cancel := context.WithCancel(context.Background())

	refresher := &Refresher{
		etcdClient:  etcdCli,
		watchCancel: cancel,
		quit:        make(chan struct{}),
	}

	go refresher.watchConfig(configKey, ctx)

	return refresher
}

func (r *Refresher) Close() error {
	close(r.quit)
	r.watchCancel()
	return r.etcdClient.Close()
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
					err := util.DecodeFromGzipJson(event.Kv.Value, replicaNotification)
					if err != nil {
						log.Printf("decode message error %+v", err)
						time.Sleep(1 * time.Second)
						continue
					}

					r.mu.Lock()
					// TODO handle replicaNotification
					// r.setBackends(replicaNotification.Backends)
					r.mu.Unlock()

					log.Println("配置已更新")
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
