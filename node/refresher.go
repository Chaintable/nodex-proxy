package node

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/Chaintable/pipeline/types"
	"github.com/Chaintable/pipeline/util"
	kafka "github.com/segmentio/kafka-go"
)

type Refresher struct {
	replicaKafkaReader *kafka.Reader
	quit               chan struct{}

	backends []string
	mu       sync.RWMutex
}

func NewRefresher(brokers []string, topic string, groupID string) *Refresher {
	return &Refresher{
		replicaKafkaReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
		}),
		quit: make(chan struct{}),
	}
}

func (r *Refresher) Close() error {
	close(r.quit)
	return r.replicaKafkaReader.Close()
}

func (r *Refresher) Refresh() error {
	for {
		select {
		case <-r.quit:
			return nil
		default:
			msg, err := r.replicaKafkaReader.FetchMessage(context.Background())
			if err != nil {
				log.Printf("fetch message error %+v", err)
				time.Sleep(1 * time.Second)
				continue
			}
			replicaNotification := &types.ReplicaStateChangeNotification{}
			err = util.DecodeFromGzipJson(msg.Value, replicaNotification)
			if err != nil {
				log.Printf("decode message error %+v", err)
				time.Sleep(1 * time.Second)
				continue
			}
			// TODO handle replicaNotification
			// r.setBackends(replicaNotification.Backends)
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
