package usage

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/google/uuid"
)

const (
	FlushInterval      = 30 * time.Second
	MaxClientIDBytes   = 256
	MaxAggregationKeys = 100_000
	flushTimeout       = 5 * time.Second
)

// Producer writes aggregated records to the configured destination.
type Producer interface {
	Write(context.Context, []Record) error
	Close() error
}

type aggregateKey struct {
	clientID string
	chainID  int64
}

type collectorOptions struct {
	interval         time.Duration
	timeout          time.Duration
	maxClientIDBytes int
	maxKeys          int
	now              func() time.Time
	newID            func() string
}

// Collector accumulates request duration by client ID and base chain ID. A
// snapshot is swapped out before every Kafka write, so a slow producer never
// blocks RPC request accounting.
type Collector struct {
	producer Producer
	options  collectorOptions

	mu       sync.Mutex
	active   map[aggregateKey]time.Duration
	closed   bool
	stop     chan struct{}
	loopDone chan struct{}

	closeOnce sync.Once
	closeErr  error
}

// NewCollector starts a usage collector with the production flush settings.
func NewCollector(producer Producer) *Collector {
	return newCollector(producer, collectorOptions{
		interval:         FlushInterval,
		timeout:          flushTimeout,
		maxClientIDBytes: MaxClientIDBytes,
		maxKeys:          MaxAggregationKeys,
		now:              time.Now,
		newID:            uuid.NewString,
	})
}

func newCollector(producer Producer, options collectorOptions) *Collector {
	if producer == nil {
		panic("usage: nil producer")
	}
	if options.interval <= 0 {
		options.interval = FlushInterval
	}
	if options.timeout <= 0 {
		options.timeout = flushTimeout
	}
	if options.maxClientIDBytes <= 0 {
		options.maxClientIDBytes = MaxClientIDBytes
	}
	if options.maxKeys <= 0 {
		options.maxKeys = MaxAggregationKeys
	}
	if options.now == nil {
		options.now = time.Now
	}
	if options.newID == nil {
		options.newID = uuid.NewString
	}

	c := &Collector{
		producer: producer,
		options:  options,
		active:   make(map[aggregateKey]time.Duration),
		stop:     make(chan struct{}),
		loopDone: make(chan struct{}),
	}
	go c.run()
	return c
}

// Record adds one completed RPC request to the current aggregation window.
func (c *Collector) Record(clientID string, chainID int64, duration time.Duration) {
	if duration < 0 {
		return
	}
	clientID = strings.TrimSpace(clientID)
	if clientID == "" {
		clientID = UnknownClientID
	} else if len(clientID) > c.options.maxClientIDBytes {
		discardedRequestsTotal.WithLabelValues("client_id_too_long").Inc()
		return
	} else {
		// TrimSpace may return a small substring backed by the entire original
		// header. Clone it so an accepted map key retains at most the configured
		// number of bytes.
		clientID = strings.Clone(clientID)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	key := aggregateKey{clientID: clientID, chainID: chainID}
	if accumulated, ok := c.active[key]; ok {
		c.active[key] = accumulated + duration
		return
	}
	if len(c.active) >= c.options.maxKeys {
		discardedRequestsTotal.WithLabelValues("aggregation_limit").Inc()
		return
	}
	c.active[key] = duration
}

func (c *Collector) run() {
	defer close(c.loopDone)
	ticker := time.NewTicker(c.options.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), c.options.timeout)
			_ = c.flush(ctx)
			cancel()
		case <-c.stop:
			return
		}
	}
}

func (c *Collector) snapshot(closeCollector bool) map[aggregateKey]time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if closeCollector {
		c.closed = true
	}
	if len(c.active) == 0 {
		return nil
	}
	snapshot := c.active
	c.active = make(map[aggregateKey]time.Duration)
	return snapshot
}

func (c *Collector) flush(ctx context.Context) error {
	return c.writeSnapshot(ctx, c.snapshot(false))
}

func (c *Collector) writeSnapshot(ctx context.Context, snapshot map[aggregateKey]time.Duration) error {
	if len(snapshot) == 0 {
		return nil
	}

	timestamp := c.options.now().UnixMilli()
	records := make([]Record, 0, len(snapshot))
	for key, duration := range snapshot {
		records = append(records, Record{
			ID:        c.options.newID(),
			ClientID:  key.clientID,
			TimeMS:    duration.Milliseconds(),
			ChainID:   key.chainID,
			Timestamp: timestamp,
		})
	}

	err := c.producer.Write(ctx, records)
	if err != nil {
		flushTotal.WithLabelValues("failed").Inc()
		recordsTotal.WithLabelValues("dropped").Add(float64(len(records)))
		log.Error("failed to publish usage records; dropping batch", err,
			log.Any("record_count", len(records)))
		return err
	}

	flushTotal.WithLabelValues("success").Inc()
	recordsTotal.WithLabelValues("sent").Add(float64(len(records)))
	return nil
}

// Close stops periodic flushing, writes the final snapshot, and closes the
// producer. Records may be lost if the supplied context expires or Kafka is
// unavailable, as usage delivery is intentionally best-effort.
func (c *Collector) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		close(c.stop)
		select {
		case <-c.loopDone:
		case <-ctx.Done():
			c.closeErr = ctx.Err()
			return
		}
		snapshot := c.snapshot(true)
		writeErr := c.writeSnapshot(ctx, snapshot)
		closeErr := c.closeProducer(ctx)
		c.closeErr = errors.Join(writeErr, closeErr)
	})
	return c.closeErr
}

func (c *Collector) closeProducer(ctx context.Context) error {
	closed := make(chan error, 1)
	go func() {
		closed <- c.producer.Close()
	}()

	select {
	case err := <-closed:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
