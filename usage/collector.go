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
	aggregationFlushThreshold = 10_000
	flushTimeout              = 5 * time.Second
)

// Producer writes aggregated records to the configured destination.
type Producer interface {
	Write(context.Context, []Record) error
	Close() error
}

type collectorOptions struct {
	interval       time.Duration
	timeout        time.Duration
	flushThreshold int
	now            func() time.Time
	newID          func() string
}

// Collector accumulates leafage read duration by client ID. A snapshot is
// swapped out before every Kafka write, so a slow producer never blocks RPC
// request accounting.
type Collector struct {
	producer Producer
	options  collectorOptions

	mu       sync.Mutex
	active   map[string]time.Duration
	closed   bool
	stop     chan struct{}
	flushNow chan struct{}
	loopDone chan struct{}

	closeOnce sync.Once
	closeErr  error
}

// NewCollector starts a usage collector with the configured report interval.
func NewCollector(producer Producer, reportInterval time.Duration) *Collector {
	return newCollector(producer, collectorOptions{
		interval:       reportInterval,
		timeout:        flushTimeout,
		flushThreshold: aggregationFlushThreshold,
		now:            time.Now,
		newID:          uuid.NewString,
	})
}

func newCollector(producer Producer, options collectorOptions) *Collector {
	if producer == nil {
		panic("usage: nil producer")
	}
	if options.interval <= 0 {
		panic("usage: report interval must be positive")
	}
	if options.timeout <= 0 {
		options.timeout = flushTimeout
	}
	if options.flushThreshold <= 0 {
		options.flushThreshold = aggregationFlushThreshold
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
		active:   make(map[string]time.Duration),
		stop:     make(chan struct{}),
		flushNow: make(chan struct{}, 1),
		loopDone: make(chan struct{}),
	}
	go c.run()
	return c
}

// Record adds one completed RPC request to the current aggregation window.
func (c *Collector) Record(clientID string, duration time.Duration) {
	if duration < 0 {
		return
	}
	rawClientID := clientID
	clientID = strings.TrimSpace(rawClientID)
	if clientID == "" {
		clientID = UnknownClientID
	} else if len(clientID) != len(rawClientID) {
		// TrimSpace may retain the original header's backing storage. Clone the
		// trimmed value so the map only owns the actual client ID bytes.
		clientID = strings.Clone(clientID)
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	if accumulated, ok := c.active[clientID]; ok {
		c.active[clientID] = accumulated + duration
		c.mu.Unlock()
		return
	}
	c.active[clientID] = duration
	keyCount := len(c.active)
	aggregationKeys.Inc()
	shouldFlush := keyCount >= c.options.flushThreshold
	c.mu.Unlock()

	if shouldFlush {
		select {
		case c.flushNow <- struct{}{}:
		default:
		}
	}
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
		case <-c.flushNow:
			ctx, cancel := context.WithTimeout(context.Background(), c.options.timeout)
			_ = c.flush(ctx)
			cancel()
		case <-c.stop:
			return
		}
	}
}

func (c *Collector) snapshot(closeCollector bool) map[string]time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if closeCollector {
		c.closed = true
	}
	if len(c.active) == 0 {
		return nil
	}
	snapshot := c.active
	c.active = make(map[string]time.Duration)
	return snapshot
}

func (c *Collector) flush(ctx context.Context) error {
	return c.writeSnapshot(ctx, c.snapshot(false))
}

func (c *Collector) writeSnapshot(ctx context.Context, snapshot map[string]time.Duration) error {
	if len(snapshot) == 0 {
		return nil
	}
	defer aggregationKeys.Sub(float64(len(snapshot)))

	timestamp := c.options.now().UnixMilli()
	records := make([]Record, 0, len(snapshot))
	for clientID, duration := range snapshot {
		usage := duration.Milliseconds()
		if usage < 1 {
			usage = 1
		}
		records = append(records, Record{
			ID:           c.options.newID(),
			ClientID:     clientID,
			Service:      ServiceLeafage,
			ResourceType: ResourceTypeRead,
			Usage:        usage,
			Timestamp:    timestamp,
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
