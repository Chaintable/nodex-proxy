package usage

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func init() {
	log.InitLogger("error")
}

type fakeProducer struct {
	mu        sync.Mutex
	batches   [][]Record
	writeErr  error
	writeCall chan struct{}
	writeWait chan struct{}
	closed    bool
}

func (p *fakeProducer) Write(ctx context.Context, records []Record) error {
	p.mu.Lock()
	copyOfRecords := append([]Record(nil), records...)
	p.batches = append(p.batches, copyOfRecords)
	err := p.writeErr
	wait := p.writeWait
	p.mu.Unlock()
	if p.writeCall != nil {
		select {
		case p.writeCall <- struct{}{}:
		default:
		}
	}
	if wait != nil {
		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (p *fakeProducer) Close() error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	return nil
}

func (p *fakeProducer) snapshot() ([][]Record, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	batches := make([][]Record, len(p.batches))
	for i := range p.batches {
		batches[i] = append([]Record(nil), p.batches[i]...)
	}
	return batches, p.closed
}

func testCollector(producer Producer, interval time.Duration) *Collector {
	var nextID atomic.Int64
	return newCollector(producer, collectorOptions{
		interval:       interval,
		timeout:        time.Second,
		flushThreshold: aggregationFlushThreshold,
		now: func() time.Time {
			return time.UnixMilli(1783568373000)
		},
		newID: func() string {
			return "id-" + time.Unix(nextID.Add(1), 0).Format("05")
		},
	})
}

func TestCollectorAggregatesByClient(t *testing.T) {
	producer := &fakeProducer{}
	collector := testCollector(producer, time.Hour)
	t.Cleanup(func() { _ = collector.Close(context.Background()) })

	collector.Record("instance:1", 100*time.Millisecond)
	collector.Record("instance:1", 320*time.Millisecond)
	collector.Record("instance:1", 180*time.Millisecond)
	collector.Record("  ", 50*time.Millisecond)

	require.NoError(t, collector.flush(context.Background()))
	batches, _ := producer.snapshot()
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 2)

	records := make(map[string]Record, len(batches[0]))
	for _, record := range batches[0] {
		records[record.ClientID] = record
		require.NotEmpty(t, record.ID)
		require.Equal(t, int64(1783568373000), record.Timestamp)
		require.Equal(t, ServiceLeafage, record.Service)
		require.Equal(t, ResourceTypeRead, record.ResourceType)
	}
	require.Equal(t, int64(50), records[UnknownClientID].Usage)
	require.Equal(t, int64(600), records["instance:1"].Usage)
}

func TestCollectorUsesAtLeastOneMillisecond(t *testing.T) {
	producer := &fakeProducer{}
	collector := testCollector(producer, time.Hour)
	t.Cleanup(func() { _ = collector.Close(context.Background()) })

	collector.Record("sub-millisecond", 600*time.Microsecond)
	collector.Record("zero", 0)
	require.NoError(t, collector.flush(context.Background()))

	batches, _ := producer.snapshot()
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 2)
	for _, record := range batches[0] {
		require.Equal(t, int64(1), record.Usage)
	}
}

func TestCollectorDropsFailedSnapshot(t *testing.T) {
	producer := &fakeProducer{writeErr: errors.New("Kafka unavailable")}
	collector := testCollector(producer, time.Hour)
	t.Cleanup(func() { _ = collector.Close(context.Background()) })

	collector.Record("client", time.Second)
	require.Error(t, collector.flush(context.Background()))

	producer.mu.Lock()
	producer.writeErr = nil
	producer.mu.Unlock()
	require.NoError(t, collector.flush(context.Background()))
	batches, _ := producer.snapshot()
	require.Len(t, batches, 1, "a failed snapshot must be dropped instead of retained")
}

func TestCollectorPeriodicFlush(t *testing.T) {
	producer := &fakeProducer{writeCall: make(chan struct{}, 1)}
	collector := testCollector(producer, 10*time.Millisecond)
	t.Cleanup(func() { _ = collector.Close(context.Background()) })
	collector.Record("client", time.Second)

	select {
	case <-producer.writeCall:
	case <-time.After(time.Second):
		t.Fatal("usage was not flushed on schedule")
	}

	batches, _ := producer.snapshot()
	require.Len(t, batches, 1)
}

func TestCollectorCloseFlushesAndRejectsNewRecords(t *testing.T) {
	producer := &fakeProducer{}
	collector := testCollector(producer, time.Hour)
	collector.Record("client", time.Second)

	require.NoError(t, collector.Close(context.Background()))
	collector.Record("client", time.Second)
	require.NoError(t, collector.Close(context.Background()))

	batches, closed := producer.snapshot()
	require.True(t, closed)
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 1)
}

func TestCollectorConcurrentRecord(t *testing.T) {
	producer := &fakeProducer{}
	collector := testCollector(producer, time.Hour)

	const requestCount = 1000
	var wg sync.WaitGroup
	for i := 0; i < requestCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			collector.Record("client", time.Millisecond)
		}()
	}
	wg.Wait()
	require.NoError(t, collector.Close(context.Background()))

	batches, _ := producer.snapshot()
	var total int64
	for _, batch := range batches {
		for _, record := range batch {
			total += record.Usage
		}
	}
	require.Equal(t, int64(requestCount), total)
}

func TestCollectorFlushesEarlyAtAggregationThreshold(t *testing.T) {
	releaseWrite := make(chan struct{})
	producer := &fakeProducer{
		writeCall: make(chan struct{}, 1),
		writeWait: releaseWrite,
	}
	collector := newCollector(producer, collectorOptions{
		interval:       time.Hour,
		timeout:        time.Second,
		flushThreshold: 2,
		now:            time.Now,
		newID:          func() string { return "id" },
	})
	collector.Record("a", time.Millisecond)
	collector.Record("b", time.Millisecond)

	select {
	case <-producer.writeCall:
	case <-time.After(time.Second):
		t.Fatal("usage was not flushed after reaching the aggregation threshold")
	}
	require.Equal(t, float64(2), testutil.ToFloat64(aggregationKeys),
		"an in-flight snapshot must remain included in the in-memory key count")

	// The first Kafka write is still blocked. New keys must accumulate in the
	// next snapshot and trigger another flush instead of being discarded.
	collector.Record("c", time.Millisecond)
	collector.Record("d", time.Millisecond)
	collector.Record("e", time.Millisecond)
	require.Equal(t, float64(5), testutil.ToFloat64(aggregationKeys))
	close(releaseWrite)

	select {
	case <-producer.writeCall:
	case <-time.After(time.Second):
		t.Fatal("usage accumulated during a Kafka write was not flushed")
	}

	require.NoError(t, collector.Close(context.Background()))
	require.Zero(t, testutil.ToFloat64(aggregationKeys))

	batches, _ := producer.snapshot()
	require.Len(t, batches, 2)
	var clients []string
	for _, batch := range batches {
		for _, record := range batch {
			clients = append(clients, record.ClientID)
		}
	}
	require.ElementsMatch(t, []string{"a", "b", "c", "d", "e"}, clients)
}

func TestCollectorReportsAggregationKeyCount(t *testing.T) {
	producer := &fakeProducer{}
	collector := testCollector(producer, time.Hour)
	t.Cleanup(func() { _ = collector.Close(context.Background()) })

	collector.Record("a", time.Millisecond)
	collector.Record("a", time.Millisecond)
	collector.Record("b", time.Millisecond)
	require.Equal(t, float64(2), testutil.ToFloat64(aggregationKeys))

	require.NoError(t, collector.flush(context.Background()))
	require.Zero(t, testutil.ToFloat64(aggregationKeys))
}

type blockingCloseProducer struct {
	release chan struct{}
}

func (p *blockingCloseProducer) Write(context.Context, []Record) error { return nil }

func (p *blockingCloseProducer) Close() error {
	<-p.release
	return nil
}

func TestCollectorCloseHonorsContext(t *testing.T) {
	producer := &blockingCloseProducer{release: make(chan struct{})}
	collector := testCollector(producer, time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, collector.Close(ctx), context.DeadlineExceeded)
	close(producer.release)
}
