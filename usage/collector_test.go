package usage

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chaintable/nodex-proxy/lib/log"
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
	closed    bool
}

func (p *fakeProducer) Write(_ context.Context, records []Record) error {
	p.mu.Lock()
	copyOfRecords := append([]Record(nil), records...)
	p.batches = append(p.batches, copyOfRecords)
	err := p.writeErr
	p.mu.Unlock()
	if p.writeCall != nil {
		select {
		case p.writeCall <- struct{}{}:
		default:
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
		interval:         interval,
		timeout:          time.Second,
		maxClientIDBytes: MaxClientIDBytes,
		maxKeys:          MaxAggregationKeys,
		now: func() time.Time {
			return time.UnixMilli(1783568373000)
		},
		newID: func() string {
			return "id-" + time.Unix(nextID.Add(1), 0).Format("05")
		},
	})
}

func TestCollectorAggregatesByClientAndChain(t *testing.T) {
	producer := &fakeProducer{}
	collector := testCollector(producer, time.Hour)
	t.Cleanup(func() { _ = collector.Close(context.Background()) })

	collector.Record("instance:1", 1, 100*time.Millisecond)
	collector.Record("instance:1", 1, 320*time.Millisecond)
	collector.Record("instance:1", 56, 180*time.Millisecond)
	collector.Record("  ", 1, 50*time.Millisecond)

	require.NoError(t, collector.flush(context.Background()))
	batches, _ := producer.snapshot()
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 3)

	records := make(map[aggregateKey]Record, len(batches[0]))
	for _, record := range batches[0] {
		records[aggregateKey{clientID: record.ClientID, chainID: record.ChainID}] = record
		require.NotEmpty(t, record.ID)
		require.Equal(t, int64(1783568373000), record.Timestamp)
	}
	require.Equal(t, int64(50), records[aggregateKey{clientID: UnknownClientID, chainID: 1}].TimeMS)
	require.Equal(t, int64(420), records[aggregateKey{clientID: "instance:1", chainID: 1}].TimeMS)
	require.Equal(t, int64(180), records[aggregateKey{clientID: "instance:1", chainID: 56}].TimeMS)
}

func TestCollectorAccumulatesBeforeConvertingToMilliseconds(t *testing.T) {
	producer := &fakeProducer{}
	collector := testCollector(producer, time.Hour)
	t.Cleanup(func() { _ = collector.Close(context.Background()) })

	collector.Record("client", 1, 600*time.Microsecond)
	collector.Record("client", 1, 600*time.Microsecond)
	require.NoError(t, collector.flush(context.Background()))

	batches, _ := producer.snapshot()
	require.Equal(t, int64(1), batches[0][0].TimeMS)
}

func TestCollectorDropsFailedSnapshot(t *testing.T) {
	producer := &fakeProducer{writeErr: errors.New("Kafka unavailable")}
	collector := testCollector(producer, time.Hour)
	t.Cleanup(func() { _ = collector.Close(context.Background()) })

	collector.Record("client", 1, time.Second)
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
	collector.Record("client", 1, time.Second)

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
	collector.Record("client", 1, time.Second)

	require.NoError(t, collector.Close(context.Background()))
	collector.Record("client", 1, time.Second)
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
			collector.Record("client", 1, time.Millisecond)
		}()
	}
	wg.Wait()
	require.NoError(t, collector.Close(context.Background()))

	batches, _ := producer.snapshot()
	var total int64
	for _, batch := range batches {
		for _, record := range batch {
			total += record.TimeMS
		}
	}
	require.Equal(t, int64(requestCount), total)
}

func TestCollectorBoundsUntrustedClientCardinality(t *testing.T) {
	producer := &fakeProducer{}
	collector := newCollector(producer, collectorOptions{
		interval:         time.Hour,
		timeout:          time.Second,
		maxClientIDBytes: 8,
		maxKeys:          2,
		now:              time.Now,
		newID:            func() string { return "id" },
	})
	t.Cleanup(func() { _ = collector.Close(context.Background()) })

	collector.Record("too-long-client", 1, time.Millisecond)
	collector.Record("a", 1, time.Millisecond)
	collector.Record("b", 1, time.Millisecond)
	collector.Record("c", 1, time.Millisecond)
	collector.Record("a", 1, time.Millisecond)
	require.NoError(t, collector.flush(context.Background()))

	batches, _ := producer.snapshot()
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 2)
	for _, record := range batches[0] {
		if record.ClientID == "a" {
			require.Equal(t, int64(2), record.TimeMS)
		}
		require.NotEqual(t, "too-long-client", record.ClientID)
		require.NotEqual(t, "c", record.ClientID)
	}
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
