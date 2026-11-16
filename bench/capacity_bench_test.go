package bench

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rahulkj/firn/internal/broker"
	"github.com/rahulkj/firn/internal/meta"
	"github.com/rahulkj/firn/internal/queue"
)

// BenchmarkProduceDurableParallel measures aggregate durable produce
// throughput with multiple goroutines fanning out over 3 partitions.
// fsyncs on distinct partition logs may overlap, but the storage stack
// (one FileLog per partition, fsync serialized per log) and the device
// bound the total. Reports the true aggregate rate via b.ReportMetric.
func BenchmarkProduceDurableParallel(b *testing.B) {
	dir := b.TempDir()
	reg := meta.NewRegistry()
	if err := reg.Create("bench", 3); err != nil {
		b.Fatal(err)
	}
	bk, err := broker.NewWithDir(reg, dir)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = bk.Close() })
	ctx := context.Background()
	value := make([]byte, 256)
	var counter atomic.Int64
	b.ResetTimer()
	start := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := counter.Add(1)
			if _, _, err := bk.Produce(ctx, "bench", []byte(fmt.Sprintf("key-%d", i)), value); err != nil {
				b.Error(err)
				return
			}
		}
	})
	elapsed := time.Since(start)
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "msgs/s")
}

// BenchmarkProduceFlushWindow measures durable produce throughput with the
// ADR-014 group commit enabled (2 ms window, 1 MiB batch cap). Expect
// orders of magnitude over the fsync-per-append baseline; measure, never
// claim, until this is committed upstream of the flag.
func BenchmarkProduceFlushWindow(b *testing.B) {
	dir := b.TempDir()
	reg := meta.NewRegistry()
	if err := reg.Create("bench", 3); err != nil {
		b.Fatal(err)
	}
	bk, err := broker.NewWithDir(reg, dir, broker.WithFlushWindow(2*time.Millisecond, 1<<20))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = bk.Close() })
	ctx := context.Background()
	value := make([]byte, 256)
	b.SetParallelism(256) // window batching needs in-flight producers; GOMAXPROCS-default parallelism starves the window
	var counter atomic.Int64
	b.ResetTimer()
	start := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := counter.Add(1)
			if _, _, err := bk.Produce(ctx, "bench", []byte(fmt.Sprintf("key-%d", i)), value); err != nil {
				b.Error(err)
				return
			}
		}
	})
	elapsed := time.Since(start)
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "msgs/s")
}

// BenchmarkQueuePullAckDurable measures durable queue-mode consume
// throughput with the internal commit path at default (fsync-per-append) and
// with -internal-flush-window. Reports deliveries/s drained.
func BenchmarkQueuePullAckDurable(b *testing.B) {
	variants := []struct {
		name           string
		batchAcks      bool
		internalWindow bool
	}{
		{"internal-window-off", false, false},
		{"internal-window-on", false, true},
		{"ack-batch-off", true, false},
	}
	for _, v := range variants {
		b.Run(v.name, func(b *testing.B) {
			dir := b.TempDir()
			reg := meta.NewRegistry()
			if err := reg.CreateWithMode("jobs", 1, meta.ModeQueue); err != nil {
				b.Fatal(err)
			}
			var bk *broker.Broker
			var err error
			if v.internalWindow {
				bk, err = broker.NewWithDir(reg, dir, broker.WithInternalFlushWindow(2*time.Millisecond, 1<<20))
			} else {
				bk, err = broker.NewWithDir(reg, dir)
			}
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = bk.Close() })
			q, err := queue.NewManager(reg, bk, dir)
			if err != nil {
				b.Fatal(err)
			}

			ctx := context.Background()
			value := make([]byte, 128)
			const seeded = 400
			for i := 0; i < seeded; i++ {
				if _, _, err := bk.Produce(ctx, "jobs", nil, value); err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			start := time.Now()
			processed := 0
			for {
				deliveries, err := q.Pull(ctx, "jobs", 10, 30*time.Second)
				if err != nil {
					b.Fatal(err)
				}
				if len(deliveries) == 0 {
					break
				}
				if v.batchAcks {
					ids := make([]string, 0, len(deliveries))
					for _, d := range deliveries {
						ids = append(ids, d.DeliveryID)
					}
					res, err := q.AckBatch(ctx, "jobs", ids)
					if err != nil {
						b.Fatal(err)
					}
					processed += res.Acked
					continue
				}
				for _, d := range deliveries {
					if err := q.Ack(ctx, "jobs", d.DeliveryID); err != nil {
						b.Fatal(err)
					}
					processed++
				}
			}
			elapsed := time.Since(start)
			b.ReportMetric(float64(processed)/elapsed.Seconds(), "deliveries/s")
		})
	}
}

// BenchmarkFetchParallel measures concurrent fetch throughput: many
// goroutines reading different partitions of one topic at once. Before the
// broker released its mutex before touching the partition log, every fetch
// serialized on the broker-wide mutex even across partitions, capping how
// many consumers could read in parallel.
func BenchmarkFetchParallel(b *testing.B) {
	dir := b.TempDir()
	reg := meta.NewRegistry()
	if err := reg.Create("bench", 8); err != nil {
		b.Fatal(err)
	}
	bk, err := broker.NewWithDir(reg, dir)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = bk.Close() })
	ctx := context.Background()
	value := make([]byte, 256)
	const seeded = 8_000
	for i := 0; i < seeded; i++ {
		if _, _, err := bk.Produce(ctx, "bench", nil, value); err != nil {
			b.Fatal(err)
		}
	}
	const perPartition = seeded / 8    // 1000 records per partition (keyless rr)
	const batches = perPartition / 100 // 100-record batches per partition
	b.SetParallelism(8)
	b.ReportAllocs()
	b.ResetTimer()
	var workerID, fetchID int64
	b.RunParallel(func(pb *testing.PB) {
		p := int32(atomic.AddInt64(&workerID, 1)-1) % 8
		for pb.Next() {
			off := int64(atomic.AddInt64(&fetchID, 1)-1) % batches * 100
			if _, err := bk.Fetch(ctx, "bench", p, off, 100); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// BenchmarkQueuePullAck measures queue-mode consume throughput: pull a
// batch of 10 and ack each delivery. Runs against an in-memory broker to
// isolate the queue machinery from fsync; the durable path is bounded by
// the produce/commit fsync already measured in BenchmarkProduce.
// Reports aggregate deliveries processed per second.
func BenchmarkQueuePullAck(b *testing.B) {
	reg := meta.NewRegistry()
	if err := reg.CreateWithMode("jobs", 1, meta.ModeQueue); err != nil {
		b.Fatal(err)
	}
	bk := broker.New(reg)
	b.Cleanup(func() { _ = bk.Close() })
	q, err := queue.NewManager(reg, bk, "")
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	value := make([]byte, 128)
	const seeded = 1_000
	for i := 0; i < seeded; i++ {
		if _, _, err := bk.Produce(ctx, "jobs", nil, value); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	start := time.Now()
	processed := 0
	// NOTE: seed is deliberately small. MemLog-backed __consumer_offsets
	// re-folds on every commit (see internal/groups/offset_store.go),
	// making commit cost O(commits so far); a large seed measures the
	// re-fold pathology, not the queue code path. Run length is capped by
	// the seed rather than b.N.
	for {
		deliveries, err := q.Pull(ctx, "jobs", 10, 30*time.Second)
		if err != nil {
			b.Fatal(err)
		}
		if len(deliveries) == 0 {
			break
		}
		for _, d := range deliveries {
			if err := q.Ack(ctx, "jobs", d.DeliveryID); err != nil {
				b.Fatal(err)
			}
			processed++
		}
	}
	elapsed := time.Since(start)
	b.ReportMetric(float64(processed)/elapsed.Seconds(), "deliveries/s")
}
