package bench

import (
	"context"
	"fmt"
	"testing"

	"github.com/rahulkj/firn/internal/broker"
	"github.com/rahulkj/firn/internal/meta"
)

func benchBroker(b *testing.B, dir string) *broker.Broker {
	_ = b
	reg := meta.NewRegistry()
	fb, err := broker.NewWithDir(reg, dir)
	if err != nil {
		panic(err)
	}
	b.Cleanup(func() { _ = fb.Close() })
	// Register a 3-partition topic once; Create is write-through but the
	// setup cost is amortized out of the loop.
	if err := reg.Create("bench", 3); err != nil {
		panic(err)
	}
	return fb
}

// BenchmarkProduce measures produce throughput to a durable (fsync-per-append)
// file log — the honest number, not the memory-only one.
func BenchmarkProduce(b *testing.B) {
	bdir := b.TempDir()
	bk := benchBroker(b, bdir)
	ctx := context.Background()
	value := make([]byte, 256)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, _, err := bk.Produce(ctx, "bench", []byte(fmt.Sprintf("key-%d", i)), value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProduceMemory(b *testing.B) {
	reg := meta.NewRegistry()
	if err := reg.Create("bench", 3); err != nil {
		b.Fatal(err)
	}
	bk := broker.New(reg)
	ctx := context.Background()
	value := make([]byte, 256)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, _, err := bk.Produce(ctx, "bench", []byte(fmt.Sprintf("key-%d", i)), value); err != nil {
			b.Fatal(err)
		}
	}
}
