package bench

import (
	"context"
	"fmt"
	"testing"

	"github.com/rahulkj/firn/internal/broker"
	"github.com/rahulkj/firn/internal/meta"
)

// BenchmarkFetch measures fetch throughput: prefetch 10k records into a
// durable log, then read them back in order.
func BenchmarkFetch(b *testing.B) {
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
	const seeded = 10_000
	for i := 0; i < seeded; i++ {
		if _, _, err := bk.Produce(ctx, "bench", []byte(fmt.Sprintf("key-%d", i)), value); err != nil {
			b.Fatal(err)
		}
	}
	const perPartition = seeded / 3
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// Per-partition seeded count is seeded/3; keep the offset inside the
		// partition's high watermark (was: offset up to seeded-128 vs a
		// per-partition watermark of ~3322 — out-of-watermark at large b.N).
		off := int64(i % (perPartition - 128))
		if _, err := bk.Fetch(ctx, "bench", int32(i%3), off, 128); err != nil {
			b.Fatal(err)
		}
	}
}
