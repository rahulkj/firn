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
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		off := int64(i % (seeded - 128))
		if _, err := bk.Fetch(ctx, "bench", int32(i%3), off, 128); err != nil {
			b.Fatal(err)
		}
	}
}
