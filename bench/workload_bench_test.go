package bench

// Workload-shaped capacity benchmark: produce and fetch with real payload
// classes, from tiny audit events to multi-MiB report files.
//
// Sizing rationale:
//   256 B  — the classic "audit KV / small JSON event"
//   4 KiB  — a typical protobuf event
//   64 KiB — a large event / small document
//   1 MiB  — a blob reference / small file
//   10 MiB — a report file
//
// Notes:
//   - produce-durable is the v0.4.0 default (fsync-per-append): it
//     completely dominates small-payload numbers.
//   - produce-window runs with -benchmem and the ADR-014 group commit
//     (2 ms window, 1 MiB cap) to show what the window buys for each
//     class. Not shipped; pending storage review.
//   - fetch seeds the durable log at the minimum viable seed for each class
//     and reads 128-record windows (or the whole thing for big files).
//   - b.SetBytes is set in every subtest so output prints MiB/s directly.
//
// Command: go test -bench=Workload -benchmem -benchtime=2s ./bench/ -run=^$

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rahulkj/firn/internal/broker"
	"github.com/rahulkj/firn/internal/meta"
)

var workloadCases = []struct {
	name    string
	payload int
	seeded  int // fetch seed; produce benchmarks use fsync-time-limited b.N
}{
	{"256B", 256, 2_000},
	{"4KiB", 4 << 10, 1_000},
	{"64KiB", 64 << 10, 200},
	{"1MiB", 1 << 20, 60},
	{"10MiB", 10 << 20, 12},
}

func BenchmarkWorkload(b *testing.B) {
	for _, tc := range workloadCases {
		value := make([]byte, tc.payload)

		b.Run(fmt.Sprintf("produce-durable/%s", tc.name), func(b *testing.B) {
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
			b.SetBytes(int64(len(value)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := bk.Produce(ctx, "bench", []byte(fmt.Sprintf("k%d", i)), value); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("produce-window/%s", tc.name), func(b *testing.B) {
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
			b.SetBytes(int64(len(value)))
			b.SetParallelism(256)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					i++
					if _, _, err := bk.Produce(ctx, "bench", []byte(fmt.Sprintf("k%d", i)), value); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})

		b.Run(fmt.Sprintf("fetch/%s", tc.name), func(b *testing.B) {
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
			for i := 0; i < tc.seeded; i++ {
				// Keyless produce rotates partition assignment round-robin,
				// giving a deterministic per-partition spread; offset reuse
				// is then bounded by seeded/3.
				if _, _, err := bk.Produce(ctx, "bench", nil, value); err != nil {
					b.Fatal(err)
				}
			}
			perPartition := tc.seeded / 3
			maxBatch := 128
			if maxBatch > perPartition {
				maxBatch = perPartition
			}
			if maxBatch < 1 {
				maxBatch = 1
			}
			b.SetBytes(int64(len(value)) * int64(maxBatch))
			b.ResetTimer()
			span := int64(perPartition - maxBatch)
			for i := 0; i < b.N; i++ {
				off := int64(0)
				if span > 0 {
					off = int64(i*maxBatch) % span
				}
				if _, err := bk.Fetch(ctx, "bench", int32(i%3), off, maxBatch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
