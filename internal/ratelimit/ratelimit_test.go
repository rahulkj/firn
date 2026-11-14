package ratelimit

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBucketBurstThenRateLimited(t *testing.T) {
	b := New(3, 1) // capacity 3, 1 token/sec refill
	for i := 0; i < 3; i++ {
		if !b.Allow("k") {
			t.Fatalf("call %d should be allowed", i+1)
		}
	}
	if b.Allow("k") {
		t.Fatal("fourth immediate call must be refused after burst")
	}
}

func TestBucketRefillsOverTime(t *testing.T) {
	b := New(1, 100) // 1 token capacity, 100 tokens/s (10ms per token)
	if !b.Allow("k") {
		t.Fatal("first call must be allowed")
	}
	if b.Allow("k") {
		t.Fatal("immediate second call must be refused")
	}
	time.Sleep(20 * time.Millisecond) // >10ms per token
	if !b.Allow("k") {
		t.Fatal("after refill, call must be allowed")
	}
}

func TestBucketCapsAtCapacity(t *testing.T) {
	b := New(2, 1000) // refill fast; should never accrue beyond capacity
	// Drain fully.
	for i := 0; i < 2; i++ {
		b.Allow("k")
	}
	time.Sleep(50 * time.Millisecond) // plenty of refill time
	// Capacity still 2, not ~51.
	for i := 0; i < 2; i++ {
		if !b.Allow("k") {
			t.Fatalf("call %d after refill must be allowed (capacity bound)", i+1)
		}
	}
	if b.Allow("k") {
		t.Fatal("must not exceed capacity even after idle refill")
	}
}

func TestBucketKeysAreIndependent(t *testing.T) {
	b := New(1, 1)
	if !b.Allow("a") {
		t.Fatal("first for a must be allowed")
	}
	if b.Allow("a") {
		t.Fatal("second for a must be refused")
	}
	if !b.Allow("b") {
		t.Fatal("first for b must be allowed (independent key)")
	}
}

func TestBucketConcurrentAccessSafe(t *testing.T) {
	b := New(100, 1000)
	var wg sync.WaitGroup
	var allowed int64
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b.Allow("k") {
				atomic.AddInt64(&allowed, 1)
			}
		}()
	}
	wg.Wait()
	if allowed > 100 {
		t.Fatalf("allowed %d, want <=100 (capacity)", allowed)
	}
	if allowed == 0 {
		t.Fatal("none allowed; expected at most 100")
	}
}

func TestSlidingWindowBurstThenRefused(t *testing.T) {
	w := NewSlidingWindow(3, time.Second)
	for i := 0; i < 3; i++ {
		if !w.Allow("ip") {
			t.Fatalf("call %d must be allowed", i+1)
		}
	}
	if w.Allow("ip") {
		t.Fatal("fourth call in window must be refused")
	}
}

func TestSlidingWindowEvictsExpiredEntries(t *testing.T) {
	w := NewSlidingWindow(2, 50*time.Millisecond)
	if !w.Allow("ip") {
		t.Fatal("first must be allowed")
	}
	if !w.Allow("ip") {
		t.Fatal("second must be allowed")
	}
	time.Sleep(60 * time.Millisecond)
	if !w.Allow("ip") {
		t.Fatal("after window, a new hit must be allowed")
	}
}

func TestSlidingWindowKeysAreIndependent(t *testing.T) {
	w := NewSlidingWindow(1, time.Second)
	if !w.Allow("a") {
		t.Fatal("first for a must be allowed")
	}
	if !w.Allow("b") {
		t.Fatal("first for b must be allowed (independent)")
	}
	if w.Allow("a") {
		t.Fatal("second for a must be refused")
	}
}

func TestSlidingWindowZeroMaxIsUnlimited(t *testing.T) {
	w := NewSlidingWindow(0, time.Second)
	for i := 0; i < 50; i++ {
		if !w.Allow("ip") {
			t.Fatalf("call %d must be allowed when max is 0 (unlimited)", i+1)
		}
	}
}
