package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func mustTB(t *testing.T, capacity int, rate float64, c *fakeClock) *TokenBucket {
	t.Helper()
	tb, err := NewTokenBucket(capacity, rate, c.Now)
	if err != nil {
		t.Fatal(err)
	}
	return tb
}

func TestTokenBucketBurstThenRefill(t *testing.T) {
	c := newFakeClock()
	tb := mustTB(t, 3, 1, c)

	for i := 0; i < 3; i++ {
		if d := tb.Allow("a"); !d.Allowed || d.Remaining != 2-i {
			t.Fatalf("request %d: got %+v", i, d)
		}
	}
	d := tb.Allow("a")
	if d.Allowed || d.RetryAfter != time.Second {
		t.Fatalf("4th request should be denied with 1s retry, got %+v", d)
	}

	c.Advance(500 * time.Millisecond)
	if d := tb.Allow("a"); d.Allowed || d.RetryAfter != 500*time.Millisecond {
		t.Fatalf("half refilled: got %+v", d)
	}
	c.Advance(500 * time.Millisecond)
	if !tb.Allow("a").Allowed {
		t.Fatal("expected one token after 1s")
	}
}

func TestTokenBucketNeverExceedsCapacity(t *testing.T) {
	c := newFakeClock()
	tb := mustTB(t, 2, 10, c)
	tb.Allow("a")
	c.Advance(time.Hour)
	allowed := 0
	for i := 0; i < 5; i++ {
		if tb.Allow("a").Allowed {
			allowed++
		}
	}
	if allowed != 2 {
		t.Fatalf("allowed %d after long idle, want capacity 2", allowed)
	}
}

func TestTokenBucketKeysAreIndependent(t *testing.T) {
	c := newFakeClock()
	tb := mustTB(t, 1, 1, c)
	if !tb.Allow("a").Allowed || !tb.Allow("b").Allowed {
		t.Fatal("first request per key must be allowed")
	}
	if tb.Allow("a").Allowed {
		t.Fatal("key a should be exhausted")
	}
}

func TestTokenBucketClockGoingBackwards(t *testing.T) {
	c := newFakeClock()
	tb := mustTB(t, 1, 1, c)
	tb.Allow("a")
	c.Advance(-time.Minute)
	if tb.Allow("a").Allowed {
		t.Fatal("a backwards clock must not mint tokens")
	}
}

func TestTokenBucketPrune(t *testing.T) {
	c := newFakeClock()
	tb := mustTB(t, 2, 1, c)
	tb.Allow("idle")
	tb.Allow("busy")
	tb.Allow("busy")
	c.Advance(1500 * time.Millisecond) // idle full since 0.5s ago, busy not yet full
	if n := tb.Prune(time.Second); n != 0 {
		t.Fatalf("pruned %d, want 0", n)
	}
	c.Advance(time.Second)
	if n := tb.Prune(time.Second); n != 1 || tb.Len() != 1 {
		t.Fatalf("pruned %d (len %d), want only the idle key", n, tb.Len())
	}
}

func TestConstructorsRejectBadConfig(t *testing.T) {
	if _, err := NewTokenBucket(0, 1, nil); err == nil {
		t.Error("capacity 0 accepted")
	}
	if _, err := NewTokenBucket(1, 0, nil); err == nil {
		t.Error("rate 0 accepted")
	}
	if _, err := NewSlidingWindow(0, time.Second, nil); err == nil {
		t.Error("limit 0 accepted")
	}
	if _, err := NewSlidingWindow(1, 0, nil); err == nil {
		t.Error("window 0 accepted")
	}
}

func TestSlidingWindowNoBoundaryBurst(t *testing.T) {
	c := newFakeClock()
	sw, _ := NewSlidingWindow(2, time.Second, c.Now)

	sw.Allow("a") // t=0
	c.Advance(900 * time.Millisecond)
	sw.Allow("a") // t=0.9
	c.Advance(200 * time.Millisecond)
	// t=1.1: the t=0 request has left the window, the t=0.9 one has not.
	if d := sw.Allow("a"); !d.Allowed || d.Remaining != 0 {
		t.Fatalf("t=1.1: got %+v", d)
	}
	d := sw.Allow("a")
	if d.Allowed || d.RetryAfter != 800*time.Millisecond {
		t.Fatalf("t=1.1 second request: got %+v, want retry 800ms", d)
	}
}

func TestConcurrentAllowIsExact(t *testing.T) {
	c := newFakeClock()
	tb := mustTB(t, 100, 1, c)
	sw, _ := NewSlidingWindow(100, time.Minute, c.Now)

	for name, l := range map[string]Limiter{"token": tb, "sliding": sw} {
		var allowed int64
		var wg sync.WaitGroup
		for g := 0; g < 50; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 10; i++ {
					if l.Allow("shared").Allowed {
						atomic.AddInt64(&allowed, 1)
					}
				}
			}()
		}
		wg.Wait()
		if allowed != 100 {
			t.Errorf("%s: allowed %d of 500 concurrent requests, want exactly 100", name, allowed)
		}
	}
}

func TestMiddleware(t *testing.T) {
	c := newFakeClock()
	tb := mustTB(t, 1, 0.5, c)
	h := Middleware(tb, ByRemoteIP, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "203.0.113.7:5555"

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("first request: %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "2" {
		t.Fatalf("second request: %d Retry-After=%q", rec.Code, rec.Header().Get("Retry-After"))
	}

	other := httptest.NewRequest(http.MethodGet, "/", nil)
	other.RemoteAddr = "198.51.100.1:1"
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, other)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("different IP should not be limited: %d", rec.Code)
	}
}

func BenchmarkTokenBucketAllow(b *testing.B) {
	tb, _ := NewTokenBucket(1_000_000, 1_000_000, nil)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tb.Allow("k")
		}
	})
}

func BenchmarkSlidingWindowAllow(b *testing.B) {
	sw, _ := NewSlidingWindow(100, time.Millisecond, nil)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sw.Allow("k")
		}
	})
}
