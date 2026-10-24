package ratelimit

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// TokenBucket allows bursts of up to Capacity requests and refills at Rate
// tokens per second. Each key has its own bucket.
type TokenBucket struct {
	capacity float64
	rate     float64 // tokens per second
	now      Clock

	mu      sync.Mutex
	buckets map[string]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

// NewTokenBucket returns a limiter with the given burst capacity and refill
// rate in tokens per second. A nil clock uses the system clock.
func NewTokenBucket(capacity int, ratePerSecond float64, clock Clock) (*TokenBucket, error) {
	if capacity < 1 {
		return nil, fmt.Errorf("ratelimit: capacity must be >= 1, got %d", capacity)
	}
	if !(ratePerSecond > 0) || math.IsInf(ratePerSecond, 0) {
		return nil, fmt.Errorf("ratelimit: rate must be a positive finite number, got %v", ratePerSecond)
	}
	if clock == nil {
		clock = systemClock
	}
	return &TokenBucket{
		capacity: float64(capacity),
		rate:     ratePerSecond,
		now:      clock,
		buckets:  make(map[string]*bucket),
	}, nil
}

// Allow consumes one token for key if one is available.
func (tb *TokenBucket) Allow(key string) Decision {
	now := tb.now()

	tb.mu.Lock()
	defer tb.mu.Unlock()

	b, ok := tb.buckets[key]
	if !ok {
		b = &bucket{tokens: tb.capacity, last: now}
		tb.buckets[key] = b
	}

	// Refill for the time elapsed. Ignore a clock that moved backwards.
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens = math.Min(tb.capacity, b.tokens+elapsed*tb.rate)
		b.last = now
	}

	if b.tokens >= 1 {
		b.tokens--
		return Decision{Allowed: true, Remaining: int(b.tokens)}
	}
	wait := time.Duration((1 - b.tokens) / tb.rate * float64(time.Second))
	return Decision{Allowed: false, Remaining: 0, RetryAfter: wait}
}

// Len reports how many keys are being tracked.
func (tb *TokenBucket) Len() int {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return len(tb.buckets)
}

// Prune forgets keys whose buckets have been full for at least idle, which
// bounds memory when many distinct keys are seen.
func (tb *TokenBucket) Prune(idle time.Duration) int {
	now := tb.now()
	tb.mu.Lock()
	defer tb.mu.Unlock()
	removed := 0
	for k, b := range tb.buckets {
		refilled := b.tokens + now.Sub(b.last).Seconds()*tb.rate
		fullSince := b.last.Add(time.Duration((tb.capacity - b.tokens) / tb.rate * float64(time.Second)))
		if refilled >= tb.capacity && now.Sub(fullSince) >= idle {
			delete(tb.buckets, k)
			removed++
		}
	}
	return removed
}
