package ratelimit

import (
	"fmt"
	"sync"
	"time"
)

// SlidingWindow allows at most Limit requests per key in any rolling Window.
// Unlike a fixed window it has no burst at window boundaries, at the cost of
// storing one timestamp per allowed request.
type SlidingWindow struct {
	limit  int
	window time.Duration
	now    Clock

	mu   sync.Mutex
	logs map[string][]time.Time
}

// NewSlidingWindow returns a limiter allowing limit requests per window.
// A nil clock uses the system clock.
func NewSlidingWindow(limit int, window time.Duration, clock Clock) (*SlidingWindow, error) {
	if limit < 1 {
		return nil, fmt.Errorf("ratelimit: limit must be >= 1, got %d", limit)
	}
	if window <= 0 {
		return nil, fmt.Errorf("ratelimit: window must be positive, got %v", window)
	}
	if clock == nil {
		clock = systemClock
	}
	return &SlidingWindow{limit: limit, window: window, now: clock, logs: make(map[string][]time.Time)}, nil
}

// Allow records a request for key if it fits in the current window.
func (sw *SlidingWindow) Allow(key string) Decision {
	now := sw.now()
	cutoff := now.Add(-sw.window)

	sw.mu.Lock()
	defer sw.mu.Unlock()

	log := sw.logs[key]
	// Drop timestamps that have left the window. The log is sorted.
	i := 0
	for i < len(log) && !log[i].After(cutoff) {
		i++
	}
	log = log[i:]

	if len(log) < sw.limit {
		log = append(log, now)
		sw.logs[key] = log
		return Decision{Allowed: true, Remaining: sw.limit - len(log)}
	}
	sw.logs[key] = log
	return Decision{Allowed: false, RetryAfter: log[0].Add(sw.window).Sub(now)}
}
