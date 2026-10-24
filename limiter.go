// Package ratelimit provides per-key rate limiters (token bucket and sliding
// window log) that are safe for concurrent use, plus HTTP middleware.
package ratelimit

import "time"

// Decision is the result of asking a limiter for permission.
type Decision struct {
	Allowed bool
	// Remaining is how many more requests the key could make right now.
	Remaining int
	// RetryAfter is how long to wait before the next request can succeed.
	// It is zero when Allowed is true.
	RetryAfter time.Duration
}

// Limiter decides whether a request identified by key may proceed.
type Limiter interface {
	Allow(key string) Decision
}
