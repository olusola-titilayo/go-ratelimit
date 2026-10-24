package ratelimit

import "time"

// Clock returns the current time. Inject a fake clock in tests to make
// limiter behaviour deterministic.
type Clock func() time.Time

func systemClock() time.Time { return time.Now() }
