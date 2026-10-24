# go-ratelimit

![CI](https://github.com/olusola-titilayo/go-ratelimit/actions/workflows/ci.yml/badge.svg)

Per-key rate limiters for Go — **token bucket** and **sliding window log** — safe for concurrent
use, with `net/http` middleware. Standard library only.

```go
limiter, _ := ratelimit.NewTokenBucket(5, 1, nil) // burst 5, refill 1 token/second
http.ListenAndServe(":8080", ratelimit.Middleware(limiter, ratelimit.ByRemoteIP, handler))
```

Over the limit, clients get `429 Too Many Requests` with a `Retry-After` header.

## Which algorithm?

| | Token bucket | Sliding window log |
|---|---|---|
| Allows short bursts | Yes, up to capacity | No |
| Boundary burst problem | — | None (unlike fixed windows) |
| Memory per key | O(1) | O(limit) timestamps |
| Good for | APIs that tolerate bursts | Strict "N per window" quotas |

## API

```go
type Decision struct {
    Allowed    bool
    Remaining  int
    RetryAfter time.Duration
}

type Limiter interface { Allow(key string) Decision }

func NewTokenBucket(capacity int, ratePerSecond float64, clock Clock) (*TokenBucket, error)
func NewSlidingWindow(limit int, window time.Duration, clock Clock) (*SlidingWindow, error)
func (tb *TokenBucket) Prune(idle time.Duration) int // drop idle keys to bound memory
func Middleware(l Limiter, key KeyFunc, next http.Handler) http.Handler
```

## Design notes

- **Deterministic tests:** limiters take an injectable `Clock`, so tests advance a fake clock
  instead of sleeping.
- **Exactness under contention:** 500 concurrent requests against a limit of 100 allow exactly
  100 — checked under `go test -race`.
- **Robustness:** a clock that jumps backwards never mints tokens; invalid configuration is
  rejected at construction.
- **Security:** `ByRemoteIP` deliberately ignores `X-Forwarded-For`, which clients can forge.
  Behind a trusted proxy, supply your own `KeyFunc`.

## Run

```bash
go test -race ./...
go test -run=^$ -bench=. .
go run ./examples/server   # then: for i in $(seq 7); do curl -i localhost:8080; done
```
