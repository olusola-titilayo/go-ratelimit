package ratelimit

import (
	"math"
	"net"
	"net/http"
	"strconv"
)

// KeyFunc extracts the rate-limit key from a request.
type KeyFunc func(r *http.Request) string

// ByRemoteIP keys requests by the client IP in r.RemoteAddr. It does not
// trust X-Forwarded-For, which clients can forge.
func ByRemoteIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// Middleware rejects requests over the limit with 429 Too Many Requests and a
// Retry-After header (whole seconds, rounded up).
func Middleware(l Limiter, key KeyFunc, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d := l.Allow(key(r))
		w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(d.Remaining))
		if !d.Allowed {
			secs := int(math.Ceil(d.RetryAfter.Seconds()))
			if secs < 1 {
				secs = 1
			}
			w.Header().Set("Retry-After", strconv.Itoa(secs))
			http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}
