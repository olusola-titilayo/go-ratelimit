// Command server is a tiny demo: 5 requests burst, refilling 1 per second, per client IP.
package main

import (
	"fmt"
	"log"
	"net/http"

	ratelimit "github.com/olusola-titilayo/go-ratelimit"
)

func main() {
	limiter, err := ratelimit.NewTokenBucket(5, 1, nil)
	if err != nil {
		log.Fatal(err)
	}
	hello := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "hello")
	})
	log.Println("listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", ratelimit.Middleware(limiter, ratelimit.ByRemoteIP, hello)))
}
