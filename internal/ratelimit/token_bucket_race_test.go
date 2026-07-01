package ratelimit

import (
	"context"
	"sync"
	"testing"
)

func TestTokenBucketConcurrent(_ *testing.T) {

	tb := NewTokenBucket(100, 100)

	var wg sync.WaitGroup

	for i := 0; i < 1000; i++ {

		wg.Add(1)

		go func() {
			defer wg.Done()

			tb.Allow(context.Background())
		}()
	}

	wg.Wait()
}
