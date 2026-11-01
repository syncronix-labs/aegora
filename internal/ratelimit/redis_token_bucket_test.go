package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newRedisTokenBucketTest(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()

	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	return s, rdb
}

func TestRedisTokenBucket_BurstCapacity(t *testing.T) {
	_, rdb := newRedisTokenBucketTest(t)
	ctx := context.Background()
	limiter := NewRedisTokenBucket(rdb, 3, 0.001)

	for i := 0; i < 3; i++ {
		ok, err := limiter.AllowFor(ctx, "user:burst")
		if err != nil || !ok {
			t.Fatalf("request %d: expected allow, got ok=%v err=%v", i, ok, err)
		}
	}

	if ok, err := limiter.AllowFor(ctx, "user:burst"); err != nil || ok {
		t.Fatalf("expected deny once the burst is spent, got ok=%v err=%v", ok, err)
	}

	// A different key gets its own bucket.
	if ok, err := limiter.AllowFor(ctx, "user:other"); err != nil || !ok {
		t.Fatalf("expected a fresh key to allow, got ok=%v err=%v", ok, err)
	}
}

func TestRedisTokenBucket_RefillsOverTime(t *testing.T) {
	s, rdb := newRedisTokenBucketTest(t)
	ctx := context.Background()
	limiter := NewRedisTokenBucket(rdb, 2, 10)
	start := time.Now()

	for i := 0; i < 2; i++ {
		if ok, err := limiter.AllowFor(ctx, "user:refill"); err != nil || !ok {
			t.Fatalf("request %d: expected allow, got ok=%v err=%v", i, ok, err)
		}
	}
	if ok, err := limiter.AllowFor(ctx, "user:refill"); err != nil || ok {
		t.Fatalf("expected deny with an empty bucket, got ok=%v err=%v", ok, err)
	}

	// 150ms at 10 tokens/s refills 1.5 tokens, enough for exactly one request.
	s.SetTime(start.Add(150 * time.Millisecond))

	if ok, err := limiter.AllowFor(ctx, "user:refill"); err != nil || !ok {
		t.Fatalf("expected allow after refill, got ok=%v err=%v", ok, err)
	}
	if ok, err := limiter.AllowFor(ctx, "user:refill"); err != nil || ok {
		t.Fatalf("expected deny after spending the refilled token, got ok=%v err=%v", ok, err)
	}
}

func TestRedisTokenBucket_RefillDoesNotExceedCapacity(t *testing.T) {
	s, rdb := newRedisTokenBucketTest(t)
	ctx := context.Background()
	limiter := NewRedisTokenBucket(rdb, 2, 10)
	start := time.Now()

	if ok, err := limiter.AllowFor(ctx, "user:capped"); err != nil || !ok {
		t.Fatalf("expected first request to allow, got ok=%v err=%v", ok, err)
	}

	// A long idle period refills at most to capacity, not beyond it.
	s.SetTime(start.Add(10 * time.Second))

	for i := 0; i < 2; i++ {
		if ok, err := limiter.AllowFor(ctx, "user:capped"); err != nil || !ok {
			t.Fatalf("post-idle request %d: expected allow, got ok=%v err=%v", i, ok, err)
		}
	}
	if ok, err := limiter.AllowFor(ctx, "user:capped"); err != nil || ok {
		t.Fatalf("expected deny after spending the capped refill, got ok=%v err=%v", ok, err)
	}
}

func TestRedisTokenBucket_DistributedBudgetSharing(t *testing.T) {
	_, rdb := newRedisTokenBucketTest(t)
	ctx := context.Background()

	// Two limiter instances simulate two gateway replicas sharing one Redis.
	limiter1 := NewRedisTokenBucket(rdb, 2, 0.001)
	limiter2 := NewRedisTokenBucket(rdb, 2, 0.001)

	if ok, err := limiter1.AllowFor(ctx, "user:distributed"); err != nil || !ok {
		t.Fatalf("limiter1 expected allow, got ok=%v err=%v", ok, err)
	}
	if ok, err := limiter2.AllowFor(ctx, "user:distributed"); err != nil || !ok {
		t.Fatalf("limiter2 expected allow, got ok=%v err=%v", ok, err)
	}

	if ok, err := limiter1.AllowFor(ctx, "user:distributed"); err != nil || ok {
		t.Fatalf("limiter1 expected deny when the shared bucket is empty, got ok=%v err=%v", ok, err)
	}
	if ok, err := limiter2.AllowFor(ctx, "user:distributed"); err != nil || ok {
		t.Fatalf("limiter2 expected deny when the shared bucket is empty, got ok=%v err=%v", ok, err)
	}
}

func TestRedisTokenBucket_FailsOpenWhenRedisIsDown(t *testing.T) {
	s, rdb := newRedisTokenBucketTest(t)
	limiter := NewRedisTokenBucket(rdb, 1, 1)
	bound := limiter.ForKey("user:outage")

	s.Close()

	if !bound.Allow(context.Background()) {
		t.Fatal("expected fail-open when Redis is unreachable")
	}
}

func TestRedisTokenBucket_Limit(t *testing.T) {
	_, rdb := newRedisTokenBucketTest(t)
	limiter := NewRedisTokenBucket(rdb, 7, 1)

	bound, ok := limiter.ForKey("user:limit").(interface{ Limit() int })
	if !ok {
		t.Fatal("expected the bound limiter to expose Limit()")
	}
	if got := bound.Limit(); got != 7 {
		t.Fatalf("Limit() = %d, want 7", got)
	}
}

func TestRedisTokenBucket_RejectsNonPositiveArguments(t *testing.T) {
	_, rdb := newRedisTokenBucketTest(t)

	tests := []struct {
		name  string
		build func()
	}{
		{name: "zero capacity", build: func() { NewRedisTokenBucket(rdb, 0, 1) }},
		{name: "negative capacity", build: func() { NewRedisTokenBucket(rdb, -1, 1) }},
		{name: "zero refill", build: func() { NewRedisTokenBucket(rdb, 1, 0) }},
		{name: "negative refill", build: func() { NewRedisTokenBucket(rdb, 1, -1) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic for a non-positive argument")
				}
			}()
			tt.build()
		})
	}
}
