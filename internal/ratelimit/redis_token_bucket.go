package ratelimit

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisTokenBucketLuaScript is the Lua script executed atomically on Redis.
// It queries Redis TIME so every gateway replica shares one clock, refills the
// bucket by the elapsed time since the last request, spends one token when one
// is available, and stores the result back under a TTL long enough to rebuild
// a full bucket if the key were evicted.
const redisTokenBucketLuaScript = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate_per_ms = tonumber(ARGV[2])

local redis_time = redis.call('TIME')
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)

local data = redis.call('HMGET', key, 'tokens', 'last_ms')
local tokens = tonumber(data[1])
local last_ms = tonumber(data[2])

if tokens == nil then
	tokens = capacity
	last_ms = now_ms
end

local elapsed = now_ms - last_ms
if elapsed > 0 then
	tokens = math.min(capacity, tokens + elapsed * rate_per_ms)
end

local allowed = 0
if tokens >= 1 then
	tokens = tokens - 1
	allowed = 1
end

-- Fixed-point formatting keeps token counts readable by Lua implementations whose
-- tonumber rejects exponent notation.
redis.call('HSET', key, 'tokens', string.format('%.9f', tokens), 'last_ms', tostring(now_ms))

local ttl_ms = math.ceil(capacity / rate_per_ms) + 1000
redis.call('PEXPIRE', key, ttl_ms)

return allowed
`

// RedisTokenBucket implements a distributed token bucket rate limiter backed by Redis.
// It executes an atomic Lua script using server time from Redis (TIME) so replicas with
// skewed clocks still share one refill timeline.
type RedisTokenBucket struct {
	client          *redis.Client
	keyPrefix       string
	capacity        int
	refillPerSecond float64
	failOpen        bool
	script          *redis.Script
}

// NewRedisTokenBucket constructs a Redis-backed token bucket.
// By default, keyPrefix is "aegora:rl:tb" and failOpen is true.
// It panics if capacity or refillPerSecond is non-positive.
func NewRedisTokenBucket(client *redis.Client, capacity int, refillPerSecond float64) *RedisTokenBucket {
	if capacity <= 0 {
		panic("ratelimit: capacity must be positive")
	}
	if refillPerSecond <= 0 {
		panic("ratelimit: refillPerSecond must be positive")
	}

	return &RedisTokenBucket{
		client:          client,
		keyPrefix:       "aegora:rl:tb",
		capacity:        capacity,
		refillPerSecond: refillPerSecond,
		failOpen:        true,
		script:          redis.NewScript(redisTokenBucketLuaScript),
	}
}

// AllowFor reports whether a request is permitted for the specified key.
// It executes a single atomic Lua script against the namespaced Redis key.
func (b *RedisTokenBucket) AllowFor(ctx context.Context, key string) (bool, error) {
	redisKey := b.keyPrefix + ":" + key
	ratePerMS := b.refillPerSecond / float64(time.Second/time.Millisecond)

	// Format with 'f' so the argument is never sent in exponent notation; Redis Lua
	// parses both, but some embedded Lua implementations only accept decimal form.
	res, err := b.script.Run(ctx, b.client, []string{redisKey}, b.capacity,
		strconv.FormatFloat(ratePerMS, 'f', -1, 64)).Int()
	if err != nil {
		return false, err
	}

	return res == 1, nil
}

// ForKey returns a keyless Limiter bound to the specified key.
// On Redis error it fails open (allows the request) per the failOpen flag.
//
// Fail-Open Rationale:
// A dead, unreachable, or degraded Redis cluster must never take down the gateway
// or block incoming user traffic. The accepted operational tradeoff is that rate
// limits are unenforced during an outage in order to preserve service availability.
func (b *RedisTokenBucket) ForKey(key string) Limiter {
	return &redisTokenBucketBound{
		bucket: b,
		key:    key,
	}
}

type redisTokenBucketBound struct {
	bucket *RedisTokenBucket
	key    string
}

var _ Limiter = (*redisTokenBucketBound)(nil)

// Allow reports whether a request is permitted, failing open if configured and Redis errors.
func (r *redisTokenBucketBound) Allow(ctx context.Context) bool {
	allowed, err := r.bucket.AllowFor(ctx, r.key)
	if err != nil {
		return r.bucket.failOpen
	}
	return allowed
}

// Limit returns the configured bucket capacity.
func (r *redisTokenBucketBound) Limit() int {
	return r.bucket.capacity
}
