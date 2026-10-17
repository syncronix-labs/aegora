package cache

import (
	"bytes"
	"context"
	"encoding/gob"
	"time"

	"github.com/redis/go-redis/v9"
)

func init() {
	gob.Register(Entry{})
}

const defaultTieredKeyPrefix = "aegora:cache:v1:"

// TieredCache implements Store as a two-tier cache combining an in-memory L1 cache
// (*Cache) backed by a distributed L2 Redis cache (*redis.Client).
//
// Read flow: L1 hit -> return. L1 miss -> L2 GET: on hit, gob-decode, verify freshness,
// backfill into L1 (re-stamped to l1TTL), return. L2 misses (redis.Nil), decode failures,
// or communication errors degrade silently to misses without failing the request.
//
// Write flow: write-through to both tiers. L2 errors are swallowed (L1 still written)
// and fail quiet.
//
// Invalidation: Invalidate removes the key from both tiers. InvalidatePrefix scans L1
// and executes an L2 SCAN loop with batched DELs as best-effort.
//
// Note: CacheMiddleware (defined in cache.go) requires zero modifications to support
// TieredCache because it programs strictly against the Store interface (Get and Put).
// TieredCache satisfies Store, allowing transparent L1/L2 multi-tier caching.
type TieredCache struct {
	l1        *Cache
	l2        *redis.Client
	l1TTL     time.Duration
	l2TTL     time.Duration
	keyPrefix string
}

var _ Store = (*TieredCache)(nil)

// NewTieredCache constructs a two-tier Store combining in-memory L1 (*Cache)
// and distributed L2 Redis (*redis.Client).
//
// Keys in Redis are namespaced with "aegora:cache:v1:".
// Panics if l1 or l2 is nil, or if l1TTL or l2TTL is non-positive.
func NewTieredCache(l1 *Cache, l2 *redis.Client, l1TTL, l2TTL time.Duration) *TieredCache {
	if l1 == nil {
		panic("cache: l1 must not be nil")
	}
	if l2 == nil {
		panic("cache: l2 must not be nil")
	}
	if l1TTL <= 0 {
		panic("cache: l1TTL must be positive")
	}
	if l2TTL <= 0 {
		panic("cache: l2TTL must be positive")
	}

	return &TieredCache{
		l1:        l1,
		l2:        l2,
		l1TTL:     l1TTL,
		l2TTL:     l2TTL,
		keyPrefix: defaultTieredKeyPrefix,
	}
}

// Get retrieves an entry for key.
//
// Tiered lookup order:
//  1. L1 lookup: on hit, return immediately.
//  2. L1 miss: perform L2 GET on Redis key (keyPrefix + key).
//  3. L2 hit: gob-decode entry and verify ExpiresAt is still fresh.
//  4. Backfill L1 with the decoded entry re-stamped to l1TTL.
//
// Fail-Quiet Rationale:
// Cache misses in L2 (redis.Nil), decode errors, network failures, or Redis downtime
// are treated as misses and degrade silently without error. Caching is purely an optimization
// and must never fail user requests or disrupt gateway traffic.
func (t *TieredCache) Get(key string) (*Entry, bool) {
	// 1. Check L1 in-memory tier
	if entry, hit := t.l1.Get(key); hit {
		return entry, true
	}

	if t.l2 == nil {
		return nil, false
	}

	// 2. Query L2 Redis tier
	redisKey := t.keyPrefix + key
	data, err := t.l2.Get(context.Background(), redisKey).Bytes()
	if err != nil {
		// Treat redis.Nil, network failure, or timeout as a cache miss.
		return nil, false
	}

	// 3. Gob-decode Entry
	var entry Entry
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&entry); err != nil {
		// Treat decode failure as a miss and degrade silently.
		return nil, false
	}

	// 4. Verify ExpiresAt is still fresh
	if !time.Now().Before(entry.ExpiresAt) {
		return nil, false
	}

	// 5. Backfill into L1 (re-stamped to l1TTL)
	backfill := &Entry{
		Status:    entry.Status,
		Header:    entry.Header.Clone(),
		Body:      bytes.Clone(entry.Body),
		StoredAt:  entry.StoredAt,
		ExpiresAt: time.Now().Add(t.l1TTL),
		ETag:      entry.ETag,
	}
	t.l1.Put(key, backfill)

	return backfill, true
}

// Put writes the entry through to both L1 (in-memory) and L2 (Redis).
//
// Each tier enforces its own TTL:
// - L1 is stored with l1TTL.
// - L2 entry has ExpiresAt stamped as now + l2TTL, and is stored with Redis TTL = l2TTL.
//
// Fail-Quiet Rationale:
// L2 write errors (serialization failure, connection error, Redis outage) are silently
// swallowed. L1 remains written and available, ensuring gateway operations never fail
// due to downstream cache backend issues.
func (t *TieredCache) Put(key string, e *Entry) {
	if e == nil {
		return
	}

	// 1. Write-through to L1 with l1TTL
	l1Entry := &Entry{
		Status:    e.Status,
		Header:    e.Header.Clone(),
		Body:      bytes.Clone(e.Body),
		StoredAt:  e.StoredAt,
		ExpiresAt: time.Now().Add(t.l1TTL),
		ETag:      e.ETag,
	}
	t.l1.Put(key, l1Entry)

	if t.l2 == nil {
		return
	}

	// 2. Write-through to L2 with l2TTL
	l2Entry := &Entry{
		Status:    e.Status,
		Header:    e.Header.Clone(),
		Body:      bytes.Clone(e.Body),
		StoredAt:  e.StoredAt,
		ExpiresAt: time.Now().Add(t.l2TTL),
		ETag:      e.ETag,
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(l2Entry); err != nil {
		// Serialization error swallowed fail-quietly
		return
	}

	redisKey := t.keyPrefix + key
	_ = t.l2.Set(context.Background(), redisKey, buf.Bytes(), t.l2TTL).Err()
}

// Invalidate deletes the cache entry from both L1 and L2 tiers.
// L2 deletion errors are swallowed as best-effort.
func (t *TieredCache) Invalidate(key string) {
	t.l1.Invalidate(key)

	if t.l2 == nil {
		return
	}

	redisKey := t.keyPrefix + key
	_ = t.l2.Del(context.Background(), redisKey).Err()
}

// InvalidatePrefix deletes all entries whose keys begin with prefix from both L1 and L2.
//
// L1 is invalidated via in-memory map scan. L2 is invalidated via a SCAN loop with MATCH
// on the namespaced prefix and batched DEL commands. Best-effort: any Redis communication
// errors terminate the scan without failing the caller.
func (t *TieredCache) InvalidatePrefix(prefix string) {
	t.l1.InvalidatePrefix(prefix)

	if t.l2 == nil {
		return
	}

	matchPattern := t.keyPrefix + prefix + "*"
	var cursor uint64
	ctx := context.Background()

	for {
		keys, nextCursor, err := t.l2.Scan(ctx, cursor, matchPattern, 100).Result()
		if err != nil {
			// Best-effort: stop scanning on Redis error
			break
		}
		if len(keys) > 0 {
			_ = t.l2.Del(ctx, keys...).Err()
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
}
