package cache

import (
	"bytes"
	"context"
	"encoding/gob"
	"net/http"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newSampleEntry(body string, expiresAt time.Time) *Entry {
	h := make(http.Header)
	h.Set("Content-Type", "application/json")
	h.Set("X-Custom", "tiered")
	return &Entry{
		Status:    http.StatusOK,
		Header:    h,
		Body:      []byte(body),
		ExpiresAt: expiresAt,
		ETag:      `W/"sample"`,
	}
}

func TestTieredCache_L1MissL2HitBackfillsL1(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer func() { _ = rdb.Close() }()

	l1 := NewCache(100, 1024*1024)
	tc := NewTieredCache(l1, rdb, 1*time.Minute, 5*time.Minute)

	const key = "GET /api/user?id=1"
	origEntry := newSampleEntry(`{"user":"alice"}`, time.Now().Add(5*time.Minute))
	origEntry.StoredAt = time.Now().Add(-10 * time.Second)

	// Directly populate L2 without populating L1 to establish an L1 miss state
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(origEntry); err != nil {
		t.Fatalf("gob encode: %v", err)
	}
	redisKey := defaultTieredKeyPrefix + key
	if err := rdb.Set(context.Background(), redisKey, buf.Bytes(), 5*time.Minute).Err(); err != nil {
		t.Fatalf("redis set: %v", err)
	}

	// Verify L1 is initially empty
	if _, hit := l1.Get(key); hit {
		t.Fatal("expected L1 to initially miss")
	}

	// First Get: hits L2, backfills into L1
	entry1, hit1 := tc.Get(key)
	if !hit1 || entry1 == nil {
		t.Fatalf("expected L2 hit on first Get, got hit=%v", hit1)
	}
	if string(entry1.Body) != `{"user":"alice"}` {
		t.Fatalf("expected body %q, got %q", `{"user":"alice"}`, string(entry1.Body))
	}
	if entry1.Header.Get("X-Custom") != "tiered" {
		t.Fatalf("expected header X-Custom tiered, got %q", entry1.Header.Get("X-Custom"))
	}
	if !entry1.StoredAt.Equal(origEntry.StoredAt) {
		t.Fatalf("L2 backfill reset stored timestamp: %v, want %v", entry1.StoredAt, origEntry.StoredAt)
	}

	// Flush L2 completely to prove the next lookup is served by backfilled L1
	s.FlushAll()
	if s.Exists(redisKey) {
		t.Fatal("expected L2 key to be flushed")
	}

	// Second Get: hits L1 (L2 is completely empty)
	entry2, hit2 := tc.Get(key)
	if !hit2 || entry2 == nil {
		t.Fatalf("expected L1 hit after L2 flush (backfill verification), got hit=%v", hit2)
	}
	if string(entry2.Body) != `{"user":"alice"}` {
		t.Fatalf("expected body %q on backfilled entry, got %q", `{"user":"alice"}`, string(entry2.Body))
	}
}

func TestTieredCache_WriteThrough(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer func() { _ = rdb.Close() }()

	l1 := NewCache(100, 1024*1024)
	tc := NewTieredCache(l1, rdb, 30*time.Second, 2*time.Minute)

	const key = "GET /api/items"
	entry := newSampleEntry(`[{"id":1},{"id":2}]`, time.Now().Add(30*time.Second))
	entry.StoredAt = time.Now().Add(-10 * time.Second)

	tc.Put(key, entry)

	// Verify L1 has the entry
	l1Entry, l1Hit := l1.Get(key)
	if !l1Hit || l1Entry == nil {
		t.Fatal("expected L1 hit after Put")
	}
	if string(l1Entry.Body) != `[{"id":1},{"id":2}]` {
		t.Fatalf("expected L1 body match, got %q", string(l1Entry.Body))
	}
	if !l1Entry.StoredAt.Equal(entry.StoredAt) {
		t.Fatalf("L1 write reset stored timestamp: %v, want %v", l1Entry.StoredAt, entry.StoredAt)
	}

	// Verify miniredis contains the namespaced key
	redisKey := defaultTieredKeyPrefix + key
	if !s.Exists(redisKey) {
		t.Fatalf("expected Redis to contain key %q", redisKey)
	}

	rawBytes, err := s.Get(redisKey)
	if err != nil {
		t.Fatalf("miniredis Get failed: %v", err)
	}

	var decoded Entry
	if err := gob.NewDecoder(bytes.NewReader([]byte(rawBytes))).Decode(&decoded); err != nil {
		t.Fatalf("failed to gob-decode entry from Redis: %v", err)
	}
	if string(decoded.Body) != `[{"id":1},{"id":2}]` {
		t.Fatalf("expected decoded Redis body match, got %q", string(decoded.Body))
	}
}

func TestTieredCache_L2TTLRespected(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer func() { _ = rdb.Close() }()

	l1 := NewCache(100, 1024*1024)
	const l2TTL = 50 * time.Millisecond
	tc := NewTieredCache(l1, rdb, 1*time.Minute, l2TTL)

	const key = "GET /api/short-lived"
	entry := newSampleEntry(`{"temp":true}`, time.Now().Add(l2TTL))

	tc.Put(key, entry)

	// Invalidate L1 so subsequent lookup must hit L2
	l1.Invalidate(key)

	// Sleep past L2 TTL
	time.Sleep(100 * time.Millisecond)
	s.FastForward(200 * time.Millisecond)

	entryAfterTTL, hit := tc.Get(key)
	if hit || entryAfterTTL != nil {
		t.Fatalf("expected miss after L2 TTL expiry, got hit=%v entry=%v", hit, entryAfterTTL)
	}
}

func TestTieredCache_L2DownDegradesGracefully(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})

	l1 := NewCache(100, 1024*1024)
	tc := NewTieredCache(l1, rdb, 1*time.Minute, 5*time.Minute)

	// Shut down Redis to simulate total outage
	s.Close()
	_ = rdb.Close()

	// Get on cold key: returns miss without error or panic
	entry, hit := tc.Get("GET /api/missing")
	if hit || entry != nil {
		t.Fatalf("expected miss when L2 is down, got hit=%v", hit)
	}

	// Put does not panic; L1 still written and accessible
	testEntry := newSampleEntry(`{"l1_only":true}`, time.Now().Add(time.Minute))
	tc.Put("GET /api/l1-only", testEntry)

	l1Result, l1Hit := tc.Get("GET /api/l1-only")
	if !l1Hit || l1Result == nil {
		t.Fatal("expected L1 to continue serving cached responses even when L2 is down")
	}
	if string(l1Result.Body) != `{"l1_only":true}` {
		t.Fatalf("expected L1 body match, got %q", string(l1Result.Body))
	}

	// Invalidate does not panic when L2 is down
	tc.Invalidate("GET /api/l1-only")
	if _, hitAfter := tc.Get("GET /api/l1-only"); hitAfter {
		t.Fatal("expected entry to be invalidated from L1")
	}

	// InvalidatePrefix does not panic when L2 is down
	tc.Put("GET /api/prefix-1", testEntry)
	tc.InvalidatePrefix("GET /api/prefix")
	if _, hitAfter := tc.Get("GET /api/prefix-1"); hitAfter {
		t.Fatal("expected prefix entry to be invalidated from L1")
	}
}

func TestTieredCache_InvalidateRemovesFromBothTiers(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer func() { _ = rdb.Close() }()

	l1 := NewCache(100, 1024*1024)
	tc := NewTieredCache(l1, rdb, 1*time.Minute, 5*time.Minute)

	const key = "GET /api/delete-me"
	redisKey := defaultTieredKeyPrefix + key
	entry := newSampleEntry(`{"delete":true}`, time.Now().Add(time.Minute))

	tc.Put(key, entry)

	// Verify both tiers have the entry
	if _, hit := l1.Get(key); !hit {
		t.Fatal("expected L1 to have entry before Invalidate")
	}
	if !s.Exists(redisKey) {
		t.Fatal("expected Redis to have entry before Invalidate")
	}

	// Invalidate removes from both tiers
	tc.Invalidate(key)

	if _, hit := l1.Get(key); hit {
		t.Fatal("expected L1 entry to be removed after Invalidate")
	}
	if s.Exists(redisKey) {
		t.Fatal("expected Redis entry to be removed after Invalidate")
	}
	if _, hit := tc.Get(key); hit {
		t.Fatal("expected TieredCache.Get to return miss after Invalidate")
	}
}

func TestTieredCache_InvalidatePrefix(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer func() { _ = rdb.Close() }()

	l1 := NewCache(100, 1024*1024)
	tc := NewTieredCache(l1, rdb, 1*time.Minute, 5*time.Minute)

	e1 := newSampleEntry(`{"user":1}`, time.Now().Add(time.Minute))
	e2 := newSampleEntry(`{"user":2}`, time.Now().Add(time.Minute))
	eOther := newSampleEntry(`{"order":1}`, time.Now().Add(time.Minute))

	tc.Put("GET /api/users/1", e1)
	tc.Put("GET /api/users/2", e2)
	tc.Put("GET /api/orders/1", eOther)

	// Invalidate prefix "GET /api/users"
	tc.InvalidatePrefix("GET /api/users")

	// The users entries should be gone from both L1 and Redis
	if _, hit := tc.Get("GET /api/users/1"); hit {
		t.Fatal("expected users/1 to be invalidated")
	}
	if _, hit := tc.Get("GET /api/users/2"); hit {
		t.Fatal("expected users/2 to be invalidated")
	}
	if s.Exists(defaultTieredKeyPrefix + "GET /api/users/1") {
		t.Fatal("expected users/1 Redis key to be deleted")
	}
	if s.Exists(defaultTieredKeyPrefix + "GET /api/users/2") {
		t.Fatal("expected users/2 Redis key to be deleted")
	}

	// The orders entry should remain intact in both tiers
	entryOther, hitOther := tc.Get("GET /api/orders/1")
	if !hitOther || entryOther == nil {
		t.Fatal("expected orders/1 to remain cached")
	}
	if !s.Exists(defaultTieredKeyPrefix + "GET /api/orders/1") {
		t.Fatal("expected orders/1 Redis key to remain")
	}
}

func TestTieredCache_CorruptAndExpiredInL2(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer func() { _ = rdb.Close() }()

	l1 := NewCache(100, 1024*1024)
	tc := NewTieredCache(l1, rdb, 1*time.Minute, 5*time.Minute)

	// 1. Corrupt data in L2: Get should return miss without panicking
	corruptKey := "GET /api/corrupt"
	_ = rdb.Set(context.Background(), defaultTieredKeyPrefix+corruptKey, []byte("not-valid-gob-bytes"), time.Minute).Err()
	if _, hit := tc.Get(corruptKey); hit {
		t.Fatal("expected miss for corrupt L2 entry")
	}

	// 2. Expired entry stored in L2: Get should verify freshness and return miss
	expiredKey := "GET /api/expired"
	expiredEntry := newSampleEntry(`{"old":true}`, time.Now().Add(-10*time.Second))
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(expiredEntry); err != nil {
		t.Fatalf("gob encode: %v", err)
	}
	_ = rdb.Set(context.Background(), defaultTieredKeyPrefix+expiredKey, buf.Bytes(), time.Minute).Err()
	if _, hit := tc.Get(expiredKey); hit {
		t.Fatal("expected miss for expired L2 entry")
	}
}

func TestTieredCache_ConstructorPanics(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer func() { _ = rdb.Close() }()

	l1 := NewCache(10, 1024)

	tests := []struct {
		name  string
		l1    *Cache
		l2    *redis.Client
		l1TTL time.Duration
		l2TTL time.Duration
	}{
		{"nil l1", nil, rdb, time.Minute, time.Minute},
		{"nil l2", l1, nil, time.Minute, time.Minute},
		{"zero l1TTL", l1, rdb, 0, time.Minute},
		{"negative l1TTL", l1, rdb, -time.Second, time.Minute},
		{"zero l2TTL", l1, rdb, time.Minute, 0},
		{"negative l2TTL", l1, rdb, time.Minute, -time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("expected panic for %s", tt.name)
				}
			}()
			_ = NewTieredCache(tt.l1, tt.l2, tt.l1TTL, tt.l2TTL)
		})
	}
}
