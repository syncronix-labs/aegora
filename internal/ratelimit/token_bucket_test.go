package ratelimit

import (
	"context"
	"testing"
)

func TestTokenBucketCapacity(t *testing.T) {

	tb := NewTokenBucket(2, 1)

	if !tb.Allow(context.Background()) {
		t.Fatal("expected token")
	}

	if !tb.Allow(context.Background()) {
		t.Fatal("expected token")
	}

	if tb.Allow(context.Background()) {
		t.Fatal("bucket should be empty")
	}
}
