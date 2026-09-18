// Package lock provides a simple Redis-based distributed lock so two
// sandbox workers can never execute actions for the same case at the
// same time. This is intentionally minimal (SET NX PX + a token check on
// release) rather than a full Redlock implementation — for a single
// Redis instance that's the right amount of complexity, and it's easy to
// explain in an interview. Swap in Redlock only if you actually run a
// multi-node Redis cluster.
package lock

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var ErrNotHeld = errors.New("lock: not held by this token")

type CaseLock struct {
	rdb *redis.Client
	ttl time.Duration
}

func New(rdb *redis.Client, ttl time.Duration) *CaseLock {
	return &CaseLock{rdb: rdb, ttl: ttl}
}

// Acquire tries once to take the lock for caseID and returns a token
// that must be passed to Release. Callers should treat a false ok as
// "someone else is already working this case" and skip, not retry in a
// tight loop — the case will come back around on the next poll/consume.
func (l *CaseLock) Acquire(ctx context.Context, caseID string) (token string, ok bool, err error) {
	token = uuid.New().String()
	key := lockKey(caseID)
	ok, err = l.rdb.SetNX(ctx, key, token, l.ttl).Result()
	if err != nil {
		return "", false, fmt.Errorf("redis setnx: %w", err)
	}
	return token, ok, nil
}

// releaseScript only deletes the key if it still holds our token, so a
// worker can never release a lock it no longer owns (e.g. because its
// TTL expired and someone else already acquired it).
const releaseScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
else
	return 0
end`

func (l *CaseLock) Release(ctx context.Context, caseID, token string) error {
	res, err := l.rdb.Eval(ctx, releaseScript, []string{lockKey(caseID)}, token).Result()
	if err != nil {
		return fmt.Errorf("redis release eval: %w", err)
	}
	if n, _ := res.(int64); n == 0 {
		return ErrNotHeld
	}
	return nil
}

func lockKey(caseID string) string {
	return "case-lock:" + caseID
}
