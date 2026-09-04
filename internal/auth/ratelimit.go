package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RateLimitPeriod is the unit entity_token.rate_limit_type counts in. Values
// mirror user_service/entity/models.py Token.RateLimitPeriod exactly - kept
// as plain lowercase strings, not an int enum, so a row is self-describing in
// both a SQL client and a Django admin dropdown with no lookup table between
// them.
type RateLimitPeriod string

const (
	RateLimitSecond RateLimitPeriod = "second"
	RateLimitMinute RateLimitPeriod = "minute"
	RateLimitHour   RateLimitPeriod = "hour"
	RateLimitDay    RateLimitPeriod = "day"
	RateLimitWeek   RateLimitPeriod = "week"
	RateLimitMonth  RateLimitPeriod = "month"
	RateLimitYear   RateLimitPeriod = "year"
)

// windowBounds returns the [start, end) of the CURRENT window for a period,
// as of now.
//
// second/minute/hour/day/week are anchored at the Unix epoch via
// time.Truncate, which is exact and monotonic - every process on every
// machine derives the identical boundary from the wall clock alone, with no
// shared state. Epoch-anchored also means "day" and "week" are not anchored
// to any particular timezone's midnight or a particular weekday; the windows
// are still exactly 24h / 7*24h wide and still line up across processes,
// which is all a rate limit actually needs.
//
// month/year are NOT fixed durations - a month is 28 to 31 days - so they
// cannot use Truncate and are anchored on the UTC calendar instead: a
// caller's window resets at the first instant of the next UTC calendar month
// or year, matching what a subscription plan described as "100k / month"
// actually promises.
func windowBounds(period RateLimitPeriod, now time.Time) (start, end time.Time, err error) {
	now = now.UTC()
	switch period {
	case RateLimitSecond:
		start = now.Truncate(time.Second)
		return start, start.Add(time.Second), nil
	case RateLimitMinute:
		start = now.Truncate(time.Minute)
		return start, start.Add(time.Minute), nil
	case RateLimitHour:
		start = now.Truncate(time.Hour)
		return start, start.Add(time.Hour), nil
	case RateLimitDay:
		start = now.Truncate(24 * time.Hour)
		return start, start.Add(24 * time.Hour), nil
	case RateLimitWeek:
		start = now.Truncate(7 * 24 * time.Hour)
		return start, start.Add(7 * 24 * time.Hour), nil
	case RateLimitMonth:
		start = time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
		return start, start.AddDate(0, 1, 0), nil
	case RateLimitYear:
		start = time.Date(now.Year(), time.January, 1, 0, 0, 0, 0, time.UTC)
		return start, start.AddDate(1, 0, 0), nil
	default:
		return time.Time{}, time.Time{}, fmt.Errorf("unrecognized rate limit period %q", period)
	}
}

// RateLimitStore is the minimal slice of Redis a fixed-window counter needs.
// Kept as an interface, not *redis.Client directly, so a test can fake it
// without a live Redis - CheckRateLimit's whole contract is these two calls,
// in this order, and nothing else about Redis is its concern.
type RateLimitStore interface {
	// Incr increments the counter at key by one, creating it at 1 if absent,
	// and returns the new value - the same contract as Redis INCR.
	Incr(ctx context.Context, key string) (int64, error)
	// Expire sets key to die after ttl. Called only by the request that just
	// created the counter (see CheckRateLimit), so this never needs to
	// report whether the key existed.
	Expire(ctx context.Context, key string, ttl time.Duration) error
}

// redisRateLimitStore adapts a *redis.Client to RateLimitStore.
type redisRateLimitStore struct {
	client *redis.Client
}

// NewRedisRateLimitStore wraps a live Redis client for CheckRateLimit.
func NewRedisRateLimitStore(client *redis.Client) RateLimitStore {
	return redisRateLimitStore{client: client}
}

func (s redisRateLimitStore) Incr(ctx context.Context, key string) (int64, error) {
	return s.client.Incr(ctx, key).Result()
}

func (s redisRateLimitStore) Expire(ctx context.Context, key string, ttl time.Duration) error {
	return s.client.Expire(ctx, key, ttl).Err()
}

// CheckRateLimit reports whether token may make one more request right now.
//
// WHY PER TOKEN, NOT PER ENTITY OR GLOBAL
// ----------------------------------------
// entity_token.rate_limit_int/rate_limit_type (user_service/entity/models.py)
// are set per credential, not per entity: the same entity can hold a
// generously-limited internal token and a tightly-limited one handed to a
// third party, without either constraining the other. This package only
// reads those columns - Django owns the schema and the admin surface to
// change them; this is the only thing that ever enforces them, the same
// division Verify already draws for authentication.
//
// WHY THE PERIOD IS DYNAMIC
// ----------------------------------
// A single fixed grain (e.g. always "per minute") cannot express "100k
// requests per month" or "5 per second" - and a subscription-tier product is
// exactly a set of different (count, period) pairs assigned per token. See
// the RateLimitPeriod / windowBounds doc for how each unit's window is
// derived.
//
// WHY REDIS, AND WHY A FIXED WINDOW
// ----------------------------------
// Every request already pays one Postgres round trip to verify the token; a
// second one to count recent requests would double that for every
// authenticated call. Redis is already a dependency of this service
// (internal/connections) for the realtime stream, and an INCR plus a
// conditional EXPIRE is two cheap round trips against a store built for
// exactly this.
//
// A fixed window (bucket keyed on the current period's start) rather than a
// sliding window or token bucket: it allows a burst at the boundary between
// two windows, which is the one imprecision this service can live with, in
// exchange for an implementation that is two Redis commands instead of a
// sorted set and a cleanup job. If that boundary burst ever becomes a real
// problem, replace this function - callers only see allowed/retryAfter/err,
// not how the count was kept.
//
// Incrementing is NOT conditional on the outcome: this call counts every
// attempt, allowed or not, as a side effect before it decides. A version
// that only counted allowed requests would let a caller probe right up to
// the limit for free on every rejected try; counting first and deciding
// second is what makes the limit actually a limit.
//
// A token with RateLimitInt nil (NULL in the database) is never throttled -
// no counter is even touched, so a token that has never been given a limit
// costs this function nothing. Once a limit IS set, the counter increments
// unconditionally from then on, so flipping the limit off and back on later
// starts counting from a live window rather than from zero.
//
// retryAfter is only meaningful when allowed is false: how long until the
// current window ends and the count resets, which is what a caller sends
// back as the Retry-After header.
func CheckRateLimit(ctx context.Context, store RateLimitStore, token *Token) (allowed bool, retryAfter time.Duration, err error) {
	if token.RateLimitInt == nil {
		return true, 0, nil
	}
	// A limit with no period (or an unrecognized one) is a data problem, not
	// a client problem - Token.clean() on the Django side is supposed to
	// make this unreachable, but a row written outside that path (a raw SQL
	// UPDATE, a bad migration) must fail loud here rather than silently
	// granting or silently denying every request on this token.
	if token.RateLimitType == nil {
		return false, 0, fmt.Errorf("token %s has rate_limit_int set with no rate_limit_type", token.ID)
	}
	period := RateLimitPeriod(*token.RateLimitType)

	now := time.Now().UTC()
	start, end, err := windowBounds(period, now)
	if err != nil {
		return false, 0, fmt.Errorf("token %s: %w", token.ID, err)
	}
	key := fmt.Sprintf("ratelimit:token:%s:%s:%d", token.ID, period, start.Unix())

	count, err := store.Incr(ctx, key)
	if err != nil {
		return false, 0, fmt.Errorf("rate limit counter: %w", err)
	}
	if count == 1 {
		// Only the request that OPENED this window sets its expiry, to the
		// time REMAINING in the window rather than the window's full length -
		// the key must die when the window ends regardless of when in it the
		// first request landed. Every later increment in the same window
		// must not push the TTL back out, or a busy token's key would never
		// die with its window - it would be kept alive by its own traffic,
		// forever (a real concern here: a "month" or "year" window's key
		// would otherwise never expire on a token used every day).
		if err := store.Expire(ctx, key, end.Sub(now)); err != nil {
			return false, 0, fmt.Errorf("rate limit expiry: %w", err)
		}
	}

	if count <= int64(*token.RateLimitInt) {
		return true, 0, nil
	}
	return false, end.Sub(now), nil
}
