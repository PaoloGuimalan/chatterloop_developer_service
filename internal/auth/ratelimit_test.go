package auth

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeRateLimitStore is an in-memory RateLimitStore, so these tests exercise
// CheckRateLimit's own logic without a live Redis - the same "fake the small
// interface" approach the rest of this codebase uses for anything that talks
// to a real dependency.
type fakeRateLimitStore struct {
	mu       sync.Mutex
	counts   map[string]int64
	expires  map[string]time.Duration
	incrErr  error
	expErr   error
	incrCall int
}

func newFakeRateLimitStore() *fakeRateLimitStore {
	return &fakeRateLimitStore{
		counts:  map[string]int64{},
		expires: map[string]time.Duration{},
	}
}

func (f *fakeRateLimitStore) Incr(_ context.Context, key string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.incrCall++
	if f.incrErr != nil {
		return 0, f.incrErr
	}
	f.counts[key]++
	return f.counts[key], nil
}

func (f *fakeRateLimitStore) Expire(_ context.Context, key string, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.expErr != nil {
		return f.expErr
	}
	f.expires[key] = ttl
	return nil
}

func intPtr(n int) *int                   { return &n }
func periodPtr(p RateLimitPeriod) *string { s := string(p); return &s }

func TestCheckRateLimitAllowsUnderTheLimit(t *testing.T) {
	store := newFakeRateLimitStore()
	token := &Token{ID: "tok-1", RateLimitInt: intPtr(5), RateLimitType: periodPtr(RateLimitMinute)}

	for i := 0; i < 5; i++ {
		allowed, _, err := CheckRateLimit(t.Context(), store, token)
		if err != nil {
			t.Fatalf("request %d: unexpected error: %v", i+1, err)
		}
		if !allowed {
			t.Fatalf("request %d: denied under the limit", i+1)
		}
	}
}

func TestCheckRateLimitDeniesOverTheLimit(t *testing.T) {
	store := newFakeRateLimitStore()
	token := &Token{ID: "tok-1", RateLimitInt: intPtr(3), RateLimitType: periodPtr(RateLimitMinute)}

	for i := 0; i < 3; i++ {
		if allowed, _, err := CheckRateLimit(t.Context(), store, token); err != nil || !allowed {
			t.Fatalf("request %d: expected allowed, got allowed=%v err=%v", i+1, allowed, err)
		}
	}

	allowed, retryAfter, err := CheckRateLimit(t.Context(), store, token)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if allowed {
		t.Fatal("4th request was allowed against a limit of 3")
	}
	if retryAfter <= 0 || retryAfter > time.Minute {
		t.Fatalf("retryAfter out of range: %v", retryAfter)
	}
}

// Denied requests still count. A version that only counted allowed requests
// would let a caller probe the boundary for free on every rejection.
func TestCheckRateLimitStillIncrementsWhenDenied(t *testing.T) {
	store := newFakeRateLimitStore()
	token := &Token{ID: "tok-1", RateLimitInt: intPtr(1), RateLimitType: periodPtr(RateLimitMinute)}

	if allowed, _, err := CheckRateLimit(t.Context(), store, token); err != nil || !allowed {
		t.Fatalf("1st request: expected allowed, got allowed=%v err=%v", allowed, err)
	}
	if allowed, _, err := CheckRateLimit(t.Context(), store, token); err != nil || allowed {
		t.Fatalf("2nd request: expected denied, got allowed=%v err=%v", allowed, err)
	}

	if got := store.incrCall; got != 2 {
		t.Fatalf("expected Incr called twice (once per attempt), got %d", got)
	}
}

func TestCheckRateLimitNilIntIsUnlimited(t *testing.T) {
	store := newFakeRateLimitStore()
	token := &Token{ID: "tok-unlimited", RateLimitInt: nil, RateLimitType: nil}

	for i := 0; i < 1000; i++ {
		allowed, _, err := CheckRateLimit(t.Context(), store, token)
		if err != nil {
			t.Fatalf("request %d: unexpected error: %v", i+1, err)
		}
		if !allowed {
			t.Fatalf("request %d: an unlimited token was denied", i+1)
		}
	}
	// Never even touches the store: an unlimited token costs this function
	// nothing, not even a Redis round trip.
	if got := store.incrCall; got != 0 {
		t.Fatalf("expected 0 Incr calls for an unlimited token, got %d", got)
	}
}

// A limit with no period is a data problem (Token.clean() on the Django side
// is supposed to make this unreachable), and must fail loud rather than
// silently allow or silently deny every request on the token.
func TestCheckRateLimitIntWithNoTypeIsAnError(t *testing.T) {
	store := newFakeRateLimitStore()
	token := &Token{ID: "tok-1", RateLimitInt: intPtr(5), RateLimitType: nil}

	allowed, _, err := CheckRateLimit(t.Context(), store, token)
	if err == nil {
		t.Fatal("expected an error for rate_limit_int set with no rate_limit_type")
	}
	if allowed {
		t.Fatal("a misconfigured token must never be reported allowed")
	}
}

func TestCheckRateLimitUnrecognizedPeriodIsAnError(t *testing.T) {
	store := newFakeRateLimitStore()
	badPeriod := "fortnight"
	token := &Token{ID: "tok-1", RateLimitInt: intPtr(5), RateLimitType: &badPeriod}

	allowed, _, err := CheckRateLimit(t.Context(), store, token)
	if err == nil {
		t.Fatal("expected an error for an unrecognized rate_limit_type")
	}
	if allowed {
		t.Fatal("a misconfigured token must never be reported allowed")
	}
}

// Every period this service is supposed to understand actually resolves a
// window instead of erroring - the exhaustive check that windowBounds and
// CheckRateLimit agree on the full RateLimitPeriod vocabulary.
func TestCheckRateLimitEveryDocumentedPeriodWorks(t *testing.T) {
	periods := []RateLimitPeriod{
		RateLimitSecond, RateLimitMinute, RateLimitHour,
		RateLimitDay, RateLimitWeek, RateLimitMonth, RateLimitYear,
	}
	for _, period := range periods {
		t.Run(string(period), func(t *testing.T) {
			store := newFakeRateLimitStore()
			token := &Token{ID: "tok-" + string(period), RateLimitInt: intPtr(1), RateLimitType: periodPtr(period)}

			allowed, _, err := CheckRateLimit(t.Context(), store, token)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !allowed {
				t.Fatal("1st request under a limit of 1 was denied")
			}
		})
	}
}

// A month or a year is not a fixed duration - windowBounds must anchor these
// on the UTC calendar, not on a truncated multiple of 30 or 365 days, or a
// "100k / month" token would reset at the wrong moment.
func TestWindowBoundsMonthIsCalendarAnchored(t *testing.T) {
	now := time.Date(2026, time.March, 17, 10, 30, 0, 0, time.UTC)
	start, end, err := windowBounds(RateLimitMonth, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantStart := time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC)
	wantEnd := time.Date(2026, time.April, 1, 0, 0, 0, 0, time.UTC)
	if !start.Equal(wantStart) {
		t.Fatalf("start = %v, want %v", start, wantStart)
	}
	if !end.Equal(wantEnd) {
		t.Fatalf("end = %v, want %v", end, wantEnd)
	}
}

func TestWindowBoundsYearIsCalendarAnchored(t *testing.T) {
	now := time.Date(2026, time.November, 5, 23, 59, 0, 0, time.UTC)
	start, end, err := windowBounds(RateLimitYear, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantStart := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	wantEnd := time.Date(2027, time.January, 1, 0, 0, 0, 0, time.UTC)
	if !start.Equal(wantStart) {
		t.Fatalf("start = %v, want %v", start, wantStart)
	}
	if !end.Equal(wantEnd) {
		t.Fatalf("end = %v, want %v", end, wantEnd)
	}
}

func TestWindowBoundsUnrecognizedPeriod(t *testing.T) {
	if _, _, err := windowBounds("fortnight", time.Now()); err == nil {
		t.Fatal("expected an error for an unrecognized period")
	}
}

// Two different tokens must never share a window, even under the same
// period in the same instant - otherwise one token's traffic would throttle
// a completely different credential.
func TestCheckRateLimitTokensDoNotShareAWindow(t *testing.T) {
	store := newFakeRateLimitStore()
	tokenA := &Token{ID: "tok-a", RateLimitInt: intPtr(1), RateLimitType: periodPtr(RateLimitMinute)}
	tokenB := &Token{ID: "tok-b", RateLimitInt: intPtr(1), RateLimitType: periodPtr(RateLimitMinute)}

	if allowed, _, err := CheckRateLimit(t.Context(), store, tokenA); err != nil || !allowed {
		t.Fatalf("token A 1st request: expected allowed, got allowed=%v err=%v", allowed, err)
	}
	if allowed, _, err := CheckRateLimit(t.Context(), store, tokenB); err != nil || !allowed {
		t.Fatalf("token B 1st request (must not be throttled by A's usage): "+
			"got allowed=%v err=%v", allowed, err)
	}
}

// Two DIFFERENT periods for the same token id must not collide either - a
// safety net against the key format itself, even though nothing lets one
// token hold two rows today.
func TestCheckRateLimitDifferentPeriodsDoNotShareAWindow(t *testing.T) {
	store := newFakeRateLimitStore()
	perMinute := &Token{ID: "tok-1", RateLimitInt: intPtr(1), RateLimitType: periodPtr(RateLimitMinute)}
	perHour := &Token{ID: "tok-1", RateLimitInt: intPtr(1), RateLimitType: periodPtr(RateLimitHour)}

	if allowed, _, err := CheckRateLimit(t.Context(), store, perMinute); err != nil || !allowed {
		t.Fatalf("per-minute 1st request: expected allowed, got allowed=%v err=%v", allowed, err)
	}
	if allowed, _, err := CheckRateLimit(t.Context(), store, perHour); err != nil || !allowed {
		t.Fatalf("per-hour 1st request: expected allowed, got allowed=%v err=%v", allowed, err)
	}
}

// Expire is set only once per window - on the request that opens it. A busy
// token that kept pushing its TTL out on every increment would never expire
// (a real concern for a "month" or "year" window on a token used daily).
func TestCheckRateLimitOnlySetsExpiryOnTheFirstRequestInAWindow(t *testing.T) {
	store := newFakeRateLimitStore()
	token := &Token{ID: "tok-1", RateLimitInt: intPtr(10), RateLimitType: periodPtr(RateLimitMinute)}

	for i := 0; i < 5; i++ {
		if _, _, err := CheckRateLimit(t.Context(), store, token); err != nil {
			t.Fatalf("request %d: unexpected error: %v", i+1, err)
		}
	}

	if got := len(store.expires); got != 1 {
		t.Fatalf("expected exactly one key to have had Expire called on it, got %d", got)
	}
}

func TestCheckRateLimitPropagatesIncrError(t *testing.T) {
	store := newFakeRateLimitStore()
	store.incrErr = errRedisDown
	token := &Token{ID: "tok-1", RateLimitInt: intPtr(5), RateLimitType: periodPtr(RateLimitMinute)}

	allowed, _, err := CheckRateLimit(t.Context(), store, token)
	if err == nil {
		t.Fatal("expected an error when the store's Incr fails")
	}
	if allowed {
		t.Fatal("a failed check must never report allowed")
	}
}

func TestCheckRateLimitPropagatesExpireError(t *testing.T) {
	store := newFakeRateLimitStore()
	store.expErr = errRedisDown
	token := &Token{ID: "tok-1", RateLimitInt: intPtr(5), RateLimitType: periodPtr(RateLimitMinute)}

	allowed, _, err := CheckRateLimit(t.Context(), store, token)
	if err == nil {
		t.Fatal("expected an error when the store's Expire fails")
	}
	if allowed {
		t.Fatal("a failed check must never report allowed")
	}
}

var errRedisDown = &fakeError{"redis unreachable"}

type fakeError struct{ msg string }

func (e *fakeError) Error() string { return e.msg }
