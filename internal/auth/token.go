// Package auth verifies `entity_token` credentials.
//
// # THIRD IMPLEMENTATION, AND WHY THAT IS TOLERABLE
//
// The canonical one is Django's (user_service/entity/services/tokens.py); Node
// has a hand-port in reusables/hooks/botTokenChecker.js. This is the third.
// Three implementations of an authentication check would normally be a bad
// smell, and the thing that keeps it honest is that the FORMAT was chosen to
// be trivially re-implementable:
//
//	clt_<12 hex prefix>_<64 hex secret>
//
// All hex, so splitting on "_" is unambiguous in every language; the stored
// value is a plain SHA-256 of the whole string, with no salt, no stretching
// and no framework-specific encoding to reproduce. There is no clever part to
// get subtly wrong. Keep in sync with:
//
//	user_service/entity/services/tokens.py
//	server/reusables/hooks/botTokenChecker.js
//
// # WHY THIS DOES NOT PORT THE PERMISSION RESOLVER
//
// Django's resolver consults explicit overrides, then role defaults, then
// entity-type defaults, then a platform default predicate. Porting all four
// layers into a third language is exactly the drift risk worth avoiding, so
// this service does not. It requires an EXPLICIT grant row in
// entity_entitypermission for every permission it gates, and consults nothing
// else.
//
// That is deliberately STRICTER than the platform, and one of those defaults
// is why. Django's _account_in_good_standing returns True for any entity with
// no Account - its reverse one-to-one raises an AttributeError subclass, so
// getattr(entity, "users", None) yields None - which means bots and realm
// entities hold every global permission by platform default, messages.send
// included. Honouring that here would leave a token's entity half constraining
// nothing at all.
//
// So an operator grants the capability on purpose, per entity, and the token
// narrows it further. Fail-closed in both directions. The cost is that a
// permission the platform would have allowed still needs a grant row, which is
// one insert and the right amount of friction for a credential that outlives
// any session.
//
// gatedPermissions is the set this service knows how to decide; Authorize
// refuses anything outside it rather than guessing.
package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	tokenNamespace = "clt"
	prefixHexLen   = 12 // 6 bytes
	secretHexLen   = 64 // 32 bytes
)

// The capabilities this service gates. Declared in the platform's catalog
// (user_service/entity/permissions.py), because Django owns that table;
// enforced here, because this service owns the API they protect.
const (
	PermissionEventsSubscribe   = "events.subscribe"
	PermissionMessagesRead      = "messages.read"
	PermissionNotificationsRead = "notifications.read"
	PermissionMessagesSend      = "messages.send"
)

// Every entry must be a GLOBAL-scoped codename in the platform catalog. A
// realm-scoped one would need the role matrix to resolve, which this service
// deliberately does not carry.
var gatedPermissions = map[string]bool{
	PermissionEventsSubscribe:   true,
	PermissionMessagesRead:      true,
	PermissionNotificationsRead: true,
	PermissionMessagesSend:      true,
}

var (
	// ErrInvalidToken covers malformed, unknown, revoked, expired and
	// deactivated alike. The caller's only correct response to all of them is
	// the same 401, and a function that distinguished them invites a handler
	// that leaks which one it was.
	ErrInvalidToken = errors.New("invalid or expired token")

	// ErrUnknownPermission means Authorize was asked about something outside
	// gatedPermissions. A programming error, not a client error.
	ErrUnknownPermission = errors.New("permission is not one this service may decide")
)

// Token is a live credential row.
type Token struct {
	ID       string
	EntityID string
	RealmID  *string
	Name     string
	Scopes   []string
}

// HasScope reports whether the token carries a codename. Half an authorization
// decision - see Authorize.
func (t *Token) HasScope(codename string) bool {
	for _, scope := range t.Scopes {
		if scope == codename {
			return true
		}
	}
	return false
}

// ParseToken splits a token string into its prefix and secret.
//
// Total by design: every rejection here costs no database round trip, which is
// what keeps a flood of garbage traffic cheap.
func ParseToken(raw string) (prefix, secret string, ok bool) {
	parts := strings.Split(strings.TrimSpace(raw), "_")
	if len(parts) != 3 {
		return "", "", false
	}
	if parts[0] != tokenNamespace {
		return "", "", false
	}
	if len(parts[1]) != prefixHexLen || len(parts[2]) != secretHexLen {
		return "", "", false
	}
	if !isHex(parts[1]) || !isHex(parts[2]) {
		return "", "", false
	}
	return parts[1], parts[2], true
}

func isHex(value string) bool {
	_, err := hex.DecodeString(value)
	return err == nil
}

func hashToken(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// Verify resolves a raw token string to a live row.
func Verify(ctx context.Context, pool *pgxpool.Pool, raw string) (*Token, error) {
	prefix, _, ok := ParseToken(raw)
	if !ok {
		return nil, ErrInvalidToken
	}

	var (
		id        string
		entityID  string
		realmID   *string
		name      string
		tokenHash string
		scopesRaw []byte
		isActive  bool
		revokedAt *time.Time
		expiresAt *time.Time
	)

	err := pool.QueryRow(ctx, `
		SELECT id, entity_id, realm_id, name, token_hash, scopes,
		       is_active, revoked_at, expires_at
		  FROM entity_token
		 WHERE prefix = $1`, prefix,
	).Scan(&id, &entityID, &realmID, &name, &tokenHash, &scopesRaw,
		&isActive, &revokedAt, &expiresAt)
	if err != nil {
		// A miss and a database error are both "cannot authenticate". The
		// error is logged by the caller; the client learns nothing either way.
		return nil, ErrInvalidToken
	}

	// Constant time: an early-exit comparison would let an attacker holding a
	// valid prefix recover the secret one character at a time from latency.
	if subtle.ConstantTimeCompare([]byte(tokenHash), []byte(hashToken(raw))) != 1 {
		return nil, ErrInvalidToken
	}

	if !isActive || revokedAt != nil {
		return nil, ErrInvalidToken
	}
	if expiresAt != nil && !expiresAt.After(time.Now()) {
		return nil, ErrInvalidToken
	}

	scopes := []string{}
	if len(scopesRaw) > 0 {
		if err := json.Unmarshal(scopesRaw, &scopes); err != nil {
			return nil, fmt.Errorf("token %s has unreadable scopes: %w", id, err)
		}
	}

	return &Token{
		ID:       id,
		EntityID: entityID,
		RealmID:  realmID,
		Name:     name,
		Scopes:   scopes,
	}, nil
}

// TouchLastUsed stamps last_used_at, throttled to once a minute per token.
//
// The throttle lives in the WHERE clause rather than in Go so that concurrent
// requests across replicas cannot each decide they are the one to write. Its
// only purpose is answering "is this credential still in use" before revoking
// it, which minute-level accuracy serves perfectly well.
func TouchLastUsed(ctx context.Context, pool *pgxpool.Pool, tokenID string) error {
	_, err := pool.Exec(ctx, `
		UPDATE entity_token
		   SET last_used_at = NOW()
		 WHERE id = $1
		   AND (last_used_at IS NULL OR last_used_at < NOW() - INTERVAL '60 seconds')`,
		tokenID)
	return err
}

// Authorize answers whether a token may exercise a permission right now.
//
// BOTH halves must hold: the scope is on the token AND the owning entity has
// been explicitly granted the permission. See the package doc for why the
// entity half is a grant lookup rather than the full resolver, and why that is
// stricter than the platform on purpose.
func Authorize(ctx context.Context, pool *pgxpool.Pool, token *Token, permission string) (bool, error) {
	if !gatedPermissions[permission] {
		return false, ErrUnknownPermission
	}
	if token == nil || !token.HasScope(permission) {
		return false, nil
	}
	return hasExplicitGrant(ctx, pool, token.EntityID, permission)
}

// hasExplicitGrant applies the resolver's first two rules and stops there: an
// unexpired deny wins over everything, then an unexpired grant, then no.
//
// realm_id IS NULL because these are global-scoped; a realm-scoped override
// row would not apply.
func hasExplicitGrant(ctx context.Context, pool *pgxpool.Pool, entityID, permission string) (bool, error) {
	rows, err := pool.Query(ctx, `
		SELECT effect
		  FROM entity_entitypermission
		 WHERE entity_id = $1
		   AND permission = $2
		   AND realm_id IS NULL
		   AND (expires_at IS NULL OR expires_at > NOW())`,
		entityID, permission)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	granted := false
	for rows.Next() {
		var effect string
		if err := rows.Scan(&effect); err != nil {
			return false, err
		}
		if effect == "deny" {
			// Explicit deny is absolute and short-circuits, matching the
			// resolver's first rule.
			return false, nil
		}
		if effect == "grant" {
			granted = true
		}
	}
	return granted, rows.Err()
}
