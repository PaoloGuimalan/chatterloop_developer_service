package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

const (
	goodPrefix = "abc123def456"
	goodSecret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	goodToken  = "clt_" + goodPrefix + "_" + goodSecret
)

func TestParseToken(t *testing.T) {
	prefix, secret, ok := ParseToken(goodToken)
	if !ok {
		t.Fatal("a well-formed token was rejected")
	}
	if prefix != goodPrefix || secret != goodSecret {
		t.Fatalf("split wrong: prefix=%q secret=%q", prefix, secret)
	}
}

func TestParseTokenTrimsSurroundingWhitespace(t *testing.T) {
	// Tokens arrive from .env files and shell variables often enough that a
	// trailing newline should not read as a forged credential.
	if _, _, ok := ParseToken("  " + goodToken + "\n"); !ok {
		t.Fatal("whitespace-padded token was rejected")
	}
}

func TestParseTokenRejectsMalformed(t *testing.T) {
	cases := map[string]string{
		"empty":             "",
		"no separators":     "nonsense",
		"wrong namespace":   "xxx_" + goodPrefix + "_" + goodSecret,
		"short prefix":      "clt_abc_" + goodSecret,
		"short secret":      "clt_" + goodPrefix + "_abc",
		"missing secret":    "clt_" + goodPrefix,
		"extra segment":     goodToken + "_more",
		"non-hex prefix":    "clt_zzzzzzzzzzzz_" + goodSecret,
		"non-hex secret":    "clt_" + goodPrefix + "_" + strings.Repeat("z", 64),
		"secret off by one": "clt_" + goodPrefix + "_" + goodSecret[:63],
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, _, ok := ParseToken(raw); ok {
				t.Fatalf("accepted a malformed token: %q", raw)
			}
		})
	}
}

// The hash is plain SHA-256 of the WHOLE token string. Django computes the
// same value in Python and Node in JavaScript; if this ever stops agreeing,
// every credential stops working at once, so it is worth pinning.
func TestHashTokenIsPlainSHA256OfTheWholeString(t *testing.T) {
	sum := sha256.Sum256([]byte(goodToken))
	want := hex.EncodeToString(sum[:])
	if got := hashToken(goodToken); got != want {
		t.Fatalf("hashToken = %s, want %s", got, want)
	}
	if len(hashToken(goodToken)) != 64 {
		t.Fatal("a sha256 hex digest must be 64 characters")
	}
}

func TestHasScope(t *testing.T) {
	token := &Token{Scopes: []string{PermissionMessagesRead, PermissionEventsSubscribe}}
	if !token.HasScope(PermissionMessagesRead) {
		t.Fatal("a held scope was reported missing")
	}
	if token.HasScope(PermissionMessagesSend) {
		t.Fatal("an absent scope was reported held")
	}
	if (&Token{}).HasScope(PermissionMessagesRead) {
		t.Fatal("a token with no scopes reported holding one")
	}
}

// Authorize must refuse anything outside gatedPermissions rather than guess.
// The set is small on purpose: every entry has to be a global-scoped codename,
// because this service does not carry the role matrix needed to resolve a
// realm-scoped one. Widening it without that context is how a deny silently
// becomes an allow.
func TestGatedPermissionsAreTheOnesWeDocument(t *testing.T) {
	want := map[string]bool{
		PermissionEventsSubscribe:   true,
		PermissionMessagesRead:      true,
		PermissionNotificationsRead: true,
		PermissionMessagesSend:      true,
		// Added with POST /v1/comments. Confirmed against
		// entity/permissions.py: COMMENTS_CREATE is in GLOBAL_SCOPED, so it
		// resolves without the role matrix this service does not carry.
		PermissionCommentsCreate: true,
	}
	if len(gatedPermissions) != len(want) {
		t.Fatalf("gatedPermissions has %d entries, expected %d - if you added one, "+
			"confirm it is GLOBAL-scoped in the platform catalog first",
			len(gatedPermissions), len(want))
	}
	for permission := range want {
		if !gatedPermissions[permission] {
			t.Fatalf("%s is documented as gated but is not in the set", permission)
		}
	}
}

func TestAuthorizeRefusesUngatedPermissions(t *testing.T) {
	token := &Token{Scopes: []string{"realm.member.view"}}
	// nil pool is fine: the permission check happens before any query, and
	// reaching the database here would itself be the bug.
	allowed, err := Authorize(t.Context(), nil, token, "realm.member.view")
	if allowed {
		t.Fatal("authorized a permission this service cannot decide")
	}
	if err != ErrUnknownPermission {
		t.Fatalf("expected ErrUnknownPermission, got %v", err)
	}
}

func TestAuthorizeDeniesWhenTheScopeIsAbsent(t *testing.T) {
	token := &Token{Scopes: []string{PermissionMessagesRead}}
	// Also before any query: a token without the scope never reaches the
	// entity half, so a nil pool proves the short-circuit.
	allowed, err := Authorize(t.Context(), nil, token, PermissionEventsSubscribe)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if allowed {
		t.Fatal("authorized without the scope on the token")
	}
}
