package auth

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type contextKey struct{}

var tokenKey contextKey

// FromContext returns the token a request authenticated with.
func FromContext(ctx context.Context) (*Token, bool) {
	token, ok := ctx.Value(tokenKey).(*Token)
	return token, ok
}

// Middleware authenticates `Authorization: Bearer clt_...`.
//
// Deliberately NOT x-access-token, which is the user session's header on the
// Django and Node surfaces. A credential that cannot be presented on the wrong
// door cannot be accepted by it by mistake.
func Middleware(pool *pgxpool.Pool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		parts := strings.Fields(header)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
			writeError(w, http.StatusUnauthorized, "Authorization header must read: Bearer <token>")
			return
		}

		token, err := Verify(r.Context(), pool, parts[1])
		if err != nil {
			writeError(w, http.StatusUnauthorized, "Invalid or expired token.")
			return
		}

		// Best-effort telemetry; never fail a request over it.
		if err := TouchLastUsed(r.Context(), pool, token.ID); err != nil {
			slog.Warn("last_used_at update failed", "token_id", token.ID, "error", err)
		}

		ctx := context.WithValue(r.Context(), tokenKey, token)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequireScope gates a handler on one permission, checking both halves.
func RequireScope(pool *pgxpool.Pool, permission string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := FromContext(r.Context())
		if !ok {
			// The route was wired without Middleware in front of it: a
			// configuration bug, not a client error.
			slog.Error("RequireScope ran with no token in context", "permission", permission)
			writeError(w, http.StatusInternalServerError, "Route is misconfigured.")
			return
		}

		allowed, err := Authorize(r.Context(), pool, token, permission)
		if err != nil {
			slog.Error("authorization check failed", "permission", permission, "error", err)
			writeError(w, http.StatusInternalServerError, "Could not verify token scope.")
			return
		}
		if !allowed {
			writeError(w, http.StatusForbidden,
				"This token is not permitted to perform this action ("+permission+").")
			return
		}

		next.ServeHTTP(w, r)
	})
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"status": false, "message": message})
}
