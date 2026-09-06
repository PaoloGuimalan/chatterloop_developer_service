package presence

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// ClientEvent is the SSE event name both clients already listen for. Not new:
// this service now emits the same event the browser path has always emitted,
// so a bot coming online is indistinguishable on the wire from a person doing
// it - which is what lets webapp and chatterloop_app render it with the code
// they already have.
const ClientEvent = "active_users"

// tokenLifetime matches Node's createJWTwExp (7 days). The value is not
// load-bearing - clients decode this payload without verifying it - but the
// two signers should not disagree about a claim they both write.
const tokenLifetime = 7 * 24 * time.Hour

// sessionMetadata is the object the clients read, under a `user` key.
//
// FIELD NAMES ARE THE CONTRACT, and they are not this service's to choose:
// webapp reads `_id`/`sessionStatus`/`sessiondate` off the decoded payload
// (reusables/hooks/sse.ts, UPDATE_ACTIVE_USERS_LIST) and chatterloop_app reads
// exactly the same three (core/utils/sse_events.dart, "active_users"). They
// are also the shape `/u/activecontacts` returns per row, so a client's
// snapshot and its live updates parse identically.
//
// `sessionStatus` rather than `status`: that is the name this object has ON
// THE WIRE. The Mongo document calls the same thing `status`, and the two are
// deliberately different - Node's own read path renames it at exactly this
// boundary too.
type sessionMetadata struct {
	ID            string      `json:"_id"`
	SessionStatus bool        `json:"sessionStatus"`
	SessionDate   sessionDate `json:"sessiondate"`
}

// sessionDate is what renders "Active 5 minutes ago" once the dot goes out.
// Only `date` is written; the browser path also writes `time` on some paths
// and the clients treat its absence as "use the date", so omitting it is a
// supported shape rather than a gap.
type sessionDate struct {
	Date string `json:"date"`
}

// SignFrame builds the JWT-wrapped body of an `active_users` frame.
//
// Signed because the event ALREADY is: Node's UpdateContactswSessionStatus
// wraps this payload with createJWTwExp, and both clients call a decode on
// `result` unconditionally. Publishing an unsigned body here would not be a
// smaller frame, it would be an unparseable one.
//
// HS256, matching jsonwebtoken's default for a string secret. Clients decode
// without verifying, so the signature is not what protects this - the channel
// is, since a frame only reaches an entity's own subscribed stream.
func SignFrame(secret, entityID string, online bool, at time.Time) (map[string]any, error) {
	if secret == "" {
		return nil, fmt.Errorf("JWT secret is not configured")
	}

	claims := jwt.MapClaims{
		"user": sessionMetadata{
			ID:            entityID,
			SessionStatus: online,
			SessionDate: sessionDate{
				// ISO8601. The clients parse this leniently and fall back to
				// the frame's own arrival time, but a value they can read
				// means "Active 5 minutes ago" rather than "Recently Active".
				Date: at.UTC().Format(time.RFC3339),
			},
		},
		"iat": at.Unix(),
		"exp": at.Add(tokenLifetime).Unix(),
	}

	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).
		SignedString([]byte(secret))
	if err != nil {
		return nil, fmt.Errorf("sign presence frame: %w", err)
	}

	// The envelope Node's publish() writes around every frame, which is what
	// the SSE bridge and this service's own /v1/events both unwrap.
	return map[string]any{
		"status": true,
		"auth":   true,
		"result": signed,
	}, nil
}
