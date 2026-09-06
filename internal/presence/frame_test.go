package presence

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const testSecret = "a-shared-platform-secret"

var signedAt = time.Date(2026, time.September, 6, 10, 30, 0, 0, time.UTC)

// decodeFrame does what a client does: pull `result` out of the envelope and
// read the payload. Verified here, unlike in the clients, because a test can
// afford to check the thing they take on trust.
func decodeFrame(t *testing.T, body map[string]any) jwt.MapClaims {
	t.Helper()

	raw, ok := body["result"].(string)
	if !ok {
		t.Fatalf("result = %#v, want a signed string", body["result"])
	}
	claims := jwt.MapClaims{}
	if _, err := jwt.ParseWithClaims(raw, claims, func(*jwt.Token) (any, error) {
		return []byte(testSecret), nil
	}); err != nil {
		t.Fatalf("frame did not verify against the shared secret: %v", err)
	}
	return claims
}

// The envelope's own two flags are what both clients gate on before they
// bother decoding - webapp checks parsedresponse.auth && .status, Flutter the
// same pair.
func TestSignFrameSetsTheFlagsClientsGateOn(t *testing.T) {
	body, err := SignFrame(testSecret, "entity-1", true, signedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if body["status"] != true {
		t.Fatalf("status = %v, want true", body["status"])
	}
	if body["auth"] != true {
		t.Fatalf("auth = %v, want true", body["auth"])
	}
}

// These three field names ARE the contract with two client codebases. They
// are also the shape /u/activecontacts returns per row, so a client's boot
// snapshot and its live updates have to parse identically.
func TestSignFrameCarriesTheFieldNamesBothClientsRead(t *testing.T) {
	body, err := SignFrame(testSecret, "entity-1", true, signedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	claims := decodeFrame(t, body)

	user, ok := claims["user"].(map[string]any)
	if !ok {
		t.Fatalf("user = %#v, want an object under the `user` key", claims["user"])
	}
	if user["_id"] != "entity-1" {
		t.Fatalf("_id = %v, want entity-1", user["_id"])
	}
	if user["sessionStatus"] != true {
		t.Fatalf("sessionStatus = %v, want true", user["sessionStatus"])
	}
	date, ok := user["sessiondate"].(map[string]any)
	if !ok {
		t.Fatalf("sessiondate = %#v, want an object", user["sessiondate"])
	}
	if _, ok := date["date"].(string); !ok {
		t.Fatalf("sessiondate.date = %#v, want a string", date["date"])
	}
}

// `sessionStatus` is the WIRE name; the Mongo document calls the same thing
// `status`. Writing the document's name here would leave every client reading
// undefined and rendering "offline" for an entity that just came online.
func TestSignFrameUsesTheWireNameNotTheDocumentName(t *testing.T) {
	body, err := SignFrame(testSecret, "entity-1", true, signedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	user := decodeFrame(t, body)["user"].(map[string]any)

	if _, wrong := user["status"]; wrong {
		t.Fatal("frame carries `status`; clients read `sessionStatus`")
	}
	if _, right := user["sessionStatus"]; !right {
		t.Fatal("frame is missing `sessionStatus`")
	}
}

func TestSignFrameCarriesOfflineToo(t *testing.T) {
	body, err := SignFrame(testSecret, "entity-1", false, signedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	user := decodeFrame(t, body)["user"].(map[string]any)

	if user["sessionStatus"] != false {
		t.Fatal("an offline frame must say sessionStatus:false, not omit it - " +
			"an absent field reads as false by accident rather than on purpose")
	}
}

// HS256 is jsonwebtoken's default for a string secret, so this is what Node
// produces for the same event. A different algorithm here would still decode
// in the clients (they do not verify) and would fail the moment anything did.
func TestSignFrameUsesHS256LikeNode(t *testing.T) {
	body, err := SignFrame(testSecret, "entity-1", true, signedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	raw := body["result"].(string)

	parsed, _, err := jwt.NewParser().ParseUnverified(raw, jwt.MapClaims{})
	if err != nil {
		t.Fatalf("unparseable token: %v", err)
	}
	if parsed.Method.Alg() != "HS256" {
		t.Fatalf("alg = %s, want HS256", parsed.Method.Alg())
	}
}

// A frame signed with the wrong secret is worse than no frame: it looks valid
// to a client that only decodes, and fails for anything that verifies. Better
// to refuse than to emit one.
func TestSignFrameRefusesWithoutASecret(t *testing.T) {
	if _, err := SignFrame("", "entity-1", true, signedAt); err == nil {
		t.Fatal("expected an error when JWT_SECRET is unset")
	}
}

// The event name is what both clients subscribe by. It is not this service's
// to rename - the browser path has always emitted it.
func TestClientEventMatchesWhatClientsListenFor(t *testing.T) {
	if ClientEvent != "active_users" {
		t.Fatalf("ClientEvent = %q, want active_users", ClientEvent)
	}
}
