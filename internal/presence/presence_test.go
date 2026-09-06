package presence

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

var at = time.Date(2026, time.September, 6, 10, 30, 0, 0, time.UTC)

// The row has to be findable by exactly the pair Node's own queries use -
// setUserSession and jwtchecker both look sessions up by (entityID,
// deviceToken), so a row keyed any other way is invisible to every reader
// that already exists.
func TestSessionUpsertIsKeyedOnEntityAndDeviceToken(t *testing.T) {
	filter, _ := sessionUpsert("entity-1", "be05e088d3bd", "Xenon (rag_service)", at)

	if got := filter["entityID"]; got != "entity-1" {
		t.Fatalf("filter entityID = %v, want entity-1", got)
	}
	if got := filter["deviceToken"]; got != "be05e088d3bd" {
		t.Fatalf("filter deviceToken = %v, want the token prefix", got)
	}
	if len(filter) != 2 {
		t.Fatalf("filter has %d keys; a third would narrow it past what "+
			"Node's own lookups match", len(filter))
	}
}

// The device token IS the prefix. Nothing generates one, which is the whole
// point - a generated value would be new on every restart and strand the
// previous row reading online forever.
func TestSessionUpsertUsesThePrefixAsTheDeviceToken(t *testing.T) {
	filter, update := sessionUpsert("entity-1", "abc123def456", "", at)
	onInsert := update["$setOnInsert"].(bson.M)

	if filter["deviceToken"] != "abc123def456" {
		t.Fatal("filter must key on the prefix")
	}
	if onInsert["deviceToken"] != "abc123def456" {
		t.Fatal("inserted row must carry the prefix as its deviceToken")
	}
}

// A reconnect must land on the same row. Anything describing the CURRENT
// connection belongs in $set; anything identifying the row belongs in
// $setOnInsert, or every hourly reconnect would rewrite the row's identity.
func TestSessionUpsertRefreshesLivenessButNotIdentity(t *testing.T) {
	_, update := sessionUpsert("entity-1", "prefix", "name", at)

	set := update["$set"].(bson.M)
	if set["status"] != true {
		t.Fatal("connect must set status true")
	}
	if set["lastSeen"] != at {
		t.Fatalf("lastSeen = %v, want %v", set["lastSeen"], at)
	}

	onInsert := update["$setOnInsert"].(bson.M)
	for _, field := range []string{"sessionID", "entityID", "deviceToken", "deviceType"} {
		if _, ok := onInsert[field]; !ok {
			t.Fatalf("%s must be written once, on insert only", field)
		}
	}
	// The inverse: identity fields must NOT also be in $set, or a reconnect
	// would rewrite them.
	for _, field := range []string{"sessionID", "deviceType"} {
		if _, ok := set[field]; ok {
			t.Fatalf("%s is in $set; a reconnect would rewrite it", field)
		}
	}
}

// A browser's row carries a real deviceType ("desktop"/"mobile"/"tablet").
// A bot's says what it is, so a device list can tell them apart rather than
// showing a bot as somebody's laptop.
func TestSessionUpsertMarksTheRowAsABot(t *testing.T) {
	_, update := sessionUpsert("entity-1", "prefix", "", at)
	onInsert := update["$setOnInsert"].(bson.M)

	if onInsert["deviceType"] != deviceTypeBot {
		t.Fatalf("deviceType = %v, want %q", onInsert["deviceType"], deviceTypeBot)
	}
}

// A bot has no Firebase registration, and a push addressed to one would go
// nowhere. The field exists so the row has a browser row's shape, but it must
// never be populated here.
func TestSessionUpsertNeverWritesAPushToken(t *testing.T) {
	_, update := sessionUpsert("entity-1", "prefix", "", at)
	onInsert := update["$setOnInsert"].(bson.M)

	value, present := onInsert["fcmToken"]
	if !present {
		t.Fatal("fcmToken should exist on the row, matching a browser session")
	}
	if value != nil {
		t.Fatalf("fcmToken = %v, want nil - nothing can deliver a push to a bot", value)
	}
}

func TestUserAgentNamesTheCredential(t *testing.T) {
	if got := userAgent("rag pipeline (prod)"); got != "developer_service client (rag pipeline (prod))" {
		t.Fatalf("userAgent = %q", got)
	}
}

// An unnamed token still has to produce something readable rather than a
// dangling "client ()".
func TestUserAgentFallsBackWhenTheTokenIsUnnamed(t *testing.T) {
	if got := userAgent(""); got != "developer_service client" {
		t.Fatalf("userAgent = %q", got)
	}
}
