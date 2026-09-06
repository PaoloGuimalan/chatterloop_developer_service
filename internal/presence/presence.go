// Package presence records that a token-authenticated client is connected and
// tells everyone entitled to see it.
//
// # WHY A BOT NEEDS A SESSION AT ALL
//
// The platform's "Active Now" dot reads one collection: Mongo `sessions`,
// keyed on (entityID, deviceToken), written by Node when a browser opens its
// SSE stream and cleared when that stream closes
// (server/routes/users/index.js, setUserSession). Nothing about that schema is
// user-specific - `entityID` is an ENTITY, and a page acting as itself already
// writes rows there today.
//
// A bot was the one participant that never appeared, not because presence was
// withheld from it but because it connects HERE, and this service had no
// reason to touch that collection. So a bot in a conversation was permanently
// dotless: not "offline", simply unrepresented.
//
// # THE DEVICE TOKEN IS THE CREDENTIAL'S PREFIX
//
// A browser generates a deviceToken once per install and reuses it across
// logins. There is no equivalent event in a bot's life: it has no install, no
// login, and no place to persist something generated at startup - so anything
// minted here would be new on every restart and leave an orphan session row
// behind each time.
//
// The token's own prefix already has the properties wanted: unique per
// credential, stable across restarts and reconnects, and public by
// construction (it travels in the clear inside every token). It also carries
// the right MEANING - a credential is issued per running instance, so "one
// prefix, one session row" says exactly what a device token says for a
// browser. Two instances sharing a credential collapse onto one row, which is
// correct: they are one bot, and rag_service refuses to run two of itself
// against one identity anyway (chatterloop/single_instance.py).
//
// # THIS SERVICE DOES THE WHOLE JOB
//
// Writing the row, resolving who may see it, signing the frame and publishing
// it all happen here, so a bot connecting costs no work anywhere else and no
// second process has to exist to watch for it. What that needs from the
// outside is one value: JWT_SECRET, the same secret Node signs the same frame
// with. The scope query is the one thing genuinely duplicated - see scope.go
// for why that is tolerable and what keeps the two copies honest.
package presence

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// deviceTypeBot is what lands in the session row's deviceType, where a browser
// writes "desktop"/"mobile"/"tablet". Kept distinct so an operator reading the
// collection can tell a bot's row from a person's at a glance, and so a future
// device list can filter them out of a human's screen.
const deviceTypeBot = "bot"

// offlineGrace is how long a disconnect waits before it is believed.
//
// This service caps a stream's lifetime, so a long-running bot disconnects and
// reconnects on a schedule - roughly hourly - and each of those is a genuine
// close followed by a genuine open a second or two later. Announce on the
// close alone and every bot's dot blinks off once an hour for no reason a
// viewer could explain.
//
// Waiting and then RE-READING the sessions collection settles it from the
// source of truth rather than from event ordering: if the client came back its
// row is live again, and if another session for the same entity was live all
// along (a page with two admins switched in) there was never anything to
// announce. Comfortably longer than the client's own reconnect backoff (2s,
// rag_service consumer.py), short enough that a real disconnect still reads as
// prompt.
const offlineGrace = 6 * time.Second

// Recorder writes one entity's session row and fans out the change.
type Recorder struct {
	Mongo    *mongo.Database
	Postgres *pgxpool.Pool
	Redis    *redis.Client
	// JWTSecret signs the client frame. Without it the row is still written
	// and the dot still appears on the next snapshot - it is the live push
	// that is lost, so a missing secret degrades rather than breaks.
	JWTSecret string
	PodName   string
}

// Connected marks this entity online and tells everyone in scope.
//
// Upsert, not insert: the row must survive reconnects. Each reconnect must
// land on the SAME row rather than accumulating one per connection.
//
// Errors are logged and swallowed. Presence is decoration: a bot whose dot
// fails to light must still receive its events, so nothing here is allowed to
// fail a stream that is otherwise working.
func (r Recorder) Connected(ctx context.Context, entityID, prefix, name string) {
	if r.Mongo == nil || entityID == "" || prefix == "" {
		return
	}
	now := time.Now().UTC()
	filter, update := sessionUpsert(entityID, prefix, name, now)

	if _, err := r.Mongo.Collection("sessions").
		UpdateOne(ctx, filter, update, options.Update().SetUpsert(true)); err != nil {
		slog.Error("presence connect write failed",
			"entity_id", entityID, "prefix", prefix, "error", err)
		return
	}
	slog.Info("presence online", "entity_id", entityID, "prefix", prefix)

	// Its own context: the fan-out must not be bounded by the request, which
	// on a long-lived stream outlives it by design anyway.
	go r.announce(entityID, true, now)
}

// Disconnected marks this entity offline, once it is actually offline.
//
// Deliberately NOT a delete: `lastSeen` is what renders "Active 2 hours ago"
// after the dot goes out, so the row has to outlive the connection. Same
// reasoning as Node's setUserSession(false), which also updates rather than
// removes.
//
// TAKES NO CALLER CONTEXT, unlike Connected, and that is load-bearing. This
// runs from a defer AFTER the stream ended, so the request context is already
// cancelled - that cancellation is the very thing that told us the client went
// away. Inheriting it would abort the write before it reached Mongo and leave
// the row reading `status: true` forever, showing the bot as permanently
// online.
//
// BLOCKS for offlineGrace before announcing. The caller is a deferred call on
// a connection that has already ended, so there is nothing left for it to hold
// up.
func (r Recorder) Disconnected(entityID, prefix string) {
	if r.Mongo == nil || entityID == "" || prefix == "" {
		return
	}
	now := time.Now().UTC()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := r.Mongo.Collection("sessions").UpdateOne(ctx,
		bson.M{"entityID": entityID, "deviceToken": prefix},
		bson.M{"$set": bson.M{"status": false, "lastSeen": now}},
	); err != nil {
		slog.Error("presence disconnect write failed",
			"entity_id", entityID, "prefix", prefix, "error", err)
		return
	}
	slog.Info("presence offline", "entity_id", entityID, "prefix", prefix)

	time.Sleep(offlineGrace)

	graceCtx, graceCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer graceCancel()

	live, err := HasLiveSession(graceCtx, r.Mongo, entityID)
	if err != nil {
		// Unreadable rather than known-live. Announcing is the safer of the
		// two: a dot that goes out a little eagerly is a smaller wrong than
		// one that stays lit for an entity that has gone.
		slog.Warn("presence live-session check failed",
			"entity_id", entityID, "error", err)
	}
	if live {
		slog.Info("presence disconnect superseded by a live session",
			"entity_id", entityID)
		return
	}

	r.announce(entityID, false, time.Now().UTC())
}

// announce resolves who may see this entity and publishes the client frame to
// each of them.
//
// One frame per recipient, addressed to `events_<entity_id>` - the channel
// Node's SSE bridge and this service's own /v1/events both already subscribe
// to, so a single write reaches browsers and API clients alike.
func (r Recorder) announce(entityID string, online bool, at time.Time) {
	if r.Redis == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	scope, err := Scope(ctx, r.Postgres, r.Mongo, entityID)
	if err != nil {
		// Partial scopes are still worth publishing - see Scope.
		slog.Warn("presence scope partially failed", "entity_id", entityID, "error", err)
	}
	if len(scope) == 0 {
		return
	}

	body, err := SignFrame(r.JWTSecret, entityID, online, at)
	if err != nil {
		slog.Error("presence frame signing failed", "entity_id", entityID, "error", err)
		return
	}

	published := 0
	for _, recipient := range scope {
		if r.publish(ctx, "events_"+recipient, ClientEvent, body) {
			published++
		}
	}
	slog.Info("presence announced",
		"entity_id", entityID, "online", online, "recipients", published)
}

// publish writes one frame in the exact envelope the platform's Redis
// publisher produces (server/reusables/redis/pubsub.js publish()), which is
// what the subscribers on the other end unwrap.
func (r Recorder) publish(ctx context.Context, channel, event string, message any) bool {
	envelope := map[string]any{
		"logType":  nil,
		"pod":      r.PodName,
		"event":    event,
		"message":  message,
		"dateTime": time.Now().UTC().Format(time.RFC3339Nano),
	}
	// encoding/json, NOT bson.MarshalExtJSON: the subscribers are Node's
	// JSON.parse and this service's own passthrough, and ExtJSON would wrap
	// values in type envelopes they would read as literal nested objects.
	body, err := json.Marshal(envelope)
	if err != nil {
		slog.Error("presence frame encode failed", "channel", channel, "error", err)
		return false
	}
	if err := r.Redis.Publish(ctx, channel, body).Err(); err != nil {
		slog.Error("presence frame publish failed", "channel", channel, "error", err)
		return false
	}
	return true
}

// sessionUpsert builds the (filter, update) pair that marks one credential's
// session online.
//
// Pure, and separate from the call that runs it, for the same reason
// messagePushData is separate from the fan-out in platform/send.go: the shape
// of a document another service reads is exactly the thing worth pinning in a
// test, and it cannot be without a live Mongo unless it is built somewhere a
// test can call.
//
// The `$set` / `$setOnInsert` split is the load-bearing part. Everything that
// describes the CURRENT connection is `$set` and so is refreshed on every
// reconnect; everything that describes the row's IDENTITY is `$setOnInsert`
// and written once, so a reconnect updates a session rather than replacing it.
func sessionUpsert(entityID, prefix, name string, now time.Time) (bson.M, bson.M) {
	filter := bson.M{"entityID": entityID, "deviceToken": prefix}
	update := bson.M{
		"$set": bson.M{
			"status":   true,
			"lastSeen": now,
			// Refreshed on every connect rather than only on insert: a token
			// renamed in admin should read as its current name here.
			"userAgent": userAgent(name),
		},
		"$setOnInsert": bson.M{
			"sessionID":   "SESSION_BOT_" + prefix,
			"entityID":    entityID,
			"deviceToken": prefix,
			"deviceType":  deviceTypeBot,
			// A bot has no browser, no OS and no meaningful IP - it is not a
			// device in a place. Explicit nulls rather than invented values,
			// so a device list renders "unknown" rather than a fiction.
			"browser": nil,
			"os":      nil,
			"ip":      nil,
			// No push token, ever: a bot has no Firebase registration and
			// nothing would deliver to it. Set so the field exists with the
			// same shape a browser's row has.
			"fcmToken": nil,
		},
	}
	return filter, update
}

// userAgent is the human-readable "what is this" for a session row, standing
// in for a browser's UA string. The token's name is what an operator typed
// when issuing it ("rag pipeline (prod)"), so it is the most useful thing to
// show in a device list.
func userAgent(name string) string {
	if name == "" {
		return "developer_service client"
	}
	return "developer_service client (" + name + ")"
}
