// Realtime activity on ONE post - the second axis of realtime, alongside the
// per-entity notification stream this service already publishes to.
//
// # WHY A BOT'S COMMENT NEEDED THIS
//
// Writing the comment row and its notifications is enough to make a bot's
// answer ARRIVE. It is not enough to make it APPEAR. The person sitting in the
// comment section they just asked a question in is not the person a
// notification is addressed to - they are already looking at the thread, and
// `events_<entity_id>` has nothing to say to them about a post they are
// reading. So a bot's reply landed in the database and the asker saw nothing
// until they reloaded: the one reader most likely to be watching was the one
// reader not told.
//
// Django closes exactly this gap for comments written through it
// (user_service/newsfeed/services/post_realtime.py). This is the same publish,
// from the write path Django does not own.
//
//	CHANNEL    `post_<post_id>` - addressed to a POST ("something happened
//	           here"), reaching whoever is reading it, as against
//	           `events_<entity_id>`, which is addressed to a PERSON
//	           ("something happened that concerns you"). A comment would
//	           otherwise have to be published once per reader, and nothing
//	           knows who the readers are.
//
//	SSE EVENT  `post_activity` - constant. The channel already scopes the
//	           stream to one post, so naming the event after the post id too
//	           would only force clients to build listener names at runtime.
//
//	BODY       {post_id, event_type, entity?, ...}, where event_type is one of
//	           "comment" | "typing" | "reaction" | "share". Clients must ignore
//	           an event_type they do not know rather than treat it as an error,
//	           which is what lets a new one be a publisher change alone.
//
// # THREE PUBLISHERS NOW WRITE THIS SHAPE
//
// Django (comment, reaction), Node (typing, in server/reusables/hooks/sse.js -
// typing is never stored, so the service that owns comment rows has no opinion
// about it) and this one. Changing the shape means changing all three, and
// that is recorded in each.
//
// This service publishes "comment" ONLY. It has no reaction or share route, so
// publishing those would be announcing activity it cannot cause.
//
// # WHAT THE BODY DELIBERATELY DOES NOT CARRY
//
// Ids and the actor's identity, never comment text. A subscriber refetches
// through the platform's comments GET, which is where post visibility is
// enforced - so this channel only ever says that there is something new to
// fetch, and the audience of the CONTENT stays exactly what it already was.
// Putting the text in the frame would hand it to every subscriber on a channel
// whose only gate is at subscribe time.
package platform

import (
	"context"
	"errors"
	"log/slog"

	"github.com/jackc/pgx/v5"
)

// The SSE event name every subscriber selects on. Node exports the same
// constant; Django spells it in post_realtime.py.
const postActivityEvent = "post_activity"

// Values `event_type` may take, spelled as Django's ACTIVITY_* constants spell
// them - a consumer filtering on the string gets nothing at all from a near
// miss. Only activityComment is published here; the rest are named so that a
// reader of this file sees the whole contract rather than the slice of it this
// service happens to use.
const (
	activityComment  = "comment"
	activityTyping   = "typing"   // Node's, and only Node's - never stored.
	activityReaction = "reaction" // Django's; no route here causes one.
	activityShare    = "share"    // reserved by the contract; nobody publishes it yet.
)

// postActivityChannel is the pub/sub channel carrying one post's activity.
// `post_<post_id>`, matching Django's post_activity_channel() and Node's
// postActivityChannel().
func postActivityChannel(postID string) string {
	return "post_" + postID
}

// postActor is WHO did it, in the shape the clients already render.
//
// `entity_id` is the field that earns the rest: a client compares it against
// its own to recognise its OWN echo, having already applied that change
// optimistically, and acting on the event again would double-count it. The
// other three are for display.
type postActor struct {
	EntityID string `json:"entity_id"`
	Handle   string `json:"handle"`
	Name     string `json:"name"`
	Type     string `json:"type"`
}

// resolveActor reads the acting entity's identity for the frame.
//
// # WHY THIS IS NOT handleOr()
//
// They look like the same lookup and they are not. handleOr mirrors Django's
// get_entity_display_username() - the "@mention" form that goes in a
// notification SENTENCE. This mirrors get_entity_profile_path() plus
// get_entity_name(), which is what _actor() publishes. The two disagree for a
// slug-less realm (this one falls back to realm_id, the mention form does not)
// and for bots (see below), so folding them into one query would force one of
// the two shapes to change. One extra indexed lookup on a write path is a
// cheap price for not conflating two identity shapes that already exist.
//
// # ONE DIVERGENCE FROM DJANGO, AND IT IS THE POINT OF THE FEATURE
//
// Django's get_entity_profile_path() has branches for an Account and a Realm
// and none for a Bot, so a bot falls through to str(entity.id) and its frames
// carry a UUID where the client renders a handle.
//
// Django can afford that because a bot entity cannot BE request.entity there,
// by two independent routes: its auth backend resolves the caller through
// Account.objects.get() plus a live device session (user/backends.py), which a
// bot has neither of, and entity switching - the only thing that makes
// request.entity something other than the account - accepts users and page
// realms only (user/entity_switch_views.py). So _actor() never sees a bot, and
// the missing branch is unreachable rather than wrong.
//
// Reproducing it HERE, where the caller is a bot essentially every time, would
// mean shipping a realtime frame that names its actor with a UUID - the one
// identity the comment section cannot render. This is not really a divergence:
// it is the only implementation of a bot actor that ever runs.
//
// It becomes one the day Django grows bot authentication or lets a user switch
// into a bot they own. At that point the fix is a bots branch in
// get_entity_profile_path(), not a change here.
//
// So bots resolve through bot_bot.handle, which is what
// get_entity_display_username() already does for a bot and what this service's
// own HandlesFor() does everywhere else. The divergence only ever replaces an
// unusable value with the usable one the platform already stores.
func resolveActor(ctx context.Context, deps Deps, entityID string) *postActor {
	if entityID == "" {
		return nil
	}

	var (
		entityType string
		handle     string
		name       string
	)
	// Anchored on entity_entity and LEFT JOINing all three detail tables, the
	// same shape as Receivers(): an entity backs exactly one of them, and
	// starting from any single table drops the other two kinds silently.
	//
	// COALESCE(r.slug, r.realm_id) is get_entity_profile_path()'s
	// `realm.slug or realm.realm_id` - a realm created without a slug still
	// has a path.
	err := deps.Postgres.QueryRow(ctx, `
		SELECT COALESCE(e.type, ''),
		       COALESCE(u.username, r.slug, r.realm_id, b.handle, ''),
		       COALESCE(u.first_name || ' ' || u.last_name, r.name, b.name, '')
		  FROM entity_entity e
		  LEFT JOIN user_account   u ON u.entity_id = e.id AND e.type = 'user'
		  LEFT JOIN community_realm r ON r.entity_id = e.id AND e.type = 'realm'
		  LEFT JOIN bot_bot        b ON b.entity_id = e.id AND e.type = 'bot'
		 WHERE e.id = $1`, entityID,
	).Scan(&entityType, &handle, &name)

	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		slog.Warn("could not resolve post activity actor", "entity_id", entityID, "error", err)
	}

	// Deliberately still returns an actor when the read failed or matched
	// nothing. entity_id is the only field a client ACTS on - it is how a
	// client recognises its own echo and declines to double-count it - and it
	// is the one field that needs no query. Dropping the whole actor to avoid
	// two empty display strings would break the echo check for everyone on the
	// channel to spare a cosmetic gap.
	actor := normalizeActor(entityID, handle, name, entityType)
	return &actor
}

// normalizeActor applies the platform's fallbacks: an entity backing none of
// the three concrete kinds displays as its own id, which is what both
// get_entity_profile_path() and get_entity_name() return for one.
//
// An id reads badly in a comment header. It reads better than an empty string,
// which renders as a nameless comment, and it is what the platform already
// does everywhere else.
func normalizeActor(entityID, handle, name, entityType string) postActor {
	if handle == "" {
		handle = entityID
	}
	if name == "" {
		name = entityID
	}
	return postActor{
		EntityID: entityID,
		Handle:   handle,
		Name:     name,
		Type:     entityType,
	}
}

// commentActivityFields is the part of the body specific to a comment.
//
// `parent_id` is the thread the row ACTUALLY LANDED IN, not the comment the
// author aimed at - replying to a reply re-parents onto the top-level ancestor
// (see storedParentFor). It is what tells a client WHICH list to refetch: null
// means the top-level list, anything else means that thread, and a thread
// nobody has expanded is nothing to refetch at all. Passing the aimed-at
// comment here instead would send readers to refetch a list the row is not in.
//
// Normalised to a JSON null rather than "", so an absent parent cannot read as
// a thread id - the same normalisation Node applies to the typing event's
// parent_id, which names the same axis.
func commentActivityFields(commentID, storedParentID string) map[string]any {
	fields := map[string]any{"comment_id": commentID, "parent_id": nil}
	if storedParentID != "" {
		fields["parent_id"] = storedParentID
	}
	return fields
}

// publishCommentCreated wakes every comment section currently open on this
// post, on any client.
//
// Distinct from the notifications written alongside it: those reach the post's
// author and the person replied to wherever they are, this reaches whoever
// happens to be reading the post right now - including people the comment
// concerns not at all.
func publishCommentCreated(ctx context.Context, deps Deps, postID, commentID, storedParentID, authorEntityID string) {
	publishPostActivity(ctx, deps, postID, activityComment,
		resolveActor(ctx, deps, authorEntityID),
		commentActivityFields(commentID, storedParentID))
}

// publishPostActivity announces activity on `postID` to whoever has it open.
//
// # NOT DEFERRED TO COMMIT, UNLIKE DJANGO'S - AND THAT IS NOT AN OVERSIGHT
//
// Django's publish_post_activity() goes through transaction.on_commit()
// because it is called from inside `with transaction.atomic()`. The event
// makes the receiver come back and READ the rows the caller is still writing,
// so publishing inline is a race the receiver usually wins: the frame leaves
// immediately, the client refetches on another connection, and it reads the
// pre-insert state - the comment section flashes and shows nothing new.
//
// Here the INSERT is its own autocommitted statement and has already returned
// by the time any of the fan-out runs, so the row is durable and visible to
// every other connection before this is called. There is nothing to defer to.
//
// IF THE COMMENT WRITE IS EVER WRAPPED IN A TRANSACTION, this publish has to
// move with it - to after the commit, not merely to the end of the block.
//
// Best-effort, like the rest of the fan-out: the comment is already committed,
// and nobody's comment should fail because the announcement did.
func publishPostActivity(ctx context.Context, deps Deps, postID, eventType string, actor *postActor, fields map[string]any) {
	if postID == "" {
		return
	}

	publishEnvelope(ctx, deps, postActivityChannel(postID), postActivityEvent,
		postActivityMessage(postID, eventType, actor, fields))
}

// postActivityMessage is the `message` half of the frame - everything inside
// the platform's envelope, which publishEnvelope wraps.
//
// Split out from the publish so the wire shape can be tested without a Redis:
// this is the part three services have to agree on, and the part where a
// renamed key fails silently. A frame with a wrong shape is still delivered,
// still parses, and simply does nothing on arrival.
func postActivityMessage(postID, eventType string, actor *postActor, fields map[string]any) map[string]any {
	body := map[string]any{
		"post_id":    postID,
		"event_type": eventType,
	}
	for field, value := range fields {
		body[field] = value
	}
	// Omitted rather than null-valued when there is no actor, matching _actor()
	// returning None - a reader treats an absent entity as "the platform did
	// this", which is not the same claim as an entity with no id.
	if actor != nil {
		body["entity"] = actor
	}

	// `message` repeats the event_type that `result` also carries. Redundant,
	// and reproduced deliberately: Django and Node both write it, and the
	// envelope's `message` field is what some clients log.
	return map[string]any{
		"status": true, "auth": true, "message": eventType, "result": body,
	}
}
