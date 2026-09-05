package platform

import (
	"encoding/json"
	"testing"
)

func TestPostActivityChannelMatchesTheOtherTwoPublishers(t *testing.T) {
	// Django's post_activity_channel() and Node's postActivityChannel() both
	// produce this. A channel name that differs by one character is a stream
	// nobody is listening to, and nothing anywhere reports an error.
	if got := postActivityChannel("post-1"); got != "post_post-1" {
		t.Fatalf("channel = %q, want %q", got, "post_post-1")
	}
}

func TestATopLevelCommentAnnouncesANullParent(t *testing.T) {
	// null means "the top-level list". An empty string would be a thread id as
	// far as a client is concerned, sending it to refetch a thread that does
	// not exist while the list the comment actually landed in stays stale.
	fields := commentActivityFields("comment-1", "")

	if fields["parent_id"] != nil {
		t.Fatalf("parent_id = %#v, want nil", fields["parent_id"])
	}
	if fields["comment_id"] != "comment-1" {
		t.Fatalf("comment_id = %#v", fields["comment_id"])
	}
}

func TestAReplyAnnouncesTheThreadItLandedIn(t *testing.T) {
	fields := commentActivityFields("comment-1", "top-1")

	if fields["parent_id"] != "top-1" {
		t.Fatalf("parent_id = %#v, want %q", fields["parent_id"], "top-1")
	}
}

// The event must name where the row LANDED, not what the author aimed at.
// Replying to a reply re-parents to the top-level ancestor, so publishing the
// aimed-at id would send every reader to refetch a thread the new comment is
// not in - and the thread it IS in would never refresh.
func TestTheAnnouncedParentIsTheStoredOneNotTheAimedAtOne(t *testing.T) {
	aimedAt := &parentComment{CommentID: "reply-1", ParentCommentID: "top-1"}

	fields := commentActivityFields("comment-1", storedParentFor(aimedAt))

	if fields["parent_id"] == aimedAt.CommentID {
		t.Fatal("published the aimed-at comment; readers would refetch the wrong thread")
	}
	if fields["parent_id"] != "top-1" {
		t.Fatalf("parent_id = %#v, want the top-level ancestor", fields["parent_id"])
	}
}

func TestTheParentIdSerialisesAsJSONNull(t *testing.T) {
	// A Go nil inside map[string]any is only a JSON null if it actually
	// reaches the encoder as one; this is the property the clients test on.
	body, err := json.Marshal(commentActivityFields("comment-1", ""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if value, present := decoded["parent_id"]; !present || value != nil {
		t.Fatalf("parent_id = %#v (present=%v), want an explicit null", value, present)
	}
}

func TestAnActorKeepsTheHandleAndNameItResolved(t *testing.T) {
	actor := normalizeActor("entity-1", "ana", "Ana Reyes", "user")

	if actor.Handle != "ana" || actor.Name != "Ana Reyes" || actor.Type != "user" {
		t.Fatalf("actor = %#v", actor)
	}
}

func TestAnUnresolvableActorFallsBackToItsIdRatherThanBlank(t *testing.T) {
	// Both get_entity_profile_path() and get_entity_name() return str(entity.id)
	// for an entity backing none of the three concrete kinds. An id reads badly
	// in a comment header; an empty string renders as a nameless comment.
	actor := normalizeActor("entity-1", "", "", "")

	if actor.Handle != "entity-1" || actor.Name != "entity-1" {
		t.Fatalf("actor = %#v, want the id in both display fields", actor)
	}
}

func TestTheActorAlwaysCarriesItsEntityID(t *testing.T) {
	// entity_id is the only field a client ACTS on: it is how a client
	// recognises its own echo and declines to double-count a change it already
	// applied optimistically. It must survive a failed identity lookup, which
	// is exactly when the display fields are empty.
	actor := normalizeActor("entity-1", "", "", "")

	if actor.EntityID != "entity-1" {
		t.Fatalf("entity_id = %q, want it preserved with no lookup", actor.EntityID)
	}
}

func TestTheActorSerialisesInTheShapeTheClientsRender(t *testing.T) {
	// Django's _actor() publishes exactly these four keys, and the webapp reads
	// them by name. A renamed field is a comment header that goes blank with
	// nothing in any log to say why.
	body, err := json.Marshal(normalizeActor("entity-1", "ana", "Ana Reyes", "user"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, field := range []string{"entity_id", "handle", "name", "type"} {
		if _, present := decoded[field]; !present {
			t.Fatalf("%q is missing from %v", field, decoded)
		}
	}
	if len(decoded) != 4 {
		t.Fatalf("expected exactly the four documented fields, got %v", decoded)
	}
}

func TestTheEventTypeIsSpelledAsDjangoSpellsIt(t *testing.T) {
	// A consumer filtering on the string gets nothing at all from a near miss,
	// and nothing raises: the frame simply goes out and is ignored.
	if activityComment != "comment" {
		t.Fatalf("event_type = %q, want %q", activityComment, "comment")
	}
	if postActivityEvent != "post_activity" {
		t.Fatalf("SSE event = %q, want %q", postActivityEvent, "post_activity")
	}
}

// The whole wire shape, in one test, because this is what three services have
// to agree on: Django's publish_post_activity(), Node's BroadcastPostActivity()
// and this. A renamed key here fails SILENTLY - the frame is delivered, it
// parses, and it does nothing on arrival.
func TestTheCommentFrameMatchesTheDocumentedShape(t *testing.T) {
	actor := normalizeActor("bot-entity", "helper", "Helper Bot", "bot")

	body, err := json.Marshal(postActivityMessage(
		"post-1", activityComment, &actor, commentActivityFields("comment-1", "top-1")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var frame struct {
		Status bool   `json:"status"`
		Auth   bool   `json:"auth"`
		Msg    string `json:"message"`
		Result struct {
			PostID    string  `json:"post_id"`
			EventType string  `json:"event_type"`
			CommentID string  `json:"comment_id"`
			ParentID  *string `json:"parent_id"`
			Entity    struct {
				EntityID string `json:"entity_id"`
				Handle   string `json:"handle"`
				Name     string `json:"name"`
				Type     string `json:"type"`
			} `json:"entity"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &frame); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	switch {
	case !frame.Status || !frame.Auth:
		t.Fatalf("status/auth = %v/%v, want true/true", frame.Status, frame.Auth)
	case frame.Msg != activityComment:
		t.Fatalf("message = %q, want the event type", frame.Msg)
	case frame.Result.PostID != "post-1":
		t.Fatalf("post_id = %q", frame.Result.PostID)
	case frame.Result.EventType != activityComment:
		t.Fatalf("event_type = %q", frame.Result.EventType)
	case frame.Result.CommentID != "comment-1":
		t.Fatalf("comment_id = %q", frame.Result.CommentID)
	case frame.Result.ParentID == nil || *frame.Result.ParentID != "top-1":
		t.Fatalf("parent_id = %v", frame.Result.ParentID)
	case frame.Result.Entity.EntityID != "bot-entity":
		t.Fatalf("entity.entity_id = %q", frame.Result.Entity.EntityID)
	case frame.Result.Entity.Handle != "helper":
		t.Fatalf("entity.handle = %q", frame.Result.Entity.Handle)
	}
}

// A bot's frame must name it by its @handle. Django's get_entity_profile_path()
// has no bot branch and falls through to str(entity.id), which it can afford
// because a bot cannot comment through Django. Here the caller is a bot nearly
// every time, so a frame carrying a UUID where the handle goes would be the one
// identity the comment section cannot render - which is the whole feature.
func TestABotsFrameCarriesItsHandleRatherThanItsUUID(t *testing.T) {
	actor := normalizeActor("00000000-0000-4000-8000-000000000001", "moderator",
		"Chatterloop Moderation", "bot")

	if actor.Handle == actor.EntityID {
		t.Fatal("a resolved bot handle was replaced by its entity id")
	}
	if actor.Handle != "moderator" {
		t.Fatalf("handle = %q, want the bot's handle", actor.Handle)
	}
}

func TestAFrameWithNoActorOmitsTheEntityKeyEntirely(t *testing.T) {
	// Absent means "the platform did this". An entity object with an empty id
	// is a different claim, and a client comparing it against its own would be
	// comparing against nothing.
	body, err := json.Marshal(postActivityMessage(
		"post-1", activityComment, nil, commentActivityFields("comment-1", "")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var frame struct {
		Result map[string]any `json:"result"`
	}
	if err := json.Unmarshal(body, &frame); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, present := frame.Result["entity"]; present {
		t.Fatalf("entity should be absent, got %v", frame.Result)
	}
}
