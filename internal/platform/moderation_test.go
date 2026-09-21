package platform

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

// The two things that can be wrong quietly here: what the route is willing to
// SAY, and what it is willing to say it ABOUT. Both are pure functions on
// purpose - a rule that can only be exercised against live Postgres and Mongo
// is a rule nothing exercises.

// --- what may be said ------------------------------------------------------

// violatingDocument is a full moderation document as the pipeline writes one,
// with every field a caller must never see populated with a findable string.
func violatingDocument() bson.M {
	return bson.M{
		"targetID":    "msg-1",
		"sourceType":  "message",
		"contentType": "video",
		"mediaURL":    "https://cdn.test/clip.mp4",
		"status":      "done",
		"transcription": "push the meeting to Thursday",
		"caption":       "a person speaking to camera",
		"shownText":     "MEETING NOTES",
		"language":      "en",
		"audio":         bson.M{"isMusic": false, "labels": bson.A{"speech"}},

		// None of this may survive.
		"moderation": bson.M{
			"verdict":       "violating",
			"categories":    bson.A{"harassment"},
			"scores":        bson.M{"harassment": 0.97},
			"topScore":      0.97,
			"unevaluated":   bson.A{"spam"},
			"autoReported":  true,
			"reportID":      "report-secret-1",
		},
		"tags":        bson.A{bson.M{"name": "guitar"}},
		"contentHash": "sha256-secret",
		"model":       bson.M{"provider": "local", "name": "whisper"},
		"error":       "provider timed out at frame 42",
		"transcriptSegments": bson.A{
			bson.M{"start": 0.0, "end": 2.0, "text": "push the meeting"},
		},
	}
}

// THE TEST THIS FILE EXISTS FOR.
//
// A verdict describes what the PLATFORM decided about somebody else's content.
// A developer credential asking what is in a video has no business learning
// it, and the failure mode is silent: a field added to the document later
// leaks until somebody reads the response closely.
func TestModerationNeverLeaksEnforcement(t *testing.T) {
	encoded, err := json.Marshal(decodeModeration(violatingDocument()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	body := string(encoded)
	for _, secret := range []string{
		"violating", "harassment", "0.97", "autoReported", "auto_reported",
		"report-secret-1", "sha256-secret", "guitar", "whisper",
		"provider timed out", "verdict", "categories", "scores",
		"unevaluated", "transcriptSegments", "transcript_segments",
	} {
		if strings.Contains(body, secret) {
			t.Fatalf("the response leaked %q:\n%s", secret, body)
		}
	}
}

// The other half of the same test: the allow-list is a list, not a wall. What
// the caller came for has to actually arrive.
func TestModerationCarriesTheContext(t *testing.T) {
	record := decodeModeration(violatingDocument())

	if record.Transcription != "push the meeting to Thursday" {
		t.Fatalf("transcription: %q", record.Transcription)
	}
	if record.Caption != "a person speaking to camera" {
		t.Fatalf("caption: %q", record.Caption)
	}
	if record.ShownText != "MEETING NOTES" {
		t.Fatalf("shown text: %q", record.ShownText)
	}
	if record.IsMusic == nil || *record.IsMusic {
		t.Fatalf("isMusic: %v", record.IsMusic)
	}
	if record.Status != "done" {
		t.Fatalf("status: %q", record.Status)
	}
}

// Null means "nothing classified the audio", which is a different thing from
// "not music" - and a bool would collapse the two.
func TestIsMusicStaysUnknownWhenNothingSaid(t *testing.T) {
	record := decodeModeration(bson.M{"targetID": "msg-1", "status": "done"})

	if record.IsMusic != nil {
		t.Fatalf("wanted unknown, got %v", *record.IsMusic)
	}
	if strings.Contains(mustJSON(t, record), "is_music") {
		t.Fatal("an unknown value must not appear in the response at all")
	}
}

// A caller has to tell "this has no transcript" from "this has not been read
// yet". Status is the only field that is never omitted, which is what makes
// that distinction expressible.
func TestStatusIsAlwaysPresent(t *testing.T) {
	for _, status := range []string{"pending", "processing", "failed", "skipped"} {
		record := decodeModeration(bson.M{"targetID": "msg-1", "status": status})

		if record.Status != status {
			t.Fatalf("wanted %q, got %q", status, record.Status)
		}
		if !strings.Contains(mustJSON(t, record), `"status":"`+status+`"`) {
			t.Fatalf("%q must survive into the response", status)
		}
	}
}

// A document written before the field existed, or one whose status is
// unreadable. "pending" is the honest reading of it.
func TestAnUnreadableStatusReadsAsPending(t *testing.T) {
	for _, raw := range []bson.M{
		{"targetID": "msg-1"},
		{"targetID": "msg-1", "status": nil},
		{"targetID": "msg-1", "status": 7},
	} {
		if got := decodeModeration(raw).Status; got != "pending" {
			t.Fatalf("%v should read as pending, got %q", raw, got)
		}
	}
}

// --- what it may be said about ---------------------------------------------

func allow(conversationID string) (bool, error)  { return true, nil }
func refuse(conversationID string) (bool, error) { return false, nil }

// THE ORACLE TEST.
//
// Without this filter a token holding messages.read could read the transcript
// of any voice note on the platform by guessing message ids. A scope says what
// KIND of thing may be read, never which ones.
func TestAMessageInAConversationYouAreNotInIsDropped(t *testing.T) {
	allowed, err := permittedMessages(
		[]string{"msg-1", "msg-2"},
		map[string]string{"msg-1": "conv-theirs", "msg-2": "conv-theirs"},
		refuse,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(allowed) != 0 {
		t.Fatalf("nothing should be readable, got %v", allowed)
	}
}

// Dropped, not refused. A 404 for the id would answer the question the check
// exists to refuse - the caller would learn the id is real.
func TestAMixedBatchReturnsOnlyWhatIsPermitted(t *testing.T) {
	allowed, err := permittedMessages(
		[]string{"mine-1", "theirs-1", "mine-2"},
		map[string]string{
			"mine-1":   "conv-mine",
			"theirs-1": "conv-theirs",
			"mine-2":   "conv-mine",
		},
		func(conversationID string) (bool, error) {
			return conversationID == "conv-mine", nil
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// In the caller's order, not the map's - Go randomises map iteration, and
	// a batch read that shuffled would look like the data changing.
	want := []string{"mine-1", "mine-2"}
	if len(allowed) != len(want) {
		t.Fatalf("wanted %v, got %v", want, allowed)
	}
	for index, id := range want {
		if allowed[index] != id {
			t.Fatalf("wanted %v, got %v", want, allowed)
		}
	}
}

// An unknown id must never inherit permission from a conversation it was never
// in - there is simply nothing to ask about.
func TestAnUnknownMessageIsDroppedWithoutBeingAsked(t *testing.T) {
	asked := 0
	allowed, err := permittedMessages(
		[]string{"ghost"},
		map[string]string{},
		func(string) (bool, error) { asked++; return true, nil },
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(allowed) != 0 {
		t.Fatalf("an unknown id must not be readable, got %v", allowed)
	}
	if asked != 0 {
		t.Fatal("an unknown id has no conversation to ask about")
	}
}

// A window of forty messages is one membership question.
func TestMembershipIsAskedOncePerConversation(t *testing.T) {
	conversations := map[string]string{}
	ids := []string{}
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		conversations[id] = "conv-1"
		ids = append(ids, id)
	}

	asked := 0
	if _, err := permittedMessages(ids, conversations,
		func(string) (bool, error) { asked++; return true, nil }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if asked != 1 {
		t.Fatalf("wanted 1 membership check, got %d", asked)
	}
}

// A check that FAILED is not a check that said no. Returning a short list
// would look exactly like "you may see less than you asked for".
func TestAFailedMembershipCheckRefusesTheWholeRead(t *testing.T) {
	boom := errors.New("postgres is unreachable")

	_, err := permittedMessages(
		[]string{"msg-1"},
		map[string]string{"msg-1": "conv-1"},
		func(string) (bool, error) { return false, boom },
	)

	if !errors.Is(err, boom) {
		t.Fatalf("wanted the failure to surface, got %v", err)
	}
}

func TestAnEmptyBatchIsNotAnError(t *testing.T) {
	allowed, err := permittedMessages(nil, nil, allow)
	if err != nil || len(allowed) != 0 {
		t.Fatalf("wanted an empty result, got %v %v", allowed, err)
	}
}

// --- the id list -----------------------------------------------------------

// A caller repeating an id must not pay for it twice, and must not be able to
// use repetition to get past the cap.
func TestDistinctDropsBlanksAndRepeats(t *testing.T) {
	got := distinct([]string{"a", "", "b", "a", "  ", "b"})

	want := []string{"a", "b", "  "}
	if len(got) != len(want) {
		t.Fatalf("wanted %v, got %v", want, got)
	}
	for index, value := range want {
		if got[index] != value {
			t.Fatalf("wanted %v, got %v", want, got)
		}
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return string(encoded)
}
