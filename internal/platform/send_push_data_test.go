package platform

import (
	"testing"
	"time"
)

// The regression this guards against: every field chatterloop_app's
// push_payload.dart actually reads for a "message" push (senderName ??
// conversationName ?? title, plus body, sentAt) was silently absent from
// `data` - the top-level queue.PushPayload.Title/Body were always correct,
// but the app's renderer never reads those for a message-type push (see
// push_payload.dart's own header: "deliberately NOT from `notification`").
// The result was a real notification with nothing in its tray - not
// specific to bots, just to this being the only sender through this send
// path so far.
func TestMessagePushDataCarriesEverythingTheRendererReadsForAOneToOne(t *testing.T) {
	sentAt := time.UnixMilli(1_721_557_200_000)
	data := messagePushData(
		"conv-1", "", "entity-neon", "@neon",
		"Pretty good, thanks!", "msg-1", "single", sentAt,
	)

	want := map[string]string{
		"type":             "message",
		"conversationId":   "conv-1",
		"conversationName": "",
		"isGroup":          "false",
		"senderId":         "entity-neon",
		"senderName":       "@neon",
		"body":             "Pretty good, thanks!",
		"sentAt":           "1721557200000",
		"messageId":        "msg-1",
	}
	for key, expected := range want {
		if got := data[key]; got != expected {
			t.Errorf("data[%q] = %q, want %q", key, got, expected)
		}
	}

	// The exact fallback chain notification_renderer.dart uses:
	// payload.senderName ?? payload.conversationName ?? payload.title ?? ''.
	// senderName alone must be enough - this is what a 1:1 push actually
	// relies on, since conversationName is empty for a single conversation.
	if data["senderName"] == "" {
		t.Fatal("senderName is empty - a 1:1 push would render with no sender at all")
	}
	if data["body"] == "" {
		t.Fatal("body is empty - the notification would show no message text")
	}
}

func TestMessagePushDataMarksAGroupConversation(t *testing.T) {
	data := messagePushData(
		"conv-2", "Design Team", "entity-a", "@paulo",
		"see you at 5", "msg-2", "group", time.Now(),
	)

	if data["isGroup"] != "true" {
		t.Fatalf("isGroup = %q, want %q for a non-single conversation", data["isGroup"], "true")
	}
	if data["conversationName"] != "Design Team" {
		t.Fatalf("conversationName = %q, want the realm name for a group", data["conversationName"])
	}
}

func TestMentionPushDataCarriesTitleAndBodyDirectly(t *testing.T) {
	// Mentions render through push_payload.dart's OTHER path (any type but
	// "message" is generic title+body from `data`), so - unlike the message
	// case above - this needs data.title/data.body themselves, not the
	// senderName/conversationName fallback chain.
	data := mentionPushData("conv-3", "entity-a", "@paulo", "@paulo mentioned you: see you at 5", "msg-3")

	if data["type"] != "mention" {
		t.Fatalf("type = %q, want %q", data["type"], "mention")
	}
	if data["title"] == "" {
		t.Fatal("title is empty - a mention push reads data.title directly, not a fallback chain")
	}
	if data["body"] == "" {
		t.Fatal("body is empty - a mention push reads data.body directly")
	}
	if got, want := data["route"], "/conversation/conv-3"; got != want {
		t.Fatalf("route = %q, want %q", got, want)
	}
}

func TestMentionPushDataBodyMatchesWhatWasPassedIn(t *testing.T) {
	// The body is composed ONCE by the caller and threaded through, rather
	// than rebuilt here - guards against the top-level queue.PushPayload.Body
	// and data.body ever being able to drift apart again.
	composed := "@paulo mentioned you: see you at 5"
	data := mentionPushData("conv-3", "entity-a", "@paulo", composed, "msg-3")

	if data["body"] != composed {
		t.Fatalf("data.body = %q, want the exact composed string %q", data["body"], composed)
	}
}
