package platform

import "testing"

// selectRepliesTo is the rule that decides whether a bot may answer a message
// that never named it. Everything either side of it is a database round trip;
// this is the part that can be wrong quietly, so it is tested on its own.

func TestSelectRepliesToKeepsOnlyRepliesToOwnMessages(t *testing.T) {
	messages := []Message{
		{MessageID: "m1", ReplyingTo: "mine-1"},
		{MessageID: "m2", ReplyingTo: "someone-elses"},
		{MessageID: "m3", ReplyingTo: "mine-2"},
	}
	own := map[string]bool{"mine-1": true, "mine-2": true}

	kept := selectRepliesTo(messages, own)

	if len(kept) != 2 {
		t.Fatalf("expected 2 replies, got %d", len(kept))
	}
	if kept[0].MessageID != "m1" || kept[1].MessageID != "m3" {
		t.Fatalf("order not preserved: %v, %v", kept[0].MessageID, kept[1].MessageID)
	}
}

// A reply to somebody else's message in a conversation the bot is merely a
// member of is the common case, and it must produce nothing at all.
func TestSelectRepliesToIgnoresRepliesToOthers(t *testing.T) {
	messages := []Message{
		{MessageID: "m1", ReplyingTo: "theirs-1"},
		{MessageID: "m2", ReplyingTo: "theirs-2"},
	}

	if kept := selectRepliesTo(messages, map[string]bool{"mine": true}); len(kept) != 0 {
		t.Fatalf("expected nothing, got %d", len(kept))
	}
}

// isReply true with an empty replyingTo is representable in the store - the
// column is Mixed and the flag is set independently - and must never be read
// as "replies to everything".
func TestSelectRepliesToIgnoresAnEmptyParent(t *testing.T) {
	messages := []Message{{MessageID: "m1", IsReply: true, ReplyingTo: ""}}

	if kept := selectRepliesTo(messages, map[string]bool{"": true, "mine": true}); len(kept) != 0 {
		t.Fatalf("expected nothing, got %d", len(kept))
	}
}

func TestSelectRepliesToWithNoOwnMessagesKeepsNothing(t *testing.T) {
	messages := []Message{{MessageID: "m1", ReplyingTo: "anything"}}

	if kept := selectRepliesTo(messages, map[string]bool{}); len(kept) != 0 {
		t.Fatalf("expected nothing, got %d", len(kept))
	}
}

// decodeMessage carries replyingTo through, which is the field the whole
// feature rests on - it was projected but unused before.
func TestDecodeMessageCarriesReplyFields(t *testing.T) {
	raw := map[string]any{
		"messageID": "m1", "sender": "human-1", "content": "and the second one?",
		"isReply": true, "replyingTo": "bot-message-9",
	}

	message, ok := decodeMessage(raw, "conv-1")
	if !ok {
		t.Fatal("expected the row to decode")
	}
	if !message.IsReply || message.ReplyingTo != "bot-message-9" {
		t.Fatalf("reply fields lost: %+v", message)
	}
	if message.ConversationID != "conv-1" {
		t.Fatalf("conversation fallback lost: %q", message.ConversationID)
	}
}
