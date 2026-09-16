package platform

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

// orderAncestors is the part of the reply walk that can be wrong quietly.
// $graphLookup returns a SET with a depth on each element and no order at all,
// so getting this backwards hands whatever reads the thread a conversation in
// reverse - which reads perfectly well and answers the wrong turn.

// depth arrives as a NumberLong from $graphLookup's depthField, so the
// fixtures use int64 - an int32 fixture would pass against an int32
// assertion and prove nothing about the real thing.
func ancestor(id, parent string, depth int64) bson.M {
	return bson.M{
		"messageID":     id,
		"replyingTo":    parent,
		"sender":        "entity-1",
		"content":       "text of " + id,
		"messageType":   "text",
		"isReply":       parent != "",
		"ancestorDepth": depth,
	}
}

// entries builds what the driver hands back for an embedded array: a bson.A,
// not a []any. The distinction is the whole point of asSlice.
func entries(documents ...bson.M) bson.A {
	out := make(bson.A, 0, len(documents))
	for _, document := range documents {
		out = append(out, document)
	}
	return out
}

func TestOrderAncestorsReturnsOldestFirst(t *testing.T) {
	// depth counts AWAY from the anchor: 0 is the parent, 2 the oldest.
	raw := entries(
		ancestor("m2", "m1", 1),
		ancestor("m3", "m2", 0),
		ancestor("m1", "", 2),
	)

	messages, truncated := orderAncestors(raw, "conv-1", 10)

	if truncated {
		t.Fatal("a chain that reached the root is not truncated")
	}
	got := []string{messages[0].MessageID, messages[1].MessageID, messages[2].MessageID}
	want := []string{"m1", "m2", "m3"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("out of order: got %v, want %v", got, want)
		}
	}
}

func TestOrderAncestorsReportsTruncationAtTheLimit(t *testing.T) {
	// The oldest one we reached still replies to something, and we have as
	// many as we asked for - the walk stopped because of the limit.
	raw := entries(
		ancestor("m9", "m8", 0),
		ancestor("m8", "m7", 1),
	)

	_, truncated := orderAncestors(raw, "conv-1", 2)

	if !truncated {
		t.Fatal("a chain cut short by the limit must say so")
	}
}

// A chain can also stop early because a link is deleted or unreadable. To
// anything consuming this that means the same thing as hitting the limit -
// what you have is not the whole thread - so it must report the same way.
func TestOrderAncestorsReportsTruncationOnABrokenLink(t *testing.T) {
	raw := entries(ancestor("m9", "m8-which-is-gone", 0))

	_, truncated := orderAncestors(raw, "conv-1", 10)

	if !truncated {
		t.Fatal("a chain with a missing parent must say so")
	}
}

func TestOrderAncestorsOnARootParentIsNotTruncated(t *testing.T) {
	raw := entries(ancestor("m1", "", 0))

	messages, truncated := orderAncestors(raw, "conv-1", 10)

	if truncated {
		t.Fatal("reaching a message that replies to nothing is the whole thread")
	}
	if len(messages) != 1 || messages[0].MessageID != "m1" {
		t.Fatalf("unexpected chain: %v", messages)
	}
}

func TestOrderAncestorsOnNoAncestors(t *testing.T) {
	messages, truncated := orderAncestors(entries(), "conv-1", 10)

	if len(messages) != 0 {
		t.Fatalf("expected nothing, got %d", len(messages))
	}
	if truncated {
		t.Fatal("a message that starts its own thread is not a truncated one")
	}
}

// Image and file messages decode to nothing. One in the middle of a thread
// must not take the readable messages down with it, and must not be silently
// presented as a complete chain either.
func TestOrderAncestorsSkipsUnreadableMessages(t *testing.T) {
	image := ancestor("m2", "m1", 0)
	image["messageType"] = "image"
	delete(image, "content")

	raw := entries(image, ancestor("m1", "", 1))

	messages, _ := orderAncestors(raw, "conv-1", 10)

	for _, message := range messages {
		if message.MessageID == "m2" {
			t.Fatal("an image message has no content to return")
		}
	}
}

func TestOrderAncestorsIgnoresMalformedEntries(t *testing.T) {
	raw := append(entries(ancestor("m1", "", 0)), any("not a document"))

	messages, _ := orderAncestors(raw, "conv-1", 10)

	if len(messages) != 1 {
		t.Fatalf("expected the one real message, got %d", len(messages))
	}
}
