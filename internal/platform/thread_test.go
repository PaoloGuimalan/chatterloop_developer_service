package platform

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

// linkedAncestors is the part of the reply walk that can be wrong quietly: it
// decides the ORDER of the thread by following links, across two stored
// shapes of `replyingTo`. Getting it backwards hands whatever reads the thread
// a conversation in reverse - which reads perfectly well and answers the wrong
// turn.

// A message whose replyingTo is the bare-id string older rows hold.
func legacy(id, parent string) bson.M {
	return bson.M{
		"messageID":   id,
		"replyingTo":  parent,
		"sender":      "entity-1",
		"content":     "text of " + id,
		"messageType": "text",
		"isReply":     parent != "",
	}
}

// A message whose replyingTo is {type: "message", id}, as every writer
// produces now. No parent is stored as "" in both shapes.
func current(id, parent string) bson.M {
	document := legacy(id, parent)
	if parent != "" {
		document["replyingTo"] = bson.M{"type": "message", "id": parent}
	}
	return document
}

func index(documents ...bson.M) map[string]bson.M {
	found := make(map[string]bson.M, len(documents))
	for _, document := range documents {
		found[document["messageID"].(string)] = document
	}
	return found
}

func ids(messages []Message) []string {
	out := make([]string, 0, len(messages))
	for _, message := range messages {
		out = append(out, message.MessageID)
	}
	return out
}

func sameIDs(t *testing.T, got []Message, want ...string) {
	t.Helper()
	gotIDs := ids(got)
	if len(gotIDs) != len(want) {
		t.Fatalf("got %v, want %v", gotIDs, want)
	}
	for i := range want {
		if gotIDs[i] != want[i] {
			t.Fatalf("out of order: got %v, want %v", gotIDs, want)
		}
	}
}

func TestLinkedAncestorsReturnsNearestFirst(t *testing.T) {
	found := index(legacy("m3", "m2"), legacy("m2", "m1"), legacy("m1", ""))

	chain, next, used := linkedAncestors(found, "m3", "conv-1", 10)

	sameIDs(t, chain, "m3", "m2", "m1")
	if next != "" || used != 3 {
		t.Fatalf("a chain that reached its root: next=%q used=%d", next, used)
	}
}

// The whole point of the rewrite: a new reply ({type, id}) pointing into a
// thread of older bare-id rows is ONE chain, followed straight through.
func TestLinkedAncestorsCrossesFromObjectToStringLinks(t *testing.T) {
	found := index(
		current("n2", "n1"),
		current("n1", "o2"),
		legacy("o2", "o1"),
		legacy("o1", ""),
	)

	chain, next, used := linkedAncestors(found, "n2", "conv-1", 10)

	sameIDs(t, chain, "n2", "n1", "o2", "o1")
	if next != "" || used != 4 {
		t.Fatalf("mixed chain not followed to its root: next=%q used=%d", next, used)
	}
}

func TestLinkedAncestorsStopsAtTheBudget(t *testing.T) {
	found := index(current("m9", "m8"), current("m8", "m7"), current("m7", ""))

	chain, next, used := linkedAncestors(found, "m9", "conv-1", 2)

	sameIDs(t, chain, "m9", "m8")
	if next != "m7" || used != 2 {
		t.Fatalf("the walk must stop at the budget and say where: next=%q used=%d", next, used)
	}
}

// A link that is not in `found` - deleted, or in another conversation - ends
// the walk with the missing id still in hand, which is what ReplyThread
// reports as truncated.
func TestLinkedAncestorsStopsOnABrokenLink(t *testing.T) {
	found := index(current("m9", "m8-which-is-gone"))

	chain, next, used := linkedAncestors(found, "m9", "conv-1", 10)

	sameIDs(t, chain, "m9")
	if next != "m8-which-is-gone" || used != 1 {
		t.Fatalf("a broken link must be reported: next=%q used=%d", next, used)
	}
}

func TestLinkedAncestorsWithAnUnreachableParent(t *testing.T) {
	chain, next, used := linkedAncestors(index(), "gone", "conv-1", 10)

	if len(chain) != 0 || used != 0 || next != "gone" {
		t.Fatalf("expected nothing followed: chain=%v next=%q used=%d", ids(chain), next, used)
	}
}

// Image and file messages decode to nothing, but they are still LINKS: a
// thread must carry on past a photo somebody replied to, without returning
// the photo itself.
func TestLinkedAncestorsFollowsButSkipsUnreadableMessages(t *testing.T) {
	image := current("m2", "m1")
	image["messageType"] = "image"
	delete(image, "content")

	found := index(current("m3", "m2"), image, legacy("m1", ""))

	chain, next, used := linkedAncestors(found, "m3", "conv-1", 10)

	sameIDs(t, chain, "m3", "m1")
	if next != "" || used != 3 {
		t.Fatalf("the image must count as a link: next=%q used=%d", next, used)
	}
}

// A reply to a post, moment or thought is not a message link: the walk
// treats it as the root, exactly like a message that replies to nothing.
func TestLinkedAncestorsStopsAtANonMessageReply(t *testing.T) {
	sendOfPost := legacy("m1", "")
	sendOfPost["replyingTo"] = bson.M{"type": "post", "id": "post-1"}
	sendOfPost["isReply"] = true

	found := index(current("m2", "m1"), sendOfPost)

	chain, next, _ := linkedAncestors(found, "m2", "conv-1", 10)

	sameIDs(t, chain, "m2", "m1")
	if next != "" {
		t.Fatalf("a post reply has no message parent, got next=%q", next)
	}
}
