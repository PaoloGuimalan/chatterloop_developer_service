package platform

import (
	"context"
	"errors"
	"sort"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// ErrMessageNotFound means the anchor is not in this conversation, or is not a
// readable message at all.
//
// One error for both, deliberately: a caller who can tell "that message is
// somewhere else" from "that message does not exist" can probe for the
// existence of messages in conversations they cannot read.
var ErrMessageNotFound = errors.New("message not found in this conversation")

// Thread is one reply lineage: a message and the chain of what it answers.
type Thread struct {
	// Oldest first, ending with the anchor. A transcript, for the same reason
	// RecentMessages is one - a model or a person handed a conversation out of
	// order reasons about the wrong turn.
	Messages []Message

	// How many ancestors were found. Zero means the anchor starts its thread.
	Depth int

	// True when the walk stopped at `limit` rather than at the root.
	//
	// This is the field a client most needs and would otherwise have to guess
	// at. Without it a truncated lineage is indistinguishable from a complete
	// one, and "the whole thread" and "the last few turns of a much longer
	// thread" call for quite different behaviour from whatever reads it.
	Truncated bool

	// The message that started the thread - the one with no `replyingTo`.
	//
	// Empty when Truncated, because then it was not reached and naming the
	// oldest message we happened to see would be a guess presented as a fact.
	RootMessageID string
}

// ReplyThread walks `replyingTo` upward from `messageID` and returns the chain.
//
// # WHY THE SERVER WALKS IT
//
// A client cannot. The parent of a reply is regularly outside any window it
// would be reasonable to fetch - somebody answering a question from forty
// turns back - and there is no route that reads a message by id, on purpose:
// one would be a way to read any message in any conversation by guessing ids.
// Walking here keeps the reachability rule where the conversation is known.
//
// # ONE ROUND TRIP
//
// $graphLookup follows the chain inside the database rather than making the
// caller issue a query per hop. This is the first aggregation in the service,
// and it earns that on a path that runs for every answer a bot gives: twenty
// hops as twenty sequential round trips to Atlas is most of a second, and the
// `messageID` index makes the same work here a single call.
//
// # THE TRAVERSAL CANNOT LEAVE THE CONVERSATION
//
// `restrictSearchWithMatch` pins every hop to this conversation. A `replyingTo`
// should never point outside its own conversation, but this walks stored data
// through ids, and a corrupted or crafted parent id would otherwise pull a
// message out of a conversation the caller has no access to - the participant
// check having been made against the conversation, not against every message
// the chain happens to reach.
func ReplyThread(
	ctx context.Context,
	db *mongo.Database,
	conversationID, messageID string,
	limit int64,
) (Thread, error) {
	if conversationID == "" || messageID == "" {
		return Thread{}, ErrMessageNotFound
	}
	if limit < 1 {
		limit = 1
	}

	// maxDepth counts hops BEYOND the first. The traversal starts at
	// `$replyingTo`, so depth 0 is already the parent, and `limit` ancestors
	// means maxDepth limit-1.
	maxDepth := limit - 1

	within := bson.M{
		"conversationID": conversationID,
		"isDeleted":      bson.M{"$ne": true},
	}

	pipeline := mongo.Pipeline{
		bson.D{{Key: "$match", Value: bson.M{
			"messageID":      messageID,
			"conversationID": conversationID,
			"isDeleted":      bson.M{"$ne": true},
		}}},
		bson.D{{Key: "$graphLookup", Value: bson.M{
			"from":                    "messages",
			"startWith":               "$replyingTo",
			"connectFromField":        "replyingTo",
			"connectToField":          "messageID",
			"as":                      "ancestors",
			"maxDepth":                maxDepth,
			"depthField":              "ancestorDepth",
			"restrictSearchWithMatch": within,
		}}},
	}

	cursor, err := db.Collection("messages").Aggregate(ctx, pipeline)
	if err != nil {
		return Thread{}, err
	}
	defer cursor.Close(ctx)

	var raws []bson.M
	if err := cursor.All(ctx, &raws); err != nil {
		return Thread{}, err
	}
	if len(raws) == 0 {
		return Thread{}, ErrMessageNotFound
	}

	root := raws[0]
	anchor, ok := decodeMessage(root, conversationID)
	if !ok {
		// An image or file message. It is a real message and a real anchor,
		// but there is nothing to return for it and nothing that reads this
		// could use it.
		return Thread{}, ErrMessageNotFound
	}

	ancestors, truncated := orderAncestors(root["ancestors"], conversationID, limit)

	thread := Thread{
		Messages:  append(ancestors, anchor),
		Depth:     len(ancestors),
		Truncated: truncated,
	}
	if !truncated && len(ancestors) > 0 {
		thread.RootMessageID = ancestors[0].MessageID
	} else if !truncated {
		// The anchor replies to nothing, so it started its own thread.
		thread.RootMessageID = anchor.MessageID
	}

	if err := resolveReplyParents(ctx, db, thread.Messages); err != nil {
		// Enrichment, not content - the same call RecentMessages makes and the
		// same reason for not failing over it.
		return thread, nil
	}
	return thread, nil
}

// orderAncestors turns $graphLookup's unordered output into oldest-first, and
// reports whether the chain was cut short.
//
// Split out because it IS the tricky part: the traversal returns a set with a
// depth on each element and no order at all, and getting this backwards
// silently hands a model the conversation in reverse.
func orderAncestors(raw any, conversationID string, limit int64) ([]Message, bool) {
	entries := asSlice(raw)
	if len(entries) == 0 {
		return nil, false
	}

	type scored struct {
		depth   int64
		message Message
	}

	found := make([]scored, 0, len(entries))
	deepest := int64(-1)
	var deepestParent string

	for _, entry := range entries {
		document := asDocument(entry)
		if document == nil {
			continue
		}
		depth := asInt64(document["ancestorDepth"])
		message, ok := decodeMessage(document, conversationID)
		if !ok {
			// An image in the middle of a thread. Skipped rather than fatal,
			// but it does break the chain, so the walk above it is reported as
			// truncated by the parent check below.
			continue
		}
		found = append(found, scored{depth: depth, message: message})
		if depth > deepest {
			deepest = depth
			deepestParent = message.ReplyingTo
		}
	}
	if len(found) == 0 {
		return nil, false
	}

	// Depth counts AWAY from the anchor, so descending depth is oldest first.
	sort.Slice(found, func(i, j int) bool { return found[i].depth > found[j].depth })

	messages := make([]Message, 0, len(found))
	for _, item := range found {
		messages = append(messages, item.message)
	}

	// Truncated when the oldest ancestor we reached still replies to something.
	//
	// Read off the DATA rather than by comparing the count to the limit. Both
	// produce the right answer when the limit is what stopped the walk, but a
	// chain also stops early on a deleted or unreadable link - and to anything
	// consuming this those mean the same thing: what you have is not the whole
	// thread. One condition covers both; the count comparison covers one and
	// quietly claims completeness for the other.
	return messages, deepestParent != ""
}

// asSlice accepts what the driver actually hands back for an embedded array.
//
// `bson.A` is a NAMED type over []interface{}, so a type assertion to []any
// does not match it and vice versa - and which one appears depends on the
// decode target. Accepting both is one line; getting it wrong returns an empty
// chain from a perfectly good one.
func asSlice(raw any) []any {
	switch value := raw.(type) {
	case bson.A:
		return value
	case []any:
		return value
	default:
		return nil
	}
}

func asDocument(raw any) map[string]any {
	switch value := raw.(type) {
	case bson.M:
		return value
	case map[string]any:
		return value
	default:
		return nil
	}
}

// asInt64 normalises a BSON number.
//
// $graphLookup's depthField is a NumberLong, which decodes as int64 - not the
// int32 an ordinary small integer arrives as. Asserting the wrong one yields
// zero for every element, which does not error: it sorts the whole chain as a
// tie and hands back a thread in arbitrary order.
func asInt64(raw any) int64 {
	switch value := raw.(type) {
	case int64:
		return value
	case int32:
		return int64(value)
	case int:
		return int64(value)
	case float64:
		return int64(value)
	default:
		return 0
	}
}
