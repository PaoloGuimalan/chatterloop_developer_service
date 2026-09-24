package platform

import (
	"context"
	"errors"

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
// # TWO LINK SHAPES, SO SEGMENTS
//
// `replyingTo` is stored as {type: "message", id} now and as a bare id string
// on older rows (decodeReplyingTo). $graphLookup follows ONE field path, so no
// single traversal can cross from one shape to the other. Each round trip
// (ancestorSegment) therefore runs two traversals from the same starting
// message - one along `replyingTo.id`, one along `replyingTo` - and the chain
// is then followed link by link through everything either returned
// (linkedAncestors). A chain written in one shape takes one round trip; one
// that crosses into older rows takes another from where the first stopped.
// Since a reply always points at an EARLIER message, a chain changes shape at
// most once in practice, so this is one or two round trips - not one per hop.
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

	collection := db.Collection("messages")

	var anchorRaw bson.M
	err := collection.FindOne(ctx, bson.M{
		"messageID":      messageID,
		"conversationID": conversationID,
		"isDeleted":      bson.M{"$ne": true},
	}).Decode(&anchorRaw)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return Thread{}, ErrMessageNotFound
		}
		return Thread{}, err
	}

	anchor, ok := decodeMessage(anchorRaw, conversationID)
	if !ok {
		// An image or file message. It is a real message and a real anchor,
		// but there is nothing to return for it and nothing that reads this
		// could use it.
		return Thread{}, ErrMessageNotFound
	}

	// Nearest first while walking; reversed into a transcript at the end.
	var nearestFirst []Message
	var hops int64
	parent := anchor.ReplyingTo

	for parent != "" && hops < limit {
		found, err := ancestorSegment(ctx, collection, conversationID, parent, limit-hops)
		if err != nil {
			return Thread{}, err
		}
		chain, next, used := linkedAncestors(found, parent, conversationID, limit-hops)
		if used == 0 {
			// The parent is not reachable - deleted, or not in this
			// conversation. The walk ends here, and `parent` still set is what
			// reports the thread as incomplete.
			break
		}
		nearestFirst = append(nearestFirst, chain...)
		hops += used
		parent = next
	}

	ancestors := make([]Message, 0, len(nearestFirst))
	for i := len(nearestFirst) - 1; i >= 0; i-- {
		ancestors = append(ancestors, nearestFirst[i])
	}

	// Truncated when the oldest link we reached still replies to something:
	// the limit stopped the walk, or a link was deleted or unreadable. To
	// anything consuming this both mean the same thing - what you have is not
	// the whole thread.
	truncated := parent != ""

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

// ancestorSegment reads the message `startID` and everything above it that
// two traversals reach - one following `replyingTo.id`, one following a
// bare-string `replyingTo` - within `budget` messages, keyed by messageID.
//
// Unordered on purpose: linkedAncestors decides the order by following the
// links themselves, which is what makes mixing two traversals safe.
func ancestorSegment(
	ctx context.Context,
	collection *mongo.Collection,
	conversationID, startID string,
	budget int64,
) (map[string]bson.M, error) {
	within := bson.M{
		"conversationID": conversationID,
		"isDeleted":      bson.M{"$ne": true},
	}

	pipeline := mongo.Pipeline{
		bson.D{{Key: "$match", Value: bson.M{
			"messageID":      startID,
			"conversationID": conversationID,
			"isDeleted":      bson.M{"$ne": true},
		}}},
	}

	// The start message is one of the `budget`; maxDepth counts hops beyond
	// the traversal's first match, so budget-1 more messages is budget-2.
	if budget > 1 {
		// The start message's own parent, whichever shape it is stored in.
		startWith := bson.M{"$ifNull": bson.A{"$replyingTo.id", "$replyingTo"}}
		for _, link := range []struct{ field, as string }{
			{"replyingTo.id", "viaObject"},
			{"replyingTo", "viaString"},
		} {
			pipeline = append(pipeline, bson.D{{Key: "$graphLookup", Value: bson.M{
				"from":                    "messages",
				"startWith":               startWith,
				"connectFromField":        link.field,
				"connectToField":          "messageID",
				"as":                      link.as,
				"maxDepth":                budget - 2,
				"restrictSearchWithMatch": within,
			}}})
		}
	}

	cursor, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var raws []bson.M
	if err := cursor.All(ctx, &raws); err != nil {
		return nil, err
	}

	found := make(map[string]bson.M)
	for _, root := range raws {
		for _, key := range []string{"viaObject", "viaString"} {
			for _, entry := range asSlice(root[key]) {
				if document := asDocument(entry); document != nil {
					if id, _ := document["messageID"].(string); id != "" {
						found[id] = document
					}
				}
			}
		}
		if id, _ := root["messageID"].(string); id != "" {
			found[id] = root
		}
	}
	return found, nil
}

// linkedAncestors follows the reply chain from `parentID` through `found`,
// for at most `budget` links, and returns the messages NEAREST FIRST, the
// parent id it stopped at ("" once the thread's root is reached), and how many
// links it followed.
//
// Split out because it IS the tricky part, and it is pure. An image or file
// in the chain is followed but not returned - there is nothing to read in it -
// so a thread keeps going past a photo someone replied to.
func linkedAncestors(
	found map[string]bson.M,
	parentID, conversationID string,
	budget int64,
) ([]Message, string, int64) {
	var chain []Message
	var used int64
	next := parentID

	for next != "" && used < budget {
		document, ok := found[next]
		if !ok {
			break
		}
		if message, readable := decodeMessage(document, conversationID); readable {
			chain = append(chain, message)
		}
		used++
		next, _ = decodeReplyingTo(document["replyingTo"])
	}
	return chain, next, used
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
