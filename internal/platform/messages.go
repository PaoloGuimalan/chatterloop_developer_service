package platform

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ErrNotAParticipant is returned for a conversation the caller is not in AND
// for one that does not exist. The two are deliberately indistinguishable: a
// caller who could tell them apart could enumerate conversation ids.
var ErrNotAParticipant = errors.New("conversation not found")

type Message struct {
	MessageID      string `json:"message_id"`
	ConversationID string `json:"conversation_id"`
	SenderEntityID string `json:"sender_entity_id"`
	Content        string `json:"content"`
	CreatedAt      int64  `json:"created_at"`
	MessageType    string `json:"message_type"`
	IsReply        bool   `json:"is_reply"`
	ReplyingTo     string `json:"replying_to"`
	SenderHandle   string `json:"sender_handle"`

	// WHO WROTE THE MESSAGE THIS ONE REPLIES TO.
	//
	// Empty unless IsReply and the parent is still readable. Resolved here
	// rather than by the client because the parent is frequently OUTSIDE the
	// window a client asked for - a follow-up an hour later replies to a
	// message forty turns back - so a consumer computing it from the returned
	// slice would get "unknown" exactly when the answer matters.
	//
	// This is what lets a bot answer a reply that does not @-mention it: the
	// whole question is whether the parent's author is the caller, and that is
	// one field comparison instead of a second fetch and a guess.
	ReplyingToSenderEntityID string `json:"replying_to_sender_entity_id"`
	ReplyingToSenderHandle   string `json:"replying_to_sender_handle"`
}

type Conversation struct {
	ConversationID   string
	ConversationType string
	ParticipantIDs   []string
}

// LoadConversation fetches a conversation and asserts the entity may read it.
//
// Access is decided by AssertMember - the SAME rule the send path uses - and
// deliberately NOT by the conversation's participant_ids alone. Those two
// disagree in a case that is completely ordinary: in a realm conversation
// membership lives in community_member, and participant_ids is only refreshed
// when SaveConversation next runs. A member added since the last message is
// therefore absent from the array while being unambiguously a member.
//
// Checking participant_ids here meant an entity could SEND to a conversation
// it could not READ, which for a bot is the worst possible half: it answers a
// mention by first fetching history, so a 404 there makes it silently never
// reply.
func LoadConversation(
	ctx context.Context,
	db *mongo.Database,
	pool *pgxpool.Pool,
	conversationID, entityID string,
) (*Conversation, error) {
	if err := AssertMember(ctx, db, pool, conversationID, entityID); err != nil {
		if errors.Is(err, ErrNoAccess) {
			return nil, ErrNotAParticipant
		}
		return nil, err
	}

	var raw bson.M
	err := db.Collection("conversations").
		FindOne(ctx, bson.M{"conversationID": conversationID}).
		Decode(&raw)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			// Membership held but no conversation document yet - a realm whose
			// chat has never been written to. An empty transcript, not a 404.
			return &Conversation{ConversationID: conversationID, ConversationType: "group"}, nil
		}
		return nil, err
	}

	participants := stringSlice(raw["participant_ids"])
	conversationType, _ := raw["conversationType"].(string)
	if conversationType == "" {
		conversationType = "single"
	}
	return &Conversation{
		ConversationID:   conversationID,
		ConversationType: conversationType,
		ParticipantIDs:   participants,
	}, nil
}

// messageProjection is shared by every message read below, so a field added
// for one caller cannot be quietly missing for another.
var messageProjection = bson.M{
	"_id": 0, "messageID": 1, "conversationID": 1, "sender": 1,
	"content": 1, "messageDate": 1, "messageType": 1,
	"isReply": 1, "replyingTo": 1,
}

// RecentMessages returns the newest `limit` messages, OLDEST FIRST so the slice
// reads as a transcript.
func RecentMessages(ctx context.Context, db *mongo.Database, conversationID string, limit int64) ([]Message, error) {
	messages, err := findMessages(ctx, db, conversationID,
		bson.M{
			"conversationID": conversationID,
			"isDeleted":      bson.M{"$ne": true},
		}, limit)
	if err != nil {
		return nil, err
	}
	if err := resolveReplyParents(ctx, db, messages); err != nil {
		// Enrichment, not content: a transcript missing the authorship of a
		// reply's parent is still a complete transcript, and failing the whole
		// read over it would take the history path down with it.
		return messages, nil
	}
	return messages, nil
}

// RepliesTo returns the recent messages in a conversation that reply to a
// message `entityID` WROTE.
//
// # WHY THIS IS A ROUTE AND NOT A CLIENT-SIDE FILTER
//
// The realtime frame for a new message carries the conversation, the sender,
// and a per-recipient `mentioner` - and nothing else. No message id and no
// `replyingTo`. So a client asking "did that message reply to me?" has one
// honest move: read. Doing it as a history fetch costs a full window on EVERY
// message in EVERY conversation the entity belongs to, which is the difference
// between a bot that scales and one that does not.
//
// Answered here it is two indexed lookups and a usually-empty result. The
// "authored by me" half is decided from the token's entity, so there is no
// version of this a caller can point at somebody else's messages.
//
// # THE WINDOW
//
// `limit` bounds the REPLIES scanned, not the conversation. A busy channel
// where nobody has replied to this entity returns an empty slice having read
// one indexed page, which is the case this route exists to make cheap.
func RepliesTo(
	ctx context.Context,
	db *mongo.Database,
	conversationID, entityID string,
	limit int64,
) ([]Message, error) {
	if entityID == "" {
		return []Message{}, nil
	}

	candidates, err := findMessages(ctx, db, conversationID,
		bson.M{
			"conversationID": conversationID,
			"isDeleted":      bson.M{"$ne": true},
			"isReply":        true,
		}, limit)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return []Message{}, nil
	}

	parentIDs := make([]string, 0, len(candidates))
	for _, message := range candidates {
		if message.ReplyingTo != "" {
			parentIDs = append(parentIDs, message.ReplyingTo)
		}
	}
	own, err := messagesAuthoredBy(ctx, db, parentIDs, entityID)
	if err != nil {
		return nil, err
	}

	replies := selectRepliesTo(candidates, own)
	for i := range replies {
		// Known by construction: a parent is in `own` precisely because its
		// sender is this entity. Filled in so a row from here has the same
		// shape as one from RecentMessages.
		replies[i].ReplyingToSenderEntityID = entityID
	}
	return replies, nil
}

// selectRepliesTo keeps the messages whose parent is in `ownParentIDs`,
// preserving order.
//
// Split out from RepliesTo because it IS the rule - a reply counts only when
// the message it replies to is mine - and a rule that decides whether a bot
// speaks unprompted deserves to be testable without a database.
func selectRepliesTo(messages []Message, ownParentIDs map[string]bool) []Message {
	kept := make([]Message, 0, len(messages))
	for _, message := range messages {
		if message.ReplyingTo == "" || !ownParentIDs[message.ReplyingTo] {
			continue
		}
		kept = append(kept, message)
	}
	return kept
}

// findMessages runs one newest-first query and returns it OLDEST FIRST, so
// every caller gets a slice that reads as a transcript while the limit still
// takes the latest window.
func findMessages(
	ctx context.Context,
	db *mongo.Database,
	conversationID string,
	filter bson.M,
	limit int64,
) ([]Message, error) {
	cursor, err := db.Collection("messages").Find(ctx, filter,
		options.Find().
			SetProjection(messageProjection).
			// Newest first so the limit takes the LATEST window; reversed below.
			SetSort(bson.D{{Key: "messageDate", Value: -1}, {Key: "_id", Value: -1}}).
			SetLimit(limit),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var raws []bson.M
	if err := cursor.All(ctx, &raws); err != nil {
		return nil, err
	}

	messages := make([]Message, 0, len(raws))
	for i := len(raws) - 1; i >= 0; i-- {
		message, ok := decodeMessage(raws[i], conversationID)
		if !ok {
			continue
		}
		messages = append(messages, message)
	}
	return messages, nil
}

// decodeMessage narrows one stored row, reporting whether it is usable.
func decodeMessage(raw bson.M, conversationID string) (Message, bool) {
	messageID, _ := raw["messageID"].(string)
	sender, _ := raw["sender"].(string)
	content, isText := raw["content"].(string)
	if messageID == "" || sender == "" {
		// No id means nothing to thread a reply under; no sender means a
		// consumer's own loop guard cannot run.
		return Message{}, false
	}
	if !isText {
		// Image and file messages store non-text content. Real messages, but
		// nothing to read or embed.
		return Message{}, false
	}
	messageType, _ := raw["messageType"].(string)
	if messageType == "" {
		messageType = "text"
	}
	isReply, _ := raw["isReply"].(bool)
	replyingTo, _ := raw["replyingTo"].(string)
	if stored, _ := raw["conversationID"].(string); stored != "" {
		conversationID = stored
	}

	return Message{
		MessageID:      messageID,
		ConversationID: conversationID,
		SenderEntityID: sender,
		Content:        content,
		CreatedAt:      ToEpochMillis(raw["messageDate"]),
		MessageType:    messageType,
		IsReply:        isReply,
		ReplyingTo:     replyingTo,
	}, true
}

// resolveReplyParents fills ReplyingToSenderEntityID for every reply in the
// slice, in ONE lookup for the whole batch.
//
// Parents are looked up by id and NOT restricted to the conversation: a
// `replyingTo` always points inside its own conversation, so adding that
// filter would only mask a data bug by blanking the field instead.
func resolveReplyParents(ctx context.Context, db *mongo.Database, messages []Message) error {
	parentIDs := make([]string, 0, len(messages))
	for _, message := range messages {
		if message.ReplyingTo != "" {
			parentIDs = append(parentIDs, message.ReplyingTo)
		}
	}
	unique := dedupe(parentIDs)
	if len(unique) == 0 {
		return nil
	}

	cursor, err := db.Collection("messages").Find(ctx,
		bson.M{"messageID": bson.M{"$in": unique}},
		options.Find().SetProjection(bson.M{"_id": 0, "messageID": 1, "sender": 1}),
	)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	var raws []bson.M
	if err := cursor.All(ctx, &raws); err != nil {
		return err
	}

	senders := make(map[string]string, len(raws))
	for _, raw := range raws {
		messageID, _ := raw["messageID"].(string)
		sender, _ := raw["sender"].(string)
		if messageID != "" && sender != "" {
			senders[messageID] = sender
		}
	}
	for i := range messages {
		// A deleted or purged parent leaves this empty rather than guessed at.
		messages[i].ReplyingToSenderEntityID = senders[messages[i].ReplyingTo]
	}
	return nil
}

// messagesAuthoredBy returns which of `messageIDs` were written by `entityID`.
//
// The authorship filter is in the QUERY, not applied to the result. This set
// is the only thing between "somebody replied to me" and "somebody replied to
// anybody", so it should not be possible to read a row out of it that belongs
// to someone else.
func messagesAuthoredBy(
	ctx context.Context,
	db *mongo.Database,
	messageIDs []string,
	entityID string,
) (map[string]bool, error) {
	unique := dedupe(messageIDs)
	if len(unique) == 0 {
		return map[string]bool{}, nil
	}

	cursor, err := db.Collection("messages").Find(ctx,
		bson.M{"messageID": bson.M{"$in": unique}, "sender": entityID},
		options.Find().SetProjection(bson.M{"_id": 0, "messageID": 1}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var raws []bson.M
	if err := cursor.All(ctx, &raws); err != nil {
		return nil, err
	}

	own := make(map[string]bool, len(raws))
	for _, raw := range raws {
		if messageID, _ := raw["messageID"].(string); messageID != "" {
			own[messageID] = true
		}
	}
	return own, nil
}

// ToEpochMillis normalises a messageDate.
//
// The column holds two live shapes and both are real: a BSON date on anything
// the Node send route wrote (it leaves the field unset and the schema's
// `default: Date.now` fills it), and an embedded {date, time} of formatted
// strings on older rows, which is what user_service's MessageDate still
// models. Deciding which one you are holding is the owning service's problem,
// which is why it is answered here rather than by every client.
//
// 0 for anything unrecognised, deliberately: that sorts an unreadable row to
// the START of history rather than the end, so a parsing gap can never make an
// old message look like the newest one.
func ToEpochMillis(value any) int64 {
	switch typed := value.(type) {
	case primitive.DateTime:
		return int64(typed)
	case time.Time:
		return typed.UnixMilli()
	case int64:
		return typed
	case int32:
		return int64(typed)
	case float64:
		return int64(typed)
	case bson.M:
		return parseDateString(typed["date"])
	case map[string]any:
		return parseDateString(typed["date"])
	}
	return 0
}

func parseDateString(value any) int64 {
	raw, ok := value.(string)
	if !ok || raw == "" {
		return 0
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05", "2006-01-02"} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UnixMilli()
		}
	}
	return 0
}

func stringSlice(value any) []string {
	raw, ok := value.(primitive.A)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if text, ok := item.(string); ok && text != "" {
			out = append(out, text)
		}
	}
	return out
}
