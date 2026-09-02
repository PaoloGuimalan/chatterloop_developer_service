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

// RecentMessages returns the newest `limit` messages, OLDEST FIRST so the slice
// reads as a transcript.
func RecentMessages(ctx context.Context, db *mongo.Database, conversationID string, limit int64) ([]Message, error) {
	cursor, err := db.Collection("messages").Find(ctx,
		bson.M{
			"conversationID": conversationID,
			"isDeleted":      bson.M{"$ne": true},
		},
		options.Find().
			SetProjection(bson.M{
				"_id": 0, "messageID": 1, "conversationID": 1, "sender": 1,
				"content": 1, "messageDate": 1, "messageType": 1,
				"isReply": 1, "replyingTo": 1,
			}).
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
		raw := raws[i]
		messageID, _ := raw["messageID"].(string)
		sender, _ := raw["sender"].(string)
		content, isText := raw["content"].(string)
		if messageID == "" || sender == "" {
			// No id means nothing to thread a reply under; no sender means a
			// consumer's own loop guard cannot run.
			continue
		}
		if !isText {
			// Image and file messages store non-text content. Real messages,
			// but nothing to read or embed.
			continue
		}
		messageType, _ := raw["messageType"].(string)
		if messageType == "" {
			messageType = "text"
		}
		isReply, _ := raw["isReply"].(bool)
		replyingTo, _ := raw["replyingTo"].(string)

		messages = append(messages, Message{
			MessageID:      messageID,
			ConversationID: conversationID,
			SenderEntityID: sender,
			Content:        content,
			CreatedAt:      ToEpochMillis(raw["messageDate"]),
			MessageType:    messageType,
			IsReply:        isReply,
			ReplyingTo:     replyingTo,
		})
	}
	return messages, nil
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
