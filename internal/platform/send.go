package platform

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strconv"
	"strings"
	"time"

	"developer_service/internal/queue"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Deps is everything sending a message touches.
type Deps struct {
	Mongo    *mongo.Database
	Postgres *pgxpool.Pool
	Redis    *redis.Client
	Queue    *queue.Publisher
	PodName  string
}

type SendRequest struct {
	ConversationID   string
	Content          string
	ConversationType string
	ReplyingTo       string
	MessageType      string
	PendingID        string
}

type SendResult struct {
	MessageID string `json:"message_id"`
	PendingID string `json:"pending_id"`
	Receivers int    `json:"receivers"`
}

var ErrEmptyContent = errors.New("refusing to send an empty message")

// SendMessage writes a message and performs the fan-out that makes it a
// message rather than a row.
//
// # WHAT SENDING ACTUALLY INVOLVES
//
// The platform's own route does six things after the insert, and a message
// that skips them is not one anyone sees properly:
//
//  1. un-archives the conversation for every participant
//  2. updates the conversation's preview / last message
//  3. publishes a realtime frame per recipient
//  4. bumps the chat interaction score
//  5. resolves link previews
//  6. sends push notifications, with a distinct payload for mentions
//
// Five of the six are here. (3) publishes straight to Redis in the platform's
// own envelope, which reaches Node's SSE bridge for browsers and this
// service's stream for API clients from one write. (4) and (6) are handed to
// the Go worker over queues it already consumes, so scoring and Firebase each
// keep one implementation.
//
// # WHAT IS DELIBERATELY NOT HERE
//
// (5) link previews. The platform resolves them by calling an internal
// preview service and then re-triggering the realtime frame. A message sent
// through this API therefore renders a bare URL rather than a preview card.
// That is a visible gap and it is listed in the README rather than papered
// over.
//
// Content tagging is also skipped. The platform gates it on a Redis presence
// key and publishes NOTHING when the moderation service is down, because that
// service's database scour picks the content up on its next start - the
// designed path, not a degraded one. Leaning on that same path here keeps one
// less payload shape in sync.
func SendMessage(ctx context.Context, deps Deps, senderEntityID string, req SendRequest) (*SendResult, error) {
	content := SanitizeForStorage(strings.TrimSpace(req.Content))
	if content == "" {
		return nil, ErrEmptyContent
	}

	// Access first: everything below writes, and the caller has no business
	// learning that a conversation exists by watching which error it gets.
	if err := AssertMember(ctx, deps.Mongo, deps.Postgres, req.ConversationID, senderEntityID); err != nil {
		return nil, err
	}

	messageID, err := uniqueMessageID(ctx, deps.Mongo)
	if err != nil {
		return nil, err
	}

	receivers, err := Receivers(ctx, deps.Mongo, deps.Postgres, req.ConversationID)
	if err != nil {
		return nil, err
	}

	// The caller's conversationType is a FALLBACK, never authoritative.
	//
	// A message must not be able to re-type the conversation it is sent to.
	// The RAG bot exposed this the hard way: its responder defaults to
	// "single", so one reply into a real group silently rewrote that
	// conversation's type and the UI started rendering a group as a DM.
	//
	// Order of truth: what the conversation already says, then the realm it
	// belongs to, then - only for a genuinely new, non-realm conversation -
	// whatever the caller claimed.
	conversationType := resolveConversationType(ctx, deps, req.ConversationID, req.ConversationType)
	messageType := req.MessageType
	if messageType == "" {
		messageType = "text"
	}
	pendingID := req.PendingID
	if pendingID == "" {
		pendingID = fmt.Sprintf("api-%d", time.Now().UnixNano())
	}

	// Mentions are matched against the conversation's own members, so an
	// @handle naming somebody who is not in it notifies nobody.
	mentioned := map[string]bool{}
	byUsername := make(map[string]string, len(receivers))
	for _, receiver := range receivers {
		byUsername[strings.ToLower(receiver.Username)] = receiver.EntityID
	}
	for _, handle := range ExtractMentions(content) {
		if entityID, ok := byUsername[strings.ToLower(handle)]; ok {
			mentioned[entityID] = true
		}
	}

	realmName := ""
	if conversationType != "single" {
		realmName = RealmName(ctx, deps.Postgres, req.ConversationID)
	}

	now := time.Now().UTC()
	document := bson.M{
		"messageID":      messageID,
		"conversationID": req.ConversationID,
		"pendingID":      pendingID,
		"sender":         senderEntityID,
		// Derived server-side by the readers, never trusted from the sender -
		// which is what stops a caller addressing people not in the
		// conversation. The platform's own route stores this empty too.
		"receivers": bson.A{},
		// The sender has seen their own message by definition.
		"seeners":    bson.A{senderEntityID},
		"content":    content,
		"isReply":    req.ReplyingTo != "",
		"replyingTo": req.ReplyingTo,
		"reactions":  bson.A{},
		"isDeleted":  false,
		// A BSON date, which is the shape the platform's send route produces
		// (it leaves the field unset and the schema default fills it). Readers
		// handle both shapes; writers should only ever produce this one.
		"messageDate":      now,
		"messageType":      messageType,
		"conversationType": conversationType,
	}
	if _, err := deps.Mongo.Collection("messages").InsertOne(ctx, document); err != nil {
		return nil, fmt.Errorf("message insert: %w", err)
	}

	// Everything past this point is fan-out. The message exists; a failure
	// here degrades how it is delivered, and must not be reported as a failure
	// to send.
	fanOut(ctx, deps, senderEntityID, req, messageID, content, conversationType,
		messageType, realmName, receivers, mentioned, now)

	return &SendResult{
		MessageID: messageID,
		PendingID: pendingID,
		Receivers: len(receivers),
	}, nil
}

func fanOut(
	ctx context.Context,
	deps Deps,
	senderEntityID string,
	req SendRequest,
	messageID, content, conversationType, messageType, realmName string,
	receivers []Receiver,
	mentioned map[string]bool,
	now time.Time,
) {
	entityIDs := make([]string, 0, len(receivers))
	for _, receiver := range receivers {
		entityIDs = append(entityIDs, receiver.EntityID)
	}

	// (1) A message un-archives the conversation for everyone in it: an
	// archived thread that receives a reply should reappear.
	if _, err := deps.Mongo.Collection("chat_history").UpdateMany(ctx,
		bson.M{"conversationID": req.ConversationID},
		bson.M{"$set": bson.M{"isArchived": false}},
	); err != nil {
		slog.Error("un-archive failed", "conversation_id", req.ConversationID, "error", err)
	}

	// (2) The conversation list renders from this, so skipping it leaves the
	// thread showing its previous message as the latest.
	if err := saveConversation(ctx, deps.Mongo, req.ConversationID, conversationType,
		entityIDs, messageID, senderEntityID, content, messageType, now); err != nil {
		slog.Error("conversation preview update failed",
			"conversation_id", req.ConversationID, "error", err)
	}

	// (3) One frame per recipient, in the platform's envelope.
	senderHandle := ""
	if handles, err := HandlesFor(ctx, deps.Postgres, []string{senderEntityID}); err == nil {
		senderHandle = handles[senderEntityID]
	}
	mentioner := map[string]any{
		"entityID":  senderEntityID,
		"username":  "@" + senderHandle,
		"realmName": nilIfEmpty(realmName),
		"isSingle":  conversationType == "single",
	}
	for _, entityID := range entityIDs {
		details := map[string]any{
			"conversationID": req.ConversationID,
			"entityID":       senderEntityID,
			"mentioner":      nil,
		}
		if mentioned[entityID] {
			details["mentioner"] = mentioner
		}
		publishFrame(ctx, deps, entityID, "messages_list", map[string]any{
			"status": true, "auth": true, "onseen": false,
			"message": details, "result": "",
		})
	}

	// (4) Rate-limited by the same Redis lock the platform uses, so a busy
	// conversation bumps once per half hour rather than once per message.
	bumpChatScore(ctx, deps, req.ConversationID, senderEntityID, entityIDs)

	// (6) The sender's own other devices are excluded; anyone @mentioned gets
	// the mention push INSTEAD of the plain one, never as well - two tray
	// entries for one message is noise, and the mention one carries the text
	// so nothing is lost by the swap.
	var plain, mentionTargets []string
	for _, entityID := range entityIDs {
		if entityID == senderEntityID {
			continue
		}
		if mentioned[entityID] {
			mentionTargets = append(mentionTargets, entityID)
		} else {
			plain = append(plain, entityID)
		}
	}

	title := realmName
	if conversationType == "single" || title == "" {
		title = "@" + senderHandle
	}
	body := content
	if messageType != "text" {
		body = "Sent an attachment"
	}

	senderName := "@" + senderHandle // matches push_payload.dart's own example: "senderName": "@paulo"

	if len(plain) > 0 {
		deps.Queue.Publish(ctx, queue.SendPush, queue.PushPayload{
			EntityIDs: plain,
			Channel:   queue.ChannelMessages,
			Title:     title,
			Body:      body,
			Tag:       req.ConversationID,
			Data: messagePushData(
				req.ConversationID, realmName, senderEntityID, senderName,
				body, messageID, conversationType, now,
			),
		})
	}
	if len(mentionTargets) > 0 {
		mentionBody := senderName + " mentioned you: " + body
		deps.Queue.Publish(ctx, queue.SendPush, queue.PushPayload{
			EntityIDs: mentionTargets,
			// The quieter Activity channel, whose tone is the sound the
			// webapp already plays for a mention.
			Channel: queue.ChannelActivity,
			Title:   title,
			Body:    mentionBody,
			Tag:     req.ConversationID,
			Data: mentionPushData(
				req.ConversationID, senderEntityID, title, mentionBody, messageID,
			),
		})
	}
}

// messagePushData is what chatterloop_app's push_payload.dart actually reads
// for a "message"-type push.
//
// Everything the tray renders comes from `data`, never from the queue.
// PushPayload.Title/Body fields above - see push_payload.dart's own header:
// "deliberately NOT from `notification`", because a data-only message is what
// lets the app build its own threaded, per-sender MessagingStyle layout
// instead of the OS's flat title+body. Title/Body are still sent (Android/
// APNs require SOME top-level content on a push), but the renderer never
// reads them for a "message" type - only `data` does, via the fallback chain
// `senderName ?? conversationName ?? title`. Before this existed, `data`
// carried none of these fields, so every message sent through this endpoint -
// currently only the chatterloop bot - rendered as an empty notification
// tray-side, even though the OS-level Title/Body were always correct. Node's
// own pushnotification.js sendMessage() sends this same field set; this now
// matches it field-for-field.
//
// Pure and dependency-free on purpose, unlike fanOut itself (Mongo/Postgres/
// Redis/Queue via Deps) - this is the part a regression would actually land
// in, and the part cheap enough to hold a real test without mocking any of
// those.
func messagePushData(
	conversationID, conversationName, senderEntityID, senderName,
	body, messageID, conversationType string, sentAt time.Time,
) map[string]string {
	return map[string]string{
		"type":             "message",
		"conversationId":   conversationID,
		"conversationName": conversationName,
		"isGroup":          strconv.FormatBool(conversationType != "single"),
		"senderId":         senderEntityID,
		"senderName":       senderName,
		"body":             body,
		"sentAt":           strconv.FormatInt(sentAt.UnixMilli(), 10),
		"messageId":        messageID,
	}
}

// mentionPushData is what push_payload.dart reads for anything OTHER than
// "message" - the generic title+body path ("the second shape" in its header
// comment). That is why this needs data.title/data.body directly, rather
// than the "message" type's senderName/conversationName fallback chain.
//
// Takes the already-composed body rather than building "@x mentioned you: "
// itself, so that text can never drift from what the top-level
// queue.PushPayload.Body (which some future path might still read) says -
// one string, read twice, not built twice.
func mentionPushData(conversationID, senderEntityID, title, body, messageID string) map[string]string {
	return map[string]string{
		"type":           "mention",
		"conversationId": conversationID,
		"title":          title,
		"body":           body,
		"senderId":       senderEntityID,
		"messageId":      messageID,
		"route":          "/conversation/" + conversationID,
	}
}

// publishFrame writes one realtime frame addressed to a PERSON.
//
// Node's SSE bridge and this service's own stream both subscribe to
// `events_<entity_id>`, so one write reaches browsers and API clients alike -
// which is why this does not go through RabbitMQ the way the platform's
// internal fan-out does.
//
// The OTHER axis is a post's own channel, addressed to whoever is reading it
// rather than to anyone in particular; see post_activity.go. Both go out in
// the same envelope, which is why publishEnvelope is separate from this.
func publishFrame(ctx context.Context, deps Deps, entityID, event string, message any) {
	publishEnvelope(ctx, deps, "events_"+entityID, event, message)
}

// publishEnvelope writes one frame to `channel` in the exact envelope the
// platform's Redis publisher produces (server/reusables/redis/pubsub.js
// publish()), which is what the subscribers on the other end unwrap.
//
// Channel-agnostic on purpose: the envelope is the platform's, not any one
// channel's, and a second copy of it per channel is a second thing to keep in
// step with the two publishers that already write it.
func publishEnvelope(ctx context.Context, deps Deps, channel, event string, message any) {
	envelope := map[string]any{
		"logType":  nil,
		"pod":      deps.PodName,
		"event":    event,
		"message":  message,
		"dateTime": time.Now().UTC().Format(time.RFC3339Nano),
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		slog.Error("frame encode failed", "channel", channel, "error", err)
		return
	}
	if err := deps.Redis.Publish(ctx, channel, body).Err(); err != nil {
		slog.Error("frame publish failed", "channel", channel, "error", err)
	}
}

func bumpChatScore(ctx context.Context, deps Deps, conversationID, actorID string, memberIDs []string) {
	if len(memberIDs) == 0 {
		return
	}
	// SET NX with a 30-minute TTL, the platform's own throttle: the first
	// message in a window scores, the rest are conversation, not signal.
	lockKey := fmt.Sprintf("chatterloop:bump_lock:%s:chat", conversationID)
	acquired, err := deps.Redis.SetNX(ctx, lockKey, "1", 30*time.Minute).Result()
	if err != nil {
		slog.Warn("score lock unavailable, skipping bump", "error", err)
		return
	}
	if !acquired {
		return
	}
	deps.Queue.Publish(ctx, queue.BumpChatScore, queue.ChatScorePayload{
		ActorID: actorID, MemberIDs: memberIDs, Action: "CHAT", IsDecrease: false,
	})
}

// saveConversation upserts the conversation's preview, mirroring the
// platform's SaveConversation. participant_ids is only overwritten when the
// caller resolved some, so a conversation whose members could not be resolved
// keeps the list it had rather than being emptied.
func saveConversation(
	ctx context.Context,
	db *mongo.Database,
	conversationID, conversationType string,
	participantIDs []string,
	messageID, sender, text, messageType string,
	now time.Time,
) error {
	set := bson.M{
		"last_message": bson.M{
			"messageID":   messageID,
			"sender":      sender,
			"text":        text,
			"messageDate": now,
			"messageType": messageType,
			"isDeleted":   false,
		},
	}
	if len(participantIDs) > 0 {
		set["participant_ids"] = participantIDs
	}

	_, err := db.Collection("conversations").UpdateOne(ctx,
		bson.M{"conversationID": conversationID},
		// conversationType goes in $setOnInsert, never $set: an existing
		// conversation keeps the type it already has, so no send can change it.
		bson.M{"$set": set, "$setOnInsert": bson.M{
			"conversationID":   conversationID,
			"conversationType": conversationType,
		}},
		options.Update().SetUpsert(true),
	)
	return err
}

// uniqueMessageID mints a 30-DIGIT id, matching the platform's makeID(30),
// whose alphabet is "0123456789" and not alphanumeric. A different alphabet
// would still be unique but would stand out in the collection and break any
// consumer that assumes the shape.
//
// crypto/rand rather than math/rand: message ids appear in URLs and payloads,
// and a predictable id is a small enumeration primitive for free.
func uniqueMessageID(ctx context.Context, db *mongo.Database) (string, error) {
	for attempt := 0; attempt < 5; attempt++ {
		candidate, err := randomDigits(30)
		if err != nil {
			return "", err
		}
		count, err := db.Collection("messages").CountDocuments(ctx,
			bson.M{"messageID": candidate}, options.Count().SetLimit(1))
		if err != nil {
			return "", fmt.Errorf("message id check: %w", err)
		}
		if count == 0 {
			return candidate, nil
		}
	}
	// 10^30 of space; five collisions means something is wrong with the
	// randomness, not with luck.
	return "", errors.New("could not mint a unique message id")
}

func randomDigits(length int) (string, error) {
	var out strings.Builder
	out.Grow(length)
	for i := 0; i < length; i++ {
		n, err := rand.Int(rand.Reader, big.NewInt(10))
		if err != nil {
			return "", err
		}
		out.WriteByte(byte('0' + n.Int64()))
	}
	return out.String(), nil
}

// resolveConversationType decides a conversation's type from the most
// authoritative source available, ignoring the caller wherever one exists.
func resolveConversationType(ctx context.Context, deps Deps, conversationID, claimed string) string {
	var existing bson.M
	if err := deps.Mongo.Collection("conversations").
		FindOne(ctx, bson.M{"conversationID": conversationID}).
		Decode(&existing); err == nil {
		if stored, _ := existing["conversationType"].(string); stored != "" {
			return stored
		}
	}

	// A conversation id that matches a realm IS that realm; its type is the
	// realm's, whatever the caller believes.
	var realmType string
	if err := deps.Postgres.QueryRow(ctx,
		`SELECT type FROM community_realm WHERE realm_id = $1`, conversationID,
	).Scan(&realmType); err == nil && realmType != "" {
		return normalizeConversationType(realmType)
	}

	return normalizeConversationType(claimed)
}

// normalizeConversationType mirrors the platform's: "server" is stored as
// "channel", everything else lowercased, empty defaults to "single".
func normalizeConversationType(conversationType string) string {
	if conversationType == "" {
		return "single"
	}
	parsed := strings.ToLower(conversationType)
	if parsed == "server" {
		return "channel"
	}
	return parsed
}

func nilIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}
