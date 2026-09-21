package platform

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Reading what the moderation pipeline already learned about a piece of
// content, so a bot answering a photo or a voice note knows what it is.
//
// # WHY THIS EXISTS
//
// A message whose content is an upload stores the CDN URL in `content` and the
// mime in `messageType`. A reader that shows the model `content` shows it a
// URL, and a model shown a URL answers the URL. moderation_service has already
// transcribed, captioned and read the text out of that same file - this route
// is the only thing that lets a caller use it.
//
// # CONTEXT, NOT ENFORCEMENT
//
// The moderation document also holds a verdict, its categories and scores,
// whether it was auto-reported and which report it became. NONE of that is
// returned. Those describe what the PLATFORM decided about somebody else's
// content, and a developer credential asking "what is in this video" has no
// business learning it. The allow-list is expressed as an explicit struct
// below rather than as a projection with omissions, so a field added to the
// document later is private by default instead of leaking until somebody
// notices.
//
// `tags` is excluded for the same reason at one remove: they are the interest
// graph's derived labels, not something the content itself says.

// moderationCollection is written by moderation_service. It lives in the same
// Mongo database as `messages` and `files`, which is why this needs no new
// connection.
const moderationCollection = "moderation"

// Source types, as moderation_service's SourceType enum spells them. A query
// is always constrained to the family it asked about, so an id that happens to
// collide across two families cannot answer for the wrong one.
var (
	messageSources = []string{"message", "message_attachment"}
	postSources    = []string{"post", "post_attachment"}
	commentSources = []string{"comment", "comment_attachment"}
)

// ModerationRecord is what a caller may be told.
//
// Every analysis field is omitempty EXCEPT Status. A caller has to be able to
// tell "this has no transcript" from "this has not been read yet", and that
// distinction is the whole reason a pending document is returned at all rather
// than filtered out.
type ModerationRecord struct {
	TargetID    string `json:"target_id"`
	SourceType  string `json:"source_type"`
	ContentType string `json:"content_type"`
	MediaURL    string `json:"media_url,omitempty"`
	// pending | processing | done | failed | skipped
	Status string `json:"status"`

	// What was learned. All optional: a document can be done and empty.
	Text          string `json:"text,omitempty"`
	Transcription string `json:"transcription,omitempty"`
	Caption       string `json:"caption,omitempty"`
	ShownText     string `json:"shown_text,omitempty"`
	Language      string `json:"language,omitempty"`
	// A pointer because null means "nothing classified the audio", which is a
	// different thing from "not music" - the same unknown-vs-negative
	// distinction the document's own verdict field makes.
	IsMusic *bool `json:"is_music,omitempty"`
}

// moderationProjection asks Mongo for exactly the fields above.
//
// Belt to the struct's braces: the allow-list would hold even without it, but
// not sending a verdict over the wire from the database in the first place
// means it cannot be logged, buffered or dumped in a panic either.
var moderationProjection = bson.M{
	"_id": 0, "targetID": 1, "sourceType": 1, "contentType": 1,
	"mediaURL": 1, "status": 1, "text": 1, "transcription": 1,
	"caption": 1, "shownText": 1, "language": 1, "audio.isMusic": 1,
}

// ModerationForMessages returns what is known about the given messages.
//
// AUTHORISATION IS PER MESSAGE, and it is the point of the function. Without
// it a token holding messages.read could read the transcript of any voice note
// on the platform by guessing message ids - the route would be an oracle over
// every private conversation at once.
//
// An id the caller may not see is DROPPED, not refused. A 404 for it would
// answer the question the check exists to refuse: the caller would learn that
// the id is real. Dropping makes "you may not see it", "it does not exist" and
// "it has no moderation document" one indistinguishable response.
func ModerationForMessages(
	ctx context.Context,
	db *mongo.Database,
	pool *pgxpool.Pool,
	messageIDs []string,
	entityID string,
) ([]ModerationRecord, error) {
	messageIDs = distinct(messageIDs)
	if len(messageIDs) == 0 {
		return []ModerationRecord{}, nil
	}

	conversations, err := conversationsOf(ctx, db, messageIDs)
	if err != nil {
		return nil, err
	}

	allowed, err := permittedMessages(messageIDs, conversations,
		func(conversationID string) (bool, error) {
			err := AssertMember(ctx, db, pool, conversationID, entityID)
			switch {
			case err == nil:
				return true, nil
			case errors.Is(err, ErrNoAccess), errors.Is(err, ErrNotAParticipant):
				return false, nil
			default:
				return false, err
			}
		})
	if err != nil {
		return nil, err
	}

	if len(allowed) == 0 {
		return []ModerationRecord{}, nil
	}
	return findModeration(ctx, db, allowed, messageSources)
}

// permittedMessages is the authorisation filter, separated from its I/O so it
// can be tested exhaustively without a database.
//
// This function is the security boundary of the whole route. Everything it
// returns is read; everything it drops is unreadable. It is pure on purpose -
// the alternative is a rule that can only be exercised against live Postgres
// and Mongo, which in practice means a rule nothing exercises.
//
// `isMember` is asked once per CONVERSATION, not once per message: a window of
// forty messages is one membership question, and asking it forty times would
// be forty round trips to get the same answer.
//
// Order follows the caller's ids rather than map iteration, so a response is
// the same on every call - Go randomises map order, and a batch read that
// shuffled would look like the data changing.
func permittedMessages(
	messageIDs []string,
	conversations map[string]string,
	isMember func(conversationID string) (bool, error),
) ([]string, error) {
	allowed := make([]string, 0, len(messageIDs))
	decided := make(map[string]bool, len(conversations))

	for _, messageID := range messageIDs {
		conversationID, known := conversations[messageID]
		if !known {
			// No row for this id. Dropped rather than looked up: an unknown id
			// must never inherit permission from a conversation it was never
			// in, and there is nothing to ask about.
			continue
		}

		permitted, seen := decided[conversationID]
		if !seen {
			var err error
			permitted, err = isMember(conversationID)
			if err != nil {
				// A membership check that FAILED is not a membership check
				// that said no. Refusing the whole read is the only safe
				// reading - the alternative silently returns a short list that
				// looks exactly like "you may see less than you asked for".
				return nil, err
			}
			decided[conversationID] = permitted
		}
		if permitted {
			allowed = append(allowed, messageID)
		}
	}
	return allowed, nil
}

// ModerationForPost returns what is known about a post and its attachments.
//
// One query for both: the publisher writes the post id into every attachment's
// `foreignID`, which is exactly what `moderation_foreign_type_idx` is for.
//
// Visibility is the SAME rule LoadPostThread applies - the post exists and is
// not deleted - because this returns nothing about a post that reading its
// comments would not already reveal. Reusing that helper rather than restating
// the rule is what makes the two move together if post privacy is ever added.
func ModerationForPost(
	ctx context.Context,
	db *mongo.Database,
	pool *pgxpool.Pool,
	postID string,
) ([]ModerationRecord, error) {
	if postID == "" {
		return nil, ErrPostNotFound
	}
	if err := assertPostReadable(ctx, pool, postID); err != nil {
		return nil, err
	}
	return findModeration(ctx, db, []string{postID}, postSources)
}

// ModerationForComments returns what is known about the given comments.
//
// Same dropping rule as messages: a comment on a deleted post, or one that
// does not exist, is simply absent from the answer.
func ModerationForComments(
	ctx context.Context,
	db *mongo.Database,
	pool *pgxpool.Pool,
	commentIDs []string,
) ([]ModerationRecord, error) {
	commentIDs = distinct(commentIDs)
	if len(commentIDs) == 0 {
		return []ModerationRecord{}, nil
	}

	rows, err := pool.Query(ctx, `
		SELECT c.comment_id
		  FROM newsfeed_comment c
		  JOIN newsfeed_post p ON p.post_id = c.post_id
		 WHERE c.comment_id = ANY($1)
		   AND c.deleted_at IS NULL
		   AND p.deleted_at IS NULL`, commentIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	allowed := make([]string, 0, len(commentIDs))
	for rows.Next() {
		var commentID string
		if err := rows.Scan(&commentID); err != nil {
			return nil, err
		}
		allowed = append(allowed, commentID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(allowed) == 0 {
		return []ModerationRecord{}, nil
	}
	return findModeration(ctx, db, allowed, commentSources)
}

// assertPostReadable mirrors the existence test LoadPostThread makes.
func assertPostReadable(ctx context.Context, pool *pgxpool.Pool, postID string) error {
	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT true
		  FROM newsfeed_post
		 WHERE post_id = $1 AND deleted_at IS NULL`, postID,
	).Scan(&exists)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrPostNotFound
	}
	return err
}

// conversationsOf maps each message id to the conversation holding it.
//
// A message id with no row is absent from the map and therefore never
// authorised - an unknown id cannot accidentally inherit permission from a
// conversation it was never in.
func conversationsOf(
	ctx context.Context,
	db *mongo.Database,
	messageIDs []string,
) (map[string]string, error) {
	cursor, err := db.Collection("messages").Find(
		ctx,
		bson.M{"messageID": bson.M{"$in": messageIDs}},
		options.Find().SetProjection(bson.M{"_id": 0, "messageID": 1, "conversationID": 1}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	found := make(map[string]string, len(messageIDs))
	for cursor.Next(ctx) {
		var raw bson.M
		if err := cursor.Decode(&raw); err != nil {
			// One unreadable row costs one message, never the batch.
			continue
		}
		messageID, _ := raw["messageID"].(string)
		conversationID, _ := raw["conversationID"].(string)
		if messageID != "" && conversationID != "" {
			found[messageID] = conversationID
		}
	}
	return found, cursor.Err()
}

// findModeration reads the documents for ids the caller has already been
// cleared for.
//
// Matched on `foreignID` rather than `targetID` because an attachment carries
// its parent's id there: one query returns a post's caption and its video, or
// a message and the upload inside it. `sourceType` narrows it to the family
// the caller asked about.
func findModeration(
	ctx context.Context,
	db *mongo.Database,
	ids []string,
	sources []string,
) ([]ModerationRecord, error) {
	cursor, err := db.Collection(moderationCollection).Find(
		ctx,
		bson.M{
			"foreignID":  bson.M{"$in": ids},
			"sourceType": bson.M{"$in": sources},
		},
		options.Find().SetProjection(moderationProjection),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	records := []ModerationRecord{}
	for cursor.Next(ctx) {
		var raw bson.M
		if err := cursor.Decode(&raw); err != nil {
			continue
		}
		records = append(records, decodeModeration(raw))
	}
	return records, cursor.Err()
}

// decodeModeration reads the document field by field.
//
// Not bson tags on the struct, deliberately. A tagged struct decodes whatever
// the document holds into whatever it names, so the day a field is renamed the
// failure is a silent empty string. Reading each one by hand means the mapping
// is visible, and it is also what keeps the allow-list a list rather than a
// hope.
func decodeModeration(raw bson.M) ModerationRecord {
	record := ModerationRecord{
		TargetID:      text(raw["targetID"]),
		SourceType:    text(raw["sourceType"]),
		ContentType:   text(raw["contentType"]),
		MediaURL:      text(raw["mediaURL"]),
		Status:        text(raw["status"]),
		Text:          text(raw["text"]),
		Transcription: text(raw["transcription"]),
		Caption:       text(raw["caption"]),
		ShownText:     text(raw["shownText"]),
		Language:      text(raw["language"]),
	}
	if audio, ok := raw["audio"].(bson.M); ok {
		if isMusic, ok := audio["isMusic"].(bool); ok {
			record.IsMusic = &isMusic
		}
	}
	if record.Status == "" {
		// A document written before the field existed, or one whose status is
		// unreadable. "pending" is the honest reading: something is there and
		// nothing has said it finished.
		record.Status = "pending"
	}
	return record
}

func text(value any) string {
	if out, ok := value.(string); ok {
		return out
	}
	return ""
}

// distinct drops blanks and repeats, so a caller repeating an id does not pay
// for it twice and cannot use repetition to get past the cap.
func distinct(values []string) []string {
	seen := make(map[string]bool, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}
