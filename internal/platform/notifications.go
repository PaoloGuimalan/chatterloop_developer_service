// Notification writing, ported from user_service's NotificationService.
//
// # WHY THIS SERVICE WRITES THEM AT ALL
//
// Reading notifications was enough while this API only read. Creating a
// comment is not: the notification IS the delivery. A reply nobody is told
// about is a row, and the person replied to never learns it happened - which
// for a bot answering in a thread means its answer is invisible to the one
// person it was written for.
//
// # THE SHAPE IS MONGOENGINE'S, NOT THIS SERVICE'S
//
// user_service/user/ext_models/mongomodels.py declares the document, and Node's
// read path in server/routes/users/index.js renders it. Both already exist, so
// every field here is dictated - notably:
//
//   - `date` is an EMBEDDED {date, time} of formatted strings, not a BSON date,
//     and `time` is written as None by every Django call site, which mongoengine
//     omits entirely rather than storing null.
//   - `referenceID` is a BACKEND id whose meaning changes per type; where the
//     client should NAVIGATE is `target`, which is a separate field precisely
//     because routing off referenceID produced confidently wrong links.
//   - `isRead` is DERIVED, never taken from a caller: a notice addressed to one
//     entity is personal and starts unread.
package platform

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Notification types this service writes. Both are Django's, spelled exactly
// as CommentsView.post() and notify_comment_mentions() spell them - a consumer
// filtering on the string gets nothing at all from a near miss.
const (
	NotificationPostComment    = "post_comment"
	NotificationCommentMention = "comment_mention"
)

// Headlines, likewise verbatim. These are stored data rather than rendered
// sentences, and the webapp sections its notification tray on them.
const (
	HeadlineRepliedComment = "Replied Comment"
	HeadlinePostComment    = "Post Comment"
	HeadlineCommentMention = "Comment Mention"
)

// NewNotification is one row to write plus the realtime ping that goes with
// it. The two always travel together in Django - a notification written
// without its SSE frame does not appear until the recipient reloads - so they
// are one struct here rather than two calls a caller could get half right.
type NewNotification struct {
	ToEntityID   string
	FromEntityID string
	// The BACKEND id for the action. For every type this service writes, the
	// new comment.
	ReferenceID string
	Type        string
	Headline    string
	// The sentence a person reads, and also the body of the realtime frame.
	Details string
	// Where the client should go: the post, anchored at the comment.
	TargetType   string
	TargetID     string
	TargetAnchor string
}

// WriteNotification inserts one notification and publishes the realtime frame
// for it.
//
// Best-effort by design, and the return value says which half succeeded rather
// than being an error: the comment it describes is already committed, and a
// notification that could not be delivered must not be reported as a comment
// that was not created.
func WriteNotification(ctx context.Context, deps Deps, notification NewNotification) bool {
	notificationID, err := uniqueNotificationID(ctx, deps.Mongo)
	if err != nil {
		slog.Error("could not mint a notification id", "error", err)
		return false
	}

	document := bson.M{
		"notificationID": notificationID,
		"referenceID":    notification.ReferenceID,
		// True on every comment call site in Django. It means "the thing this
		// refers to is still live", and it is the reader's cue that the row is
		// actionable rather than historical.
		"referenceStatus": true,
		"toUserID":        notification.ToEntityID,
		"fromUserID":      notification.FromEntityID,
		"content": bson.M{
			"headline": notification.Headline,
			"details":  notification.Details,
		},
		"date": bson.M{"date": pythonDateString(time.Now())},
		"type": notification.Type,
		// Personal, therefore unread. Django DERIVES this and ignores what the
		// caller passes; the only rows that start read are broadcasts to an
		// audience selector, which nothing here writes.
		"isRead": false,
	}
	// Written only when there is one, matching add_notification: an empty
	// embedded document is indistinguishable from "no target" to the reader,
	// which treats absent as "infer from type".
	if notification.TargetType != "" && notification.TargetID != "" {
		document["target"] = bson.M{
			"type":         notification.TargetType,
			"supportingID": notification.TargetID,
			"anchor":       notification.TargetAnchor,
		}
	}

	if _, err := deps.Mongo.Collection("notifications").InsertOne(ctx, document); err != nil {
		slog.Error("notification insert failed",
			"to", notification.ToEntityID, "type", notification.Type, "error", err)
		return false
	}

	// The frame Django publishes alongside every one of these: a status, a
	// sentence, and no subject at all. That shapelessness is deliberate on the
	// platform's side - clients respond by refetching the store - so there is
	// nothing here to enrich and nothing a consumer should try to parse out of
	// it.
	publishFrame(ctx, deps, notification.ToEntityID, "notifications", map[string]any{
		"status": true, "auth": true, "message": notification.Details, "result": "",
	})
	return true
}

// pythonDateString renders a timestamp the way Django's add_notification does:
// str(datetime.now().astimezone()), e.g. "2026-09-03 09:50:12.345678+08:00".
//
// Matched rather than improved because existing rows are in this format and
// the readers parse what they find. The one divergence is a whole-second
// timestamp, which Python prints without a fractional part and this always
// prints with six zeroes - a display string either way, and not one anything
// sorts on (the notification list orders by _id).
func pythonDateString(at time.Time) string {
	return at.Local().Format("2006-01-02 15:04:05.000000-07:00")
}

// uniqueNotificationID mints "NTF_" plus 20 digits, matching Django's
// generate_random_digit(20) - which draws from [10^19, 10^20), so the leading
// digit is never zero and the string is always exactly 20 characters.
//
// crypto/rand rather than math/rand, for the same reason message ids use it:
// these appear in payloads, and a predictable id is a free enumeration
// primitive.
func uniqueNotificationID(ctx context.Context, db *mongo.Database) (string, error) {
	for attempt := 0; attempt < 5; attempt++ {
		candidate, err := randomNotificationDigits()
		if err != nil {
			return "", err
		}
		id := "NTF_" + candidate
		count, err := db.Collection("notifications").CountDocuments(ctx,
			bson.M{"notificationID": id}, options.Count().SetLimit(1))
		if err != nil {
			return "", fmt.Errorf("notification id check: %w", err)
		}
		if count == 0 {
			return id, nil
		}
	}
	return "", fmt.Errorf("could not mint a unique notification id")
}

func randomNotificationDigits() (string, error) {
	// [10^19, 10^20) - the range generate_random_digit(20) draws from.
	low := new(big.Int).Exp(big.NewInt(10), big.NewInt(19), nil)
	span := new(big.Int).Mul(low, big.NewInt(9))
	offset, err := rand.Int(rand.Reader, span)
	if err != nil {
		return "", err
	}
	return offset.Add(offset, low).String(), nil
}
