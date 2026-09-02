package platform

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Mention struct {
	NotificationID string `json:"notification_id"`
	CommentID      string `json:"comment_id"`
	PostID         string `json:"post_id"`
	AuthorEntityID string `json:"author_entity_id"`
	Text           string `json:"text"`
	AuthorHandle   string `json:"author_handle"`
}

// CommentMentions returns unread `comment_mention` notifications addressed to
// one entity, with the comment text already resolved.
//
// Resolving the text here is the point. A mention arrives as a Mongo
// notification but the comment's words live in Postgres (newsfeed_comment), so
// a consumer doing this itself needs both stores - which is exactly the direct
// database access this API exists to remove.
//
// This endpoint does NOT mark anything read. `isRead` is the owning entity's
// UI state, and a machine silently consuming it would change what a human sees
// in their own notification tray. Deduplication is the consumer's job, keyed on
// comment id.
func CommentMentions(
	ctx context.Context,
	db *mongo.Database,
	pool *pgxpool.Pool,
	entityID string,
	limit int64,
) ([]Mention, error) {
	cursor, err := db.Collection("notifications").Find(ctx,
		bson.M{
			"toUserID": entityID,
			"type":     "comment_mention",
			"isRead":   false,
		},
		options.Find().
			SetProjection(bson.M{
				"_id": 0, "notificationID": 1, "referenceID": 1,
				"fromUserID": 1, "target": 1,
			}).
			SetSort(bson.D{{Key: "_id", Value: -1}}).
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

	pending := make([]Mention, 0, len(raws))
	commentIDs := make([]string, 0, len(raws))
	authorIDs := make([]string, 0, len(raws))

	for _, raw := range raws {
		// referenceID is the BACKEND id and for this type it is the comment.
		// The post to open lives on the client-facing target - see the
		// NotificationTarget docs, which exist because routing off referenceID
		// was wrong for other notification types.
		commentID, _ := raw["referenceID"].(string)
		if commentID == "" {
			continue
		}
		notificationID, _ := raw["notificationID"].(string)
		authorID, _ := raw["fromUserID"].(string)

		postID := ""
		if target, ok := raw["target"].(bson.M); ok {
			postID, _ = target["supportingID"].(string)
		}

		pending = append(pending, Mention{
			NotificationID: notificationID,
			CommentID:      commentID,
			PostID:         postID,
			AuthorEntityID: authorID,
		})
		commentIDs = append(commentIDs, commentID)
		if authorID != "" {
			authorIDs = append(authorIDs, authorID)
		}
	}

	if len(pending) == 0 {
		return []Mention{}, nil
	}

	texts, err := commentTexts(ctx, pool, commentIDs)
	if err != nil {
		return nil, err
	}
	handles, err := HandlesFor(ctx, pool, authorIDs)
	if err != nil {
		// Cosmetic enrichment only: never worth failing a fetch over.
		handles = map[string]string{}
	}

	// A mention whose comment has since been deleted has no text and is not
	// answerable. Dropped here so an unanswerable item never reaches a consumer
	// that would have to invent the same rule.
	resolved := make([]Mention, 0, len(pending))
	for _, mention := range pending {
		text := texts[mention.CommentID]
		if text == "" {
			continue
		}
		mention.Text = text
		mention.AuthorHandle = handles[mention.AuthorEntityID]
		resolved = append(resolved, mention)
	}
	return resolved, nil
}

func commentTexts(ctx context.Context, pool *pgxpool.Pool, commentIDs []string) (map[string]string, error) {
	unique := dedupe(commentIDs)
	if len(unique) == 0 {
		return map[string]string{}, nil
	}
	rows, err := pool.Query(ctx, `
		SELECT comment_id, COALESCE(text, '')
		  FROM newsfeed_comment
		 WHERE comment_id = ANY($1)
		   AND deleted_at IS NULL`, unique)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	texts := make(map[string]string, len(unique))
	for rows.Next() {
		var id, text string
		if err := rows.Scan(&id, &text); err != nil {
			return nil, err
		}
		texts[id] = text
	}
	return texts, rows.Err()
}
