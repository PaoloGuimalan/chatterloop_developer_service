package platform

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Comment activity kinds. Both arrive as unread notifications addressed to one
// entity, and a consumer usually treats them differently - a mention is an
// invitation to speak, a reply is a conversation already in progress - so the
// kind travels with the row rather than being inferred from which route
// returned it.
const (
	KindMention = "mention"
	KindReply   = "reply"
)

type Mention struct {
	NotificationID string `json:"notification_id"`
	CommentID      string `json:"comment_id"`
	PostID         string `json:"post_id"`
	AuthorEntityID string `json:"author_entity_id"`
	Text           string `json:"text"`
	AuthorHandle   string `json:"author_handle"`
	// "mention" or "reply". See the constants above.
	Kind string `json:"kind"`

	// WHEN THE NOTIFICATION WAS WRITTEN, epoch millis.
	//
	// Taken from the document's ObjectId, which encodes its insertion time to
	// the second - not from the embedded `date.date`, which is a formatted
	// Python string whose exact shape has changed over the life of the
	// collection. The _id is also what this endpoint already sorts on, so age
	// and order agree by construction.
	//
	// This exists because notifications are DURABLE. Unlike a realtime frame,
	// which is gone if nobody was listening, an unread notification waits - so
	// a consumer that answers everything it finds will, on its next start,
	// answer everything that accumulated while it was down. Without a
	// timestamp there is no way for it to tell that backlog from live traffic.
	CreatedAt int64 `json:"created_at"`
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
	return commentActivity(ctx, db, pool, entityID, limit, KindMention)
}

// CommentReplies returns unread notifications saying somebody REPLIED to a
// comment this entity wrote.
//
// # HOW A REPLY IS TOLD APART FROM A COMMENT ON YOUR POST
//
// Django writes both branches of CommentsView.post() as type `post_comment`:
// "somebody commented on your post" goes to the post's author, "somebody
// replied to your comment" goes to the replied-to comment's author. Only the
// second is a reply, and only the second should make a bot speak - answering
// the first would mean answering every comment on every post the bot ever
// made.
//
// The two are separated STRUCTURALLY, on the referenced comment's own row:
//
//	parent_comment_id IS NULL  -> a top-level comment -> "commented on your post"
//	parent_comment_id NOT NULL -> a reply             -> "replied to your comment"
//
// and not on content_headline, which is a display string. Those two agree
// today; the column cannot drift, because a reply that stored no parent would
// not render as a reply either.
//
// # WHAT THE RECIPIENT ALREADY PROVES
//
// Django addresses the reply notification to `replied_to.entity` - the author
// of the comment being answered. So a row reaching this function is already
// "somebody replied to something YOU wrote"; nothing here has to re-derive it,
// and nothing a caller passes can widen it.
//
// Note the platform's own gap, which is not this service's to close: that
// branch is skipped when the replier is the post's author (`post.entity !=
// entity` is part of the condition), so a post owner replying to a comment on
// their own post notifies nobody and is invisible here.
//
// # THE LIMIT BOUNDS NOTIFICATIONS, NOT REPLIES
//
// Both branches share one notification type, so `limit` is spent on the newest
// `post_comment` rows and only then filtered. On a busy post owned by the
// caller, "commented on your post" rows can therefore crowd replies out of the
// window. Newest-first ordering means what falls off is the oldest, so raising
// `limit` is the whole remedy; splitting the two apart would need a new
// notification type, which is Django's to add.
func CommentReplies(
	ctx context.Context,
	db *mongo.Database,
	pool *pgxpool.Pool,
	entityID string,
	limit int64,
) ([]Mention, error) {
	return commentActivity(ctx, db, pool, entityID, limit, KindReply)
}

// commentActivity is the shared body of the two routes above: the same Mongo
// read, the same Postgres join, differing only in the notification type asked
// for and in whether a reply's parent is required.
func commentActivity(
	ctx context.Context,
	db *mongo.Database,
	pool *pgxpool.Pool,
	entityID string,
	limit int64,
	kind string,
) ([]Mention, error) {
	notificationType := "comment_mention"
	if kind == KindReply {
		notificationType = "post_comment"
	}

	cursor, err := db.Collection("notifications").Find(ctx,
		bson.M{
			"toUserID": entityID,
			"type":     notificationType,
			"isRead":   false,
		},
		options.Find().
			// _id is KEPT, unlike every other read in this service: it carries
			// the insertion timestamp that CreatedAt is derived from.
			SetProjection(bson.M{
				"_id": 1, "notificationID": 1, "referenceID": 1,
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
		// referenceID is the BACKEND id and for both of these types it is the
		// comment. The post to open lives on the client-facing target - see the
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

		createdAt := int64(0)
		if objectID, ok := raw["_id"].(primitive.ObjectID); ok {
			createdAt = objectID.Timestamp().UnixMilli()
		}

		pending = append(pending, Mention{
			NotificationID: notificationID,
			CommentID:      commentID,
			PostID:         postID,
			AuthorEntityID: authorID,
			Kind:           kind,
			CreatedAt:      createdAt,
		})
		commentIDs = append(commentIDs, commentID)
		if authorID != "" {
			authorIDs = append(authorIDs, authorID)
		}
	}

	if len(pending) == 0 {
		return []Mention{}, nil
	}

	comments, err := commentRows(ctx, pool, commentIDs)
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
		comment, found := comments[mention.CommentID]
		if !found || comment.text == "" {
			continue
		}
		// The structural reply test. A `post_comment` notification whose
		// comment is top-level is "somebody commented on your post", which is
		// not an answer to anything this entity said.
		if kind == KindReply && !comment.hasParent {
			continue
		}
		mention.Text = comment.text
		mention.AuthorHandle = handles[mention.AuthorEntityID]
		resolved = append(resolved, mention)
	}
	return resolved, nil
}

// commentRow is the part of newsfeed_comment these routes need: the words, and
// whether the row is a reply at all.
type commentRow struct {
	text      string
	hasParent bool
}

func commentRows(ctx context.Context, pool *pgxpool.Pool, commentIDs []string) (map[string]commentRow, error) {
	unique := dedupe(commentIDs)
	if len(unique) == 0 {
		return map[string]commentRow{}, nil
	}
	rows, err := pool.Query(ctx, `
		SELECT comment_id, COALESCE(text, ''), parent_comment_id IS NOT NULL
		  FROM newsfeed_comment
		 WHERE comment_id = ANY($1)
		   AND deleted_at IS NULL`, unique)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	comments := make(map[string]commentRow, len(unique))
	for rows.Next() {
		var id, text string
		var hasParent bool
		if err := rows.Scan(&id, &text, &hasParent); err != nil {
			return nil, err
		}
		comments[id] = commentRow{text: text, hasParent: hasParent}
	}
	return comments, rows.Err()
}
