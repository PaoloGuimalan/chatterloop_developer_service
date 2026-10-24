package platform

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostComment is one comment in a post's thread.
type PostComment struct {
	CommentID string `json:"comment_id"`
	// The top-level comment this one hangs under, empty when it IS top-level.
	// Threads are flattened to two levels, so this is the only nesting there
	// is - see CreateComment.
	ParentCommentID string `json:"parent_comment_id"`
	AuthorEntityID  string `json:"author_entity_id"`
	AuthorHandle    string `json:"author_handle"`
	Text            string `json:"text"`
	CreatedAt       int64  `json:"created_at"`
	IsAnonymous     bool   `json:"is_anonymous"`
}

// PostThread is a post and its comments - everything readable about the
// conversation happening under it.
type PostThread struct {
	PostID         string        `json:"post_id"`
	AuthorEntityID string        `json:"author_entity_id"`
	AuthorHandle   string        `json:"author_handle"`
	Caption        string        `json:"caption"`
	CreatedAt      int64         `json:"created_at"`
	Comments       []PostComment `json:"comments"`
	IsAnonymous    bool          `json:"is_anonymous"`
	IsOwner        bool          `json:"is_owner"`
	Community      *Community    `json:"community,omitempty"`
}

// LoadPostThread returns a post and the newest `limit` comments on it, OLDEST
// FIRST so the slice reads as a thread.
//
// # WHY THIS EXISTS
//
// A bot answering a comment had nothing to answer FROM. The conversation
// surface has GET /v1/conversations/{id}/messages, and the bot indexes that
// window before retrieving - so a message reply is grounded in the chat. The
// post surface had no equivalent, so `post:<id>` was a permanently empty
// tenant and every comment answer came back "I don't have any context for that
// yet", no matter which model generated it.
//
// That is not a retrieval-tuning problem. There was no read.
//
// # THE CAPTION IS PART OF THE ANSWER
//
// Returned alongside the comments, and not as a nicety: a comment thread is
// *about* the post, and a thread read without it is a conversation with the
// first turn deleted. "Does this still apply?" is unanswerable without knowing
// what "this" was.
//
// # ACCESS, AND ONE HONEST DIVERGENCE
//
// Django serves a post's comments to ANYONE - CommentsView.get() is AllowAny
// and strips authentication entirely for guests - and applies no post-privacy
// filter while doing so. Requiring a valid entity_token here is therefore
// already stricter than the platform, and this function deliberately does not
// invent a visibility rule the platform does not have: reproducing
// post_visibility.py as a fourth implementation is exactly the drift risk this
// service avoids elsewhere.
//
// If the platform ever gates comment reads, this route must adopt that rule
// rather than keep its own.
func LoadPostThread(
	ctx context.Context,
	pool *pgxpool.Pool,
	postID string,
	limit int64,
) (*PostThread, error) {
	return LoadPostThreadForViewer(ctx, pool, postID, limit, "")
}

// LoadPostThreadForViewer applies the live post audience, including membership
// for private communities. The old wrapper cannot read community posts.
func LoadPostThreadForViewer(ctx context.Context, pool *pgxpool.Pool, postID string, limit int64, viewer string) (*PostThread, error) {
	if postID == "" {
		return nil, ErrPostNotFound
	}
	access, err := loadPostAccess(ctx, pool, postID, viewer)
	if err != nil {
		return nil, err
	}
	if !access.permits(viewer, false) {
		return nil, ErrPostNotFound
	}
	if limit < 1 {
		limit = 1
	}
	if limit > 200 {
		limit = 200
	}

	var (
		authorEntityID string
		caption        string
		datePosted     time.Time
		isAnonymous    bool
	)
	err = pool.QueryRow(ctx, `
		SELECT entity_id, COALESCE(caption, ''), date_posted, is_anonymous
		  FROM newsfeed_post
		 WHERE post_id = $1 AND deleted_at IS NULL`, postID,
	).Scan(&authorEntityID, &caption, &datePosted, &isAnonymous)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, ErrPostNotFound
	case err != nil:
		return nil, err
	}

	thread := &PostThread{
		PostID:         postID,
		AuthorEntityID: authorEntityID,
		Caption:        caption,
		CreatedAt:      datePosted.UnixMilli(),
		Comments:       []PostComment{},
		IsAnonymous:    isAnonymous,
		IsOwner:        authorEntityID == viewer,
		Community:      access.Community,
	}

	// Newest first so the limit takes the LATEST window, reversed below - the
	// same convention as RecentMessages, so a consumer indexing both surfaces
	// does not have to remember which way each one runs.
	rows, err := pool.Query(ctx, `
		SELECT comment_id, parent_comment_id, entity_id,
		       COALESCE(text, ''), created_at, is_anonymous
		  FROM newsfeed_comment
		 WHERE post_id = $1 AND deleted_at IS NULL
		 ORDER BY created_at DESC, comment_id DESC
		 LIMIT $2`, postID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	collected := make([]PostComment, 0, limit)
	for rows.Next() {
		var (
			comment   PostComment
			parent    *string
			createdAt time.Time
		)
		if err := rows.Scan(&comment.CommentID, &parent, &comment.AuthorEntityID,
			&comment.Text, &createdAt, &comment.IsAnonymous); err != nil {
			return nil, err
		}
		if parent != nil {
			comment.ParentCommentID = *parent
		}
		comment.CreatedAt = createdAt.UnixMilli()
		if comment.IsAnonymous {
			comment.AuthorEntityID = ""
		}
		collected = append(collected, comment)
	}
	if thread.IsAnonymous {
		thread.AuthorEntityID = ""
		authorEntityID = ""
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := len(collected) - 1; i >= 0; i-- {
		thread.Comments = append(thread.Comments, collected[i])
	}

	// Handles for the post's author and every commenter, in one query - the
	// same reason RecentMessages resolves them server-side. A transcript of
	// anonymous turns is materially worse input for anything reading it.
	entityIDs := make([]string, 0, len(thread.Comments)+1)
	entityIDs = append(entityIDs, authorEntityID)
	for _, comment := range thread.Comments {
		entityIDs = append(entityIDs, comment.AuthorEntityID)
	}
	handles, err := HandlesFor(ctx, pool, entityIDs)
	if err != nil {
		// Cosmetic: a thread without handles is still a thread, and nothing
		// downstream decides anything on one.
		return thread, nil
	}
	thread.AuthorHandle = handles[authorEntityID]
	for i := range thread.Comments {
		thread.Comments[i].AuthorHandle = handles[thread.Comments[i].AuthorEntityID]
	}
	return thread, nil
}
