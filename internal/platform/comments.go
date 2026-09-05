package platform

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"developer_service/internal/queue"

	"github.com/jackc/pgx/v5"
)

var (
	// ErrPostNotFound covers a post id that does not exist and one that has
	// been soft-deleted. Same reasoning as ErrNotAParticipant: a caller who
	// could tell them apart could enumerate post ids.
	ErrPostNotFound = errors.New("post not found")

	// ErrParentCommentNotFound covers a parent that does not exist, has been
	// soft-deleted, or lives on a different post.
	ErrParentCommentNotFound = errors.New("comment not found")

	// ErrEmptyComment is the comment equivalent of ErrEmptyContent.
	ErrEmptyComment = errors.New("refusing to post an empty comment")
)

type CommentRequest struct {
	PostID string
	// The comment being replied to, or empty for a top-level comment. This is
	// what the author AIMED at; where the row is stored may differ - see the
	// flattening note on CreateComment.
	ParentID string
	Text     string
}

type CommentResult struct {
	CommentID string `json:"comment_id"`
	PostID    string `json:"post_id"`
	// Where the row was actually stored. Differs from RepliedTo when replying
	// to a reply, and returning it is what lets a caller see that happen
	// rather than discover it later from a thread that reads oddly.
	ParentCommentID string `json:"parent_comment_id"`
	RepliedTo       string `json:"replied_to"`
	// Entities told about this comment: the person replied to or the post's
	// author, plus anyone @mentioned.
	Notified []string `json:"notified"`
}

// commentAuthorPreview is how much of the replied-to comment Django quotes back
// in the notification sentence.
const commentAuthorPreview = 30

// CreateComment writes a comment and performs the fan-out that makes it visible.
//
// # THREADS ARE FLATTENED TO TWO LEVELS, AND THAT IS NOT COSMETIC
//
// Django's CommentsView.post() separates two ideas that a naive implementation
// would conflate:
//
//	replied_to      what the author aimed at
//	parent_comment  where the row is stored = replied_to.parent_comment OR replied_to
//
// Replying to a REPLY re-parents to that reply's top-level ancestor rather than
// nesting a third time. The thread then stays one paginated list per top-level
// comment, and a soft-deleted middle comment cannot strand grandchildren with
// no reachable parent. Getting this wrong produces rows the reader cannot
// paginate, so it is reproduced exactly.
//
// The person actually replied to is still notified - that comes off
// `replied_to`, not off where the row landed.
//
// # WHAT ELSE HAPPENS, AND WHAT DELIBERATELY DOES NOT
//
// Performed here, because a comment without them is a row rather than a
// comment:
//
//  1. the insert
//  2. the post_activity frame on the POST's channel, which is what makes the
//     comment appear in a comment section somebody already has open - see
//     post_activity.go for why a notification cannot do that job
//  3. update_ranking_score - the worker owns comments_count
//  4. bump_interest_affinity - carrying the POST's interest ids
//  5. the reply / post-comment notification, with its own per-entity frame
//  6. comment-mention notifications, one per entity named
//
// NOT performed, each for a stated reason:
//
//   - CONTENT TAGGING. Django gates it on a Redis presence key and publishes
//     nothing when the moderation service is down, because that service's
//     database scour picks the row up on its next start - the designed path,
//     not a degraded one. The message route leans on the same path for the same
//     reason.
//
//   - HASHTAG -> INTEREST LINKING. This one is a real gap, not a shared path: a
//     comment's hashtags are linked to its parent post by Django alone, and the
//     moderation sink's LINKABLE set covers posts only. Reproducing it means
//     WIDENING interests_interest from a fifth implementation of a normaliser
//     whose own file says four already have to agree, and whose failure mode is
//     silent - a second interest row for a tag that already exists. A missing
//     link is recoverable; a duplicated taxonomy is not. So a "#tag" in a
//     comment posted through this API does not tag the post. It is in the
//     README rather than papered over.
func CreateComment(ctx context.Context, deps Deps, authorEntityID string, req CommentRequest) (*CommentResult, error) {
	// Sanitised, unlike Django, which stores what the client sent. That is safe
	// there because every client escapes before posting; an API caller has no
	// such client, so the same markup a browser would have neutered would go in
	// raw. Stripping tags can only ever remove capability, so this diverges in
	// the direction that cannot bite.
	text := SanitizeForStorage(strings.TrimSpace(req.Text))
	if text == "" {
		return nil, ErrEmptyComment
	}

	post, err := loadPost(ctx, deps, req.PostID)
	if err != nil {
		return nil, err
	}

	var repliedTo *parentComment
	if req.ParentID != "" {
		repliedTo, err = loadParentComment(ctx, deps, req.ParentID, req.PostID)
		if err != nil {
			return nil, err
		}
	}

	storedParentID := storedParentFor(repliedTo)

	commentID, err := newCommentID()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()

	if _, err := deps.Postgres.Exec(ctx, `
		INSERT INTO newsfeed_comment
		       (comment_id, parent_comment_id, post_id, text, attachment,
		        entity_id, created_at, updated_at, deleted_by_id, deleted_at)
		VALUES ($1, NULLIF($2, ''), $3, $4, NULL, $5, $6, NULL, NULL, NULL)`,
		commentID, storedParentID, req.PostID, text, authorEntityID, now,
	); err != nil {
		return nil, fmt.Errorf("comment insert: %w", err)
	}

	// Everything past this point is fan-out. The comment exists; a failure here
	// degrades how it is delivered and must not be reported as a failure to
	// post.
	notified := commentFanOut(ctx, deps, authorEntityID, req, commentID, text, post, repliedTo)

	return &CommentResult{
		CommentID:       commentID,
		PostID:          req.PostID,
		ParentCommentID: storedParentID,
		RepliedTo:       req.ParentID,
		Notified:        notified,
	}, nil
}

// storedParentFor is the flattening rule: WHERE a comment is stored, given
// what it was aimed at.
//
// A reply to a reply re-parents to that reply's top-level ancestor; a reply to
// a top-level comment parents to it; a top-level comment has no parent. Its own
// function because it is the one decision that shapes the thread, and getting
// it wrong produces rows the reader cannot paginate rather than an error.
func storedParentFor(repliedTo *parentComment) string {
	if repliedTo == nil {
		return ""
	}
	if repliedTo.ParentCommentID != "" {
		return repliedTo.ParentCommentID
	}
	return repliedTo.CommentID
}

type postRow struct {
	PostID   string
	EntityID string
}

type parentComment struct {
	CommentID string
	EntityID  string
	// The top-level ancestor, empty when this comment IS top-level.
	ParentCommentID string
	Text            string
}

func loadPost(ctx context.Context, deps Deps, postID string) (*postRow, error) {
	if postID == "" {
		return nil, ErrPostNotFound
	}
	var entityID string
	err := deps.Postgres.QueryRow(ctx, `
		SELECT entity_id
		  FROM newsfeed_post
		 WHERE post_id = $1 AND deleted_at IS NULL`, postID).Scan(&entityID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// Django's own view does NOT filter deleted_at here and would happily
		// comment on a removed post. Refusing is a divergence that can only
		// prevent nonsense, so it is one worth having.
		return nil, ErrPostNotFound
	case err != nil:
		return nil, err
	}
	return &postRow{PostID: postID, EntityID: entityID}, nil
}

func loadParentComment(ctx context.Context, deps Deps, parentID, postID string) (*parentComment, error) {
	var (
		entityID string
		ancestor *string
		text     *string
		onPost   string
	)
	err := deps.Postgres.QueryRow(ctx, `
		SELECT entity_id, parent_comment_id, text, post_id
		  FROM newsfeed_comment
		 WHERE comment_id = $1 AND deleted_at IS NULL`, parentID,
	).Scan(&entityID, &ancestor, &text, &onPost)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, ErrParentCommentNotFound
	case err != nil:
		return nil, err
	}

	// Django does not check this and would re-parent a comment onto a thread
	// belonging to a different post, producing a row neither post can render
	// coherently. Refused here.
	if onPost != postID {
		return nil, ErrParentCommentNotFound
	}

	parent := &parentComment{CommentID: parentID, EntityID: entityID}
	if ancestor != nil {
		parent.ParentCommentID = *ancestor
	}
	if text != nil {
		parent.Text = *text
	}
	return parent, nil
}

func commentFanOut(
	ctx context.Context,
	deps Deps,
	authorEntityID string,
	req CommentRequest,
	commentID, text string,
	post *postRow,
	repliedTo *parentComment,
) []string {
	// FIRST, and matching the order Django publishes in - but the reason is
	// its own: this is the only part of the fan-out somebody is WAITING on.
	// The queues below feed workers, the notifications below feed trays; this
	// feeds a comment section that is open right now, with a person watching
	// it. Everything after it can be a second late without anyone noticing.
	//
	// storedParentFor is called again rather than threaded through: it is the
	// same pure function that decided where the row went, so the two cannot
	// disagree, and `parent_id` must name where the row LANDED rather than
	// what the author aimed at - see commentActivityFields.
	publishCommentCreated(ctx, deps, req.PostID, commentID,
		storedParentFor(repliedTo), authorEntityID)

	deps.Queue.Publish(ctx, queue.UpdateRankingScore, queue.RankingPayload{
		PostID: req.PostID, UpdateType: "comment", IsDecrease: false,
	})

	// The post's interests as they stood BEFORE this comment, matching the
	// order Django publishes in: a hashtag introduced by this very comment
	// should not also bump its author for having engaged with the topic they
	// just invented.
	if interestIDs, err := postInterestIDs(ctx, deps, req.PostID); err != nil {
		slog.Error("could not read post interests", "post_id", req.PostID, "error", err)
	} else {
		deps.Queue.Publish(ctx, queue.BumpInterestAffinity, queue.InterestAffinityPayload{
			EntityID: authorEntityID, InterestIDs: interestIDs,
			Action: "COMMENT", IsDecrease: false,
		})
	}

	authorHandle := "@" + handleOr(ctx, deps, authorEntityID)

	// Entities already pinged for THIS comment, so a mention does not arrive as
	// a second notification for the same event.
	notified := []string{}

	switch {
	case repliedTo != nil:
		// Django's exact condition, including the second clause: the reply
		// notification is skipped when the replier is the POST's author. That
		// means a post owner replying to a comment on their own post notifies
		// nobody. It reads like an oversight and it may be one, but changing it
		// here would make two services disagree about whether a notification
		// exists, so it is reproduced and documented instead.
		if repliedTo.EntityID != authorEntityID && post.EntityID != authorEntityID {
			details := fmt.Sprintf("%s replied to your comment %q",
				authorHandle, truncateForPreview(repliedTo.Text))
			if WriteNotification(ctx, deps, NewNotification{
				ToEntityID: repliedTo.EntityID, FromEntityID: authorEntityID,
				ReferenceID: commentID,
				Type:        NotificationPostComment, Headline: HeadlineRepliedComment,
				Details: details,
				// Opens the post at the REPLY, which is the new thing to read.
				TargetType: "post", TargetID: req.PostID, TargetAnchor: commentID,
			}) {
				notified = append(notified, repliedTo.EntityID)
			}
		}
	default:
		if post.EntityID != authorEntityID {
			details := authorHandle + " commented on your post."
			if WriteNotification(ctx, deps, NewNotification{
				ToEntityID: post.EntityID, FromEntityID: authorEntityID,
				ReferenceID: commentID,
				Type:        NotificationPostComment, Headline: HeadlinePostComment,
				Details:    details,
				TargetType: "post", TargetID: req.PostID, TargetAnchor: commentID,
			}) {
				notified = append(notified, post.EntityID)
			}
		}
	}

	notified = append(notified,
		notifyCommentMentions(ctx, deps, authorEntityID, authorHandle,
			commentID, req.PostID, text, notified)...)
	return notified
}

// notifyCommentMentions pings everyone the comment names who has not already
// been pinged for it.
//
// A mention IS the text - nothing about the parse is persisted, and the client
// highlights handles at render time. Parsing happens on write purely to send
// these, which is the one thing a comment does not get for free the way a
// message does: a conversation's participants are already there to notify,
// while somebody mentioned in a comment may have nothing to do with the post.
func notifyCommentMentions(
	ctx context.Context,
	deps Deps,
	authorEntityID, authorHandle string,
	commentID, postID, text string,
	alreadyNotified []string,
) []string {
	mentioned, err := ResolveMentions(ctx, deps.Postgres, text, authorEntityID)
	if err != nil {
		slog.Error("could not resolve comment mentions", "comment_id", commentID, "error", err)
		return nil
	}
	if len(mentioned) == 0 {
		return nil
	}

	skip := map[string]bool{authorEntityID: true}
	for _, entityID := range alreadyNotified {
		skip[entityID] = true
	}

	details := authorHandle + " mentioned you in a comment."
	notified := make([]string, 0, len(mentioned))
	for _, entity := range mentioned {
		if skip[entity.EntityID] {
			continue
		}
		skip[entity.EntityID] = true
		if WriteNotification(ctx, deps, NewNotification{
			ToEntityID: entity.EntityID, FromEntityID: authorEntityID,
			ReferenceID: commentID,
			Type:        NotificationCommentMention, Headline: HeadlineCommentMention,
			Details: details,
			// The post the comment lives on, anchored at the comment doing the
			// mentioning.
			TargetType: "post", TargetID: postID, TargetAnchor: commentID,
		}) {
			notified = append(notified, entity.EntityID)
		}
	}
	return notified
}

func postInterestIDs(ctx context.Context, deps Deps, postID string) ([]int64, error) {
	rows, err := deps.Postgres.Query(ctx, `
		SELECT interest_id FROM interests_postinterestlink WHERE post_id = $1`, postID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Non-nil so the payload carries [] rather than null; the worker returns
	// early on an empty list either way, but null is a shape nothing else on
	// this queue produces.
	interestIDs := []int64{}
	for rows.Next() {
		var interestID int64
		if err := rows.Scan(&interestID); err != nil {
			return nil, err
		}
		interestIDs = append(interestIDs, interestID)
	}
	return interestIDs, rows.Err()
}

// handleOr resolves one entity's @handle, falling back to its id.
//
// The fallback mirrors get_entity_display_username(), which returns str(entity.
// id) for an entity backing none of the three concrete kinds. An id in a
// notification sentence reads badly, but it reads better than an empty "@ "
// and it is what the platform already does.
func handleOr(ctx context.Context, deps Deps, entityID string) string {
	handles, err := HandlesFor(ctx, deps.Postgres, []string{entityID})
	if err != nil {
		slog.Warn("could not resolve author handle", "entity_id", entityID, "error", err)
		return entityID
	}
	if handle := handles[entityID]; handle != "" {
		return handle
	}
	return entityID
}

// newCommentID mints a random UUID in the canonical hyphenated lowercase form,
// which is what Django's `default=uuid.uuid4` stores once the CharField
// stringifies it.
//
// Hand-rolled rather than adding a UUID module: this service's dependency list
// is deliberately short, and version 4 is 16 random bytes with six bits fixed.
// There is no clever part to get wrong, and a dependency that is present is a
// dependency someone can reach for.
func newCommentID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("comment id: %w", err)
	}
	bytes[6] = (bytes[6] & 0x0f) | 0x40 // version 4
	bytes[8] = (bytes[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x",
		bytes[0:4], bytes[4:6], bytes[6:8], bytes[8:10], bytes[10:16]), nil
}

// truncateForPreview quotes back the first 30 CHARACTERS of the comment being
// replied to, matching Django's parent_text[:30] + "...".
//
// Runes, not bytes: Python slices characters, so a byte slice would cut a
// multi-byte character in half and put a replacement glyph in somebody's
// notification tray.
func truncateForPreview(text string) string {
	runes := []rune(text)
	if len(runes) <= commentAuthorPreview {
		return text
	}
	return string(runes[:commentAuthorPreview]) + "..."
}
