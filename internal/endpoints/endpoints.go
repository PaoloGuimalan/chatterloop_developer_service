// Package endpoints holds the HTTP surface.
//
// Everything is versioned in the path from the first commit: the consumer is a
// deployed service, not a browser that reloads, so when a shape has to change
// both versions need to be live at once for as long as it takes to roll
// clients forward. That discipline is why the reply routes were added BESIDE
// the read routes they resemble rather than as flags on them.
//
// The write routes - /v1/messages/send and /v1/comments - each reproduce a
// fan-out that lives in another service, and each documents what it does not
// reproduce. See platform/send.go and platform/comments.go; the gaps are named
// there rather than discovered later.
package endpoints

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"developer_service/internal/auth"
	"developer_service/internal/config"
	"developer_service/internal/connections"
	"developer_service/internal/platform"
	"developer_service/internal/queue"
	"developer_service/internal/stream"
)

type Handlers struct {
	Conns *connections.Pool
	Cfg   *config.Config
	Queue *queue.Publisher
}

// Health is unauthenticated on purpose: it is for the orchestrator, which has
// no credential and should not need one to learn whether to restart this pod.
// It reports only liveness - no version, no dependency detail, nothing worth
// scraping.
func (h *Handlers) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": true})
}

// Ready reports whether the dependencies are actually reachable, which is a
// different question from Health and deserves a different endpoint: a pod that
// cannot reach Redis should stop receiving traffic without being killed and
// restarted into the same failure.
func (h *Handlers) Ready(w http.ResponseWriter, r *http.Request) {
	if err := h.Conns.Redis.Ping(r.Context()).Err(); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": false, "dependency": "redis",
		})
		return
	}
	if err := h.Conns.Postgres.Ping(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": false, "dependency": "postgres",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": true})
}

// WhoAmI describes the calling credential.
//
// No scope of its own: a credential may always describe itself, and requiring
// a scope to discover your scopes is a bootstrapping problem with no upside.
// It returns no secret - the prefix is public by construction, since it
// travels in the clear inside every token.
func (h *Handlers) WhoAmI(w http.ResponseWriter, r *http.Request) {
	token, ok := auth.FromContext(r.Context())
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"status": false})
		return
	}
	// The handle is resolved rather than stored on the token, and it is worth
	// the query: a consumer that has to be TOLD its own @handle in
	// configuration can be told the wrong one, and the failure is silent -
	// mention matching just never fires, with nothing in any log to say why.
	// Answering it here means a client can verify what it was configured with
	// instead of trusting it.
	handle := ""
	if handles, err := platform.HandlesFor(r.Context(), h.Conns.Postgres,
		[]string{token.EntityID}); err == nil {
		handle = handles[token.EntityID]
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":    true,
		"entity_id": token.EntityID,
		"handle":    handle,
		"realm_id":  token.RealmID,
		"scopes":    token.Scopes,
		"token": map[string]any{
			"id":              token.ID,
			"name":            token.Name,
			"rate_limit_int":  token.RateLimitInt,
			"rate_limit_type": token.RateLimitType,
		},
	})
}

// PostComments serves a post and the newest comments on it, oldest first.
//
// The read a bot answering a comment needs and did not have. Without it the
// tenant `post:<id>` is permanently empty and every comment answer is
// ungrounded - which reads exactly like a broken model and is in fact a
// missing endpoint.
//
// The caption comes back with the comments deliberately; see LoadPostThread.
func (h *Handlers) PostComments(w http.ResponseWriter, r *http.Request) {
	if _, ok := auth.FromContext(r.Context()); !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"status": false})
		return
	}
	postID := r.PathValue("postID")
	limit := readLimit(r, 50, 200)

	thread, err := platform.LoadPostThread(r.Context(), h.Conns.Postgres, postID, limit)
	if err != nil {
		if errors.Is(err, platform.ErrPostNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]any{
				"status": false, "message": "Post not found.",
			})
			return
		}
		slog.Error("post thread read failed", "post_id", postID, "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"status": false})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":           true,
		"post_id":          thread.PostID,
		"author_entity_id": thread.AuthorEntityID,
		"author_handle":    thread.AuthorHandle,
		"caption":          thread.Caption,
		"created_at":       thread.CreatedAt,
		"count":            len(thread.Comments),
		"comments":         thread.Comments,
	})
}

// Events streams the calling entity's realtime frames.
//
// The entity is taken from the TOKEN, never from the request. There is no
// version of this that accepts an entity id parameter, because there is no
// legitimate caller for one - and a parameter that exists is a parameter
// someone will eventually pass someone else's value to.
func (h *Handlers) Events(w http.ResponseWriter, r *http.Request) {
	token, ok := auth.FromContext(r.Context())
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"status": false})
		return
	}

	stream.Serve(r.Context(), w, h.Conns.Redis, token.EntityID, stream.Options{
		Heartbeat:   h.Cfg.Heartbeat,
		MaxLifetime: h.Cfg.MaxStreamLifetime,
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// --------------------------------------------------------------- data ------

// ConversationMessages serves the newest messages in one conversation, oldest
// first so the array reads as a transcript.
func (h *Handlers) ConversationMessages(w http.ResponseWriter, r *http.Request) {
	token, ok := auth.FromContext(r.Context())
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"status": false})
		return
	}
	conversationID := r.PathValue("conversationID")
	limit := readLimit(r, 50, 200)

	conversation, err := platform.LoadConversation(r.Context(), h.Conns.Mongo, h.Conns.Postgres, conversationID, token.EntityID)
	if err != nil {
		if errors.Is(err, platform.ErrNotAParticipant) {
			// 404 rather than 403: a caller who is not a participant should
			// not be able to tell an existing conversation from one that never
			// existed.
			writeJSON(w, http.StatusNotFound, map[string]any{
				"status": false, "message": "Conversation not found.",
			})
			return
		}
		slog.Error("conversation lookup failed", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"status": false})
		return
	}

	messages, err := platform.RecentMessages(r.Context(), h.Conns.Mongo, conversationID, limit)
	if err != nil {
		slog.Error("history read failed", "conversation_id", conversationID, "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"status": false})
		return
	}
	h.resolveHandles(r, messages)

	writeJSON(w, http.StatusOK, map[string]any{
		"status":            true,
		"conversation_id":   conversationID,
		"conversation_type": conversation.ConversationType,
		"count":             len(messages),
		"messages":          messages,
	})
}

// ConversationReplies serves the messages in one conversation that reply to a
// message the CALLER wrote, oldest first.
//
// This exists because the realtime frame cannot answer the question. It says a
// message arrived, from whom, and whether it mentioned you - never what it
// says, never its id, and never what it replies to. A client that wants to
// know "was that a reply to me?" would otherwise have to pull a full history
// window on every message in every conversation it is a member of.
//
// Whose replies is taken from the TOKEN. There is no parameter for it, for the
// same reason /v1/mentions/comments takes no entity id.
func (h *Handlers) ConversationReplies(w http.ResponseWriter, r *http.Request) {
	token, ok := auth.FromContext(r.Context())
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"status": false})
		return
	}
	conversationID := r.PathValue("conversationID")
	// A smaller default than the history route on purpose: this is a probe run
	// on many frames, and the answer it usually returns is "none".
	limit := readLimit(r, 25, 100)

	// The same access RULE as the history route - AssertMember, which is also
	// what the send path uses - but not via LoadConversation, whose second
	// query fetches a conversation document this route has no use for. On a
	// path that runs per frame, a read that answers nothing is worth not
	// making. 404 rather than 403 for the same reason as everywhere else: a
	// caller who is not a participant should not be able to tell an existing
	// conversation from one that never existed.
	if err := platform.AssertMember(r.Context(), h.Conns.Mongo, h.Conns.Postgres, conversationID, token.EntityID); err != nil {
		if errors.Is(err, platform.ErrNoAccess) {
			writeJSON(w, http.StatusNotFound, map[string]any{
				"status": false, "message": "Conversation not found.",
			})
			return
		}
		slog.Error("membership check failed", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"status": false})
		return
	}

	replies, err := platform.RepliesTo(r.Context(), h.Conns.Mongo, conversationID, token.EntityID, limit)
	if err != nil {
		slog.Error("reply read failed", "conversation_id", conversationID, "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"status": false})
		return
	}
	h.resolveHandles(r, replies)

	writeJSON(w, http.StatusOK, map[string]any{
		"status":          true,
		"conversation_id": conversationID,
		"count":           len(replies),
		"messages":        replies,
	})
}

// resolveHandles fills SenderHandle and ReplyingToSenderHandle in ONE query
// for the whole slice, senders and replied-to authors together.
//
// Best-effort: a handle that cannot be resolved leaves the field empty rather
// than failing the read. Nothing decides anything on a handle - the entity ids
// alongside them are what callers compare - so an empty one costs legibility
// and nothing else.
func (h *Handlers) resolveHandles(r *http.Request, messages []platform.Message) {
	entityIDs := make([]string, 0, len(messages)*2)
	for _, message := range messages {
		entityIDs = append(entityIDs, message.SenderEntityID, message.ReplyingToSenderEntityID)
	}
	handles, err := platform.HandlesFor(r.Context(), h.Conns.Postgres, entityIDs)
	if err != nil {
		return
	}
	for i := range messages {
		messages[i].SenderHandle = handles[messages[i].SenderEntityID]
		messages[i].ReplyingToSenderHandle = handles[messages[i].ReplyingToSenderEntityID]
	}
}

// CommentMentions serves unread comment mentions addressed to the caller.
//
// Scoped to the token's entity and not parameterisable: there is no version of
// this that takes a `for_entity` argument, because there is no legitimate
// caller for one.
func (h *Handlers) CommentMentions(w http.ResponseWriter, r *http.Request) {
	token, ok := auth.FromContext(r.Context())
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"status": false})
		return
	}
	limit := readLimit(r, 25, 100)

	mentions, err := platform.CommentMentions(r.Context(), h.Conns.Mongo, h.Conns.Postgres, token.EntityID, limit)
	if err != nil {
		slog.Error("mention read failed", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"status": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": true, "count": len(mentions), "mentions": mentions,
	})
}

// CommentReplies serves unread notifications saying somebody replied to a
// comment the caller wrote.
//
// A SIBLING ROUTE RATHER THAN A FLAG ON THE ONE ABOVE. Both answer "what
// comment activity is addressed to me", but they are different notification
// types with different rules for what counts, and folding them together would
// have meant either changing what /v1/mentions/comments returns to an existing
// consumer, or a query parameter that changes the meaning of the result.
// Versioning discipline here is the same as everywhere else in this service:
// old shape untouched, new shape beside it.
func (h *Handlers) CommentReplies(w http.ResponseWriter, r *http.Request) {
	token, ok := auth.FromContext(r.Context())
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"status": false})
		return
	}
	limit := readLimit(r, 25, 100)

	replies, err := platform.CommentReplies(r.Context(), h.Conns.Mongo, h.Conns.Postgres, token.EntityID, limit)
	if err != nil {
		slog.Error("comment reply read failed", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"status": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": true, "count": len(replies), "replies": replies,
	})
}

type sendBody struct {
	ConversationID   string `json:"conversationID"`
	Content          string `json:"content"`
	ConversationType string `json:"conversationType"`
	ReplyingTo       string `json:"replyingTo"`
	MessageType      string `json:"messageType"`
	PendingID        string `json:"pendingID"`
}

// SendMessage writes a message as the calling entity.
func (h *Handlers) SendMessage(w http.ResponseWriter, r *http.Request) {
	token, ok := auth.FromContext(r.Context())
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"status": false})
		return
	}

	var body sendBody
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"status": false, "message": "Body must be JSON.",
		})
		return
	}
	if body.ConversationID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"status": false, "message": "conversationID is required.",
		})
		return
	}
	if len(body.Content) > maxContentLength {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"status":  false,
			"message": "content exceeds " + strconv.Itoa(maxContentLength) + " characters.",
		})
		return
	}

	result, err := platform.SendMessage(r.Context(), h.deps(), token.EntityID, platform.SendRequest{
		ConversationID:   body.ConversationID,
		Content:          body.Content,
		ConversationType: body.ConversationType,
		ReplyingTo:       body.ReplyingTo,
		MessageType:      body.MessageType,
		PendingID:        body.PendingID,
	})
	switch {
	case errors.Is(err, platform.ErrEmptyContent):
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"status": false, "message": "refusing to send an empty message.",
		})
		return
	case errors.Is(err, platform.ErrNoAccess):
		writeJSON(w, http.StatusNotFound, map[string]any{
			"status": false, "message": "Conversation not found.",
		})
		return
	case err != nil:
		slog.Error("send failed", "conversation_id", body.ConversationID, "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"status": false, "message": "Could not send the message.",
		})
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"status":     true,
		"message_id": result.MessageID,
		"pending_id": result.PendingID,
		"receivers":  result.Receivers,
	})
}

type commentBody struct {
	PostID string `json:"postID"`
	// The comment being replied to. Omit for a top-level comment. Named for
	// what the caller MEANS - the comment they are answering - not for where
	// the row ends up, which flattening may change.
	ParentID string `json:"parentID"`
	Text     string `json:"text"`
}

// CreateComment posts a comment as the calling entity.
//
// The route the bot needed and did not have. Reading a comment mention was
// possible from the first commit; answering one was not, so a bot could be
// addressed in a comment thread and had no way to speak in it.
//
// Threads flatten to two levels - a reply to a reply re-parents to its
// top-level ancestor - so the response returns both `replied_to` (what was
// aimed at) and `parent_comment_id` (where the row landed). A caller that
// assumes those are the same is wrong exactly one level down, and returning
// both is how it finds out here rather than from a thread that reads oddly.
func (h *Handlers) CreateComment(w http.ResponseWriter, r *http.Request) {
	token, ok := auth.FromContext(r.Context())
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"status": false})
		return
	}

	var body commentBody
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"status": false, "message": "Body must be JSON.",
		})
		return
	}
	if body.PostID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"status": false, "message": "postID is required.",
		})
		return
	}
	if len(body.Text) > maxCommentLength {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"status":  false,
			"message": "text exceeds " + strconv.Itoa(maxCommentLength) + " characters.",
		})
		return
	}

	result, err := platform.CreateComment(r.Context(), h.deps(), token.EntityID, platform.CommentRequest{
		PostID:   body.PostID,
		ParentID: body.ParentID,
		Text:     body.Text,
	})
	switch {
	case errors.Is(err, platform.ErrEmptyComment):
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"status": false, "message": "refusing to post an empty comment.",
		})
		return
	case errors.Is(err, platform.ErrPostNotFound):
		writeJSON(w, http.StatusNotFound, map[string]any{
			"status": false, "message": "Post not found.",
		})
		return
	case errors.Is(err, platform.ErrParentCommentNotFound):
		// Also returned for a parent that belongs to a DIFFERENT post, which
		// Django would have accepted and re-parented across posts.
		writeJSON(w, http.StatusNotFound, map[string]any{
			"status": false, "message": "Comment not found.",
		})
		return
	case err != nil:
		slog.Error("comment failed", "post_id", body.PostID, "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"status": false, "message": "Could not post the comment.",
		})
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"status":            true,
		"comment_id":        result.CommentID,
		"post_id":           result.PostID,
		"parent_comment_id": result.ParentCommentID,
		"replied_to":        result.RepliedTo,
		"notified":          result.Notified,
	})
}

func (h *Handlers) deps() platform.Deps {
	return platform.Deps{
		Mongo:    h.Conns.Mongo,
		Postgres: h.Conns.Postgres,
		Redis:    h.Conns.Redis,
		Queue:    h.Queue,
		PodName:  h.Cfg.PodName,
	}
}

const maxContentLength = 5000

// Django's comment text is an unbounded TextField. A cap is added here for the
// same reason the message route has one: an unbounded write from a credential
// that outlives any session is not a capability worth granting.
const maxCommentLength = 5000

// readLimit clamps a caller's limit. Caps, not defaults: a client asking for
// ten thousand messages has a bug, and serving it is how one bug becomes an
// outage in the store.
func readLimit(r *http.Request, fallback, ceiling int64) int64 {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fallback
	}
	if value < 1 {
		return 1
	}
	if value > ceiling {
		return ceiling
	}
	return value
}
