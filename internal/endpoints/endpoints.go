// Package endpoints holds the HTTP surface.
//
// Three routes, versioned in the path from the first commit: the consumer is a
// deployed service, not a browser that reloads, so when a shape has to change
// both versions need to be live at once for as long as it takes to roll
// clients forward.
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
	writeJSON(w, http.StatusOK, map[string]any{
		"status":    true,
		"entity_id": token.EntityID,
		"realm_id":  token.RealmID,
		"scopes":    token.Scopes,
		"token": map[string]any{
			"id":   token.ID,
			"name": token.Name,
		},
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

	conversation, err := platform.LoadConversation(r.Context(), h.Conns.Mongo, conversationID, token.EntityID)
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

	senders := make([]string, 0, len(messages))
	for _, message := range messages {
		senders = append(senders, message.SenderEntityID)
	}
	if handles, err := platform.HandlesFor(r.Context(), h.Conns.Postgres, senders); err == nil {
		for i := range messages {
			messages[i].SenderHandle = handles[messages[i].SenderEntityID]
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":            true,
		"conversation_id":   conversationID,
		"conversation_type": conversation.ConversationType,
		"count":             len(messages),
		"messages":          messages,
	})
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
