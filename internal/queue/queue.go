// Package queue publishes jobs to the platform's RabbitMQ.
//
// Same protocol as the other two publishers - user_service's RabbitMQClient
// and worker_service's own Publish(): a plain JSON body on a named durable
// queue, sent to the DEFAULT exchange with the queue name as the routing key.
// Nothing here declares an exchange or a binding, because none of them do.
//
// Publishing is BEST-EFFORT and never fails the request that triggered it. A
// broker outage should cost a push notification, not a message that was
// already written.
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Queue names, one per listener the Go worker registers in its
// internal/startup/init.go. Referencing a constant rather than a literal is
// what makes a typo a compile error instead of a message published to a queue
// nobody consumes.
const (
	SendPush      = "send_push"
	BumpChatScore = "bump_chat_score"
	// Both published by Django's CommentsView.post(); reproduced when a
	// comment is created through this API so a bot's comment moves the same
	// counters a person's does.
	UpdateRankingScore   = "update_ranking_score"
	BumpInterestAffinity = "bump_interest_affinity"
)

// PushPayload mirrors worker_service's rabbitmq.SendPushPayload exactly. The
// worker resolves device tokens from EntityIDs itself, so Tokens stays empty.
type PushPayload struct {
	EntityIDs  []string          `json:"entity_ids"`
	Tokens     []string          `json:"tokens,omitempty"`
	Channel    string            `json:"channel"`
	Title      string            `json:"title"`
	Body       string            `json:"body"`
	Tag        string            `json:"tag,omitempty"`
	ImageURL   string            `json:"image_url,omitempty"`
	OSRendered bool              `json:"os_rendered"`
	Data       map[string]string `json:"data,omitempty"`
}

// Channels the mobile client renders. Kept in sync with worker_service's
// push.go constants.
const (
	ChannelMessages = "chatterloop_messages_v2"
	ChannelActivity = "chatterloop_activity_v2"
)

// ChatScorePayload mirrors what Node's bumpChatScore publishes.
type ChatScorePayload struct {
	ActorID    string   `json:"actor_id"`
	MemberIDs  []string `json:"member_ids"`
	Action     string   `json:"action"`
	IsDecrease bool     `json:"is_decrease"`
}

// RankingPayload mirrors worker_service's UpdateRankingPayload. The worker
// owns comments_count the same way it owns likes_count, so a comment written
// here must publish this or the count never moves.
type RankingPayload struct {
	PostID     string `json:"post_id"`
	UpdateType string `json:"update_type"`
	IsDecrease bool   `json:"is_decrease"`
}

// InterestAffinityPayload mirrors worker_service's BumpInterestAffinityPayload.
//
// InterestIDs are int64 and not strings, because interests_interest.id is a
// bigint - Django's publishers put JSON numbers on the wire. The worker's own
// comment records what a []string declaration cost the last time somebody got
// this wrong: the payload failed to unmarshal, the listener consumes with
// autoAck, and every bump silently vanished.
type InterestAffinityPayload struct {
	EntityID    string  `json:"entity_id"`
	InterestIDs []int64 `json:"interest_ids"`
	Action      string  `json:"action"`
	IsDecrease  bool    `json:"is_decrease"`
}

type Publisher struct {
	url string

	mu       sync.Mutex
	conn     *amqp.Connection
	channel  *amqp.Channel
	declared map[string]bool
}

func New(url string) *Publisher {
	return &Publisher{url: url, declared: map[string]bool{}}
}

// Publish hands one job to the worker. Returns false when it could not be
// handed over; callers log and carry on.
func (p *Publisher) Publish(ctx context.Context, queue string, payload any) bool {
	if p.url == "" {
		slog.Warn("rabbitmq not configured, dropping job", "queue", queue)
		return false
	}

	body, err := json.Marshal(payload)
	if err != nil {
		slog.Error("could not encode job", "queue", queue, "error", err)
		return false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.ensureChannel(queue); err != nil {
		slog.Error("rabbitmq unavailable", "queue", queue, "error", err)
		return false
	}

	err = p.channel.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
		ContentType: "application/json",
		// 2 = persistent, so a queued job outlives a broker restart.
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
	if err != nil {
		slog.Error("publish failed", "queue", queue, "error", err)
		// Drop the channel so the next call rebuilds rather than reusing one
		// that has gone away underneath us - a hosted broker reaps idle
		// connections and nothing here sends heartbeats between requests.
		p.reset()
		return false
	}
	return true
}

func (p *Publisher) ensureChannel(queue string) error {
	if p.conn == nil || p.conn.IsClosed() {
		conn, err := amqp.Dial(p.url)
		if err != nil {
			return fmt.Errorf("dial: %w", err)
		}
		p.conn = conn
		p.channel = nil
		p.declared = map[string]bool{}
	}
	if p.channel == nil || p.channel.IsClosed() {
		channel, err := p.conn.Channel()
		if err != nil {
			return fmt.Errorf("channel: %w", err)
		}
		p.channel = channel
		p.declared = map[string]bool{}
	}
	if !p.declared[queue] {
		// Declared durable on first use, same as the Go worker and Django do,
		// so whichever service starts first the queue exists.
		if _, err := p.channel.QueueDeclare(queue, true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare %s: %w", queue, err)
		}
		p.declared[queue] = true
	}
	return nil
}

func (p *Publisher) reset() {
	if p.channel != nil {
		_ = p.channel.Close()
		p.channel = nil
	}
	if p.conn != nil {
		_ = p.conn.Close()
		p.conn = nil
	}
	p.declared = map[string]bool{}
}

func (p *Publisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reset()
}
