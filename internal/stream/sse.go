// Package stream turns one entity's Redis channel into an SSE response.
//
// # WHY THIS SERVICE EXISTS AT ALL
//
// The platform publishes realtime frames to `events_<entity_id>`. Node already
// forwards them to browsers, and Django could in principle do the same - but
// Django is served by gunicorn with three SYNC workers, where one held-open
// stream occupies a whole worker for its lifetime. Three subscribers would
// deadlock the API that the rest of the platform depends on.
//
// A goroutine per connection costs a few kilobytes instead of a process slot,
// which is the entire reason this is Go and not another Django view.
//
// # WHAT IS NOT SOLVED HERE
//
// Redis pub/sub is fire-and-forget: frames published while a client is
// disconnected are gone, and there is no replay. That is acceptable because
// the frames are notifications, not the record - they say "something happened
// in this conversation", carrying no message text - so a client that missed
// one recovers by reading the REST API, not by replaying the stream. A client
// that needs guaranteed delivery needs a different transport, and this package
// should not be quietly extended into pretending it is one.
package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

// Frame is the envelope the platform publishes. Node writes it in
// reusables/redis/pubsub.js publish(); this only needs the discriminator.
type Frame struct {
	Event string `json:"event"`
}

// ChannelFor is the pub/sub channel carrying one entity's frames.
func ChannelFor(entityID string) string {
	return "events_" + entityID
}

type Options struct {
	Heartbeat   time.Duration
	MaxLifetime time.Duration
}

// Serve streams `events_<entityID>` to the client until it disconnects, the
// lifetime cap expires, or the server shuts down.
func Serve(ctx context.Context, w http.ResponseWriter, rdb *redis.Client, entityID string, opts Options) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		// Without flushing, every frame sits in a buffer and the stream is a
		// slow way of delivering nothing.
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Proxies that buffer will hold frames until the buffer fills, which for a
	// low-traffic stream means delivering a mention several minutes late.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	// Tell the client the stream is live before anything has happened, so a
	// connect can be distinguished from a hang.
	fmt.Fprintf(w, "event: ready\ndata: {\"entity_id\":%q}\n\n", entityID)
	flusher.Flush()

	ctx, cancel := context.WithTimeout(ctx, opts.MaxLifetime)
	defer cancel()

	channel := ChannelFor(entityID)
	sub := rdb.Subscribe(ctx, channel)
	defer func() {
		if err := sub.Close(); err != nil {
			slog.Debug("subscription close failed", "channel", channel, "error", err)
		}
	}()

	messages := sub.Channel()
	heartbeat := time.NewTicker(opts.Heartbeat)
	defer heartbeat.Stop()

	slog.Info("stream opened", "entity_id", entityID)
	defer slog.Info("stream closed", "entity_id", entityID)

	for {
		select {
		case <-ctx.Done():
			// Client hung up, or the lifetime cap fired. Both are ordinary.
			return

		case message, open := <-messages:
			if !open {
				return
			}
			name := frameEvent(message.Payload)
			// The payload is forwarded verbatim. This service does not parse,
			// reshape or filter the platform's frames - doing so would put a
			// third definition of their schema in a third repo, and the two
			// that exist already have to agree.
			if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", name, message.Payload); err != nil {
				return
			}
			flusher.Flush()

		case <-heartbeat.C:
			// A comment line: valid SSE, ignored by every client, and enough
			// to keep an idle connection from being reaped by a proxy.
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// frameEvent pulls the discriminator out of a frame so clients can select on
// the SSE event name rather than re-parsing the body.
//
// Falls back to "message" for anything unreadable rather than dropping the
// frame: an unknown shape is still an event the client should know about, and
// silently swallowing it would make a schema change look like silence.
func frameEvent(payload string) string {
	var frame Frame
	if err := json.Unmarshal([]byte(payload), &frame); err != nil || frame.Event == "" {
		return "message"
	}
	return frame.Event
}
