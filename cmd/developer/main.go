// Command developer runs the chatterloop developer API.
//
// Today it does one thing: stream an entity's realtime frames over SSE,
// authenticated by an `entity_token`. It exists as its own service because
// long-lived connections and gunicorn's sync workers are a bad match - three
// subscribers would consume every worker user_service has - and because the
// developer-facing API is going to be extracted from Django anyway. Starting
// it here means that extraction is a move, not a rewrite.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"developer_service/internal/auth"
	"developer_service/internal/config"
	"developer_service/internal/connections"
	"developer_service/internal/endpoints"
	"developer_service/internal/queue"

	"github.com/joho/godotenv"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))
	_ = godotenv.Load()

	cfg, err := config.Load()
	if err != nil {
		slog.Error("configuration is incomplete", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conns, err := connections.Open(ctx, cfg.PostgresDSN, cfg.RedisAddr, cfg.RedisUsername, cfg.RedisPassword)
	if err != nil {
		slog.Error("could not reach dependencies", "error", err)
		os.Exit(1)
	}
	defer conns.Close()

	publisher := queue.New(cfg.RabbitMQURL)
	defer publisher.Close()

	handlers := &endpoints.Handlers{Conns: conns, Cfg: cfg, Queue: publisher}

	// One limiter, shared by every route below: it is stateless (the window
	// key already carries the token id), so there is no reason for each route
	// to hold its own.
	rateLimiter := auth.NewRedisRateLimitStore(conns.Redis)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handlers.Health)
	mux.HandleFunc("GET /ready", handlers.Ready)

	// Authenticated routes. WhoAmI needs only a valid credential; Events also
	// needs the scope, checked against both the token and the entity.
	mux.Handle("GET /v1/whoami", auth.Middleware(conns.Postgres, rateLimiter,
		http.HandlerFunc(handlers.WhoAmI)))
	mux.Handle("GET /v1/events", auth.Middleware(conns.Postgres, rateLimiter,
		auth.RequireScope(conns.Postgres, auth.PermissionEventsSubscribe,
			http.HandlerFunc(handlers.Events))))
	mux.Handle("GET /v1/conversations/{conversationID}/messages",
		auth.Middleware(conns.Postgres, rateLimiter,
			auth.RequireScope(conns.Postgres, auth.PermissionMessagesRead,
				http.HandlerFunc(handlers.ConversationMessages))))
	// "Which recent messages here reply to something I wrote." Same scope as
	// reading the conversation, because that is what it is - a filtered read of
	// the same messages, answered server-side so a client does not have to pull
	// the whole window to find out.
	mux.Handle("GET /v1/conversations/{conversationID}/replies",
		auth.Middleware(conns.Postgres, rateLimiter,
			auth.RequireScope(conns.Postgres, auth.PermissionMessagesRead,
				http.HandlerFunc(handlers.ConversationReplies))))
	mux.Handle("GET /v1/mentions/comments", auth.Middleware(conns.Postgres, rateLimiter,
		auth.RequireScope(conns.Postgres, auth.PermissionNotificationsRead,
			http.HandlerFunc(handlers.CommentMentions))))
	mux.Handle("GET /v1/comments/replies", auth.Middleware(conns.Postgres, rateLimiter,
		auth.RequireScope(conns.Postgres, auth.PermissionNotificationsRead,
			http.HandlerFunc(handlers.CommentReplies))))
	// Gated on notifications.read because that is the scope a consumer holds
	// to LEARN a comment concerns it, and this is the follow-on read for
	// exactly that. The platform catalog has no comment-read codename to use
	// instead, and inventing one here would mean a Django migration before
	// this service could gate anything on it. Move it if the catalog gains
	// `comments.read`.
	mux.Handle("GET /v1/posts/{postID}/comments", auth.Middleware(conns.Postgres, rateLimiter,
		auth.RequireScope(conns.Postgres, auth.PermissionNotificationsRead,
			http.HandlerFunc(handlers.PostComments))))
	mux.Handle("POST /v1/messages/send", auth.Middleware(conns.Postgres, rateLimiter,
		auth.RequireScope(conns.Postgres, auth.PermissionMessagesSend,
			http.HandlerFunc(handlers.SendMessage))))
	mux.Handle("POST /v1/comments", auth.Middleware(conns.Postgres, rateLimiter,
		auth.RequireScope(conns.Postgres, auth.PermissionCommentsCreate,
			http.HandlerFunc(handlers.CreateComment))))

	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		// No WriteTimeout, deliberately: it applies to the whole response, and
		// on an SSE stream that means cutting every client off at the timeout
		// regardless of health. The stream's own MaxLifetime is the bound.
		IdleTimeout: 2 * time.Minute,
	}

	go func() {
		slog.Info("developer_service listening", "port", cfg.Port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")

	// Long enough for open streams to end cleanly rather than as a client-side
	// connection error, short enough that a deploy is not held hostage by one.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Warn("graceful shutdown incomplete", "error", err)
	}
}
