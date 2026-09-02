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

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handlers.Health)
	mux.HandleFunc("GET /ready", handlers.Ready)

	// Authenticated routes. WhoAmI needs only a valid credential; Events also
	// needs the scope, checked against both the token and the entity.
	mux.Handle("GET /v1/whoami", auth.Middleware(conns.Postgres,
		http.HandlerFunc(handlers.WhoAmI)))
	mux.Handle("GET /v1/events", auth.Middleware(conns.Postgres,
		auth.RequireScope(conns.Postgres, auth.PermissionEventsSubscribe,
			http.HandlerFunc(handlers.Events))))
	mux.Handle("GET /v1/conversations/{conversationID}/messages",
		auth.Middleware(conns.Postgres,
			auth.RequireScope(conns.Postgres, auth.PermissionMessagesRead,
				http.HandlerFunc(handlers.ConversationMessages))))
	mux.Handle("GET /v1/mentions/comments", auth.Middleware(conns.Postgres,
		auth.RequireScope(conns.Postgres, auth.PermissionNotificationsRead,
			http.HandlerFunc(handlers.CommentMentions))))
	mux.Handle("POST /v1/messages/send", auth.Middleware(conns.Postgres,
		auth.RequireScope(conns.Postgres, auth.PermissionMessagesSend,
			http.HandlerFunc(handlers.SendMessage))))

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
