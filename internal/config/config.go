// Package config reads the service's settings from the environment.
//
// Variable names match the other chatterloop services on purpose - DB_HOST,
// REDIS_HOST and friends are already set wherever these run, so this service
// drops into an existing deployment without a new secrets story.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port string

	// Stamped into every realtime frame, matching what the Node publisher
	// writes, so a frame can be traced back to the process that emitted it.
	PodName string

	// Where fan-out jobs go. The Go worker already consumes send_push and
	// bump_chat_score, so this service publishes rather than reimplementing
	// Firebase or the scoring rules.
	RabbitMQURL string

	// Postgres holds entity_token, which is the only thing this service reads
	// from a database. It is the same table Django owns and Node verifies
	// against; see internal/auth for why three implementations exist and what
	// keeps them honest.
	PostgresDSN string

	// Redis carries the realtime frames. The platform publishes to
	// `events_<entity_id>`; this service subscribes and forwards.
	RedisAddr     string
	RedisUsername string
	RedisPassword string

	// How often a stream emits a comment line when nothing has happened.
	// Without it an idle connection is indistinguishable from a dead one and
	// the first proxy in the path closes it.
	Heartbeat time.Duration

	// Hard ceiling on a single stream. Long-lived connections accumulate; a
	// bounded lifetime turns "leaked forever" into "reconnects hourly", and
	// every client already needs reconnect logic for the network anyway.
	MaxStreamLifetime time.Duration
}

func Load() (*Config, error) {
	cfg := &Config{
		Port:              env("PORT", "8890"),
		PodName:           env("POD_NAME", env("HOSTNAME", "podless")),
		RabbitMQURL:       rabbitURL(),
		RedisAddr:         fmt.Sprintf("%s:%s", env("REDIS_HOST", "localhost"), env("REDIS_PORT", "6379")),
		RedisUsername:     os.Getenv("REDIS_USERNAME"),
		RedisPassword:     os.Getenv("REDIS_PASSWORD"),
		Heartbeat:         envDuration("SSE_HEARTBEAT_SECONDS", 20*time.Second),
		MaxStreamLifetime: envDuration("SSE_MAX_LIFETIME_SECONDS", time.Hour),
	}

	// DATABASE_URL wins when set (that is how most hosts inject it); otherwise
	// assemble from the same parts worker_service uses.
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		cfg.PostgresDSN = dsn
	} else {
		host := os.Getenv("DB_HOST")
		if host == "" {
			return nil, fmt.Errorf("either DATABASE_URL or DB_HOST must be set")
		}
		cfg.PostgresDSN = fmt.Sprintf(
			"postgres://%s:%s@%s:%s/%s",
			os.Getenv("DB_USERNAME"),
			os.Getenv("DB_PASSWORD"),
			host,
			env("DB_PORT", "5432"),
			os.Getenv("DB_NAME"),
		)
	}

	return cfg, nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		return fallback
	}
	return time.Duration(seconds) * time.Second
}

// rabbitURL assembles the broker URL from the same parts worker_service and
// user_service use, or takes RABBITMQ_URL whole when a host injects it.
func rabbitURL() string {
	if url := os.Getenv("RABBITMQ_URL"); url != "" {
		return url
	}
	host := os.Getenv("RABBITMQ_HOST")
	if host == "" {
		return ""
	}
	protocol := env("RABBITMQ_PROTOCOL", "amqp")
	vhost := os.Getenv("RABBITMQ_VHOST")
	return fmt.Sprintf("%s://%s:%s@%s:%s/%s",
		protocol,
		os.Getenv("RABBITMQ_USER"),
		os.Getenv("RABBITMQ_PASS"),
		host,
		env("RABBITMQ_PORT", "5672"),
		vhost,
	)
}
