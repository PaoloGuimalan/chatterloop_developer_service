// Package config reads the service's settings from the environment.
//
// Variable names match the other chatterloop services on purpose - DB_HOST,
// REDIS_HOST and friends are already set wherever these run, so this service
// drops into an existing deployment without a new secrets story.
package config

import (
	"strings"
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

	// The release this build came from, set from the image tag at deploy time.
	// Reported by GET /version so a rollout can be confirmed from outside.
	AppVersion string

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

	// Signs the `active_users` presence frame. The SAME secret Node signs the
	// same event with (server/reusables/hooks/jwthelper.js createJWTwExp) -
	// both clients call a decode on that frame unconditionally, so a frame
	// this service published unsigned would be unparseable rather than merely
	// unverified.
	//
	// The one value this service needs supplied to own presence end to end
	// rather than handing the fan-out to another process. Already set
	// wherever Node runs, so it is the same "no new secrets story" this
	// package's doc describes - not a new secret, a shared one.
	//
	// OPTIONAL, and degrades rather than breaks: with it unset the session
	// row is still written and the dot still appears on the next
	// /u/activecontacts snapshot; only the live push is lost.
	JWTSecret string

	// Where a handle is a page on the web client, used to build the
	// `profile_redirect` on a searched entity.
	//
	// Defaulted rather than required: it is a public origin, not a secret, and
	// a deployment that never sets it should still return working links rather
	// than half a URL. Set it on staging so results do not link people into
	// production.
	WebClientBaseURL string
}

func Load() (*Config, error) {
	cfg := &Config{
		Port:              env("PORT", "8890"),
		PodName:           env("POD_NAME", env("HOSTNAME", "podless")),
		AppVersion:        env("APP_VERSION", "dev"),
		RabbitMQURL:       rabbitURL(),
		RedisAddr:         fmt.Sprintf("%s:%s", env("REDIS_HOST", "localhost"), env("REDIS_PORT", "6379")),
		RedisUsername:     os.Getenv("REDIS_USERNAME"),
		RedisPassword:     os.Getenv("REDIS_PASSWORD"),
		Heartbeat:         envDuration("SSE_HEARTBEAT_SECONDS", 20*time.Second),
		MaxStreamLifetime: envDuration("SSE_MAX_LIFETIME_SECONDS", time.Hour),
		JWTSecret:         os.Getenv("JWT_SECRET"),
		WebClientBaseURL:  strings.TrimRight(env("WEB_CLIENT_BASE_URL", "https://chatterloop.app"), "/"),
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
