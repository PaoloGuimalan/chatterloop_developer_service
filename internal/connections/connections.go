// Package connections owns the three backends this service talks to.
//
// Postgres verifies tokens and resolves handles, Mongo holds the conversations
// and notifications the API reads and the messages it writes, and Redis
// carries the realtime frames it streams.
//
// Cassandra is deliberately absent, and so is any HTTP client to another
// chatterloop service: this API answers from the stores directly, the same way
// user_service does, rather than proxying somebody else's endpoints.
package connections

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/mongo"
)

type Pool struct {
	Postgres *pgxpool.Pool
	Redis    *redis.Client
	Mongo    *mongo.Database
}

func Open(ctx context.Context, postgresDSN, redisAddr, redisUser, redisPassword string) (*Pool, error) {
	pgConfig, err := pgxpool.ParseConfig(postgresDSN)
	if err != nil {
		return nil, fmt.Errorf("postgres dsn: %w", err)
	}

	// Small on purpose. Every request here does at most two short reads
	// (verify the token, check one grant) and then spends its life on a Redis
	// subscription holding no Postgres connection at all - so a large pool
	// would sit idle while competing with the services that actually need one.
	pgConfig.MaxConns = 10
	pgConfig.MaxConnIdleTime = 5 * time.Minute

	pgPool, err := pgxpool.NewWithConfig(ctx, pgConfig)
	if err != nil {
		return nil, fmt.Errorf("postgres pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pgPool.Ping(pingCtx); err != nil {
		pgPool.Close()
		return nil, fmt.Errorf("postgres unreachable: %w", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Username: redisUser,
		Password: redisPassword,
	})
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		pgPool.Close()
		return nil, fmt.Errorf("redis unreachable: %w", err)
	}

	mongoDB, err := openMongo(ctx)
	if err != nil {
		pgPool.Close()
		_ = rdb.Close()
		return nil, err
	}

	slog.Info("connections established",
		"postgres", "ok", "redis", redisAddr, "mongo", mongoDB.Name())
	return &Pool{Postgres: pgPool, Redis: rdb, Mongo: mongoDB}, nil
}

func (p *Pool) Close() {
	if p.Postgres != nil {
		p.Postgres.Close()
	}
	if p.Redis != nil {
		if err := p.Redis.Close(); err != nil {
			slog.Warn("redis close failed", "error", err)
		}
	}
}
