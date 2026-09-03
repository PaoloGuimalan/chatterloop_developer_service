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

	"github.com/jackc/pgx/v5"
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

	// NO NAMED PREPARED STATEMENTS. This is required, not a tuning choice.
	//
	// The platform's Postgres is reached through a TRANSACTION-POOLING proxy
	// (Supabase's PgBouncer; the host is *.pooler.supabase.com). pgx's default
	// exec mode prepares each query once under a generated name and caches it
	// per CLIENT connection - but a transaction pooler hands successive
	// transactions to different SERVER sessions, so that name is either absent
	// on the new session or already taken by another client:
	//
	//	SQLSTATE 26000  prepared statement "stmtcache_..." does not exist
	//	SQLSTATE 42P05  prepared statement "stmtcache_..." already exists
	//
	// Both surfaced here as authentication failures, because Verify treats any
	// database error as an unusable credential - so the symptom was a valid
	// token being rejected with "Invalid or expired token", intermittently,
	// with nothing wrong with the token.
	//
	// QueryExecModeExec still uses the extended protocol - parameters stay out
	// of the SQL string, so this costs nothing in injection safety - it just
	// sends an unnamed statement per call instead of relying on a name the
	// server may not have. The two caches are emptied for the same reason:
	// both key on a connection identity the pooler does not preserve.
	pgConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	pgConfig.ConnConfig.StatementCacheCapacity = 0
	pgConfig.ConnConfig.DescriptionCacheCapacity = 0

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
