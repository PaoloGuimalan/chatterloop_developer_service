// Package testutil provides explicit disposable fixtures, never application configuration.
package testutil

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Postgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("REALM_LIFECYCLE_TEST_DB_URL")
	if dsn == "" {
		t.Skip("requires explicit disposable PostgreSQL fixture")
	}
	parsed, err := url.Parse(dsn)
	if err != nil || !strings.HasPrefix(parsed.Path, "/test_") || (parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "localhost") {
		t.Fatal("only explicit local test_* databases are allowed")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("realm_lifecycle_dev_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, `CREATE SCHEMA "`+schema+`"`); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanup, `DROP SCHEMA "`+schema+`" CASCADE`); err != nil {
			t.Error(err)
		}
		admin.Close()
	})
	_, err = pool.Exec(ctx, `CREATE TABLE entity_entity (id text PRIMARY KEY, type text);
	 CREATE TABLE community_realm (id text PRIMARY KEY, realm_id text, entity_id text, type text,
	 parent_id text, is_active boolean DEFAULT TRUE, is_private boolean DEFAULT FALSE, deletion_requested_at timestamptz);
	 CREATE TABLE community_member (member_id text, realm_id text, entity_id text);`)
	if err != nil {
		t.Fatal(err)
	}
	return pool
}
