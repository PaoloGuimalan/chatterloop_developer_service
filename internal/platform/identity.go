// Package platform reads and writes chatterloop's own stores.
//
// This is the same access user_service has - Django models the very same Mongo
// collections and Postgres tables - not a client of somebody else's API. That
// is deliberate: an API gateway that proxied another service's endpoints would
// inherit their shapes, their auth assumptions and their latency, and every
// change would need both to move together.
package platform

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// HandlesFor maps entity ids to @handles, for users, realms and bots alike.
//
// One query, not one per entity. The three-way union matters: resolving only
// user_account - which is what the platform's own GetEntityHandles did until
// recently - leaves a realm or a bot with an empty name, and a transcript of
// anonymous turns is materially worse input for anything reading it.
//
// Entities backing none of the three are absent from the map rather than
// guessed at, so callers must fall back.
func HandlesFor(ctx context.Context, pool *pgxpool.Pool, entityIDs []string) (map[string]string, error) {
	unique := dedupe(entityIDs)
	if len(unique) == 0 {
		return map[string]string{}, nil
	}

	rows, err := pool.Query(ctx, `
		SELECT entity_id, username AS handle FROM user_account WHERE entity_id = ANY($1)
		 UNION ALL
		SELECT entity_id, slug   AS handle FROM community_realm WHERE entity_id = ANY($1)
		 UNION ALL
		SELECT entity_id, handle            FROM bot_bot        WHERE entity_id = ANY($1)`,
		unique)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	handles := make(map[string]string, len(unique))
	for rows.Next() {
		var entityID, handle string
		if err := rows.Scan(&entityID, &handle); err != nil {
			return nil, err
		}
		// First writer wins, matching the union order and the platform's own
		// precedence: an entity should back exactly one of these anyway.
		if _, seen := handles[entityID]; !seen {
			handles[entityID] = handle
		}
	}
	return handles, rows.Err()
}

func dedupe(values []string) []string {
	seen := make(map[string]bool, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}
