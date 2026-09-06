package presence

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// scopeSQL is the contacts half of the presence scope.
//
// SECOND IMPLEMENTATION. The same query exists in Node, at
// server/reusables/hooks/presence.js getPresenceScope, and the two must agree:
// that one backs the `/u/activecontacts` snapshot a client pulls on boot, this
// one backs the `active_users` push it receives afterwards. If the pull were
// wider than the push, dots would light on load and then silently stop
// updating; narrower, and a dot would appear from nowhere on the first change.
//
// Kept as deliberately identical text so a diff between the two files shows
// any drift immediately - the same discipline the token format uses across its
// three implementations (see internal/auth's package doc).
//
// ENTITY-GENERIC. It resolves the counterpart once in the CASE and filters on
// THAT, rather than requiring user_account on both sides - which is what used
// to drop every page and bot counterpart before the query returned. The
// visibility rule mirrors the platform's own (user_service entity/utils.py
// entity_side_is_visible): a user must be active AND verified, a realm or bot
// only active. `is_verified` on those two is the display BADGE, not an access
// gate, so requiring it would hide every unbadged page.
const scopeSQL = `
    SELECT DISTINCT
      CASE WHEN c.action_by_id = $1 THEN c.involved_entity_id
           ELSE c.action_by_id END AS counterpart_id
    FROM entity_connection c
    JOIN entity_entity p
      ON p.id = CASE WHEN c.action_by_id = $1 THEN c.involved_entity_id
                     ELSE c.action_by_id END
    LEFT JOIN user_account   u ON u.entity_id = p.id AND p.type = 'user'
    LEFT JOIN community_realm r ON r.entity_id = p.id AND p.type = 'realm'
    LEFT JOIN bot_bot         b ON b.entity_id = p.id AND p.type = 'bot'
    WHERE
      (c.action_by_id = $1 OR c.involved_entity_id = $1)
      AND c.action_by_id <> c.involved_entity_id
      AND c.status = TRUE
      AND COALESCE(u.is_active, r.is_active, b.is_active, FALSE) = TRUE
      -- Users only: a NULL u row (page/bot) makes this TRUE and passes.
      AND COALESCE(u.is_verified, TRUE) = TRUE
`

// Scope returns every entity that must be told when entityID's presence
// changes.
//
// TWO SOURCES, UNIONED, because either alone is wrong:
//
//   - CONTACTS. You can be connected to someone you have never messaged, and
//     their dot should still light up in the contacts list.
//   - DM COUNTERPARTS. You can share a conversation with someone who is not a
//     contact at all. Not an edge case, and the case that matters most here: a
//     bot CANNOT be a contact (can_connect is false for every one, so no
//     entity_connection row is ever written), so a DM with one exists only as
//     a Mongo conversation document. Contacts-only is precisely why bots never
//     had a dot - nobody was ever in scope to be told.
//
// GROUP CO-MEMBERS ARE DELIBERATELY EXCLUDED. They share a conversation in the
// literal sense, but a group header renders the static string "Members are
// Active" rather than a per-entity dot, so including them would buy no visible
// change while turning one connect in a 500-member server into 500 published
// frames.
//
// A failure in either half is logged by the caller and the other half still
// counts: a partial scope reaches fewer people than it should, which is a
// worse dot, while an error that aborts the whole thing is no dot at all.
func Scope(ctx context.Context, pool *pgxpool.Pool, db *mongo.Database, entityID string) ([]string, error) {
	seen := map[string]struct{}{}
	var firstErr error

	if pool != nil {
		rows, err := pool.Query(ctx, scopeSQL, entityID)
		if err != nil {
			firstErr = fmt.Errorf("contacts scope: %w", err)
		} else {
			for rows.Next() {
				var counterpart *string
				if err := rows.Scan(&counterpart); err != nil {
					firstErr = fmt.Errorf("contacts scope scan: %w", err)
					break
				}
				if counterpart != nil && *counterpart != "" {
					seen[*counterpart] = struct{}{}
				}
			}
			rows.Close()
			if err := rows.Err(); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("contacts scope: %w", err)
			}
		}
	}

	if db != nil {
		// Single conversations only - see the group note above.
		// `participant_ids` is indexed (server schema/messages/conversation.js),
		// so this is a covered lookup rather than a scan.
		cursor, err := db.Collection("conversations").Find(ctx,
			bson.M{"conversationType": "single", "participant_ids": entityID},
			options.Find().SetProjection(bson.M{"participant_ids": 1, "_id": 0}),
		)
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("conversation scope: %w", err)
		} else if err == nil {
			var docs []struct {
				ParticipantIDs []string `bson:"participant_ids"`
			}
			if err := cursor.All(ctx, &docs); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("conversation scope decode: %w", err)
			}
			for _, doc := range docs {
				for _, participant := range doc.ParticipantIDs {
					if participant != "" {
						seen[participant] = struct{}{}
					}
				}
			}
		}
	}

	delete(seen, entityID)

	scope := make([]string, 0, len(seen))
	for id := range seen {
		scope = append(scope, id)
	}
	return scope, firstErr
}

// HasLiveSession reports whether this entity still has ANY session marked
// online.
//
// One connection ending is not the entity going offline. A page is online
// while any admin is switched into it, and this service caps a stream's
// lifetime so a bot's own disconnect is routinely followed by its reconnect
// seconds later. The browser SSE path asks exactly this question before
// broadcasting a disconnect (server routes/users/index.js), and this is the
// same check for the streams that end here.
func HasLiveSession(ctx context.Context, db *mongo.Database, entityID string) (bool, error) {
	if db == nil {
		return false, nil
	}
	count, err := db.Collection("sessions").CountDocuments(ctx,
		bson.M{"entityID": entityID, "status": true})
	if err != nil {
		return false, err
	}
	return count > 0, nil
}
