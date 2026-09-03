package platform

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// MAX_MENTIONS_PER_COMMENT, from newsfeed/services/comment_mentions.py. Past
// this the input is a spam vector rather than a conversation, and every
// mention costs a notification write plus a realtime publish.
const maxMentionsPerComment = 20

// MentionedEntity is one entity a comment addressed by @handle.
type MentionedEntity struct {
	EntityID string
	Handle   string
}

// ResolveMentions turns the @handles in a comment into the entities they name.
//
// Ported from resolve_mentioned_entities(), and the three things it does that
// a naive lookup would not are the three that matter:
//
//  1. THREE NAMESPACES. A handle can be an Account.username, a Realm.slug or a
//     Bot.handle, so a page or a bot is mentionable exactly like a person.
//     Resolving only user_account - which is what the platform's own handle
//     lookup did until recently - silently drops the other two.
//  2. VISIBILITY. The same bar entity_side_is_visible() applies elsewhere: an
//     active AND verified account, an active realm, or an active bot. Bots are
//     not required to be verified, for the same reason a realm is not - there
//     is no verification concept to gate on.
//  3. BLOCKING. A block in EITHER direction removes the entity. Being blocked
//     by somebody must not be routed around by mentioning them.
//
// The author is dropped here rather than at notify time: nobody needs telling
// they wrote their own comment.
//
// A handle matching nothing is simply text, which is the whole point of the
// design - typing "@" costs nothing and breaks nothing.
func ResolveMentions(
	ctx context.Context,
	pool *pgxpool.Pool,
	text, authorEntityID string,
) ([]MentionedEntity, error) {
	candidates := mentionCandidates(text)
	if len(candidates) == 0 {
		return nil, nil
	}

	rows, err := pool.Query(ctx, `
		SELECT entity_id, lower(username) AS handle
		  FROM user_account
		 WHERE lower(username) = ANY($1) AND is_active AND is_verified
		 UNION ALL
		SELECT entity_id, lower(slug)     AS handle
		  FROM community_realm
		 WHERE lower(slug) = ANY($1) AND is_active
		 UNION ALL
		SELECT entity_id, lower(handle)   AS handle
		  FROM bot_bot
		 WHERE lower(handle) = ANY($1) AND is_active`, candidates)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	resolved := make([]MentionedEntity, 0, len(candidates))
	seen := map[string]bool{}
	for rows.Next() {
		var entityID, handle string
		if err := rows.Scan(&entityID, &handle); err != nil {
			return nil, err
		}
		if entityID == "" || entityID == authorEntityID || seen[entityID] {
			continue
		}
		seen[entityID] = true
		resolved = append(resolved, MentionedEntity{EntityID: entityID, Handle: handle})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(resolved) == 0 {
		return nil, nil
	}

	// The three namespaces are NOT mutually exclusive today - bot handles are
	// unique only among bots, so a bot and a user can share one. Where that
	// happens both resolve and both are notified, which is the safe direction:
	// over-notifying is a nuisance, silently addressing the wrong entity is a
	// bug. This mirrors Django rather than "fixing" it.

	blocked, err := blockedEntityIDs(ctx, pool, authorEntityID)
	if err != nil {
		// A blocking read that failed must not become a notification sent to
		// somebody who blocked the author. Refusing to resolve any mention is
		// the safe direction: the comment still posts, it just notifies nobody.
		return nil, err
	}

	kept := make([]MentionedEntity, 0, len(resolved))
	for _, entity := range resolved {
		if blocked[entity.EntityID] {
			continue
		}
		kept = append(kept, entity)
		if len(kept) == maxMentionsPerComment {
			break
		}
	}
	return kept, nil
}

// mentionCandidates is ExtractMentions, lowercased, plus the dot-stripped form
// of anything ending in one.
//
// The extra form is not a second parser - it is exactly what
// extract_mention_handles() does on top of the shared pattern, and it exists
// because the pattern's character class includes "." and is greedy: "thanks
// @ana." captures "ana.", the trailing lookahead already satisfied by
// end-of-string. Only one of "ana." and "ana" can be a real handle, so offering
// both keeps resolution unambiguous while making the common sentence work.
//
// ExtractMentions itself is left alone deliberately: it is pinned to the
// platform's pattern by a drift test, and a mention it counts that the
// messenger does not is a bug in whichever drifted.
func mentionCandidates(text string) []string {
	handles := ExtractMentions(text)
	if len(handles) == 0 {
		return nil
	}

	candidates := make([]string, 0, len(handles)*2)
	seen := map[string]bool{}
	for _, raw := range handles {
		handle := strings.ToLower(raw)
		for _, candidate := range []string{handle, strings.TrimRight(handle, ".")} {
			if candidate == "" || seen[candidate] {
				continue
			}
			seen[candidate] = true
			candidates = append(candidates, candidate)
		}
		if len(candidates) >= maxMentionsPerComment {
			break
		}
	}
	return candidates
}

// blockedEntityIDs returns every entity in a block relationship with this one,
// in EITHER direction - the same set get_blocked_account_ids() builds.
func blockedEntityIDs(ctx context.Context, pool *pgxpool.Pool, entityID string) (map[string]bool, error) {
	rows, err := pool.Query(ctx, `
		SELECT blocker_id, blocked_id
		  FROM entity_block
		 WHERE blocker_id = $1 OR blocked_id = $1`, entityID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	blocked := map[string]bool{}
	for rows.Next() {
		var blocker, target string
		if err := rows.Scan(&blocker, &target); err != nil {
			return nil, err
		}
		blocked[blocker] = true
		blocked[target] = true
	}
	// The entity is in its own relationships by construction; discarding it
	// keeps the set meaning "everyone I may not reach".
	delete(blocked, entityID)
	return blocked, rows.Err()
}
