package platform

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// The entity kinds a handle can name. The same three namespaces ResolveMentions
// resolves across, and for the same reason: a page or a bot is addressable
// exactly like a person, so a directory that returned only people would be
// wrong in a way that is invisible until somebody tries to mention a page.
const (
	EntityKindUser  = "user"
	EntityKindRealm = "realm"
	EntityKindBot   = "bot"
)

// Below this a search matches most of the platform and the result is noise
// rather than an answer. Two characters is enough to be deliberate.
const minSearchLength = 2

// FoundEntity is one searchable identity.
type FoundEntity struct {
	EntityID string `json:"entity_id"`
	Kind     string `json:"kind"`
	Handle   string `json:"handle"`
	Name     string `json:"name"`
	Profile  string `json:"profile"`
}

// SearchEntities finds users, realms and bots by handle or name.
//
// # WHAT THIS IS FOR
//
// "Who is @ana?" and "is there a page for support?" are questions a bot cannot
// answer from a conversation it is in. Everything it can see is whoever
// happens to have spoken. This is the directory lookup that makes an agent
// able to act on a name somebody gave it.
//
// # THE VISIBILITY BAR IS THE PLATFORM'S, NOT A NEW ONE
//
// An account must be active AND verified; a realm and a bot active. That is
// exactly entity_side_is_visible(), the same bar ResolveMentions applies - so
// anything findable here was already addressable by typing its handle, and
// this adds reach rather than access.
//
// # BLOCKING APPLIES IN BOTH DIRECTIONS
//
// A block either way removes the entity, matching mentions. Search would
// otherwise be a way around a block: find the handle here, address it there.
//
// # EVERY TEXT COLUMN IS COALESCED
//
// Not defensive habit: community_realm.slug is NULL on most active realms,
// because a realm gets a slug only once somebody sets one. Scanning that into
// a string fails the whole query, so the search 500s on any term that happens
// to match a slugless realm by NAME - which looks like an outage on one search
// term and works fine on the next.
//
// # SYSTEM BOTS ARE NOT DISCOVERABLE
//
// `is_system` marks a bot that belongs to the platform rather than to anyone -
// it answers commands, is not a member of any conversation, and cannot be
// joined or addressed. Returning it here would offer a handle that leads
// nowhere, which is worse than not finding it.
//
// Note this is a SEARCH, not a resolver. GetSenderDetails and HandlesFor must
// keep finding system bots by entity id, or their messages render with a blank
// name - discovery and resolution want opposite answers here.
//
// # THE VIEWER IS NOT IN ITS OWN RESULTS
//
// An agent searching for somebody to talk to does not mean itself, and a bot
// discovering its own handle in a directory is a small but reliable way to
// produce a conversation with itself.
func SearchEntities(
	ctx context.Context,
	pool *pgxpool.Pool,
	query string,
	kinds []string,
	limit int64,
	viewerEntityID string,
) ([]FoundEntity, error) {
	term := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(query), "@")))
	if len(term) < minSearchLength {
		return nil, nil
	}
	if limit < 1 {
		limit = 1
	}

	want := kindFilter(kinds)

	// A prefix match is ranked first and a contained match after, because
	// "ana" should find @ana before @banana. LIKE with a leading wildcard
	// cannot use an index, which is acceptable at this size and is the reason
	// the limit is low - this is a lookup, not a listing.
	prefix := term + "%"
	contains := "%" + term + "%"

	rows, err := pool.Query(ctx, `
		SELECT entity_id, kind, handle, name, profile FROM (
			SELECT entity_id,
			       'user'::text AS kind,
			       coalesce(username,'') AS handle,
			       btrim(coalesce(first_name,'') || ' ' || coalesce(last_name,'')) AS name,
			       coalesce(profile,'') AS profile
			  FROM user_account
			 WHERE is_active AND is_verified
			   AND (lower(coalesce(username,'')) LIKE $2
			     OR lower(coalesce(first_name,'') || ' ' || coalesce(last_name,'')) LIKE $2)
			 UNION ALL
			SELECT entity_id, 'realm'::text,
			       coalesce(slug,''), coalesce(name,''), coalesce(profile,'')
			  FROM community_realm
			 WHERE is_active
			   AND (lower(coalesce(slug,'')) LIKE $2 OR lower(coalesce(name,'')) LIKE $2)
			 UNION ALL
			SELECT entity_id, 'bot'::text,
			       coalesce(handle,''), coalesce(name,''), coalesce(profile,'')
			  FROM bot_bot
			 WHERE is_active AND NOT is_system
			   AND (lower(coalesce(handle,'')) LIKE $2 OR lower(coalesce(name,'')) LIKE $2)
		) AS found
		 WHERE entity_id IS NOT NULL
		   AND entity_id <> $4
		   AND ($5::text[] IS NULL OR kind = ANY($5))
		 ORDER BY (lower(handle) LIKE $1) DESC, length(handle), lower(handle)
		 LIMIT $3`,
		prefix, contains, limit, viewerEntityID, want)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	found := make([]FoundEntity, 0, limit)
	for rows.Next() {
		var entity FoundEntity
		if err := rows.Scan(&entity.EntityID, &entity.Kind, &entity.Handle,
			&entity.Name, &entity.Profile); err != nil {
			return nil, err
		}
		found = append(found, entity)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(found) == 0 {
		return nil, nil
	}

	blocked, err := blockedEntityIDs(ctx, pool, viewerEntityID)
	if err != nil {
		// A blocking read that failed must not turn into results that route
		// around a block. Refusing the search is the safe direction, and the
		// same one ResolveMentions takes.
		return nil, err
	}

	kept := make([]FoundEntity, 0, len(found))
	for _, entity := range found {
		if blocked[entity.EntityID] {
			continue
		}
		kept = append(kept, entity)
	}
	return kept, nil
}

// kindFilter narrows to the requested kinds, or nil for all of them.
//
// Unknown values are dropped rather than rejected: a client asking for a kind
// this service does not have should get the kinds it does, not an error about
// a typo in a filter.
func kindFilter(kinds []string) []string {
	if len(kinds) == 0 {
		return nil
	}
	allowed := map[string]bool{
		EntityKindUser: true, EntityKindRealm: true, EntityKindBot: true,
	}
	want := make([]string, 0, len(kinds))
	seen := map[string]bool{}
	for _, kind := range kinds {
		kind = strings.ToLower(strings.TrimSpace(kind))
		if allowed[kind] && !seen[kind] {
			seen[kind] = true
			want = append(want, kind)
		}
	}
	if len(want) == 0 {
		return nil
	}
	return want
}
