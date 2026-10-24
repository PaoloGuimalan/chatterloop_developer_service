package platform

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrAnonymousOutsideCommunity = errors.New("anonymous contributions require a community post")

type Community struct {
	RealmID   string `json:"realm_id"`
	EntityID  string `json:"entity_id"`
	IsPrivate bool   `json:"is_private"`
}

// postAccess is resolved from storage, never from client audience claims.
type postAccess struct {
	AuthorEntityID   string
	OrdinaryReadable bool
	CommunityCount   int
	CommunityActive  bool
	CommunityPrivate bool
	CommunityMember  bool
	Community        *Community
}

func (p postAccess) permits(viewer string, write bool) bool {
	if p.CommunityCount == 0 {
		return p.OrdinaryReadable
	}
	return viewer != "" && p.CommunityCount == 1 && p.CommunityActive &&
		((!write && !p.CommunityPrivate) || p.CommunityMember)
}

type postQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func loadPostAccess(ctx context.Context, db postQuerier, postID, viewer string) (*postAccess, error) {
	if postID == "" {
		return nil, ErrPostNotFound
	}
	var p postAccess
	var realmID, entityID string
	err := db.QueryRow(ctx, `
		SELECT p.entity_id,
		       COALESCE((COALESCE(p.privacy_status, 'public') = 'public' OR p.entity_id = NULLIF($2, '')
		        OR (p.privacy_status = 'connections' AND EXISTS (
		          SELECT 1 FROM entity_connection c
		          WHERE c.status = TRUE AND
		          ((c.action_by_id = p.entity_id AND c.involved_entity_id = $2)
		           OR (c.involved_entity_id = p.entity_id AND c.action_by_id = $2))))
		        OR (p.privacy_status = 'custom' AND EXISTS (
		          SELECT 1 FROM newsfeed_postprivacy pp WHERE pp.post_id = p.post_id AND pp.allowed_entity_id = $2))), false),
		       scope.total, COALESCE(scope.active, false), COALESCE(scope.private, false),
		       COALESCE(scope.member, false), COALESCE(scope.realm_id, ''), COALESCE(scope.entity_id, '')
		FROM newsfeed_post p
		CROSS JOIN LATERAL (
		  SELECT COUNT(DISTINCT r.realm_id)::int AS total, bool_and(r.is_active) AS active,
		         bool_or(r.is_private) AS private, MIN(r.realm_id) AS realm_id, MIN(r.entity_id) AS entity_id,
		         bool_and(EXISTS (SELECT 1 FROM community_member m WHERE m.realm_id = r.id AND m.entity_id = $2)) AS member
		  FROM newsfeed_posttag t JOIN community_realm r ON r.entity_id = t.entity_id AND r.type = 'community'
		  WHERE t.post_id = p.post_id
		) scope
		WHERE p.post_id = $1 AND p.deleted_at IS NULL`, postID, viewer).Scan(
		&p.AuthorEntityID, &p.OrdinaryReadable, &p.CommunityCount, &p.CommunityActive,
		&p.CommunityPrivate, &p.CommunityMember, &realmID, &entityID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrPostNotFound
	}
	if err != nil {
		return nil, err
	}
	if p.CommunityCount > 0 {
		p.Community = &Community{RealmID: realmID, EntityID: entityID, IsPrivate: p.CommunityPrivate}
	}
	return &p, nil
}

func validateCommentAnonymity(anonymous bool, communities int) error {
	if anonymous && communities == 0 {
		return ErrAnonymousOutsideCommunity
	}
	return nil
}

// Marshal defensively: even internal callers constructing a response by hand
// cannot accidentally publish the stored owner of an anonymous contribution.
func (p PostThread) MarshalJSON() ([]byte, error) {
	type plain PostThread
	if p.IsAnonymous {
		p.AuthorEntityID = ""
		p.AuthorHandle = ""
	}
	return json.Marshal(plain(p))
}

func (p PostComment) MarshalJSON() ([]byte, error) {
	type plain PostComment
	if p.IsAnonymous {
		p.AuthorEntityID = ""
		p.AuthorHandle = ""
	}
	return json.Marshal(plain(p))
}

func (p Mention) MarshalJSON() ([]byte, error) {
	type plain Mention
	if p.IsAnonymous {
		p.AuthorEntityID = ""
		p.AuthorHandle = ""
	}
	return json.Marshal(plain(p))
}

// CommunityFrameAllowed rechecks a scoped notification when it leaves an open
// event stream. An earlier producer check cannot authorize a former member.
func CommunityFrameAllowed(ctx context.Context, pool *pgxpool.Pool, viewer, payload string) bool {
	return communityFrameAllowed(payload, viewer, func(postID string) (*postAccess, error) {
		return loadPostAccess(ctx, pool, postID, viewer)
	})
}

func communityFrameAllowed(payload, viewer string, lookup func(string) (*postAccess, error)) bool {
	var envelope struct {
		Message struct {
			Result json.RawMessage `json:"result"`
		} `json:"message"`
	}
	if json.Unmarshal([]byte(payload), &envelope) != nil {
		return true
	}
	var result struct {
		PostID      string `json:"post_id"`
		CommunityID string `json:"community_entity_id"`
	}
	if json.Unmarshal(envelope.Message.Result, &result) != nil || result.CommunityID == "" {
		return true
	}
	if result.PostID == "" {
		return false
	}
	access, err := lookup(result.PostID)
	return err == nil && access != nil && access.CommunityCount == 1 && access.CommunityMember && access.permits(viewer, false)
}
