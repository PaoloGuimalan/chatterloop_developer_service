package platform

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// ErrNoAccess mirrors the platform's "You do not have access to this
// conversation". Returned for a conversation the caller is not party to and
// for one that does not exist, so the two stay indistinguishable.
var ErrNoAccess = errors.New("no access to this conversation")

// AssertMember ports the platform's isRealmMember (server/reusables/models/
// realms.js), which despite the name is the access check for EVERY
// conversation, realm-backed or not.
//
//  1. A conversation id that matches a community_realm row is a realm
//     conversation, and membership is decided there.
//  2. Anything else is a DM, provable by either an entity_connection row (the
//     normal "add contact" flow) or a Mongo conversation listing the entity as
//     a participant. Not every DM has a Connection - some only ever existed as
//     a Conversations doc - so either is sufficient.
//
// ONE DELIBERATE DIVERGENCE: the platform AUTO-JOINS the caller to a public
// conference's contacts before checking membership. Access is granted here on
// the same condition, but the contact write is not performed. A developer
// credential quietly adding rows to somebody's contact list as a side effect of
// reading a conversation is not a behaviour worth reproducing, and this service
// has no write path to that table by design.
func AssertMember(
	ctx context.Context,
	db *mongo.Database,
	pool *pgxpool.Pool,
	conversationID, entityID string,
) error {
	var (
		realmEntityID *string
		realmType     string
		isPrivate     bool
	)
	err := pool.QueryRow(ctx, `
		SELECT entity_id, type, is_private
		  FROM community_realm
		 WHERE realm_id = $1`, conversationID,
	).Scan(&realmEntityID, &realmType, &isPrivate)

	switch {
	case err == nil:
		return assertRealmMember(ctx, pool, conversationID, entityID,
			realmEntityID, realmType, isPrivate)
	case errors.Is(err, pgx.ErrNoRows):
		return assertDirectMember(ctx, db, pool, conversationID, entityID)
	default:
		return err
	}
}

func assertRealmMember(
	ctx context.Context,
	pool *pgxpool.Pool,
	realmID, entityID string,
	realmEntityID *string,
	realmType string,
	isPrivate bool,
) error {
	// An open conference is readable by anyone; the platform expresses this by
	// auto-joining, we express it by allowing.
	if realmType == "conference" && !isPrivate {
		return nil
	}

	// A page's own entity can never appear as a Member row of its own realm -
	// community_member only ever holds personal accounts - so once acting AS
	// this realm the lookup below would always miss and wrongly deny it access
	// to its own conversation.
	if realmEntityID != nil && *realmEntityID == entityID {
		return nil
	}

	var memberID string
	err := pool.QueryRow(ctx, `
		SELECT member_id FROM community_member
		 WHERE entity_id = $1 AND realm_id = $2 LIMIT 1`,
		entityID, realmID,
	).Scan(&memberID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNoAccess
	}
	return err
}

func assertDirectMember(
	ctx context.Context,
	db *mongo.Database,
	pool *pgxpool.Pool,
	conversationID, entityID string,
) error {
	var one int
	err := pool.QueryRow(ctx, `
		SELECT 1 FROM entity_connection
		 WHERE connection_id = $1 AND (action_by_id = $2 OR involved_entity_id = $2)
		 LIMIT 1`, conversationID, entityID,
	).Scan(&one)
	if err == nil {
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	count, err := db.Collection("conversations").CountDocuments(ctx, bson.M{
		"conversationID":  conversationID,
		"participant_ids": entityID,
	})
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrNoAccess
	}
	return nil
}

// Receiver is one party to a conversation.
type Receiver struct {
	EntityID string
	Username string
}

// Receivers ports GetAllReceivers (server/reusables/models/messages.js): three
// sources tried in order, first non-empty wins.
//
//  1. entity_connection, for a DM keyed by its connection id.
//  2. community_member, for a realm conversation.
//  3. the Mongo conversation's participant_ids, for conversations that were
//     never either.
//
// Each query anchors on entity_entity and LEFT JOINs both detail tables rather
// than starting from user_account, because a participant can be a switched-to
// realm entity, which has no user_account row.
func Receivers(
	ctx context.Context,
	db *mongo.Database,
	pool *pgxpool.Pool,
	conversationID string,
) ([]Receiver, error) {
	byConnection, err := queryReceivers(ctx, pool, `
		SELECT p.id, COALESCE(u.username, r.slug, b.handle, '')
		  FROM entity_connection uc
		  JOIN entity_entity p ON p.id = uc.involved_entity_id
		  LEFT JOIN user_account u ON u.entity_id = p.id AND p.type = 'user'
		  LEFT JOIN community_realm r ON r.entity_id = p.id AND p.type = 'realm'
		  LEFT JOIN bot_bot b ON b.entity_id = p.id AND p.type = 'bot'
		 WHERE uc.connection_id = $1`, conversationID)
	if err != nil {
		return nil, err
	}
	if len(byConnection) > 0 {
		return byConnection, nil
	}

	byMembership, err := queryReceivers(ctx, pool, `
		SELECT p.id, COALESCE(u.username, r.slug, b.handle, '')
		  FROM community_member uc
		  JOIN entity_entity p ON p.id = uc.entity_id
		  LEFT JOIN user_account u ON u.entity_id = p.id AND p.type = 'user'
		  LEFT JOIN community_realm r ON r.entity_id = p.id AND p.type = 'realm'
		  LEFT JOIN bot_bot b ON b.entity_id = p.id AND p.type = 'bot'
		 WHERE uc.realm_id = $1`, conversationID)
	if err != nil {
		return nil, err
	}
	if len(byMembership) > 0 {
		return byMembership, nil
	}

	var raw bson.M
	if err := db.Collection("conversations").
		FindOne(ctx, bson.M{"conversationID": conversationID}).
		Decode(&raw); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, err
	}
	participants := stringSlice(raw["participant_ids"])
	if len(participants) == 0 {
		return nil, nil
	}

	return queryReceivers(ctx, pool, `
		SELECT p.id, COALESCE(u.username, r.slug, b.handle, '')
		  FROM entity_entity p
		  LEFT JOIN user_account u ON u.entity_id = p.id AND p.type = 'user'
		  LEFT JOIN community_realm r ON r.entity_id = p.id AND p.type = 'realm'
		  LEFT JOIN bot_bot b ON b.entity_id = p.id AND p.type = 'bot'
		 WHERE p.id = ANY($1)`, participants)
}

func queryReceivers(ctx context.Context, pool *pgxpool.Pool, sql string, arg any) ([]Receiver, error) {
	rows, err := pool.Query(ctx, sql, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var receivers []Receiver
	for rows.Next() {
		var receiver Receiver
		if err := rows.Scan(&receiver.EntityID, &receiver.Username); err != nil {
			return nil, err
		}
		receivers = append(receivers, receiver)
	}
	return receivers, rows.Err()
}

// RealmName is the display name of a realm conversation, parent-qualified the
// way the platform renders it ("Server | Channel"). Empty for a DM.
func RealmName(ctx context.Context, pool *pgxpool.Pool, realmID string) string {
	var (
		name     string
		parentID *string
	)
	err := pool.QueryRow(ctx, `
		SELECT name, parent_id FROM community_realm WHERE realm_id = $1`, realmID,
	).Scan(&name, &parentID)
	if err != nil {
		return ""
	}
	if parentID != nil && *parentID != "" {
		if parent := RealmName(ctx, pool, *parentID); parent != "" {
			return parent + " | " + name
		}
	}
	return name
}
