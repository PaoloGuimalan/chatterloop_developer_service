package platform

import (
	"context"
	"developer_service/internal/realmaccess"
	"errors"
	"testing"
	"time"

	"developer_service/internal/testutil"
)

func TestSDKWriteScopeSerializesSourcePageAndRequiresResolvedTarget(t *testing.T) {
	db := testutil.Postgres(t)
	ctx := t.Context()
	if _, err := db.Exec(ctx, `INSERT INTO entity_entity VALUES ('page-entity','realm'),('user','user'); INSERT INTO community_realm(id,realm_id,entity_id,type) VALUES ('page','page','page-entity','page')`); err != nil {
		t.Fatal(err)
	}
	tx, err := realmaccess.LockWrite(ctx, db, nil, []string{"page-entity"})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	timeout, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := db.Exec(timeout, `UPDATE community_realm SET is_active=FALSE WHERE id='page'`); err == nil {
		t.Fatal("source deactivated while SDK persistence held its scope")
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `UPDATE community_realm SET is_active=FALSE WHERE id='page'`); err != nil {
		t.Fatal(err)
	}
	if _, err := realmaccess.LockWrite(ctx, db, nil, []string{"page-entity"}); err == nil {
		t.Fatal("inactive page admitted to ordinary comment write")
	}
	if _, err := realmaccess.LockWrite(ctx, db, []string{"deleted-room"}, []string{"user"}); err == nil {
		t.Fatal("deleted group became SDK direct conversation")
	}
}

func TestInactiveRealmBlocksDeveloperMessagesBeforeMongoAccess(t *testing.T) {
	db := testutil.Postgres(t)
	ctx := t.Context()
	_, err := db.Exec(ctx, `CREATE TABLE entity_connection (connection_id text, action_by_id text, involved_entity_id text, status boolean);
	 INSERT INTO community_realm(id,realm_id,entity_id,type,is_active)
	 VALUES ('room','room','room-entity','conference',FALSE)`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if failure := recover(); failure != nil {
			t.Fatalf("inactive realm reached Mongo: %v", failure)
		}
	}()
	if err := AssertMember(ctx, nil, db, "room", "owner"); !errors.Is(err, ErrNoAccess) {
		t.Fatalf("inactive room authorized: %v", err)
	}
}

func TestDeveloperPostReadsDenyInactiveAuthorsAndDeletingDestinations(t *testing.T) {
	db := testutil.Postgres(t)
	ctx := t.Context()
	_, err := db.Exec(ctx, `CREATE TABLE newsfeed_post (post_id text, entity_id text, privacy_status text, deleted_at timestamptz);
	 CREATE TABLE newsfeed_posttag (post_id text, entity_id text);
	 CREATE TABLE entity_connection (action_by_id text, involved_entity_id text, status boolean);
	 CREATE TABLE newsfeed_postprivacy (post_id text, allowed_entity_id text);
	 INSERT INTO entity_entity VALUES ('user','user'),('page-entity','realm');
	 INSERT INTO community_realm(id,realm_id,entity_id,type,is_active) VALUES ('page','page','page-entity','page',FALSE);
	 INSERT INTO community_realm(id,realm_id,entity_id,type,deletion_requested_at) VALUES ('community','community','community-entity','community',NOW());
	 INSERT INTO newsfeed_post(post_id,entity_id,privacy_status) VALUES ('page-post','page-entity','public'),('scoped','user','public');
	 INSERT INTO newsfeed_posttag VALUES ('scoped','community-entity');
	 INSERT INTO community_member VALUES ('member','community','viewer');`)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"page-post", "scoped"} {
		access, err := loadPostAccess(ctx, db, id, "viewer")
		if err != nil {
			t.Fatal(err)
		}
		if access.permits("viewer", false) {
			t.Fatalf("inactive content %s remained readable", id)
		}
	}
}
