package auth

import (
	"errors"
	"testing"

	"developer_service/internal/testutil"
)

func TestTokenCannotOutliveInactiveRealmOwnerOrConfinement(t *testing.T) {
	db := testutil.Postgres(t)
	ctx := t.Context()
	_, err := db.Exec(ctx, `CREATE TABLE entity_token (
	 id text, entity_id text, realm_id text, name text, token_hash text, scopes jsonb,
	 is_active boolean, revoked_at timestamptz, expires_at timestamptz, rate_limit_int integer, rate_limit_type text, prefix text);
	 INSERT INTO entity_entity VALUES ('page-entity','realm');
	 INSERT INTO community_realm(id,realm_id,entity_id,type) VALUES ('page','page','page-entity','page'), ('server','server','server-entity','server');`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(ctx, `INSERT INTO entity_token(id,entity_id,name,token_hash,scopes,is_active,prefix)
	 VALUES ('token','page-entity','Fixture',$1,'["events.subscribe"]',TRUE,$2)`, hashToken(goodToken), goodPrefix)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, db, goodToken); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `UPDATE community_realm SET is_active=FALSE WHERE id='page'`); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, db, goodToken); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("inactive page token remained usable: %v", err)
	}
	if _, err := db.Exec(ctx, `UPDATE community_realm SET is_active=TRUE WHERE id='page';
	 UPDATE community_realm SET is_active=FALSE WHERE id='server'; UPDATE entity_token SET realm_id='server'`); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, db, goodToken); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("inactive confinement remained usable: %v", err)
	}
}
