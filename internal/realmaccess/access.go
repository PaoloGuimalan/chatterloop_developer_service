// Package realmaccess checks availability only, never membership or privacy.
package realmaccess

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type Querier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}
type State struct{ Found, Active bool }

func Check(ctx context.Context, db Querier, identifier string) (State, error) {
	var state State
	err := db.QueryRow(ctx, `WITH targets AS (SELECT * FROM community_realm WHERE id=$1 OR realm_id=$1)
	 SELECT EXISTS(SELECT 1 FROM targets), EXISTS(SELECT 1 FROM targets)
	 AND NOT EXISTS(SELECT 1 FROM targets t WHERE NOT `+ActiveSQL("t")+`)`, identifier).Scan(&state.Found, &state.Active)
	return state, err
}

// entityExpression is a fixed, code-owned SQL column expression, never input.
func EntityAvailableSQL(entityExpression string) string {
	return `EXISTS (SELECT 1 FROM entity_entity lifecycle_owner WHERE lifecycle_owner.id = ` + entityExpression + `
	 AND (lifecycle_owner.type <> 'realm' OR EXISTS (SELECT 1 FROM community_realm lifecycle_realm
	 WHERE lifecycle_realm.entity_id = lifecycle_owner.id AND ` + ActiveSQL("lifecycle_realm") + `)))`
}

func ActiveSQL(alias string) string {
	return `(` + alias + `.is_active AND ` + alias + `.deletion_requested_at IS NULL AND CASE WHEN ` + alias + `.parent_id IS NULL THEN TRUE ELSE EXISTS(
 WITH RECURSIVE ancestry AS (
 SELECT seed.id,seed.parent_id,seed.is_active,seed.deletion_requested_at FROM community_realm seed WHERE seed.id=` + alias + `.parent_id
 UNION SELECT p.id,p.parent_id,p.is_active,p.deletion_requested_at FROM ancestry a
 JOIN community_realm p ON p.id=a.parent_id WHERE a.parent_id IS NOT NULL
 ) SELECT COUNT(*) FROM ancestry HAVING COUNT(*)>0
 AND SUM(CASE WHEN is_active=TRUE AND deletion_requested_at IS NULL THEN 0 ELSE 1 END)=0
 AND SUM(CASE WHEN parent_id IS NULL THEN 1 ELSE 0 END)>0) END)`
}
