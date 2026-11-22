package realmaccess

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"sort"
)

var ErrUnavailable = errors.New("realm write scope unavailable")

type writeNode struct {
	id, entity string
	parent     *string
	active     bool
}

// Only SDK persistence uses this transaction. Source pages and resolved realm
// targets share the same root-first locks as lifecycle transitions.
func LockWrite(ctx context.Context, pool *pgxpool.Pool, requiredRealms, entities []string) (pgx.Tx, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	handedOff := false
	defer func() {
		if !handedOff {
			_ = tx.Rollback(ctx)
		}
	}()
	read := func() (map[string]writeNode, map[string]bool, error) {
		rows, err := tx.Query(ctx, `WITH RECURSIVE ancestry AS (
   SELECT id AS origin,id,entity_id,parent_id,(is_active AND deletion_requested_at IS NULL) AS active FROM community_realm
   WHERE id=ANY($1::text[]) OR realm_id=ANY($1::text[]) OR entity_id=ANY($2::text[])
   UNION SELECT chain.origin,p.id,p.entity_id,p.parent_id,(p.is_active AND p.deletion_requested_at IS NULL)
   FROM ancestry chain JOIN community_realm p ON p.id=chain.parent_id
  ) SELECT origin,id,entity_id,parent_id,active FROM ancestry`, requiredRealms, entities)
		if err != nil {
			return nil, nil, err
		}
		defer rows.Close()
		nodes := map[string]writeNode{}
		starts := map[string]bool{}
		for rows.Next() {
			var origin string
			var n writeNode
			if err := rows.Scan(&origin, &n.id, &n.entity, &n.parent, &n.active); err != nil {
				return nil, nil, err
			}
			nodes[n.id] = n
			starts[origin] = true
		}
		return nodes, starts, rows.Err()
	}
	validate := func(nodes map[string]writeNode, starts map[string]bool) error {
		for _, id := range requiredRealms {
			if !starts[id] {
				return ErrUnavailable
			}
		}
		rows, err := tx.Query(ctx, `SELECT id,type FROM entity_entity WHERE id=ANY($1::text[])`, entities)
		if err != nil {
			return err
		}
		found := map[string]bool{}
		for rows.Next() {
			var id, kind string
			if err := rows.Scan(&id, &kind); err != nil {
				rows.Close()
				return err
			}
			found[id] = true
			if kind == "realm" {
				exists := false
				for start := range starts {
					if nodes[start].entity == id {
						exists = true
					}
				}
				if !exists {
					rows.Close()
					return ErrUnavailable
				}
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, id := range entities {
			if id != "" && !found[id] {
				return ErrUnavailable
			}
		}
		for start := range starts {
			seen := map[string]bool{}
			id := start
			for {
				n, ok := nodes[id]
				if !ok || seen[id] || !n.active {
					return ErrUnavailable
				}
				seen[id] = true
				if n.parent == nil {
					break
				}
				id = *n.parent
			}
		}
		return nil
	}
	nodes, starts, err := read()
	if err != nil {
		return nil, err
	}
	if err := validate(nodes, starts); err != nil {
		return nil, err
	}
	if len(starts) == 0 {
		return nil, nil
	}
	type lock struct {
		id, root string
		depth    int
	}
	var locks []lock
	for id, n := range nodes {
		root := id
		depth := 0
		for n.parent != nil {
			root = *n.parent
			n = nodes[root]
			depth++
		}
		locks = append(locks, lock{id, root, depth})
	}
	sort.Slice(locks, func(i, j int) bool {
		a, b := locks[i], locks[j]
		if a.root != b.root {
			return a.root < b.root
		}
		if a.depth != b.depth {
			return a.depth < b.depth
		}
		return a.id < b.id
	})
	for _, l := range locks {
		var id string
		if err := tx.QueryRow(ctx, `SELECT id FROM community_realm WHERE id=$1 FOR SHARE`, l.id).Scan(&id); err != nil {
			return nil, ErrUnavailable
		}
	}
	fresh, origins, err := read()
	if err != nil {
		return nil, err
	}
	if len(fresh) != len(nodes) {
		return nil, ErrUnavailable
	}
	for id := range fresh {
		if _, held := nodes[id]; !held {
			return nil, ErrUnavailable
		}
	}
	if err := validate(fresh, origins); err != nil {
		return nil, err
	}
	handedOff = true
	return tx, nil
}
