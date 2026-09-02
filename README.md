# developer_service

The chatterloop developer API. Token-authenticated access to conversations,
mentions and the realtime event stream, for anything that is not a person at a
browser — bots first, third-party integrations next.

```
                     Authorization: Bearer clt_…
  bot / integration ──────────────────────────────► developer_service (Go)
                                                          │
                        ┌─────────────────────────────────┼──────────────────┐
                        ▼                 ▼               ▼                  ▼
                    Postgres           Mongo           Redis             RabbitMQ
                 entity_token      conversations   events_<entity>      send_push
                 permissions       messages        (sub + pub)          bump_chat_score
                 comment text      notifications
```

## Routes

| method | path | scope |
|---|---|---|
| `GET` | `/health` | — (orchestrator liveness) |
| `GET` | `/ready` | — (dependency reachability) |
| `GET` | `/v1/whoami` | authentication only |
| `GET` | `/v1/events` | `events.subscribe` |
| `GET` | `/v1/conversations/{id}/messages` | `messages.read` |
| `GET` | `/v1/mentions/comments` | `notifications.read` |
| `POST` | `/v1/messages/send` | `messages.send` |

Versioned in the path from the first commit: the consumers are deployed
services, not browsers that reload, so a shape change needs both versions live
while clients roll forward.

## Why this is a separate service, in Go

`user_service` runs under gunicorn with **three sync workers**, where one
held-open SSE stream occupies a whole worker for its lifetime — three
subscribers would deadlock the API the rest of the platform depends on. A
goroutine per connection costs kilobytes instead of a process slot.

The REST routes live here too rather than in Django because the developer API
is going to be extracted anyway. Starting it in its own service makes that a
move rather than a rewrite.

## Authorization is an intersection, and it is strict

A request is allowed only when **both** hold:

1. the scope is on the token, and
2. the owning entity has an **explicit grant** in `entity_entitypermission`.

The second half is deliberately stricter than the platform's own resolver,
which would also honour role, entity-type and platform defaults. One of those
defaults is why: Django's `_account_in_good_standing` returns `True` for any
entity with **no** `Account` — its reverse one-to-one raises an `AttributeError`
subclass, so `getattr(entity, "users", None)` yields `None` — which means bots
and realm entities hold *every* global permission by platform default,
`messages.send` included. Honouring that here would leave the entity half of
the intersection constraining nothing at all.

So a capability is granted on purpose, per entity, and the token narrows it
further. Revoking the grant narrows every token that entity owns, with no token
edit and no revocation list to fan out.

## Issuing a token by hand

Django owns the `entity_token` table; nothing issues tokens yet. Until the
developer dashboard exists, insert rows directly.

**1 — generate the token and its hash.** The secret is shown once; the database
only ever holds its SHA-256.

```bash
python3 -c "
import hashlib, secrets, uuid
prefix, secret = secrets.token_hex(6), secrets.token_hex(32)
token = f'clt_{prefix}_{secret}'
print('id      ', uuid.uuid4())
print('prefix  ', prefix)
print('hash    ', hashlib.sha256(token.encode()).hexdigest())
print()
print('TOKEN (store now, it is not recoverable):')
print(token)
"
```

**2 — insert the token.** Substitute the four values printed above and the
entity that will own it.

```sql
INSERT INTO entity_token
  (id, entity_id, name, description, prefix, token_hash, scopes,
   realm_id, created_by_id, created_at, expires_at, last_used_at,
   revoked_at, is_active)
VALUES
  ('<id>', '<entity-id>', 'rag pipeline (prod)', '',
   '<prefix>', '<hash>',
   '["messages.read","notifications.read","messages.send","events.subscribe"]'::jsonb,
   NULL, NULL, NOW(), NOW() + INTERVAL '90 days', NULL, NULL, true);
```

`scopes` is a JSON array of catalog codenames. `expires_at` may be `NULL` for
no expiry — a service credential that never expires should be a decision, not
a default.

**3 — grant the same capabilities to the entity.** Without these the token is
issued but inert, because authorization is an intersection.

```sql
INSERT INTO entity_entitypermission
  (id, entity_id, permission, effect, realm_id, reason, created_by_id, created_at, expires_at)
SELECT gen_random_uuid()::text, '<entity-id>', codename, 'grant', NULL,
       'developer API access', NULL, NOW(), NULL
  FROM unnest(ARRAY[
    'messages.read', 'notifications.read', 'messages.send', 'events.subscribe'
  ]) AS codename;
```

**4 — check it.**

```bash
curl -H "Authorization: Bearer clt_…" https://<host>/v1/whoami
```

To revoke: Django admin (Entity → Tokens → *Revoke selected*), or
`UPDATE entity_token SET revoked_at = NOW(), is_active = false WHERE prefix = '<prefix>';`

## Sending a message

`POST /v1/messages/send` performs the fan-out that makes a message a message
rather than a row: it un-archives the conversation for every participant,
updates the conversation preview, publishes a realtime frame per recipient,
bumps the chat interaction score, and sends push notifications — with a
distinct payload for anyone `@mentioned`, who gets the mention push *instead
of* the plain one rather than as well as it.

Realtime frames are published straight to `events_<entity_id>` in the
platform's own envelope, so one write reaches Node's SSE bridge for browsers
and this service's `/v1/events` for API clients.

Push and interaction scoring are handed to the Go worker over queues it already
consumes (`send_push`, `bump_chat_score`), so Firebase and the scoring rules
each keep one implementation.

### Two side effects this does NOT perform

- **Link previews.** The platform resolves them by calling an internal preview
  service and re-triggering the frame. A message sent through this API renders
  a bare URL instead of a preview card.
- **Content tagging.** The platform gates it on a Redis presence key and
  publishes nothing when the moderation service is down, because that service's
  database scour picks the content up on its next start — the designed path,
  not a degraded one. This leans on the same path.

Both are visible gaps, listed here rather than papered over.

## Ported logic, and what keeps it honest

Three pieces are hand-ported from the platform, and each carries the reason it
could not simply be shared:

| ported | from | guard |
|---|---|---|
| token verification | `entity/services/tokens.py` | format is all-hex + plain SHA-256, chosen to have no clever part to get wrong |
| mention extraction | `transformers.js` / `comment_mentions.py` | Go's RE2 has no lookahead; a drift test reads both live sources |
| conversation access | `models/realms.js` `isRealmMember` | one documented divergence — see below |

`ExtractMentions` is hand-rolled rather than a regex because the platform's
pattern backtracks: `.` is both a valid handle character and a valid
terminator, so `@foo.bar<` matches `foo`, which no leftmost-longest RE2 rewrite
finds. Both it and the HTML sanitiser were verified by *running* the platform's
JavaScript, not by reading it — two sanitiser cases were counter-intuitive and
the reading was wrong the first time.

**Access divergence:** the platform auto-joins a caller to a public
conference's contacts before checking membership. Access is granted here on the
same condition, but the contact write is not performed — a developer credential
quietly adding rows to somebody's contact list as a side effect of reading is
not a behaviour worth reproducing.

## Development

```bash
cp .env.example .env    # fill in the platform's Postgres, Mongo and Redis
go test ./...
go run ./cmd/developer
```

`go test ./...` needs no infrastructure: everything covered is pure logic. The
drift test skips itself when the platform sources are not on disk.
