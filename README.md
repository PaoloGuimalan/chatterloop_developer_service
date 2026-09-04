# developer_service

The chatterloop developer API. Token-authenticated access to conversations,
comments, mentions and the realtime event stream, for anything that is not a
person at a browser — bots first, third-party integrations next.

```
                     Authorization: Bearer clt_…
  bot / integration ──────────────────────────────► developer_service (Go)
                                                          │
                        ┌─────────────────────────────────┼──────────────────┐
                        ▼                 ▼               ▼                  ▼
                    Postgres           Mongo           Redis             RabbitMQ
                 entity_token      conversations   events_<entity>      send_push
                 permissions       messages        (sub + pub)          bump_chat_score
                 comments          notifications                        update_ranking_score
                 posts, handles                                         bump_interest_affinity
```

## Routes

| method | path | scope |
|---|---|---|
| `GET` | `/health` | — (orchestrator liveness) |
| `GET` | `/ready` | — (dependency reachability) |
| `GET` | `/v1/whoami` | authentication only |
| `GET` | `/v1/events` | `events.subscribe` |
| `GET` | `/v1/conversations/{id}/messages` | `messages.read` |
| `GET` | `/v1/conversations/{id}/replies` | `messages.read` |
| `GET` | `/v1/mentions/comments` | `notifications.read` |
| `GET` | `/v1/comments/replies` | `notifications.read` |
| `GET` | `/v1/posts/{id}/comments` | `notifications.read` |
| `POST` | `/v1/messages/send` | `messages.send` |
| `POST` | `/v1/comments` | `comments.create` |

Versioned in the path from the first commit: the consumers are deployed
services, not browsers that reload, so a shape change needs both versions live
while clients roll forward.

**[API.md](API.md) is the reference** — every field, header, limit, response
shape and error code, plus how to run and deploy this. What follows here is why
it is built the way it is.

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
   revoked_at, is_active, rate_limit_int, rate_limit_type)
VALUES
  ('<id>', '<entity-id>', 'rag pipeline (prod)', '',
   '<prefix>', '<hash>',
   '["messages.read","notifications.read","messages.send","comments.create","events.subscribe"]'::jsonb,
   NULL, NULL, NOW(), NOW() + INTERVAL '90 days', NULL, NULL, true, NULL, NULL);
```

`scopes` is a JSON array of catalog codenames. `expires_at` may be `NULL` for
no expiry — a service credential that never expires should be a decision, not
a default.

`rate_limit_int` / `rate_limit_type` are a pair: both `NULL` (the default for
every existing token) means unlimited; set both together to cap how many
requests *this one credential* may make per window, independent of every
other token the same entity holds. `rate_limit_type` is one of `second`,
`minute`, `hour`, `day`, `week`, `month`, `year` — e.g. `(100000, 'month')`
for a subscription tier, `(5, 'second')` for an internal service. Setting one
without the other is rejected by `Token.clean()` on the Django side.

Enforced by this service (`internal/auth/ratelimit.go`) against Redis, on
every authenticated route — a token over its limit gets
`429 Too Many Requests` with a `Retry-After` header, before the request
reaches its handler. `month`/`year` windows reset on the UTC calendar
boundary (the 1st of the month/January), not on a fixed 30- or 365-day
timer.

**3 — grant the same capabilities to the entity.** Without these the token is
issued but inert, because authorization is an intersection.

```sql
INSERT INTO entity_entitypermission
  (id, entity_id, permission, effect, realm_id, reason, created_by_id, created_at, expires_at)
SELECT gen_random_uuid()::text, '<entity-id>', codename, 'grant', NULL,
       'developer API access', NULL, NOW(), NULL
  FROM unnest(ARRAY[
    'messages.read', 'notifications.read', 'messages.send', 'comments.create',
    'events.subscribe'
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

## Being replied to, without being named

A bot that only answers `@handle` cannot hold a conversation: the second turn
has to re-address it, every time. The two `replies` routes exist so a consumer
can answer a *direct reply to something it said* without the handle, and
without loosening anything else.

**Why the realtime frame cannot answer this.** `messages_list` carries the
conversation, the sending entity, and a per-recipient `mentioner` — that is
all. No message id, no `replyingTo`. So a consumer asking "did that message
reply to me?" has one honest move: read. Doing it as a history fetch costs a
full window on *every* message in *every* conversation it belongs to. Answered
here it is two indexed lookups returning, usually, nothing.

```
GET /v1/conversations/{id}/replies?limit=25   → messages replying to MINE
GET /v1/comments/replies?limit=25             → "somebody replied to your comment"
```

Whose replies is taken from the **token**, never from a parameter — the same
rule as `/v1/mentions/comments`. There is no version of either that can be
pointed at somebody else's messages.

`/v1/conversations/{id}/messages` also gained
`replying_to_sender_entity_id` / `replying_to_sender_handle` on every row.
Resolved server-side because the parent is regularly *outside* the window the
caller asked for — a follow-up an hour later replies to a message forty turns
back — so a client computing it from the returned slice would read "unknown"
exactly when the answer matters.

### Telling a comment reply from a comment on your post

Django writes both branches of `CommentsView.post()` as type `post_comment`:
"commented on your post" goes to the post's author, "replied to your comment"
goes to the replied-to comment's author. Only the second is a reply, and only
the second should make a bot speak — answering the first means answering every
comment on every post it ever made.

They are separated **structurally**, on the referenced comment's own row —
`parent_comment_id IS NULL` is a top-level comment, `NOT NULL` is a reply — and
not on `content_headline`, which is a display string. The recipient does the
rest: Django addresses a reply notification to `replied_to.entity`, so a row
that arrives is already *somebody replied to something you wrote*.

**A platform gap this inherits:** that branch is skipped when the replier is
the post's author (`post.entity != entity` is part of the condition), so a post
owner replying to a comment on their own post notifies nobody and is invisible
here. Closing it is a change in `user_service`, not this service.

## Posting a comment

`POST /v1/comments` is the route a bot needed and did not have. Reading a
comment mention worked from the first commit; *answering* one did not, so a bot
could be addressed in a comment thread and had no way to speak in it.

```json
{ "postID": "...", "parentID": "<the comment being answered>", "text": "..." }
```

No author field — that comes from the token. No attachment — a URL accepted
from a caller is a different trust question from a file uploaded through the
platform's own path.

### Threads flatten, and the response says so

Django separates two ideas that a naive implementation conflates:

| | |
|---|---|
| `replied_to` | what the author aimed at |
| `parent_comment` | where the row is stored — `replied_to.parent_comment` **or** `replied_to` |

Replying to a *reply* re-parents to that reply's top-level ancestor rather than
nesting a third time. The thread then stays one paginated list per top-level
comment, and a soft-deleted middle comment cannot strand grandchildren with no
reachable parent. The response returns both `replied_to` and
`parent_comment_id`, so a caller sees the flattening happen rather than
discovering it later from a thread that reads oddly. The person actually
replied to is still notified — that comes off `replied_to`, not off where the
row landed.

### The fan-out

Performed, because a comment without them is a row rather than a comment: the
insert, `update_ranking_score` (the worker owns `comments_count`),
`bump_interest_affinity` carrying the *post's* interest ids, the
reply/post-comment notification with its realtime frame, and one
comment-mention notification per entity named — resolved across all three
handle namespaces (`user_account.username`, `community_realm.slug`,
`bot_bot.handle`), filtered by the platform's visibility bar, and dropped for
anyone in a block relationship with the author in either direction.

**Two side effects this does NOT perform:**

- **Content tagging.** Same reasoning as the message route: Django publishes
  nothing when the moderation service is down because that service's database
  scour picks the row up on its next start — the designed path, not a degraded
  one.
- **Hashtag → interest linking.** This one is a real gap rather than a shared
  path: a comment's hashtags are linked to its parent post by Django alone, and
  the moderation sink's `LINKABLE` set covers posts only. Reproducing it means
  *widening* `interests_interest` from a fifth implementation of a normaliser
  whose own file records that four already have to agree, and whose failure
  mode is silent — a second interest row for a tag that already exists. A
  missing link is recoverable; a duplicated taxonomy is not. So `#tag` in a
  comment posted through this API does not tag the post.

### Three divergences from Django, all narrowing

Django's own `CommentsView.post()` is more permissive in three places, and each
is tightened here because the looser behaviour can only produce nonsense:

| | Django | here |
|---|---|---|
| deleted post | `Post.objects.get()` — comments on it happily | 404 |
| parent on another post | re-parents across posts | 404 |
| markup in the text | stored as sent | tags stripped |

The last one matters most. Django is safe storing raw text because every client
escapes before posting; an API caller has no such client, so markup a browser
would have neutered goes in raw. Stripping can only ever remove capability.

**A platform gap this reproduces rather than fixes:** the reply notification is
skipped when the replier is the post's own author (`post.entity != entity` is
part of Django's condition), so a post owner replying to a comment on their own
post notifies nobody. It reads like an oversight, but changing it here would
make two services disagree about whether a notification exists.

## Reading a post's thread

`GET /v1/posts/{id}/comments` returns a post's caption and its comments. It
exists because a bot answering a comment had nothing to answer *from*.

The conversation surface has always had `/messages`, so a consumer can index
that window before retrieving and a message reply is grounded in the chat. The
post surface had no equivalent, which made `post:<id>` a permanently empty
tenant — every comment answer came back "I don't have any context for that yet"
regardless of which model generated it. That reads like a retrieval problem and
was a missing read.

The caption comes back with the comments deliberately: a thread is *about* the
post, and reading one without it is a conversation with the first turn deleted.

**Access divergence.** Django serves a post's comments to anyone —
`CommentsView.get()` is `AllowAny` and applies no post-privacy filter — so
requiring a valid token here is already stricter than the platform. This route
does not invent a visibility rule the platform does not have; reproducing
`post_visibility.py` would be a fourth implementation and exactly the drift risk
this service avoids. If the platform ever gates comment reads, this must adopt
that rule rather than keep its own.

## Ported logic, and what keeps it honest

Three pieces are hand-ported from the platform, and each carries the reason it
could not simply be shared:

| ported | from | guard |
|---|---|---|
| token verification | `entity/services/tokens.py` | format is all-hex + plain SHA-256, chosen to have no clever part to get wrong |
| mention extraction | `transformers.js` / `comment_mentions.py` | Go's RE2 has no lookahead; a drift test reads both live sources |
| conversation access | `models/realms.js` `isRealmMember` | one documented divergence — see below |
| comment fan-out | `newsfeed/views.py` `CommentsView.post` | the flattening rule is its own function with its own tests |
| notification writing | `user/services/mongohelpers.py` | the document shape is mongoengine's and Node's reader's, not this service's |

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
