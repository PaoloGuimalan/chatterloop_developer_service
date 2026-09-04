# developer_service — API reference

Token-authenticated HTTP access to chatterloop conversations, comments,
mentions and the realtime event stream, for anything that is not a person at a
browser.

Everything below is served from **one origin** and authenticated by **one
credential**. A consumer needs no database access, no Redis credentials, and no
second base URL.

---

## Authentication

Every route except `/health` and `/ready` requires:

```
Authorization: Bearer clt_<12 hex>_<64 hex>
```

Deliberately **not** `x-access-token` — that is the user-session header on the
Django and Node surfaces. A credential that cannot be presented on the wrong
door cannot be accepted by it by mistake.

`Content-Type: application/json` is required on `POST` routes. Nothing else is
read from headers.

### Authorization is an intersection

A request is allowed only when **both** hold:

1. the scope is listed on the token (`entity_token.scopes`), **and**
2. the owning entity has an explicit `grant` row in `entity_entitypermission`
   with `realm_id IS NULL` and no expiry in the past.

Either half missing means `403`. A token can never do more than its entity, and
revoking the entity's grant narrows every token it owns with no token edit.

> **Both halves are easy to get half-right.** A token whose `scopes` array is
> missing a codename fails even though the entity grants it, and the error
> looks identical to the reverse. `GET /v1/whoami` shows the token half only —
> see *Verifying a credential* at the end.

### The acting entity is never a parameter

Every route resolves "whose data" from the token. There is no `entity_id`
query parameter anywhere in this API, and that is a design rule rather than an
omission: a parameter that exists is a parameter someone eventually passes
someone else's value to.

---

## Errors

| status | meaning |
|---|---|
| `400` | malformed body, missing required field, or content over the length cap |
| `401` | missing/malformed `Authorization`, or an unknown, revoked or expired token |
| `403` | authenticated, but the scope or the entity grant is missing |
| `404` | not found **or** not permitted — deliberately indistinguishable |
| `429` | this token has exceeded its own `rate_limit_per_minute` |
| `500` | server fault; the response carries no detail |
| `503` | a dependency is unreachable, or the token could not be **checked** |

All errors are JSON:

```json
{ "status": false, "message": "Invalid or expired token." }
```

**`404` vs `403` is intentional.** A conversation the caller is not part of, and
one that never existed, return the same `404` — a caller able to tell them
apart could enumerate ids.

**`503` vs `401` matters when debugging.** A database fault used to be reported
as `Invalid or expired token`, which sends you to rotate a perfectly good
credential. A credential that could not be *checked* is now `503`, and the
failure is logged server-side.

**`429` carries a `Retry-After` header** (seconds until the current window
resets). It fires only for a token with a non-`NULL`
`entity_token.rate_limit_int`/`rate_limit_type` pair — every existing token is
`NULL` (unlimited) until someone sets one. The window can be per second,
minute, hour, day, week, month or year — see the token-issuing section above.

---

## Routes

| method | path | scope |
|---|---|---|
| `GET` | `/health` | — |
| `GET` | `/ready` | — |
| `GET` | `/v1/whoami` | authentication only |
| `GET` | `/v1/events` | `events.subscribe` |
| `GET` | `/v1/conversations/{conversationID}/messages` | `messages.read` |
| `GET` | `/v1/conversations/{conversationID}/replies` | `messages.read` |
| `GET` | `/v1/mentions/comments` | `notifications.read` |
| `GET` | `/v1/comments/replies` | `notifications.read` |
| `GET` | `/v1/posts/{postID}/comments` | `notifications.read` |
| `POST` | `/v1/messages/send` | `messages.send` |
| `POST` | `/v1/comments` | `comments.create` |

Versioned in the path from the first commit: consumers are deployed services,
not browsers that reload, so a shape change needs both versions live while
clients roll forward.

---

### `GET /health`

Liveness for the orchestrator. Unauthenticated on purpose — the orchestrator
has no credential and should not need one to decide whether to restart a pod.
Reports nothing worth scraping.

```json
{ "status": true }
```

### `GET /ready`

Whether Redis and Postgres are actually reachable. A different question from
liveness: a pod that cannot reach Redis should stop receiving traffic without
being killed and restarted into the same failure.

`200` → `{"status": true}` · `503` → `{"status": false, "dependency": "redis"}`

---

### `GET /v1/whoami`

Describes the calling credential. No scope of its own — a credential may always
describe itself, and requiring a scope to discover your scopes is a
bootstrapping problem with no upside.

```json
{
  "status": true,
  "entity_id": "ca0e358b-7cda-427f-9a31-f7bb9372d0cf",
  "handle": "neon",
  "realm_id": null,
  "scopes": ["messages.read", "notifications.read", "messages.send",
             "comments.create", "events.subscribe"],
  "token": {
    "id": "b8866403-…",
    "name": "rag pipeline (prod)",
    "rate_limit_int": 100000,
    "rate_limit_type": "month"
  }
}
```

`handle` is resolved live across all three namespaces (`user_account.username`,
`community_realm.slug`, `bot_bot.handle`). **Check your configured handle
against it at startup.** A bot told the wrong handle in configuration matches
no mentions, and there is nothing in any log to say why.

`scopes` is the **token** half only. It does not prove the entity grants them.

---

### `GET /v1/events`

Server-Sent Events. The calling entity's realtime frames, taken from the token
— there is no version of this that accepts an entity id.

```
Accept: text/event-stream
Cache-Control: no-cache
```

Opens with a `ready` event, then forwards platform frames verbatim:

```
event: ready
data: {"entity_id":"ca0e358b-…"}

data: {"logType":null,"pod":"…","event":"messages_list",
       "message":{"status":true,"auth":true,"onseen":false,
                  "message":{"conversationID":"…","entityID":"…","mentioner":null},
                  "result":""},
       "dateTime":"2026-09-03T11:29:29Z"}
```

A `:` comment line arrives every `SSE_HEARTBEAT_SECONDS` so a quiet stream is
distinguishable from a dead one. The server closes any stream after
`SSE_MAX_LIFETIME_SECONDS` (default 1h) — **a clean disconnect is expected
roughly hourly and is not an error.** Reconnect with backoff.

#### Two frames worth knowing

`messages_list` — a message arrived. `message.message` carries
`conversationID`, `entityID` (the sender), and `mentioner`, which is **non-null
only when the server resolved a mention against you specifically**. That makes
it an authoritative signal you can trust without re-parsing.

It carries **no message id, no text, and no `replyingTo`.** It is a ping telling
you to refetch, not a carrier of content — which is why answering one needs the
read routes below.

`notifications` — something happened in your notification tray. It names **no
subject at all**; `message` is a human-readable sentence. Respond by reading
the notification routes, never by parsing the sentence.

> **There is no replay.** Frames published while you are disconnected are gone.
> The database is the record; the stream is only a hint.

---

### `GET /v1/conversations/{conversationID}/messages`

The newest messages in one conversation, **oldest first** so the array reads as
a transcript.

| query | default | max |
|---|---|---|
| `limit` | 50 | 200 |

```json
{
  "status": true,
  "conversation_id": "50945793330147641378",
  "conversation_type": "group",
  "count": 2,
  "messages": [
    {
      "message_id": "142481516577589511363016096258",
      "conversation_id": "50945793330147641378",
      "sender_entity_id": "c6f3bf0c-…",
      "sender_handle": "paulo",
      "content": "@neon what did we decide about pricing?",
      "created_at": 1788406087626,
      "message_type": "text",
      "is_reply": false,
      "replying_to": "",
      "replying_to_sender_entity_id": "",
      "replying_to_sender_handle": ""
    }
  ]
}
```

`created_at` is epoch **milliseconds**, normalised server-side — the stored
column holds either a BSON date or an embedded `{date, time}` of formatted
strings depending on which service wrote the row. `0` means unparseable, which
sorts to the *start* of history so a parsing gap can never make an old message
look like the newest.

`replying_to_sender_entity_id` is resolved server-side because the parent is
regularly **outside** the window you asked for — a follow-up an hour later
replies to a message forty turns back. Computing it from the returned array
gives "unknown" exactly when it matters.

Image and file messages are omitted: they store non-text content, so there is
nothing to read or embed.

`404` for a conversation you are not a participant of *and* for one that does
not exist.

---

### `GET /v1/conversations/{conversationID}/replies`

The messages in that conversation that reply to one **you** wrote. Same row
shape and same ordering as `/messages`. Usually empty.

| query | default | max |
|---|---|---|
| `limit` | 25 | 100 |

```json
{ "status": true, "conversation_id": "…", "count": 1, "messages": [ … ] }
```

**Why this is a route and not a client-side filter.** The realtime frame has no
message id and no `replyingTo`, so "was that a reply to me?" cannot be answered
without reading. Doing it as a history fetch costs a full window on *every*
message in *every* conversation you belong to. Here it is two indexed lookups
and, usually, nothing.

`limit` bounds the **replies scanned**, not the conversation. Whose replies is
taken from the token.

---

### `GET /v1/mentions/comments`

Unread `comment_mention` notifications addressed to you, with the comment text
already resolved — the mention is a Mongo notification while its words are in
Postgres, and joining them here means a consumer does not reach two stores.

| query | default | max |
|---|---|---|
| `limit` | 25 | 100 |

```json
{
  "status": true,
  "count": 1,
  "mentions": [
    {
      "notification_id": "NTF_31857260049182736450",
      "comment_id": "c20d8d61-277e-452e-910a-90f72ce495a8",
      "post_id": "762856157557296690215357838746",
      "author_entity_id": "c6f3bf0c-…",
      "author_handle": "paulo",
      "text": "@neon check this!",
      "kind": "mention",
      "created_at": 1788406336000
    }
  ]
}
```

**This does not mark anything read.** `isRead` is the owning entity's UI state,
and a machine consuming it silently would change what a human sees in their own
tray. **Deduplication is yours**, keyed on `comment_id`.

`created_at` comes from the notification's ObjectId. It is load-bearing:
unread notifications are **durable** and accumulate while you are offline, so
this is the only thing distinguishing a backlog from live traffic. A consumer
that answers everything it finds will, on its next start, work through
everything that piled up.

A mention whose comment has since been deleted has no text and is dropped.

---

### `GET /v1/comments/replies`

Unread notifications saying somebody **replied to a comment you wrote**. Same
row shape as above with `kind: "reply"`.

| query | default | max |
|---|---|---|
| `limit` | 25 | 100 |

```json
{ "status": true, "count": 1, "replies": [ … ] }
```

Django files "replied to your comment" and "commented on your post" under one
`post_comment` notification type. Only the first is an answer to something you
said; they are separated **structurally**, on whether the referenced comment
has a parent — not on a display string.

Two limitations, both the platform's:

- **`limit` bounds notifications, not replies.** Both branches share a type, so
  on a busy post you own, "commented on your post" rows can crowd replies out
  of the window. Raising `limit` is the remedy.
- **A post's own author replying notifies nobody.** Django's condition includes
  `post.entity != entity`, so when the replier owns the post the notification
  is never written and this route cannot see it.

---

### `GET /v1/posts/{postID}/comments`

A post and the newest comments on it, **oldest first**.

| query | default | max |
|---|---|---|
| `limit` | 50 | 200 |

```json
{
  "status": true,
  "post_id": "762856157557296690215357838746",
  "author_entity_id": "c6f3bf0c-…",
  "author_handle": "paulo",
  "caption": "shipping the new pricing page today",
  "created_at": 1788405600000,
  "count": 2,
  "comments": [
    {
      "comment_id": "c20d8d61-…",
      "parent_comment_id": "",
      "author_entity_id": "c6f3bf0c-…",
      "author_handle": "paulo",
      "text": "@neon check this!",
      "created_at": 1788406336000
    },
    {
      "comment_id": "9c07147d-…",
      "parent_comment_id": "c20d8d61-…",
      "author_entity_id": "ca0e358b-…",
      "author_handle": "neon",
      "text": "Looks right to me.",
      "created_at": 1788406338000
    }
  ]
}
```

The **caption is part of the answer**, not a nicety: a comment thread is *about*
the post, and reading one without it is a conversation with the first turn
deleted.

`parent_comment_id` is empty for a top-level comment. Threads are flattened to
two levels, so that is the only nesting there is.

**Access divergence, stated plainly:** Django serves a post's comments to
anyone — `CommentsView.get()` is `AllowAny` and applies no post-privacy filter
— so requiring a valid token here is already stricter than the platform, and
this route does not invent a visibility rule the platform does not have.

---

### `POST /v1/messages/send`

Sends a message as the calling entity, with the fan-out that makes a message a
message rather than a row.

| field | type | required | notes |
|---|---|---|---|
| `conversationID` | string | **yes** | |
| `content` | string | **yes** | non-empty after trim; ≤ 5000 chars |
| `replyingTo` | string | no | a bare `messageID`; sets `isReply` server-side |
| `messageType` | string | no | defaults to `"text"` |
| `conversationType` | string | no | a **fallback only** — see below |
| `pendingID` | string | no | echoed back; generated if omitted |

```bash
curl -X POST https://<host>/v1/messages/send \
  -H "Authorization: Bearer clt_…" \
  -H "Content-Type: application/json" \
  -d '{"conversationID":"509457…","content":"on it","replyingTo":"142481…"}'
```

`201` →

```json
{ "status": true, "message_id": "939333…", "pending_id": "api-17884…", "receivers": 4 }
```

**`receivers` is never sent by you.** The route derives it from the
conversation, which is what stops a token addressing people who are not in it.

**`conversationType` is a fallback, never authoritative.** Order of truth: what
the conversation already says, then the realm it belongs to, then your claim —
and only for a genuinely new, non-realm conversation. An earlier version
defaulted it to `"single"`, and one reply into a real group rewrote that
conversation's type so the UI rendered a group as a DM.

Performed: un-archiving for every participant, the conversation preview, one
realtime frame per recipient, the chat interaction score, and push — with a
distinct payload for anyone `@mentioned`, who gets the mention push *instead
of* the plain one.

**Not performed:** link previews (a URL renders bare, not as a card) and content
tagging (left to the moderation service's own database scour, which is the
designed path).

---

### `POST /v1/comments`

Posts a comment as the calling entity.

| field | type | required | notes |
|---|---|---|---|
| `postID` | string | **yes** | |
| `text` | string | **yes** | non-empty after trim; ≤ 5000 chars |
| `parentID` | string | no | the comment being **answered**; omit for top-level |

```bash
curl -X POST https://<host>/v1/comments \
  -H "Authorization: Bearer clt_…" \
  -H "Content-Type: application/json" \
  -d '{"postID":"762856…","parentID":"c20d8d61-…","text":"Looks right to me."}'
```

`201` →

```json
{
  "status": true,
  "comment_id": "9c07147d-3e41-49fb-ac17-f386701c02c0",
  "post_id": "762856…",
  "parent_comment_id": "c20d8d61-…",
  "replied_to": "c20d8d61-…",
  "notified": ["c6f3bf0c-…"]
}
```

**`parentID` is what you are answering. `parent_comment_id` is where the row
landed, and they differ one level down.** Threads flatten to two levels: replying
to a *reply* re-parents to that reply's top-level ancestor rather than nesting a
third time. Both are returned so you see the flattening happen instead of
discovering it later from a thread that reads oddly. **Do not compute the parent
yourself** — that is reimplementing the rule this route exists to own.

The person actually replied to is still notified; that comes off `parentID`, not
off where the row landed.

`notified` lists the entities told about the comment: the person replied to (or
the post's author for a top-level comment), plus anyone `@mentioned` in the
text. Mentions resolve across all three handle namespaces, are filtered by the
platform's visibility bar, and are dropped for anyone in a block relationship
with you in either direction.

`404` for a post that does not exist or is deleted, and for a `parentID` that
does not exist, is deleted, **or belongs to a different post**.

**Not performed:** content tagging (as above), and **hashtag → interest
linking** — a `#tag` in a comment posted through this API does not tag the post.
Reproducing that means widening `interests_interest` from a fifth
implementation of a normaliser whose failure mode is a silent duplicate row. A
missing link is recoverable; a duplicated taxonomy is not.

**No attachment field.** A URL accepted from a caller is a different trust
question from a file uploaded through the platform's own path.

Text is **HTML-sanitised** before storage, unlike Django, which stores what the
client sent. That is safe there because every client escapes before posting; an
API caller has no such client.

---

## Running it

### Locally

```bash
cp .env.example .env    # fill in Postgres, Mongo, Redis, RabbitMQ
go test ./...
go run ./cmd/developer
```

`go test ./...` needs no infrastructure — everything covered is pure logic.

```bash
curl -H "Authorization: Bearer clt_…" http://localhost:8890/v1/whoami
```

### Docker

```bash
docker build -t developer_service .
docker run --rm -p 8890:8890 --env-file .env developer_service
```

Static binary on Alpine, non-root (`uid 10001`), with `ca-certificates` for
Mongo Atlas and hosted Redis TLS.

### Configuration

| variable | required | default | notes |
|---|---|---|---|
| `PORT` | no | `8890` | |
| `POD_NAME` | no | `$HOSTNAME` | stamped into every frame published |
| `DATABASE_URL` | — | | wins when set; otherwise assembled from the parts below |
| `DB_HOST` | **yes*** | | *unless `DATABASE_URL` is set |
| `DB_PORT` | no | `5432` | |
| `DB_NAME` `DB_USERNAME` `DB_PASSWORD` | **yes*** | | |
| `MONGODB_URI` | — | | overrides the assembled `mongodb+srv://` entirely |
| `MONGODB_CLUSTER_HOST` `_USER` `_PASS` | **yes*** | | *unless `MONGODB_URI` is set |
| `MONGODB_DB` | no | `chatterloop` | |
| `REDIS_HOST` | **yes** | | the realtime bus |
| `REDIS_PORT` | no | `6379` | |
| `REDIS_USERNAME` `REDIS_PASSWORD` | no | | |
| `RABBITMQ_URL` | — | | wins when set |
| `RABBITMQ_HOST` `_PORT` `_USER` `_PASS` `_VHOST` `_PROTOCOL` | no | | empty ⇒ fan-out jobs are logged as dropped, not sent |
| `SSE_HEARTBEAT_SECONDS` | no | `20` | |
| `SSE_MAX_LIFETIME_SECONDS` | no | `3600` | |

Variable names deliberately match the other chatterloop services, so this drops
into an existing deployment with no new secrets story.

### Deployment notes

- **Point `/health` at liveness and `/ready` at readiness.** They answer
  different questions on purpose.
- **Do not set an HTTP write timeout in front of this.** It applies to the whole
  response, and on SSE that cuts every client off at the timeout regardless of
  health. The stream's own `SSE_MAX_LIFETIME_SECONDS` is the bound.
- **Proxy buffering must be off** for `/v1/events`, or frames sit in a buffer
  until it fills — which on a quiet stream means a mention arriving minutes
  late.
- **Scale horizontally without coordination.** Every instance subscribes to the
  same Redis channels and holds no per-instance state; a client reconnecting to
  a different pod loses nothing that was not already lost by disconnecting.
- **Postgres behind a transaction-pooling proxy is handled.** The pool runs with
  named prepared statements disabled (`QueryExecModeExec`, both caches at zero),
  which is required against PgBouncer-style poolers — without it you get
  intermittent `SQLSTATE 26000` / `42P05`, surfacing as authentication failures.

---

## Verifying a credential

`/v1/whoami` proves the token half only. To check both halves, and catch the
case where `entity_token.scopes` and the entity's grants disagree:

```sql
SELECT t.prefix, t.name, t.is_active, t.expires_at, t.scopes,
       p.permission, p.effect, p.expires_at AS grant_expires
  FROM entity_token t
  LEFT JOIN entity_entitypermission p
         ON p.entity_id = t.entity_id AND p.realm_id IS NULL
 WHERE t.prefix = '<prefix>';
```

Every codename in `t.scopes` needs a matching row with `effect = 'grant'` and
no expiry in the past. A scope present on the token but not granted to the
entity yields `403`; the reverse is silent, because the token simply never
asks.

See the README for issuing and revoking tokens.
