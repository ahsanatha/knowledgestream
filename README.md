# KnowledgeStream

Real-time Knowledge Volatility Engine — consumes Wikimedia EventStreams, computes per-page volatility scores, and fans out updates to subscribers.

## Pipeline

```
EventStream (SSE) → Ingest → Normalize → Engine → Store (Redis)
                                          ↘ Router → WebSocket
```

### Ingest (`internal/ingest/`)
Subscribes to the Wikimedia `recentchange` SSE stream via `r3labs/sse`. Filters out canary events and non-main-namespace pages. Drops bot edits. Pushes `KnowledgeChange` structs into a buffered channel.

### Normalize (`internal/normalize/normalize.go`)
Parses raw JSON events into structured `KnowledgeChange` records. Key fields:

- `Title`, `Wiki`, `Editor` — the page, project, and user
- `BytesChanged` — signed byte delta (old → new length)
- `Reverted` — boolean, set by `detectRevert()` which inspects:

  **Comment markers** (case-insensitive): `undid revision`, `reverted`, `revert to`, `rvv`, `restored revision`, and the shorthand `rv ` / `rvt ` with word boundary guard.

  **Change tags** (MediaWiki API): `mw-reverted`, `mw-rollback`, `mw-undo`, `mw-manual-revert`.

The revert signal is the single strongest indicator of content churn — edits that exist to undo other edits.

### Volatility Engine (`internal/volatility/engine.go`)
Maintains a 7-day sliding window of `DayBucket`s per page title. Each bucket tracks:

| Field     | Description |
|-----------|-------------|
| EditCount | Number of edits in that day |
| Editors   | HyperLogLog sketch (distinct editors) |
| Magnitude | Sum of \|bytes changed\| — gross churn |
| NetBytes  | Signed sum of bytes changed |
| Reverts   | Edits flagged as reverts |

#### Scoring formula

Score is a product of four signals:

```
score = intensity × contention × churn × recency × revertBoost
```

**Intensity** — `log(1 + edits + 2·reverts)`. Raw activity volume, with reverts counted double.

**Contention** — `clamp(edits / editors, 1, 8)`. Few editors driving many edits = tug-of-war. Many editors with few edits each = stable collaboration. This is the inverse of the old `log(editors)` term which rewarded the stable case.

**Churn** — `clamp(gross / (1 + |net|), 0.5, 12)`. High when gross bytes ≫ net bytes — text oscillating in place via reverts and reversions. A single +50KB rewrite scores ~1; 25 reverts of a small paragraph score much higher.

**Recency** — `floor + (1−floor) · Σ(weighted edits / total edits)`. Buckets decay exponentially with τ=3 days. Activity 6 days ago barely counts; activity now counts fully.

**Revert boost** — `1 + reverts`. Reverberations are the primary signal, so they multiply directly.

### Fanout Router (`internal/fanout/router.go`)
In-memory publish/subscribe: `map[topic]map[clientID]pushFunc` guarded by `RWMutex`. Clients subscribe to topics (page titles) and receive real-time `RouteMessage` (change + score).

### WebSocket Gateway (`internal/gateway/websocket.go`)
Upgrades HTTP to WebSocket using `gorilla/websocket`. Supports `subscribe`/`unsubscribe` message types. Broadcasts every change+score to all connected clients.

### Redis Store (`internal/ranking/store.go`)
Maintains a sorted set (`volatility:rankings`) keyed by page title with score as rank value. Separate hash entries store full `TopicRank` structs (title, wiki, edit count, unique editors, gross/net bytes, reverts, churn score, wiki count).

## API Endpoints

| Route | Description |
|-------|-------------|
| `GET /ws` | WebSocket upgrade |
| `GET /api/rankings?n=20` | Top N volatile pages |
| `GET /api/topics` | List all active page titles |
| `GET /api/topics/:title` | Score details for one page |
| `GET /api/stats` | Events processed + active topic count |
| `GET /health` | Health check |

## Running

```
docker compose up --build
```

Listens on port 8080 by default (`LISTEN_ADDR`), connects to Redis at `localhost:6379` (`REDIS_ADDR`), and subscribes to the Wikimedia EventStreams SSE endpoint (`SSE_URL`).

## Scoring tunables

Defined as constants in `engine.go`:

| Constant | Value | Effect |
|----------|-------|--------|
| `recencyTau` | 3.0 | Day-decay constant |
| `recencyFloor` | 0.3 | Minimum recency multiplier |
| `contentionCap` | 8.0 | Ceiling on edits/editors ratio |
| `churnCap` | 12.0 | Ceiling on gross/net ratio |
| `churnFloor` | 0.5 | Floor on gross/net ratio |
| `revertWeight` | 2.0 | Reverts count double in intensity |
