# KnowledgeStream

> Real-time **Knowledge Volatility Engine** — consumes Wikimedia EventStreams, scores per-page controversy, and pushes live updates over WebSocket.

**Which Wikipedia page is the most contested right now?** Not the most viewed, not the most edited — the most *contested*. KVE answers that in real time.

![Go](https://img.shields.io/badge/-Go-00ADD8?logo=go&logoColor=white&style=flat-square)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)
![Status: demo](https://img.shields.io/badge/Status-demo-orange?style=flat-square)

---

## The problem with counting edits

The obvious approach: count edits per page, sort descending. Run it and you get *Wikipedia:Signpost* at the top — a weekly newsletter, updated by bots on a schedule. Below that, a single article that received one massive 50,000-byte rewrite by one person.

Both score high on edits. Neither is controversial.

That's the problem with measuring *activity*. It collapses three completely different phenomena into one number:

- a bot running a scheduled update
- a single author doing a large rewrite
- two people locked in an edit war over a contested paragraph

Only the third one is what "controversial" actually means. The metric needs to tell them apart.

## What controversy looks like

Controversy on Wikipedia has a specific shape:

- multiple people editing the same page
- edits being undone — reverted — by other editors
- text oscillating: added, removed, added again
- all of this happening recently, not last week

None of these signals alone is sufficient. The signal only becomes clear when multiple dimensions spike **simultaneously**. That's the insight behind the scoring formula.

## The volatility formula

```
volatility = intensity × contention × churn × recency × revertBoost
```

Five factors. Each captures a dimension the others miss. Multiplied — so a page needs to score meaningfully on several dimensions at once, not just dominate one.

> The constants below are hand-picked, not derived from principled optimization. I tuned them until the leaderboard surfaced pages that matched my intuition for "contested." They work well enough to be interesting and are almost certainly not optimal. Treat them as a starting point.

| Factor | Formula | Captures |
|---|---|---|
| **Intensity** | `log(1 + edits + 2·reverts)` | Raw activity, reverts weighted double |
| **Contention** | `clamp(edits ÷ editors, 1, 8)` | Tug-of-war: many edits from few editors |
| **Churn** | `clamp(gross ÷ (1 + \|net\|), 0.5, 12)` | Text oscillating in place vs. net change |
| **Recency** | `floor + (1−floor) · Σ(weighted/total)` | Exponential decay, τ = 3 days |
| **Revert boost** | `1 + reverts` | Reverberations compound directly |

## Worked example

Two pages, same edit count, same recency, nearly identical byte volume. Page A is a contested topic. Page B is a popular page rewritten by many editors.

```
Page A (contested):             Page B (high activity, low conflict):
  edits = 60, reverts = 18        edits = 60, reverts = 0
  editors = 4                     editors = 55
  gross = 42,000                  gross = 50,000
  net   = 900                     net   = 49,500

intensity   = log(97)    ≈ 4.57   intensity   = log(61)    ≈ 4.11
contention  = clamp(15)  = 8.00   contention  = clamp(1.09) = 1.09
churn       = clamp(46.6) = 12.00 churn       = clamp(1.01) = 1.01
recency     ≈ 1.00                recency     ≈ 1.00
revertBoost = 19                  revertBoost = 1

Volatility ≈ 8,336                Volatility ≈ 4.52
```

Same edit count. ~1,800× score difference — because reverts, editor concentration, and oscillating text compound multiplicatively. Activity alone gets you nowhere near the top.

## Detecting reverts from raw events

Wikimedia's event stream doesn't flag reverts explicitly. Every edit arrives as the same JSON — no `is_revert: true` field. Detection requires inspecting two fields on every event:

**Edit comments** (case-insensitive substring): `undid revision`, `reverted`, `revert to`, `rvv`, `restored revision`, plus `rv ` / `rvt ` with word-boundary guards.

**MediaWiki change tags**: `mw-reverted`, `mw-rollback`, `mw-undo`, `mw-manual-revert`.

This happens at the normalizer layer — before the scoring engine sees an event. Without this, the formula has no way to distinguish churn from work.

## Architecture

```
Wikimedia SSE
     ↓
Ingestor        drops bots, canary events
     ↓
Normalizer      JSON → KnowledgeChange + revert detection
     ↓
┌────────────┬─────────────┐
↓            ↓             ↓
Volatility   Fanout        Redis Rankings
Engine       Router        ZADD / ZREVRANGE
7-day HLL    map[topic]    
↓
WebSocket Gateway
gorilla/websocket broadcast
```

The entire system is a single Go binary. One process handles SSE ingestion, scoring, subscription routing, and WebSocket push.

### Internal packages

| Package | Responsibility |
|---|---|
| `internal/ingest` | SSE consumer — filters bots and canary events |
| `internal/normalize` | JSON → `KnowledgeChange`, revert detection |
| `internal/volatility` | 7-day sliding window + HLL sketches, scoring |
| `internal/fanout` | In-memory pub/sub, `map[topic]map[clientID]pushFunc` |
| `internal/gateway` | WebSocket upgrade + broadcast |
| `internal/ranking` | Redis sorted set persistence |

### Volatility Engine details

Maintains a 7-day sliding window of `DayBucket`s per page title. Each bucket tracks:

- **EditCount** — number of edits in that day
- **Editors** — HyperLogLog sketch (~1.5% error at 16KB per sketch, via `axiomhq/hyperloglog`)
- **Magnitude** — sum of |bytes changed| (gross churn)
- **NetBytes** — signed sum of bytes changed
- **Reverts** — edits flagged as reverts

Buckets decay exponentially with τ = 3 days. Seven rotating day-buckets advance at midnight UTC. Old data evicts automatically — no cleanup jobs needed.

## API endpoints

| Route | Description |
|---|---|
| `GET /ws` | WebSocket upgrade |
| `GET /api/rankings?n=20` | Top N volatile pages |
| `GET /api/topics` | List all active page titles |
| `GET /api/topics/:title` | Score details for one page |
| `GET /api/stats` | Events processed + active topic count |
| `GET /health` | Health check |

## Running

```bash
docker compose up --build
```

Listens on `:8080` (`LISTEN_ADDR`), connects to Redis at `localhost:6379` (`REDIS_ADDR`), subscribes to the Wikimedia EventStreams endpoint (`SSE_URL`).

## Scoring tunables

Defined as constants in `internal/volatility/engine.go`:

| Constant | Value | Effect |
|---|---|---|
| `recencyTau` | 3.0 | Day-decay constant |
| `recencyFloor` | 0.3 | Minimum recency multiplier |
| `contentionCap` | 8.0 | Ceiling on edits/editors ratio |
| `churnCap` | 12.0 | Ceiling on gross/net ratio |
| `churnFloor` | 0.5 | Floor on gross/net ratio |
| `revertWeight` | 2.0 | Reverts count double in intensity |

## The stack

| Layer | Technology |
|---|---|
| Language | Go 1.26 |
| SSE client | `r3labs/sse/v2` |
| WebSocket | `gorilla/websocket` |
| Cardinality | `axiomhq/hyperloglog` |
| Cache / rankings | Redis 7 (`go-redis/v9`) |
| Deployment | Docker Compose |

## Honest limits

- **The constants need real tuning.** Currently eyeballed. The honest next step is labeling a few hundred pages as "contested / not contested" and fitting bounds against ground truth.
- **Revert detection is heuristic.** Comment-string matching misses reverts with unusual summaries and false-positives on edits that merely *mention* reverting. The `mw-reverted` tag is more reliable but isn't on every event.
- **Fanout doesn't scale horizontally.** Fine for a demo. Real deployment serving many clients would need a shared pub/sub layer.

## The takeaway

The whole project hinges on one decision: not measuring activity. Edit count is the obvious metric — easy to compute, captures something real, and wrong for this question. The right metric required defining what "contested" actually means, decomposing it into signals that could be measured independently, and combining them so activity alone couldn't fake a high score.

That pattern — interrogating whether your metric measures the thing you actually care about — is the part worth keeping.

---

A deeper walk-through is on my blog: [Which Wikipedia Page Is the Most Controversial Right Now?](https://www.ahsanatha.com/writing/kve)