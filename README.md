# indexer-pred

Fetch Polymarket **event markets** and **trade history** into CSV files (layout compatible with DOME exports).

Uses free public APIs (no API key):

- [Gamma API](https://gamma-api.polymarket.com) — event + market metadata
- [Data API](https://data-api.polymarket.com) — trades per `conditionId`

## Setup

```bash
cd indexer-pred
pip install -r requirements.txt
```

## Default events

Configured in `events.json` and baked into `fetch_events.py`:

| Slug | URL |
|------|-----|
| `english-premier-league-winner` | https://polymarket.com/event/english-premier-league-winner |
| `2026-nba-champion` | https://polymarket.com/event/2026-nba-champion |
| `uefa-champions-league-winner` | https://polymarket.com/event/uefa-champions-league-winner |

## Run

**Fast pass (markets only, ~seconds):**

```bash
python fetch_events.py --markets-only
```

**Full pass (markets + all trades per market — can take a long time):**

```bash
python fetch_events.py
```

**Limit trades while testing:**

```bash
python fetch_events.py --max-trades-per-market 5000
```

**Only open markets:**

```bash
python fetch_events.py --active-only
```

## Output (`output/`)

| File | Contents |
|------|----------|
| `markets_{slug}.csv` | One row per outcome market (clean catalog) |
| `DOME_{slug}_{date}.csv` | DOME-style wide CSV (market cols repeated per trade) |
| `manifest.json` | Run summary + row counts |

## Notes

- On-chain fields (`block_no`, `log_index`, `order_hash`) are empty when sourced from the Data API; DOME chain exports fill those.
- `maker_address` is mapped from `proxyWallet` on each trade.
- **Data API history cap:** pagination `offset` cannot exceed **3000**, so each market returns at most **~4,000** trades (offsets 0–3000 at `limit=1000`). That is the maximum available from this API without chain/subgraph indexing; DOME-style exports with full history use a different source.
- Optional `--max-trades-per-market N` stops earlier for quick tests.
- Full runs over many markets can take a long time; use `--markets-only` for a fast metadata pass first.
