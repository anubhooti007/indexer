#!/usr/bin/env python3
"""
Fetch Polymarket event markets + trades and write CSV files (DOME-compatible layout).

Sources (free, no API key):
  - Gamma API:  https://gamma-api.polymarket.com/events/slug/{slug}
  - Data API:   https://data-api.polymarket.com/trades?market={conditionId}
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

GAMMA_BASE = "https://gamma-api.polymarket.com"
DATA_API_BASE = "https://data-api.polymarket.com"

# Polymarket Data API rejects offset > 3000 ("max historical activity offset of 3000 exceeded").
# With page_size=1000 that is up to 4,000 trades per market (offsets 0, 1000, 2000, 3000).
MAX_DATA_API_OFFSET = 3000
DEFAULT_PAGE_SIZE = 1000

# Matches DOME export column order (market metadata + trade fill)
DOME_COLUMNS = [
    "condition_id",
    "market_id",
    "market_slug",
    "title",
    "close_time",
    "category",
    "last_synced",
    "primary_token_id",
    "token_ids",
    "tags",
    "secondary_token_id",
    "end_date",
    "event_slug",
    "negative_risk_id",
    "gamma_tags",
    "volume_1wk",
    "volume_1mo",
    "volume_1yr",
    "volume_total",
    "resolution_source",
    "start_date",
    "image",
    "primary_token_label",
    "secondary_token_label",
    "outcome_token_id",
    "block_no",
    "tx_hash",
    "log_index",
    "token_id",
    "shares",
    "price",
    "side",
    "ingestion_time",
    "block_timestamp",
    "order_hash",
    "maker_address",
    "taker_address",
    "denormalized_shares",
]

MARKET_COLUMNS = [
    "condition_id",
    "market_id",
    "market_slug",
    "title",
    "event_slug",
    "event_title",
    "active",
    "closed",
    "end_date",
    "start_date",
    "close_time",
    "primary_token_id",
    "secondary_token_id",
    "token_ids",
    "outcomes",
    "outcome_prices",
    "volume_total",
    "volume_1wk",
    "volume_1mo",
    "volume_1yr",
    "liquidity",
    "negative_risk_id",
    "gamma_tags",
    "group_item_title",
    "last_synced",
]

DEFAULT_EVENTS = [
    "english-premier-league-winner",
    "2026-nba-champion",
    "uefa-champions-league-winner",
]

logger = logging.getLogger(__name__)


def parse_json_field(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def format_set_like(values: list[str] | None) -> str:
    if not values:
        return ""
    return "{" + ",".join(repr(v) for v in values) + "}"


def ts_to_iso(ts: int | float | None) -> str:
    if ts is None:
        return ""
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.000")
    except (OSError, ValueError, OverflowError):
        return ""


def now_synced() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


class PolymarketFetcher:
    def __init__(self, timeout: int = 60, sleep_s: float = 0.15):
        self.session = requests.Session()
        self.session.headers.update(
            {"Accept": "application/json", "User-Agent": "indexer-pred/1.0"}
        )
        self.timeout = timeout
        self.sleep_s = sleep_s

    def _get(
        self,
        url: str,
        params: dict | None = None,
        retries: int = 4,
        *,
        stop_on_offset_cap: bool = False,
    ) -> Any:
        last_err: Exception | None = None
        for attempt in range(retries):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                if stop_on_offset_cap and resp.status_code == 400:
                    body = resp.text
                    if "max historical activity offset" in body:
                        logger.debug("Offset cap reached for %s params=%s", url, params)
                        return []
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, ValueError) as e:
                last_err = e
                wait = min(2**attempt, 8)
                logger.warning("GET %s failed (%s), retry in %ss", url, e, wait)
                time.sleep(wait)
        raise RuntimeError(f"Failed after {retries} attempts: {url}") from last_err

    def fetch_event(self, slug: str) -> dict:
        url = f"{GAMMA_BASE}/events/slug/{slug}"
        logger.info("Fetching event: %s", slug)
        data = self._get(url)
        if not isinstance(data, dict):
            raise ValueError(f"Unexpected event response for {slug}")
        time.sleep(self.sleep_s)
        return data

    def fetch_trades_page(
        self, condition_id: str, *, limit: int, offset: int
    ) -> list[dict]:
        params = {
            "market": condition_id,
            "limit": limit,
            "offset": offset,
            "takerOnly": "false",
        }
        data = self._get(
            f"{DATA_API_BASE}/trades",
            params=params,
            stop_on_offset_cap=True,
        )
        if not isinstance(data, list):
            return []
        time.sleep(self.sleep_s)
        return data

    def fetch_all_trades(
        self,
        condition_id: str,
        *,
        page_size: int = DEFAULT_PAGE_SIZE,
        max_trades: int | None = None,
    ) -> tuple[list[dict], bool]:
        """Returns (trades, hit_api_offset_cap)."""
        out: list[dict] = []
        offset = 0
        hit_cap = False
        while offset <= MAX_DATA_API_OFFSET:
            batch = self.fetch_trades_page(condition_id, limit=page_size, offset=offset)
            if batch is None:
                batch = []
            if not batch and offset > 0:
                break
            out.extend(batch)
            if max_trades and len(out) >= max_trades:
                return out[:max_trades], hit_cap
            if len(batch) < page_size:
                break
            next_offset = offset + page_size
            if next_offset > MAX_DATA_API_OFFSET:
                if len(batch) == page_size:
                    hit_cap = True
                break
            offset = next_offset
        return out, hit_cap


def build_market_row(event: dict, market: dict) -> dict[str, Any]:
    slug = event.get("slug", "")
    tags = [t.get("label", "") for t in (event.get("tags") or []) if t.get("label")]
    category = tags[0] if tags else "Other"

    token_ids = parse_json_field(market.get("clobTokenIds")) or []
    if not isinstance(token_ids, list):
        token_ids = []
    token_ids = [str(t) for t in token_ids]

    outcomes = parse_json_field(market.get("outcomes")) or []
    if not isinstance(outcomes, list):
        outcomes = []
    outcomes = [str(o) for o in outcomes]

    primary_label = outcomes[0] if outcomes else "Yes"
    secondary_label = outcomes[1] if len(outcomes) > 1 else "No"

    return {
        "condition_id": market.get("conditionId", ""),
        "market_id": str(market.get("id", "")),
        "market_slug": market.get("slug", ""),
        "title": market.get("question", market.get("title", "")),
        "close_time": market.get("closedTime") or market.get("endDate") or "",
        "category": category,
        "last_synced": now_synced(),
        "primary_token_id": token_ids[0] if token_ids else "",
        "token_ids": format_set_like(token_ids),
        "tags": format_set_like(tags[:5] if tags else ["binary"]),
        "secondary_token_id": token_ids[1] if len(token_ids) > 1 else "",
        "end_date": market.get("endDate") or market.get("endDateIso") or "",
        "event_slug": slug,
        "negative_risk_id": market.get("negRiskMarketID") or event.get("negRiskMarketID") or "",
        "gamma_tags": format_set_like(tags),
        "volume_1wk": market.get("volume1wkClob") or market.get("volume1wk") or 0,
        "volume_1mo": market.get("volume1moClob") or market.get("volume1mo") or 0,
        "volume_1yr": market.get("volume1yrClob") or market.get("volume1yr") or 0,
        "volume_total": market.get("volumeClob") or market.get("volumeNum") or market.get("volume") or 0,
        "resolution_source": market.get("resolutionSource") or "",
        "start_date": market.get("startDate") or market.get("startDateIso") or "",
        "image": market.get("image") or event.get("image") or "",
        "primary_token_label": primary_label,
        "secondary_token_label": secondary_label,
        "outcome_token_id": token_ids[1] if len(token_ids) > 1 else "",
        # Extra fields for markets-only CSV
        "event_title": event.get("title", ""),
        "active": market.get("active"),
        "closed": market.get("closed"),
        "outcomes": json.dumps(outcomes),
        "outcome_prices": market.get("outcomePrices", ""),
        "liquidity": market.get("liquidityClob") or market.get("liquidity") or "",
        "group_item_title": market.get("groupItemTitle") or "",
    }


def trade_to_dome_row(market_row: dict[str, Any], trade: dict) -> dict[str, Any]:
    row = {k: market_row.get(k, "") for k in DOME_COLUMNS}
    size = trade.get("size", "")
    row.update(
        {
            "token_id": str(trade.get("asset", "")),
            "shares": size,
            "price": trade.get("price", ""),
            "side": trade.get("side", ""),
            "ingestion_time": now_synced(),
            "block_timestamp": ts_to_iso(trade.get("timestamp")),
            "tx_hash": trade.get("transactionHash", ""),
            "denormalized_shares": size,
            # Not available from Data API (on-chain fields in DOME exports)
            "block_no": "",
            "log_index": "",
            "order_hash": "",
            "maker_address": trade.get("proxyWallet", ""),
            "taker_address": "",
        }
    )
    return row


def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    logger.info("Wrote %s (%d rows)", path, len(rows))


def append_csv_rows(path: Path, columns: list[str], rows: list[dict], *, write_header: bool) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "w" if write_header else "a"
    with path.open(mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def process_event(
    fetcher: PolymarketFetcher,
    slug: str,
    output_dir: Path,
    *,
    skip_trades: bool,
    max_trades_per_market: int | None,
    active_only: bool,
) -> dict[str, int]:
    event = fetcher.fetch_event(slug)
    markets = event.get("markets") or []
    if not isinstance(markets, list):
        markets = []

    if active_only:
        markets = [m for m in markets if m.get("active") and not m.get("closed")]

    market_rows = [build_market_row(event, m) for m in markets]
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    markets_path = output_dir / f"markets_{slug}.csv"
    write_csv(markets_path, MARKET_COLUMNS, market_rows)

    dome_path = output_dir / f"DOME_{slug}_{date_str}.csv"
    trade_count = 0
    dome_row_count = 0
    markets_hit_cap = 0

    if skip_trades:
        dome_only = [{k: m.get(k, "") for k in DOME_COLUMNS} for m in market_rows]
        write_csv(dome_path, DOME_COLUMNS, dome_only)
        dome_row_count = len(dome_only)
    else:
        if dome_path.exists():
            dome_path.unlink()
        for i, mrow in enumerate(market_rows, start=1):
            cid = mrow["condition_id"]
            if not cid:
                continue
            logger.info(
                "[%s] %d/%d trades for: %s",
                slug,
                i,
                len(markets),
                mrow.get("market_slug") or mrow.get("title"),
            )
            try:
                trades, hit_cap = fetcher.fetch_all_trades(
                    cid, max_trades=max_trades_per_market
                )
            except Exception as e:
                logger.error(
                    "[%s] Failed trades for %s: %s — continuing",
                    slug,
                    mrow.get("market_slug"),
                    e,
                )
                continue
            if hit_cap:
                markets_hit_cap += 1
                logger.warning(
                    "[%s] %s: hit Data API offset cap (%s trades max per market)",
                    slug,
                    mrow.get("market_slug"),
                    (MAX_DATA_API_OFFSET // DEFAULT_PAGE_SIZE + 1) * DEFAULT_PAGE_SIZE,
                )
            trade_count += len(trades)
            batch_rows = [trade_to_dome_row(mrow, t) for t in trades]
            append_csv_rows(dome_path, DOME_COLUMNS, batch_rows, write_header=(dome_row_count == 0))
            dome_row_count += len(batch_rows)
            logger.info(
                "[%s] %s: +%d trades (event total so far: %d dome rows)",
                slug,
                mrow.get("market_slug"),
                len(trades),
                dome_row_count,
            )

    return {
        "markets": len(market_rows),
        "trades": trade_count,
        "dome_rows": dome_row_count,
        "markets_hit_api_cap": markets_hit_cap,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch Polymarket events to CSV (markets + trades)."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "output",
        help="Directory for CSV output",
    )
    parser.add_argument(
        "--events",
        nargs="*",
        default=DEFAULT_EVENTS,
        help="Event slugs to fetch",
    )
    parser.add_argument(
        "--markets-only",
        action="store_true",
        help="Skip trade history (fast; metadata rows only in DOME CSV)",
    )
    parser.add_argument(
        "--active-only",
        action="store_true",
        help="Only include markets that are active and not closed",
    )
    parser.add_argument(
        "--max-trades-per-market",
        type=int,
        default=None,
        help="Cap trades per market (default: all available)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=90,
        help="HTTP timeout seconds",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    fetcher = PolymarketFetcher(timeout=args.timeout)
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    summary: dict[str, dict[str, int]] = {}
    for slug in args.events:
        try:
            stats = process_event(
                fetcher,
                slug,
                output_dir,
                skip_trades=args.markets_only,
                max_trades_per_market=args.max_trades_per_market,
                active_only=args.active_only,
            )
            summary[slug] = stats
        except Exception as e:
            logger.error("Failed event %s: %s", slug, e, exc_info=True)
            summary[slug] = {"error": 1}

    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "events": args.events,
                "markets_only": args.markets_only,
                "active_only": args.active_only,
                "max_trades_per_market": args.max_trades_per_market,
                "summary": summary,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    logger.info("Manifest: %s", manifest_path)

    if any(s.get("error") for s in summary.values()):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
