#!/usr/bin/env python3
"""
dummy comment
Wistia Stats Events -> S3 Incremental Loader
============================================

Pulls viewing-session / event data from the Wistia Stats API:

    GET https://api.wistia.com/modern/stats/events

...and writes it to S3 as newline-delimited JSON (JSONL), partitioned by
the event's `created_at` date.

Features
--------
* Offset pagination via `page` + `per_page` (max 100 per page).
* Incremental pulls: a high-water mark (last seen `created_at` / `updated_at`)
  is persisted to a JSON state object in S3. Each subsequent run only fetches
  events on/after the high-water mark's *date* (Wistia only exposes date-level
  `start_date` / `end_date` filters, not raw timestamps), then client-side
  filters to the exact timestamp so no row is missed or double-counted.
* Backfill mode: if no state exists, pulls from a configurable look-back
  window (default: 2 years — Wistia's max retention).
* Idempotent / safe to re-run: rows are de-duplicated by `event_key`, keeping
  the freshest `updated_at`.
* Retries with exponential backoff for 429 / 5xx responses.

Required environment variables
------------------------------
    WISTIA_API_TOKEN      Bearer token (Stats API, "Read detailed stats" scope)
    AWS_ACCESS_KEY_ID     (or rely on IAM role / default credential chain)
    AWS_SECRET_ACCESS_KEY
    S3_BUCKET             target bucket name
    S3_PREFIX             optional key prefix, e.g. "raw/wistia/stats_events"

Optional environment variables
------------------------------
    WISTIA_API_VERSION    header value, default "2026-07"
    STATE_S3_KEY          override path for the state file inside the bucket
    LOOKBACK_DAYS         backfill window when no state exists (default 730)
    PER_PAGE              page size, default 100 (Wistia max)
    MAX_PAGES_PER_RUN     safety cap, default 10000
    START_DATE / END_DATE manual override (YYYY-MM-DD) — disables incremental
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator
from urllib.parse import urlencode
from requests.auth import HTTPBasicAuth
from awsglue.utils import getResolvedOptions  # type: ignore[reportMissingImports]

import boto3
import requests

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

# 1. Standard Glue arguments that you WILL define in your Job Parameters
# Glue requires these to be explicitly declared if you use getResolvedOptions
mandatory_args = ['WISTIA_API_TOKEN']

# Safely extract mandatory args
try:
    args = getResolvedOptions(sys.argv, mandatory_args)
    WISTIA_API_TOKEN = args['WISTIA_API_TOKEN']
    os.environ['WISTIA_API_TOKEN'] = WISTIA_API_TOKEN
except Exception:
    WISTIA_API_TOKEN = ""

# 2. Helper function to safely read optional '--' parameters from sys.argv
def get_optional_arg(arg_name, default_value):
    prefix = f"--{arg_name}"
    for i, arg in enumerate(sys.argv):
        if arg == prefix and i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default_value

# 3. Apply your exact fallback values using the helper function
WISTIA_BASE_URL = "https://api.wistia.com/modern/stats/events"
WISTIA_API_VERSION = get_optional_arg("WISTIA_API_VERSION", "2026-07")

S3_BUCKET = get_optional_arg("S3_BUCKET", "wistia-analytics-raw-data")
S3_PREFIX = get_optional_arg("S3_PREFIX", "wistia_stats_events/")
STATE_S3_KEY = get_optional_arg("STATE_S3_KEY", "wistia_stats_events_state.json")

LOOKBACK_DAYS = int(get_optional_arg("LOOKBACK_DAYS", "10"))
PER_PAGE = int(get_optional_arg("PER_PAGE", "100"))
MAX_PAGES_PER_RUN = int(get_optional_arg("MAX_PAGES_PER_RUN", "100"))
MANUAL_START_DATE = get_optional_arg("START_DATE", None)
MANUAL_END_DATE = get_optional_arg("END_DATE", None)
 

MAX_RETRIES = 5
RETRY_BACKOFF_BASE = 2.0  # seconds
REQUEST_TIMEOUT = 60      # seconds

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("wistia-loader")


# --------------------------------------------------------------------------- #
# State management
# --------------------------------------------------------------------------- #

@dataclass
class State:
    """High-water marks persisted between runs."""
    last_created_at: str | None = None   # ISO-8601 UTC timestamp
    last_updated_at: str | None = None   # ISO-8601 UTC timestamp
    last_run_date: str | None = None     # YYYY-MM-DD

    def to_dict(self) -> dict[str, Any]:
        return {
            "last_created_at": self.last_created_at,
            "last_updated_at": self.last_updated_at,
            "last_run_date": self.last_run_date,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "State":
        return cls(
            last_created_at=data.get("last_created_at"),
            last_updated_at=data.get("last_updated_at"),
            last_run_date=data.get("last_run_date"),
        )


def load_state(s3: boto3.client) -> State:
    """Load incremental state from S3; returns empty State if not present."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=STATE_S3_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        log.info("Loaded state from s3://%s/%s", S3_BUCKET, STATE_S3_KEY)
        return State.from_dict(data)
    except s3.exceptions.NoSuchKey:
        log.info("No existing state file — starting fresh (backfill).")
    except Exception:
        log.exception("Failed to load state; starting fresh.")
    return State()


def save_state(s3: boto3.client, state: State) -> None:
    body = json.dumps(state.to_dict(), indent=2).encode("utf-8")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=STATE_S3_KEY,
        Body=body,
        ContentType="application/json",
    )
    log.info("Saved state to s3://%s/%s", S3_BUCKET, STATE_S3_KEY)


# --------------------------------------------------------------------------- #
# Wistia API client
# --------------------------------------------------------------------------- #

def _build_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {WISTIA_API_TOKEN}",
        "X-Wistia-API-Version": WISTIA_API_VERSION,
        "Accept": "application/json",
    }


def _request_with_retry(params: dict[str, Any]) -> list[dict[str, Any]]:
    """GET one page with retries; returns the JSON list of events."""
    url = f"{WISTIA_BASE_URL}?{urlencode(params)}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                WISTIA_BASE_URL,
                params=params,
                headers=_build_headers(),
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                wait = RETRY_BACKOFF_BASE ** attempt
                log.warning(
                    "HTTP %d — retrying in %.1fs (attempt %d/%d)",
                    resp.status_code, wait, attempt, MAX_RETRIES,
                )
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            wait = RETRY_BACKOFF_BASE ** attempt
            log.warning("Request error: %s — retrying in %.1fs", exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"Exhausted retries for params={params}")


def fetch_events(
    start_date: str,
    end_date: str,
) -> Iterator[list[dict[str, Any]]]:
    """
    Yield pages of events between start_date and end_date (inclusive).

    Wistia paginates with `page` + `per_page`; the response is a plain JSON
    array. We stop when a page returns fewer than `PER_PAGE` records (or
    an empty array).
    """
    page = 1
    total = 0
    while page <= MAX_PAGES_PER_RUN:
        params = {
            "page": page,
            "per_page": PER_PAGE,
            "start_date": start_date,
            "end_date": end_date,
        }
        events = _request_with_retry(params)
        if not events:
            log.info("Page %d returned 0 events — done.", page)
            break
        yield events
        total += len(events)
        log.info(
            "Fetched page %d: %d events (running total %d) for %s..%s",
            page, len(events), total, start_date, end_date,
        )
        if len(events) < PER_PAGE:
            break
        page += 1
    else:
        log.warning("Reached MAX_PAGES_PER_RUN=%d — stopping to avoid infinite loop.", MAX_PAGES_PER_RUN)


# --------------------------------------------------------------------------- #
# S3 write helpers
# --------------------------------------------------------------------------- #

def _partition_key(event: dict[str, Any]) -> str:
    """
    Build an S3 object key for a single event, partitioned by created_at date.

    Example: raw/wistia/stats_events/created_at=2026-09-28/2026-09-28T13-05-00Z_abc123.jsonl
    """
    created_raw = event.get("created_at") or event.get("created") or ""
    # created_at may be ISO-8601 with 'T' and 'Z'
    try:
        dt = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        dt = datetime.now(timezone.utc)
    date_str = dt.strftime("%Y-%m-%d")
    ts_str = dt.strftime("%Y-%m-%dT%H-%M-%SZ")
    event_id = (
        event.get("event_key")
        or event.get("id")
        or event.get("key")
        or "unknown"
    )
    safe_id = str(event_id).replace("/", "_")
    return f"{S3_PREFIX}/created_at={date_str}/{ts_str}_{safe_id}.jsonl"


def _batch_partition_key(start_date: str, page_num: int) -> str:
    """
    Builds a single S3 object key for a batch page.
    Example: wistia_stats_events/run_date=2026-09-29/page_001_1727641200.jsonl
    """
    epoch_ts = int(time.time())
    return f"{S3_PREFIX}run_date={start_date}/page_{page_num:03d}_{epoch_ts}.jsonl"


def append_events_to_s3(
    s3: boto3.client,
    events: list[dict[str, Any]],
    seen_keys: set[str],
    start_date: str,
    page_num: int,
) -> tuple[int, int]:
    """
    Deduplicates events on the current page, bundles them into a single 
    multi-line JSONL body, and uploads them to S3 in one API transaction.
    """
    written = 0
    skipped = 0
    
    # 1. Deduplicate events within this batch page
    by_key: dict[str, dict[str, Any]] = {}
    for ev in events:
        ek = ev.get("event_key") or ev.get("id") or ""
        if not ek:
            by_key[f"__nokey_{len(by_key)}"] = ev
            continue
        existing = by_key.get(ek)
        if existing is None or _ev_ts(ev) > _ev_ts(existing):
            by_key[ek] = ev

    # 2. Filter out keys already processed in earlier pages
    valid_batch_events = []
    for ek, ev in by_key.items():
        if ek in seen_keys:
            skipped += 1
            continue
        
        valid_batch_events.append(ev)
        if not ek.startswith("__nokey_"):
            seen_keys.add(ek)
        written += 1

    # 3. If there are fresh events, compile them and write to S3 as one object
    if valid_batch_events:
        key = _batch_partition_key(start_date, page_num)
        
        # Merge all records into a single multi-line string
        lines = [json.dumps(ev, ensure_ascii=False) for ev in valid_batch_events]
        body = ("\n".join(lines) + "\n").encode("utf-8")
        
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=S3_PREFIX + key,
            Body=body,
            ContentType="application/x-ndjson",
        )
        log.info("Successfully bundled and wrote %d events to s3://%s/%s", len(valid_batch_events), S3_BUCKET, key)
    else:
        log.info("No new events to write for page %d.", page_num)

    return written, skipped


def _ev_ts(ev: dict[str, Any]) -> str:
    """Comparable timestamp string for an event (updated_at > created_at)."""
    return ev.get("updated_at") or ev.get("created_at") or ""


# --------------------------------------------------------------------------- #
# Date helpers
# --------------------------------------------------------------------------- #

def _to_date_str(ts: str | None) -> str | None:
    """Convert an ISO timestamp to YYYY-MM-DD; return None if invalid."""
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def compute_date_range(state: State) -> tuple[str, str]:
    """
    Determine the start_date / end_date for this run.

    Priority:
      1. Manual START_DATE / END_DATE overrides (full backfill / ad-hoc).
      2. Incremental: start_date = high-water-mark date (minus 1 day buffer
         because Wistia filters at date granularity, not timestamp),
         end_date = today.
      3. Initial backfill: start = today - LOOKBACK_DAYS, end = today.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if MANUAL_START_DATE and MANUAL_END_DATE:
        return MANUAL_START_DATE, MANUAL_END_DATE
    if MANUAL_START_DATE:
        return MANUAL_START_DATE, today

    # Incremental: use the last run's high-water mark.
    hw = _to_date_str(state.last_created_at) or _to_date_str(state.last_updated_at)
    if hw:
        # Subtract 1 day as a safety buffer for date-granularity filtering.
        hw_dt = datetime.fromisoformat(hw)
        from datetime import timedelta
        start = (hw_dt - timedelta(days=1)).strftime("%Y-%m-%d")
        log.info("Incremental run: start_date=%s (HWM %s), end_date=%s", start, hw, today)
        return start, today

    # Initial backfill.
    from datetime import timedelta
    start = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    log.info("Backfill run: start_date=%s, end_date=%s (lookback=%d days)", start, today, LOOKBACK_DAYS)
    return start, today


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main() -> int:
    if not WISTIA_API_TOKEN:
        log.error("WISTIA_API_TOKEN env var is required.")
        return 1
    if not S3_BUCKET:
        log.error("S3_BUCKET env var is required.")
        return 1

    s3 = boto3.client("s3")
    state = load_state(s3)

    start_date, end_date = compute_date_range(state)
    log.info("Run window: %s -> %s", start_date, end_date)

    # High-water marks for client-side filtering.
    hwm_created = state.last_created_at or ""
    hwm_updated = state.last_updated_at or ""

    seen_keys: set[str] = set()
    total_written = 0
    total_skipped = 0
    max_created = hwm_created
    max_updated = hwm_updated

    for page_num, page_events in enumerate(fetch_events(start_date, end_date), start=1):
        # Client-side incremental filter: keep only events strictly newer than
        # the high-water mark on created_at OR updated_at (so updated rows are
        # re-synced even if created_at is old).
        filtered: list[dict[str, Any]] = []
        for ev in page_events:
            ev_created = ev.get("created_at") or ev.get("received_at") or""
            ev_updated = ev.get("updated_at") or ev.get("created_at") or ev.get("received_at") or""
            if ev_created > hwm_created or ev_updated > hwm_updated:
                filtered.append(ev)

        if not filtered:
            continue

        written, skipped = append_events_to_s3(s3, filtered, seen_keys, start_date, page_num)
        total_written += written
        total_skipped += skipped

        # Update high-water marks.
        for ev in filtered:
            c = ev.get("created_at") or ev.get("received_at") or ""
            u = ev.get("updated_at") or ev.get("received_at") or ""
            if c > max_created:
                max_created = c
            if u > max_updated:
                max_updated = u

    # Persist state.
    new_state = State(
        last_created_at=max_created or None,
        last_updated_at=max_updated or None,
        last_run_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    )
    save_state(s3, new_state)

    log.info(
        "Run complete. Written=%d, Skipped(dedup)=%d, "
        "new HWM created_at=%s, updated_at=%s",
        total_written, total_skipped,
        new_state.last_created_at, new_state.last_updated_at,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
