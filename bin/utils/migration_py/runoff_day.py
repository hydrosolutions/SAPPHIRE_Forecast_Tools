"""Runoff DAY CSV-to-API push helper (P1a).

Called from ``bin/initialize_runoff_day_history.sh`` as
``python3 -m migration_py.runoff_day``. Reads a CSV with header
``code,date,discharge``, applies an optional ``--cutoff`` date filter (rows
strictly before ``cutoff``) and an optional ``--station-filter`` (single code),
and POSTs records to the preprocessing API in batches.

Rule (universal safe-write per architecture Q2 layer 2): only non-NULL fields
are sent — rows without a parseable discharge are skipped, never sent with
``discharge=null``. The DAY payload intentionally omits ``predictor``.

Idempotency: the service upserts on ``(horizon_type, code, date)``; reruns are
safe.

Stdlib-only. Verified by ``migration_py._audit.audit_stdlib_only``.
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import datetime
import json
import logging
import sys
import urllib.error
import urllib.request
from pathlib import Path

# Intra-package sibling import — relative form (level >= 1) is the explicit
# pattern recognized by ``_audit.collect_imported_roots`` as intra-package and
# skipped from the stdlib check.
from . import _common

logger = logging.getLogger("migration_py.runoff_day")


# ---------------------------------------------------------------------------
# Record building
# ---------------------------------------------------------------------------


def _parse_discharge(raw: str) -> float | None:
    """Return a float discharge, or None if unparseable / null-like."""
    s = (raw or "").strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _build_record(code: str, date_str: str, discharge: float) -> dict:
    """Build the per-row API payload for a DAY runoff record.

    Returns a dict containing only non-NULL fields required by the
    ``RunoffCreate`` schema. ``predictor`` is intentionally omitted (DAY rows
    have no predictor); the universal safe-write rule applies.

    Args:
        code: station code (non-empty).
        date_str: ISO date string ``YYYY-MM-DD``.
        discharge: parsed float discharge value.

    Returns:
        dict with keys ``horizon_type, code, date, discharge, horizon_value,
        horizon_in_year``.
    """
    d = datetime.date.fromisoformat(date_str)
    # horizon_value for DAY = day-of-month (1..31)
    # horizon_in_year for DAY = day-of-year (1..366)
    return {
        "horizon_type": "day",
        "code": code,
        "date": date_str,
        "discharge": discharge,
        "horizon_value": d.day,
        "horizon_in_year": d.timetuple().tm_yday,
    }


def _read_filtered_records(
    csv_path: Path,
    *,
    cutoff: str | None,
    station_filter: str | None,
) -> tuple[list[dict], dict[str, int], set[str], str | None, str | None]:
    """Read CSV and return (records, counters, distinct_codes, date_min, date_max).

    Args:
        csv_path: path to the runoff_day CSV (header ``code,date,discharge``).
        cutoff: optional ISO date; rows with ``date >= cutoff`` are dropped.
            None means full-import (no date filter).
        station_filter: optional single station code; rows whose ``code`` does
            not match are dropped. None means no station filter.

    Returns:
        Tuple ``(records, counters, distinct_codes, date_min, date_max)`` where:
            - records: list of payload dicts (only non-NULL, post-filter).
            - counters: dict with ``source_row_count``, ``filtered_row_count``,
              ``skipped_null``, ``skipped_parse``, ``skipped_cutoff``,
              ``skipped_station``.
            - distinct_codes: set of codes that survived ALL filters.
            - date_min / date_max: source CSV date range (pre-filter) for
              dry-run inventory.
    """
    counters = {
        "source_row_count": 0,
        "filtered_row_count": 0,
        "skipped_null": 0,
        "skipped_parse": 0,
        "skipped_cutoff": 0,
        "skipped_station": 0,
    }
    records: list[dict] = []
    distinct_codes: set[str] = set()
    date_min: str | None = None
    date_max: str | None = None

    with csv_path.open(newline="") as f:
        reader = csv.DictReader(f)
        required = {"code", "date", "discharge"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"CSV {csv_path.name} is missing required column(s): {sorted(missing)}"
            )
        for row in reader:
            counters["source_row_count"] += 1
            code = (row.get("code") or "").strip()
            date_str = (row.get("date") or "").strip()[:10]
            discharge_raw = (row.get("discharge") or "").strip()

            # Track source date range pre-filter (for dry-run inventory).
            if date_str:
                if date_min is None or date_str < date_min:
                    date_min = date_str
                if date_max is None or date_str > date_max:
                    date_max = date_str

            if not code or not date_str:
                counters["skipped_parse"] += 1
                continue

            # Station filter (applied before cutoff for clarity in counters).
            if station_filter is not None and code != station_filter:
                counters["skipped_station"] += 1
                continue

            # Cutoff filter (strictly less than cutoff).
            if cutoff is not None and date_str >= cutoff:
                counters["skipped_cutoff"] += 1
                continue

            discharge = _parse_discharge(discharge_raw)
            if discharge is None:
                counters["skipped_null"] += 1
                continue

            try:
                record = _build_record(code, date_str, discharge)
            except ValueError:
                # Bad date (not ISO) — count as parse skip.
                counters["skipped_parse"] += 1
                continue

            records.append(record)
            distinct_codes.add(code)
            counters["filtered_row_count"] += 1

    return records, counters, distinct_codes, date_min, date_max


# ---------------------------------------------------------------------------
# HTTP POST
# ---------------------------------------------------------------------------


def _post_batch(
    batch: list[dict],
    url: str,
    *,
    timeout: float = 120.0,
) -> tuple[bool, str]:
    """POST a single batch envelope to the API.

    The preprocessing API expects ``{"data": [<record>, ...]}`` per the
    ``RunoffBulkCreate`` schema. Returns ``(ok, message)``.
    """
    body = json.dumps({"data": batch}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
            return (200 <= resp.status < 300), f"HTTP {resp.status}"
    except urllib.error.HTTPError as e:
        body_txt = ""
        with contextlib.suppress(Exception):
            body_txt = e.read().decode("utf-8", errors="replace")[:200]
        return False, f"HTTP {e.code}: {body_txt}"
    except Exception as e:  # noqa: BLE001
        return False, f"{type(e).__name__}: {e}"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python3 -m migration_py.runoff_day",
        description="Push runoff DAY CSV rows to the preprocessing API.",
    )
    p.add_argument(
        "--csv-path",
        required=True,
        type=Path,
        help="Path to the runoff_day CSV inside the container.",
    )
    p.add_argument(
        "--api-url",
        required=True,
        help="Preprocessing API endpoint URL (e.g. http://localhost:8002/runoff/).",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Records per POST batch (default 500).",
    )
    p.add_argument(
        "--cutoff",
        default=None,
        help="ISO date; rows with date >= cutoff are dropped (pre-cutoff mode).",
    )
    p.add_argument(
        "--station-filter",
        default=None,
        help="Filter to a single station code.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Read + filter only; do NOT POST.",
    )
    return p


def _print_dry_run_inventory(
    *,
    csv_path: Path,
    counters: dict[str, int],
    distinct_codes: set[str],
    date_min: str | None,
    date_max: str | None,
    mode: str,
    cutoff: str | None,
) -> None:
    """Emit the §4.4 runbook dry-run inventory block.

    Lines are printed on stdout (the wrapper tees them into its log file).
    Station codes are NEVER printed individually — only the redacted count
    via ``log_redacted_station_count``.
    """
    print(f"MODE={mode}" + (f" (cutoff={cutoff})" if cutoff else " (target empty)"))
    print("TARGET_TABLE=runoffs")
    print(f"CUTOFF={cutoff if cutoff else 'none'}")
    print(f"SOURCE_FILES=['{csv_path}']")
    print(f"SOURCE_ROW_COUNT={counters['source_row_count']}")
    print(f"FILTERED_ROW_COUNT={counters['filtered_row_count']}")
    print(f"SOURCE_DATE_MIN={date_min if date_min else 'none'}")
    print(f"SOURCE_DATE_MAX={date_max if date_max else 'none'}")
    print(f"DISTINCT_STATION_COUNT_REDACTED={len(distinct_codes)}")
    print(
        f"SKIPPED_NULL={counters['skipped_null']} "
        f"SKIPPED_PARSE={counters['skipped_parse']} "
        f"SKIPPED_CUTOFF={counters['skipped_cutoff']} "
        f"SKIPPED_STATION={counters['skipped_station']}"
    )
    # Redacted log line (count only, never the actual codes).
    _common.log_redacted_station_count(
        logger, sorted(distinct_codes), message_prefix="post_filter_stations"
    )


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    csv_path: Path = args.csv_path
    if not csv_path.is_file():
        print(f"ERROR: CSV not found: {csv_path}", file=sys.stderr)
        return 1

    # MODE is decided by the wrapper from the psql query; we just receive
    # the cutoff (or None) and pass it through.
    cutoff: str | None = args.cutoff
    mode = "pre-cutoff" if cutoff else "full-import"

    records, counters, distinct_codes, date_min, date_max = _read_filtered_records(
        csv_path,
        cutoff=cutoff,
        station_filter=args.station_filter,
    )

    _print_dry_run_inventory(
        csv_path=csv_path,
        counters=counters,
        distinct_codes=distinct_codes,
        date_min=date_min,
        date_max=date_max,
        mode=mode,
        cutoff=cutoff,
    )

    if args.dry_run:
        print("DRY RUN: no POSTs attempted.")
        return 0

    if not records:
        print("No records to POST after filtering; exiting 0.")
        return 0

    n = len(records)
    batch_size = max(1, args.batch_size)
    n_batches = (n + batch_size - 1) // batch_size
    sent = 0
    failed = 0
    for i in range(0, n, batch_size):
        batch = records[i : i + batch_size]
        batch_num = i // batch_size + 1
        ok, msg = _post_batch(batch, args.api_url)
        if ok:
            sent += len(batch)
        else:
            failed += len(batch)
            print(
                f"  batch {batch_num}/{n_batches} FAILED: {msg}",
                file=sys.stderr,
            )
        if batch_num % 20 == 0 or batch_num == n_batches:
            print(f"  progress {batch_num}/{n_batches} ({sent}/{n} sent)")

    print(f"GRAND TOTAL: sent={sent} failed={failed} (of {n} eligible records)")
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
