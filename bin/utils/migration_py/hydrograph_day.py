"""Hydrograph DAY CSV-to-API push helper (P3).

Called from ``bin/initialize_hydrograph_day_history.sh`` as
``python3 -m migration_py.hydrograph_day``. Reads a CSV with a header that
includes ``code``, ``date``, ``day_of_year``, optional stat columns
(``count``, ``mean``, ``std``, ``min``, ``max``, ``norm``), optional quantile
columns (``5%``/``25%``/``50%``/``75%``/``95%``, normalized to
``q05``/``q25``/``q50``/``q75``/``q95``), and DYNAMICALLY DISCOVERED
year-named columns (e.g. ``2025``, ``2026``) that map to ``previous`` and
``current`` in the payload.

Year-column discovery (P3-specific):
    The source CSV's year columns rotate every January. The legacy in-service
    migrator hardcoded ``'2024'``/``'2025'`` (data_migrator.py:264-265) and
    silently mismapped after each new year. This module finds all 4-digit
    year-named columns, sorts numerically, and maps:
        - ``current``  = LAST year column (newest)
        - ``previous`` = SECOND-TO-LAST year column

    The dry-run inventory prints:
        ``HYDROGRAPH_YEAR_MAPPING={previous: 2025, current: 2026}``
    so the operator validates the mapping before any write.

Universal safe-write rule (architecture Q2 layer 2):
    Hydrograph rows have many nullable stat/quantile/year fields. Only non-NULL
    source fields are sent — fields absent from the source CSV row are simply
    omitted from the payload (never sent as ``null``). The service-side
    ``_has_changes + setattr`` path would otherwise overwrite existing
    non-NULL targets with incoming NULL.

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
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

# Intra-package sibling import — relative form (level >= 1) is the explicit
# pattern recognized by ``_audit.collect_imported_roots`` as intra-package and
# skipped from the stdlib check.
from . import _common

logger = logging.getLogger("migration_py.hydrograph_day")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_YEAR_COL_RE = re.compile(r"^\d{4}$")

# CSV quantile column names -> payload key. The CSV emits names like '5%' /
# '95%'; the API schema expects 'q05' / 'q95'. If the source CSV already uses
# normalized names ('q05'..'q95'), those are also accepted as-is.
_QUANTILE_CSV_TO_PAYLOAD: dict[str, str] = {
    "5%": "q05",
    "25%": "q25",
    "50%": "q50",
    "75%": "q75",
    "95%": "q95",
}

# Required columns in every source row (parse fails otherwise).
_REQUIRED_COLUMNS: frozenset[str] = frozenset({"code", "date"})


# ---------------------------------------------------------------------------
# Value parsing
# ---------------------------------------------------------------------------


def _parse_float(raw: str | None) -> float | None:
    """Return a float, or None if unparseable / null-like."""
    s = (raw or "").strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    try:
        v = float(s)
    except ValueError:
        return None
    # Reject non-finite floats (NaN, +/-Inf) — they cannot be JSON-serialized
    # safely (Python's json.dumps emits "NaN"/"Infinity" which are not valid
    # JSON per RFC 7159 and the receiving Pydantic schema would reject them).
    if v != v or v == float("inf") or v == float("-inf"):
        return None
    return v


def _parse_int(raw: str | None) -> int | None:
    """Return an int, or None if unparseable / null-like."""
    s = (raw or "").strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    try:
        # Tolerate "10.0" style values (CSV exports sometimes coerce ints).
        return int(float(s))
    except (ValueError, OverflowError):
        return None


# ---------------------------------------------------------------------------
# Year-column discovery
# ---------------------------------------------------------------------------


def _discover_year_columns(header: list[str]) -> tuple[int, int]:
    """Find the two newest 4-digit year columns in ``header``.

    The legacy in-service migrator hardcoded ``'2024'``/``'2025'`` and broke
    silently every January. This function instead inspects the actual CSV
    header.

    Args:
        header: list of CSV column names (e.g. ``['code', 'date', '2025',
            '2026', 'mean']``).

    Returns:
        Tuple ``(previous_year, current_year)`` where ``previous_year`` is the
        second-newest 4-digit column and ``current_year`` is the newest.

    Raises:
        ValueError: if fewer than 2 columns match ``^\\d{4}$``. The hydrograph
            DAY CSV is required to expose both a previous-year and a
            current-year column; missing either breaks the
            ``previous``/``current`` payload mapping.
    """
    year_cols = sorted(int(name) for name in header if _YEAR_COL_RE.match(name or ""))
    if len(year_cols) < 2:
        raise ValueError(
            "hydrograph_day CSV header must contain at least 2 year columns "
            f"(found {len(year_cols)}: {year_cols!r}). "
            "Year columns rotate each January and are required to map the "
            "'previous' and 'current' payload fields."
        )
    # Sort guarantees year_cols[-1] > year_cols[-2].
    return (year_cols[-2], year_cols[-1])


# ---------------------------------------------------------------------------
# Record building
# ---------------------------------------------------------------------------


def _build_record(
    row: dict[str, str],
    year_map: dict[str, int],
) -> dict | None:
    """Build the per-row API payload for a hydrograph DAY record.

    Implements the universal safe-write rule: only non-NULL fields are
    included in the returned dict. Required fields per ``HydrographBase`` are
    always sent if present in the row; nullable stat/quantile/year fields are
    omitted when absent or unparseable.

    Args:
        row: CSV row as ``{column_name: value_str}``.
        year_map: ``{'previous': <int>, 'current': <int>}`` from
            ``_discover_year_columns``.

    Returns:
        A payload dict, or ``None`` if the row cannot satisfy the required
        fields (caller treats this as a parse skip).
    """
    code = (row.get("code") or "").strip()
    date_str = (row.get("date") or "").strip()[:10]
    if not code or not date_str:
        return None
    try:
        d = datetime.date.fromisoformat(date_str)
    except ValueError:
        return None

    # day_of_year is required by HydrographBase; if not in CSV, derive from date.
    day_of_year_raw = row.get("day_of_year")
    day_of_year = _parse_int(day_of_year_raw)
    if day_of_year is None:
        day_of_year = d.timetuple().tm_yday

    rec: dict = {
        "horizon_type": "day",
        "code": code,
        "date": date_str,
        "horizon_value": d.day,
        "horizon_in_year": d.timetuple().tm_yday,
        "day_of_year": day_of_year,
    }

    # --- Optional integer stat ---
    count = _parse_int(row.get("count"))
    if count is not None:
        rec["count"] = count

    # --- Optional float stats ---
    for col in ("mean", "std", "min", "max", "norm"):
        v = _parse_float(row.get(col))
        if v is not None:
            rec[col] = v

    # --- Optional quantiles (accept both '5%'-style and 'q05'-style names) ---
    for csv_name, payload_key in _QUANTILE_CSV_TO_PAYLOAD.items():
        v = _parse_float(row.get(csv_name))
        if v is not None:
            rec[payload_key] = v
        else:
            # Fallback to the already-normalized key if present in CSV.
            v2 = _parse_float(row.get(payload_key))
            if v2 is not None:
                rec[payload_key] = v2

    # --- Year-column mapping -> previous / current ---
    prev_year = year_map["previous"]
    curr_year = year_map["current"]
    prev_val = _parse_float(row.get(str(prev_year)))
    if prev_val is not None:
        rec["previous"] = prev_val
    curr_val = _parse_float(row.get(str(curr_year)))
    if curr_val is not None:
        rec["current"] = curr_val

    return rec


def _read_filtered_records(
    csv_path: Path,
    *,
    cutoff: str | None,
    station_filter: str | None,
) -> tuple[
    list[dict],
    dict[str, int],
    set[str],
    str | None,
    str | None,
    dict[str, int],
]:
    """Read CSV and return (records, counters, distinct_codes, date_min,
    date_max, year_map).

    Args:
        csv_path: path to the hydrograph_day CSV.
        cutoff: optional ISO date; rows with ``date >= cutoff`` are dropped.
            None means full-import (no date filter).
        station_filter: optional single station code; rows whose ``code`` does
            not match are dropped. None means no station filter.

    Returns:
        Tuple ``(records, counters, distinct_codes, date_min, date_max,
        year_map)`` where:
            - records: list of payload dicts (only non-NULL fields, post-filter).
            - counters: dict with ``source_row_count``, ``filtered_row_count``,
              ``skipped_parse``, ``skipped_cutoff``, ``skipped_station``.
            - distinct_codes: set of codes that survived ALL filters.
            - date_min / date_max: source CSV date range (pre-filter) for
              dry-run inventory.
            - year_map: ``{'previous': <year>, 'current': <year>}`` discovered
              from the CSV header.

    Raises:
        ValueError: if required columns are missing or fewer than 2 year
            columns exist in the header.
    """
    counters = {
        "source_row_count": 0,
        "filtered_row_count": 0,
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
        header = list(reader.fieldnames or [])
        missing = _REQUIRED_COLUMNS - set(header)
        if missing:
            raise ValueError(
                f"CSV {csv_path.name} is missing required column(s): {sorted(missing)}"
            )

        prev_year, curr_year = _discover_year_columns(header)
        year_map = {"previous": prev_year, "current": curr_year}

        for row in reader:
            counters["source_row_count"] += 1
            code = (row.get("code") or "").strip()
            date_str = (row.get("date") or "").strip()[:10]

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

            record = _build_record(row, year_map)
            if record is None:
                counters["skipped_parse"] += 1
                continue

            records.append(record)
            distinct_codes.add(code)
            counters["filtered_row_count"] += 1

    return records, counters, distinct_codes, date_min, date_max, year_map


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
    ``HydrographBulkCreate`` schema. Returns ``(ok, message)``.
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
        prog="python3 -m migration_py.hydrograph_day",
        description="Push hydrograph DAY CSV rows to the preprocessing API.",
    )
    p.add_argument(
        "--csv-path",
        required=True,
        type=Path,
        help="Path to the hydrograph_day CSV inside the container.",
    )
    p.add_argument(
        "--api-url",
        required=True,
        help="Preprocessing API endpoint URL (e.g. http://localhost:8002/hydrograph/).",
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
    year_map: dict[str, int] | None,
) -> None:
    """Emit the §4.4 runbook dry-run inventory block.

    Lines are printed on stdout (the wrapper tees them into its log file).
    Station codes are NEVER printed individually — only the redacted count
    via ``log_redacted_station_count``.

    Additionally prints the P3-specific
    ``HYDROGRAPH_YEAR_MAPPING={previous: <year>, current: <year>}`` line so
    the operator validates the dynamic year-column discovery before any write.
    """
    print(f"MODE={mode}" + (f" (cutoff={cutoff})" if cutoff else " (target empty)"))
    print("TARGET_TABLE=hydrographs")
    print(f"CUTOFF={cutoff if cutoff else 'none'}")
    print(f"SOURCE_FILES=['{csv_path}']")
    print(f"SOURCE_ROW_COUNT={counters['source_row_count']}")
    print(f"FILTERED_ROW_COUNT={counters['filtered_row_count']}")
    print(f"SOURCE_DATE_MIN={date_min if date_min else 'none'}")
    print(f"SOURCE_DATE_MAX={date_max if date_max else 'none'}")
    print(f"DISTINCT_STATION_COUNT_REDACTED={len(distinct_codes)}")
    print(
        f"SKIPPED_PARSE={counters['skipped_parse']} "
        f"SKIPPED_CUTOFF={counters['skipped_cutoff']} "
        f"SKIPPED_STATION={counters['skipped_station']}"
    )
    if year_map is not None:
        print(
            "HYDROGRAPH_YEAR_MAPPING="
            f"{{previous: {year_map['previous']}, current: {year_map['current']}}}"
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

    try:
        records, counters, distinct_codes, date_min, date_max, year_map = _read_filtered_records(
            csv_path,
            cutoff=cutoff,
            station_filter=args.station_filter,
        )
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    _print_dry_run_inventory(
        csv_path=csv_path,
        counters=counters,
        distinct_codes=distinct_codes,
        date_min=date_min,
        date_max=date_max,
        mode=mode,
        cutoff=cutoff,
        year_map=year_map,
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
