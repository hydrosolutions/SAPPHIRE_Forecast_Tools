"""LR (linear regression) forecast laptop-export -> CSV -> API helper (P4a).

Called from ``bin/initialize_lr_forecast_history.sh`` as
``python3 -m migration_py.lr_forecast``. Reads a CSV that was exported on
the operator's laptop (via ``bin/export_lr_forecast_history.sh`` /
``psql -X \\copy`` from ``sapphire-postprocessing-db``) and POSTs records to
the postprocessing API ``/lr-forecast/`` endpoint.

Unlike the CSV-source wrappers (P1a runoff DAY, P1b meteo, P3 hydrograph
DAY), the LR forecasts source CSV does NOT live under
``intermediate_data/`` on the deployment server. It is a laptop-side export
copied to the server (``scp``) into a wrapper temp dir. The wrapper validates
its sidecar ``<csv>.manifest`` via ``migration_py._common.validate_manifest``
before any POST.

Source DB: ``sapphire-postprocessing-db`` (NOT preprocessing-db). The
``lr_forecasts`` table lives in the postprocessing service.

Schema specifics (from
``sapphire/services/postprocessing/app/schemas.py::LRForecastBase`` and
``models.py::LRForecast``):

- The DB unique key is ``(horizon_type, code, date)``. There is NO
  ``model_type`` column — LR is implicit in this table (which is why LR rows
  are filtered OUT of ``combined_forecasts`` by the P-postprocessing
  combined_forecasts migrator).
- ``horizon_type`` is a lowercase enum: ``"pentad"`` / ``"decade"`` (architecture
  §Q4 lock — preserve API enum values, do NOT uppercase). Uppercase would be
  rejected at the Pydantic boundary.
- Required fields: ``horizon_type``, ``code``, ``date``, ``horizon_value``,
  ``horizon_in_year``.
- Nullable fields: ``discharge_avg``, ``predictor``, ``slope``, ``intercept``,
  ``forecasted_discharge``, ``q_mean``, ``q_std_sigma``, ``delta``,
  ``rsquared``.

CSV column mapping (mirrors ``LRForecastDataMigrator.prepare_pentad_data`` /
``prepare_decade_data`` in
``sapphire/services/postprocessing/app/data_migrator.py``):

- pentad CSV: ``pentad_in_month`` -> ``horizon_value``,
  ``pentad_in_year`` -> ``horizon_in_year``
- decade CSV: ``decad_in_month`` -> ``horizon_value``,
  ``decad_in_year`` -> ``horizon_in_year``

The ``--horizon`` CLI arg selects pentad vs decade and drives the column
mapping. A mismatch between ``--horizon`` and what the source CSV actually
contains is detected by missing required column.

Universal safe-write rule (architecture §Q2 layer 2): nullable fields with a
NULL / empty / NaN source value are OMITTED from the payload — never sent as
``null``. The service-side ``_has_changes + setattr`` path would otherwise
overwrite existing non-NULL targets with incoming NULL.

Idempotency: the postprocessing service upserts on
``(horizon_type, code, date)``; reruns are safe.

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

logger = logging.getLogger("migration_py.lr_forecast")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Allowed --horizon values and the API enum each maps to (architecture §Q4).
_ALLOWED_HORIZONS: frozenset[str] = frozenset({"pentad", "decade"})

# Per-horizon CSV column mapping for horizon_value / horizon_in_year.
# Mirrors LRForecastDataMigrator in sapphire/services/postprocessing/app/data_migrator.py.
_HORIZON_COLUMN_MAP: dict[str, dict[str, str]] = {
    "pentad": {
        "horizon_value": "pentad_in_month",
        "horizon_in_year": "pentad_in_year",
    },
    "decade": {
        "horizon_value": "decad_in_month",
        "horizon_in_year": "decad_in_year",
    },
}

# Required columns in every source row (in addition to the per-horizon
# horizon_value/horizon_in_year columns). Parse fails otherwise.
_REQUIRED_BASE_COLUMNS: frozenset[str] = frozenset({"code", "date"})

# Nullable float fields. Order is preserved for stable dry-run output.
_NULLABLE_FLOAT_FIELDS: tuple[str, ...] = (
    "discharge_avg",
    "predictor",
    "slope",
    "intercept",
    "forecasted_discharge",
    "q_mean",
    "q_std_sigma",
    "delta",
    "rsquared",
)


# ---------------------------------------------------------------------------
# Value parsing
# ---------------------------------------------------------------------------


def _parse_float(raw: str | None) -> float | None:
    """Return a float, or None if unparseable / null-like.

    Rejects non-finite floats (NaN, +/-Inf) so the payload cannot embed
    ``"NaN"``/``"Infinity"`` literals (invalid JSON per RFC 7159; rejected by
    the receiving Pydantic schema).
    """
    s = (raw or "").strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    try:
        v = float(s)
    except ValueError:
        return None
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
# Record building
# ---------------------------------------------------------------------------


def _build_record(row: dict[str, str], horizon: str) -> dict | None:
    """Build the per-row API payload for an LR forecast record.

    Implements the universal safe-write rule: only non-NULL nullable fields
    are included in the returned dict. Required fields are always included
    when they parse.

    Args:
        row: CSV row as ``{column_name: value_str}``.
        horizon: ``"pentad"`` or ``"decade"`` — selects the
            horizon_value / horizon_in_year source columns.

    Returns:
        A payload dict, or ``None`` if the row cannot satisfy the required
        fields (caller treats this as a parse skip).
    """
    if horizon not in _ALLOWED_HORIZONS:
        # Defensive: callers normalize this; raise an explicit error if not.
        raise ValueError(f"horizon must be one of {sorted(_ALLOWED_HORIZONS)}, got {horizon!r}")

    code = (row.get("code") or "").strip()
    date_str = (row.get("date") or "").strip()[:10]
    if not code or not date_str:
        return None
    # Validate date format; reject malformed.
    try:
        datetime.date.fromisoformat(date_str)
    except ValueError:
        return None

    col_map = _HORIZON_COLUMN_MAP[horizon]
    horizon_value = _parse_int(row.get(col_map["horizon_value"]))
    horizon_in_year = _parse_int(row.get(col_map["horizon_in_year"]))
    if horizon_value is None or horizon_in_year is None:
        # Required fields missing/unparseable → parse skip.
        return None

    rec: dict = {
        "horizon_type": horizon,
        "code": code,
        "date": date_str,
        "horizon_value": horizon_value,
        "horizon_in_year": horizon_in_year,
    }

    # Nullable float fields — only include non-NULL values.
    for field in _NULLABLE_FLOAT_FIELDS:
        v = _parse_float(row.get(field))
        if v is not None:
            rec[field] = v

    return rec


def _read_filtered_records(
    csv_path: Path,
    *,
    horizon: str,
    cutoff: str | None,
    station_filter: str | None,
) -> tuple[
    list[dict],
    dict[str, int],
    set[str],
    str | None,
    str | None,
]:
    """Read CSV and return (records, counters, distinct_codes, date_min, date_max).

    Args:
        csv_path: path to the LR-forecast export CSV.
        horizon: ``"pentad"`` or ``"decade"`` — selects column mapping.
        cutoff: optional ISO date; rows with ``date >= cutoff`` are dropped.
            None means full-import (no date filter).
        station_filter: optional single station code; rows whose ``code``
            does not match are dropped. None means no station filter.

    Returns:
        Tuple ``(records, counters, distinct_codes, date_min, date_max)``
        where:
            - records: list of payload dicts (only non-NULL fields, post-filter).
            - counters: dict with ``source_row_count``, ``filtered_row_count``,
              ``skipped_parse``, ``skipped_cutoff``, ``skipped_station``.
            - distinct_codes: set of codes that survived ALL filters.
            - date_min / date_max: source CSV date range (pre-filter) for
              dry-run inventory.

    Raises:
        ValueError: if required columns are missing for the given horizon.
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

    if horizon not in _ALLOWED_HORIZONS:
        raise ValueError(f"horizon must be one of {sorted(_ALLOWED_HORIZONS)}, got {horizon!r}")

    with csv_path.open(newline="") as f:
        reader = csv.DictReader(f)
        header = list(reader.fieldnames or [])
        # Validate required columns: base + per-horizon columns.
        col_map = _HORIZON_COLUMN_MAP[horizon]
        required = _REQUIRED_BASE_COLUMNS | {col_map["horizon_value"], col_map["horizon_in_year"]}
        missing = required - set(header)
        if missing:
            raise ValueError(
                f"CSV {csv_path.name} is missing required column(s) for "
                f"horizon={horizon!r}: {sorted(missing)}"
            )

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

            # Station filter (before cutoff for clarity in counters).
            if station_filter is not None and code != station_filter:
                counters["skipped_station"] += 1
                continue

            # Cutoff filter (strictly less than cutoff).
            if cutoff is not None and date_str >= cutoff:
                counters["skipped_cutoff"] += 1
                continue

            record = _build_record(row, horizon)
            if record is None:
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

    The postprocessing API expects ``{"data": [<record>, ...]}`` per the
    ``LRForecastBulkCreate`` schema. Returns ``(ok, message)``.
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
        prog="python3 -m migration_py.lr_forecast",
        description="Push LR-forecast CSV-export rows to the postprocessing API.",
    )
    p.add_argument(
        "--csv-path",
        required=True,
        type=Path,
        help="Path to the LR-forecast export CSV inside the container.",
    )
    p.add_argument(
        "--api-url",
        required=True,
        help=("Postprocessing API endpoint URL (e.g. http://localhost:8003/lr-forecast/)."),
    )
    p.add_argument(
        "--horizon",
        required=True,
        choices=sorted(_ALLOWED_HORIZONS),
        help="Which lr_forecasts horizon this CSV represents.",
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
    horizon: str,
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
    print("TARGET_TABLE=lr_forecasts")
    print(f"HORIZON={horizon}")
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

    horizon: str = args.horizon

    # MODE is decided by the wrapper from the psql query; we just receive
    # the cutoff (or None) and pass it through.
    cutoff: str | None = args.cutoff
    mode = "pre-cutoff" if cutoff else "full-import"

    try:
        records, counters, distinct_codes, date_min, date_max = _read_filtered_records(
            csv_path,
            horizon=horizon,
            cutoff=cutoff,
            station_filter=args.station_filter,
        )
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    _print_dry_run_inventory(
        csv_path=csv_path,
        horizon=horizon,
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
