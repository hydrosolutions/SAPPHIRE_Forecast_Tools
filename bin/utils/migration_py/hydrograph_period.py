"""Hydrograph PENTAD/DECADE laptop-export CSV-to-API push helper (P2b).

Called from ``bin/initialize_hydrograph_period_history.sh`` as
``python3 -m migration_py.hydrograph_period``. Reads a CSV produced by the
laptop-side ``bin/export_hydrograph_period_history.sh`` script (a direct dump
of the laptop's ``sapphire-preprocessing-db.hydrographs`` rows filtered on
``horizon_type IN ('PENTAD','DECADE')``) plus its sidecar ``.manifest`` file,
validates the manifest (5 required keys per P0 contract), and POSTs the rows
to the deployment server's preprocessing API.

P2b sibling to P2a (runoff PENTAD/DECADE) — same shape (laptop-export → CSV +
manifest → server-side import), different table (``hydrographs`` instead of
``runoffs``) and different payload (wide-stat row with quantiles + year-mapped
``previous``/``current`` instead of ``discharge`` + ``predictor``).

Year-column handling decision (per brief §4.2):
    The laptop-side ``export_hydrograph_period_history.sh`` exports
    ``previous`` / ``current`` from the DB AS-IS using those literal column
    names (the DB row model defines them, per
    ``sapphire/services/preprocessing/app/schemas.py:59-60``). NO year-column
    discovery is performed in this module — that pattern is specific to the
    CSV-source P3 hydrograph DAY wrapper, which reads
    ``intermediate_data/hydrograph_day.csv`` (a wide CSV where columns rotate
    annually). Here the source is a DB row, so the field names are stable.

    Rationale: keeping ``previous``/``current`` as the canonical CSV column
    names matches the payload, avoids per-deployment / per-year discovery
    surprises, and keeps the export script trivial (a single
    ``COPY (SELECT ... previous, current ...) TO STDOUT``).

Universal safe-write rule (architecture §Q2 layer 2):
    Hydrograph PENTAD/DECADE rows have many nullable stat / quantile /
    year-mapped fields. By default the wrapper sends ONLY non-NULL source
    fields (enrichment-only). The service-side ``_has_changes`` + ``setattr``
    path overwrites existing non-NULL targets with incoming NULL, so the
    wrapper never injects null.

``--strict-merge`` opt-in (future flag):
    The brief asks for a documented ``--strict-merge`` design decision. The
    flag is parsed by the CLI but not yet implemented (logs a warning and
    falls back to enrichment-only). If an operator observes stat erasure in
    production, a follow-up PR enables read-before-merge: GET target row,
    merge incoming non-NULL with existing target, POST merged record. The
    current default keeps parity with P1a / P1b / P3 / snow (all
    enrichment-only).

Manifest contract (per P0 ``_common.validate_manifest`` + brief §4.1):
    Sidecar at ``<csv_path>.manifest`` must contain 5 keys:
        export_type=hydrograph_period
        row_count=<int>          # excluding header
        station_count=<int>      # distinct ``code`` values in CSV
        date_min=<YYYY-MM-DD>    # min(date) in CSV
        date_max=<YYYY-MM-DD>    # max(date) in CSV

Idempotency: the service upserts on ``(horizon_type, code, date)``; reruns
are safe.

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

logger = logging.getLogger("migration_py.hydrograph_period")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Horizons this module supports. Service-side schema uses uppercase
# ``HorizonType`` values ("PENTAD", "DECADE"); payloads serialize as the
# lowercase enum value form ("pentad", "decade") for consistency with the
# P1a / P3 wrappers (the FastAPI router accepts either case via the enum).
_HORIZON_TO_PAYLOAD: dict[str, str] = {
    "pentad": "pentad",
    "decade": "decade",
}

# Required columns in every source CSV row (parse fails otherwise). These are
# the minimum needed to construct the natural key + ``horizon_value`` +
# ``horizon_in_year`` + ``day_of_year``.
_REQUIRED_COLUMNS_BASE: frozenset[str] = frozenset(
    {"code", "date", "horizon_value", "horizon_in_year", "day_of_year"}
)

# Nullable float stat / quantile / year-mapped fields. The CSV may use either
# the canonical payload key (``q05``..``q95``) OR the legacy percent-style
# (``5%``..``95%``); the parser accepts both forms. The wrapper sends only
# non-NULL fields per the safe-write rule.
_FLOAT_FIELDS: tuple[str, ...] = (
    "mean",
    "std",
    "min",
    "max",
    "norm",
    "previous",
    "current",
)

_QUANTILE_CSV_TO_PAYLOAD: dict[str, str] = {
    "5%": "q05",
    "25%": "q25",
    "50%": "q50",
    "75%": "q75",
    "95%": "q95",
}


# ---------------------------------------------------------------------------
# Value parsing
# ---------------------------------------------------------------------------


def _parse_float(raw: str | None) -> float | None:
    """Return a float, or None if unparseable / null-like / non-finite."""
    s = (raw or "").strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    try:
        v = float(s)
    except ValueError:
        return None
    # Reject NaN / +/-Inf — they cannot be JSON-serialized safely and the
    # Pydantic schema would reject them.
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
    """Build the per-row API payload for a hydrograph PENTAD/DECADE record.

    Implements the universal safe-write rule: only non-NULL fields are
    included in the returned dict. Required fields per ``HydrographBase``
    (``horizon_type``, ``code``, ``date``, ``horizon_value``,
    ``horizon_in_year``, ``day_of_year``) are always included; every other
    field is omitted when the source value is absent / unparseable.

    Row ``horizon_type`` validation: if the source CSV row includes a
    ``horizon_type`` column, its value (lowercased) is compared to the CLI
    ``horizon`` argument. A mismatch causes this function to return ``None``
    (the row is dropped). This catches mixed-horizon CSV files and the
    common export mistake of passing --horizon=pentad for a decade CSV.

    Args:
        row: CSV row as ``{column_name: value_str}``.
        horizon: ``"pentad"`` or ``"decade"`` (lowercase payload form).

    Returns:
        A payload dict matching ``HydrographCreate``, or ``None`` if the row
        cannot satisfy the required fields OR if the row's ``horizon_type``
        column (when present) does not match ``horizon`` (caller treats this
        as a parse skip with mismatch warning).
    """
    if horizon not in _HORIZON_TO_PAYLOAD:
        return None

    # Validate row horizon_type against CLI horizon when the column is present.
    row_horizon_raw = row.get("horizon_type")
    if row_horizon_raw is not None:
        row_horizon = row_horizon_raw.strip().lower()
        if row_horizon and row_horizon != horizon:
            return None

    code = (row.get("code") or "").strip()
    date_str = (row.get("date") or "").strip()[:10]
    if not code or not date_str:
        return None
    try:
        datetime.date.fromisoformat(date_str)
    except ValueError:
        return None

    horizon_value = _parse_int(row.get("horizon_value"))
    horizon_in_year = _parse_int(row.get("horizon_in_year"))
    day_of_year = _parse_int(row.get("day_of_year"))
    if horizon_value is None or horizon_in_year is None or day_of_year is None:
        return None

    rec: dict = {
        "horizon_type": _HORIZON_TO_PAYLOAD[horizon],
        "code": code,
        "date": date_str,
        "horizon_value": horizon_value,
        "horizon_in_year": horizon_in_year,
        "day_of_year": day_of_year,
    }

    # --- Optional integer stat ---
    count = _parse_int(row.get("count"))
    if count is not None:
        rec["count"] = count

    # --- Optional float stats / norm / year-mapped previous/current ---
    for col in _FLOAT_FIELDS:
        v = _parse_float(row.get(col))
        if v is not None:
            rec[col] = v

    # --- Optional quantiles (accept canonical ``q05``-style and legacy
    #     percent-style ``5%`` column names) ---
    for csv_name, payload_key in _QUANTILE_CSV_TO_PAYLOAD.items():
        v = _parse_float(row.get(payload_key))
        if v is not None:
            rec[payload_key] = v
            continue
        # Fallback: legacy percent-style header.
        v2 = _parse_float(row.get(csv_name))
        if v2 is not None:
            rec[payload_key] = v2

    return rec


def _read_filtered_records_with_manifest(
    csv_path: Path,
    manifest_path: Path,
    horizon: str,
    *,
    cutoff: str | None,
    station_filter: str | None,
) -> tuple[
    list[dict],
    dict[str, int],
    set[str],
    str | None,
    str | None,
]:
    """Read CSV (manifest-validated) and return per-record state.

    Manifest validation is performed BEFORE record building so a stale or
    cross-org CSV is rejected up front (catches the security-relevant leak
    case described in P0 §Q5).

    Args:
        csv_path: path to the export CSV.
        manifest_path: path to the sibling ``.manifest`` file (passed for
            error message clarity; the actual validation reads
            ``<csv_path>.manifest`` via ``_common.validate_manifest``).
        horizon: ``"pentad"`` or ``"decade"``.
        cutoff: optional ISO date; rows with ``date >= cutoff`` are dropped.
            None means full-import (no date filter).
        station_filter: optional single station code; rows whose ``code``
            does not match are dropped. None means no station filter.

    Returns:
        Tuple ``(records, counters, distinct_codes, date_min, date_max)`` where:
            - records: list of payload dicts (only non-NULL fields, post-filter).
            - counters: dict with ``source_row_count``, ``filtered_row_count``,
              ``skipped_parse``, ``skipped_cutoff``, ``skipped_station``.
            - distinct_codes: set of codes that survived ALL filters.
            - date_min / date_max: source CSV date range (pre-filter).

    Raises:
        ValueError: if required columns are missing.
        _common.ManifestError (or subclass): if manifest validation fails.
    """
    # Validate manifest first; any failure raises an explicit subclass of
    # ManifestError (caught by main() and reported with non-zero exit).
    _common.validate_manifest(csv_path, "hydrograph_period")

    # manifest_path is documented for the caller; we don't read it directly
    # here because validate_manifest derives the sidecar path itself.
    _ = manifest_path

    counters = {
        "source_row_count": 0,
        "filtered_row_count": 0,
        "skipped_parse": 0,
        "skipped_cutoff": 0,
        "skipped_station": 0,
        "skipped_horizon_mismatch": 0,
    }
    records: list[dict] = []
    distinct_codes: set[str] = set()
    date_min: str | None = None
    date_max: str | None = None

    with csv_path.open(newline="") as f:
        reader = csv.DictReader(f)
        header = list(reader.fieldnames or [])
        missing = _REQUIRED_COLUMNS_BASE - set(header)
        if missing:
            raise ValueError(
                f"CSV {csv_path.name} is missing required column(s): {sorted(missing)}"
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

            # Station filter (applied before cutoff for clarity in counters).
            if station_filter is not None and code != station_filter:
                counters["skipped_station"] += 1
                continue

            # Cutoff filter (strictly less than cutoff).
            if cutoff is not None and date_str >= cutoff:
                counters["skipped_cutoff"] += 1
                continue

            # Horizon-type mismatch check (before _build_record so we can
            # count mismatches distinctly and emit a first-mismatch warning).
            row_horizon_raw = row.get("horizon_type")
            if row_horizon_raw is not None:
                row_horizon = row_horizon_raw.strip().lower()
                if row_horizon and row_horizon != horizon:
                    counters["skipped_horizon_mismatch"] += 1
                    if counters["skipped_horizon_mismatch"] == 1:
                        logger.warning(
                            "Row horizon_type mismatch: row has %r but CLI --horizon=%r; "
                            "dropping row (code=%r date=%r). Further mismatches counted "
                            "but not individually logged.",
                            row_horizon,
                            horizon,
                            code,
                            date_str,
                        )
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
        prog="python3 -m migration_py.hydrograph_period",
        description=(
            "Push hydrograph PENTAD/DECADE laptop-export CSV rows to the preprocessing API (P2b)."
        ),
    )
    p.add_argument(
        "--csv-path",
        required=True,
        type=Path,
        help="Path to the hydrograph_period export CSV inside the container.",
    )
    p.add_argument(
        "--horizon",
        required=True,
        choices=sorted(_HORIZON_TO_PAYLOAD.keys()),
        help="Which horizon the CSV holds: 'pentad' or 'decade'.",
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
        "--strict-merge",
        action="store_true",
        help=(
            "Opt-in read-before-merge (NOT YET IMPLEMENTED — logs a warning "
            "and falls back to enrichment-only). Future flag for ops who "
            "observe stat erasure under the default enrichment-only path."
        ),
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
    strict_merge: bool,
) -> None:
    """Emit the §4.4 runbook dry-run inventory block.

    Lines are printed on stdout (the wrapper tees them into its log file).
    Station codes are NEVER printed individually — only the redacted count
    via ``log_redacted_station_count``.
    """
    print(f"MODE={mode}" + (f" (cutoff={cutoff})" if cutoff else " (target empty)"))
    print("TARGET_TABLE=hydrographs")
    print(f"HORIZON_TYPE={horizon.lower()}")
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
        f"SKIPPED_STATION={counters['skipped_station']} "
        f"SKIPPED_HORIZON_MISMATCH={counters.get('skipped_horizon_mismatch', 0)}"
    )
    # Safe-write policy line so operators see the active mode in the inventory.
    if strict_merge:
        print("SAFE_WRITE_POLICY=strict-merge (NOT YET IMPLEMENTED — using enrichment-only)")
    else:
        print("SAFE_WRITE_POLICY=enrichment-only (default)")
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
    # The manifest is the sidecar at <csv_path>.manifest; validate_manifest
    # derives the path from the CSV path. Surface a clear error if missing.
    manifest_path: Path = Path(str(csv_path) + ".manifest")
    if not manifest_path.is_file():
        print(f"ERROR: manifest not found: {manifest_path}", file=sys.stderr)
        return 1

    if args.strict_merge:
        logger.warning(
            "--strict-merge requested but NOT YET IMPLEMENTED; falling back "
            "to enrichment-only safe-write. See gi_draft and runbook §6.2 "
            "for the design decision."
        )

    # MODE is decided by the wrapper from the psql query; we just receive
    # the cutoff (or None) and pass it through.
    cutoff: str | None = args.cutoff
    mode = "pre-cutoff" if cutoff else "full-import"

    horizon: str = args.horizon

    try:
        records, counters, distinct_codes, date_min, date_max = (
            _read_filtered_records_with_manifest(
                csv_path,
                manifest_path,
                horizon,
                cutoff=cutoff,
                station_filter=args.station_filter,
            )
        )
    except _common.ManifestError as exc:
        print(f"ERROR: manifest validation failed: {exc}", file=sys.stderr)
        return 1
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
        strict_merge=bool(args.strict_merge),
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
