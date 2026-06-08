"""ML forecast CSV-to-API push helper (P4b — laptop-export pattern).

Called from ``bin/initialize_ml_forecast_history.sh`` as
``python3 -m migration_py.ml_forecast``. Reads a CSV exported from the
laptop's ``sapphire-postprocessing-db`` (table ``forecasts``) filtered to
ML model rows (``model_type IN ('TFT','TiDE','TSMixer')``) and POSTs the
records to the postprocessing API ``/forecast/`` endpoint.

Two architectural quirks documented in the P4b gi_draft:

1. **Enum case sensitivity (Stage A §E live test):** the API ``ModelType``
   enum values are MIXED-CASE — ``TFT``, ``TiDE``, ``TSMixer`` — not all
   uppercase. The legacy on-disk directory naming uses uppercase
   (``predictions/TFT/``, ``predictions/TIDE/``, ``predictions/TSMIXER/``).
   This module exposes ``MODEL_DIR_TO_API`` to map between them. Any source
   string outside the three known dir / API spellings is rejected with
   ``UnknownMLModelTypeError``.

2. **Default horizon storage = ``day`` (user-lock L6):** modern ML CSV-derived
   writes go in as ``horizon_type='day'`` regardless of the caller's pentad /
   decade workflow. This matches the operational writer at
   ``apps/machine_learning/scr/utils_ml_forecast.py:_write_ml_forecast_to_api``
   (commit ``1cb3495``). The opt-in ``--preserve-legacy-ml-horizons`` flag
   instead preserves the source row's ``horizon_type`` (``PENTAD`` / ``DECADE``)
   for the legacy rows that were emitted by pre-1cb3495 code paths. The flag
   emits a prominent WARNING when active.

Universal safe-write rule (architecture Q2 layer 2):
    ML forecast rows have nullable quantile / discharge fields. Only non-NULL
    fields are sent — fields absent / unparseable in the source CSV are
    OMITTED from the payload (never sent as ``null``). The service-side
    upsert path would overwrite existing non-NULL targets with incoming NULL
    otherwise.

Manifest contract (P0):
    The export sidecar ``<csv>.manifest`` must be valid per
    ``migration_py._common.validate_manifest`` with ``export_type=ml_forecast``.
    The wrapper invokes ``umh_validate_export_manifest`` BEFORE calling this
    Python helper; this module assumes the CSV is the manifested file and
    only re-uses the path for content reads.

Idempotency: the postprocessing service upserts on
``(horizon_type, code, model_type, date, target)``; reruns are safe.

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

logger = logging.getLogger("migration_py.ml_forecast")


# ---------------------------------------------------------------------------
# Enum case mapping (Stage A §E live-test result)
# ---------------------------------------------------------------------------

# Maps a source spelling (whether the upstream uses the legacy on-disk
# uppercase ``TIDE``/``TSMIXER`` dir convention, or the API mixed-case form)
# to the canonical API enum value as defined in
# ``sapphire/services/postprocessing/app/models.py::ModelType``.
MODEL_DIR_TO_API: dict[str, str] = {
    "TFT": "TFT",
    "TIDE": "TiDE",
    "TSMIXER": "TSMixer",
    # The API mixed-case values are accepted as-is (idempotent map).
    "TiDE": "TiDE",
    "TSMixer": "TSMixer",
}

# The set of canonical API model_type values that this wrapper migrates.
_API_ML_MODELS: frozenset[str] = frozenset({"TFT", "TiDE", "TSMixer"})

# Allowed horizon_type values when ``--preserve-legacy-ml-horizons`` is set.
# Source CSV strings may be lower-case or upper-case; normalize on read.
_LEGACY_HORIZON_TYPES: frozenset[str] = frozenset({"pentad", "decade"})


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class UnknownMLModelTypeError(ValueError):
    """Raised when a source CSV ``model_type`` cannot be mapped to a known
    API ModelType. Surfaces operator typos and forward-compat ambiguities
    early (before any POST).
    """


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
    # Reject non-finite floats: ``json.dumps`` would emit NaN / Infinity
    # which are not valid JSON per RFC 7159; the API schema would reject.
    if v != v or v == float("inf") or v == float("-inf"):
        return None
    return v


def _parse_int(raw: str | None) -> int | None:
    """Return an int, or None if unparseable / null-like."""
    s = (raw or "").strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    try:
        return int(float(s))
    except (ValueError, OverflowError):
        return None


def _normalize_iso_date(raw: str | None) -> str | None:
    """Return a YYYY-MM-DD string, or None if unparseable.

    Accepts ``2024-05-30``, ``2024-05-30 00:00:00``, ``2024-05-30T00:00:00``
    and similar prefixes (CSV exports from psql sometimes emit timestamps).
    """
    s = (raw or "").strip()
    if not s:
        return None
    head = s[:10]
    try:
        datetime.date.fromisoformat(head)
    except ValueError:
        return None
    return head


# ---------------------------------------------------------------------------
# Model-type resolution
# ---------------------------------------------------------------------------


def resolve_model_type(raw: str | None) -> str:
    """Map a source ``model_type`` string to the canonical API ModelType value.

    Args:
        raw: source CSV ``model_type`` cell (e.g. ``"TFT"``, ``"TIDE"``,
            ``"TSMIXER"``, or already-canonical ``"TiDE"``/``"TSMixer"``).

    Returns:
        Canonical API value: one of ``"TFT"``, ``"TiDE"``, ``"TSMixer"``.

    Raises:
        UnknownMLModelTypeError: if ``raw`` is empty / None or not in
            ``MODEL_DIR_TO_API``.
    """
    s = (raw or "").strip()
    if not s:
        raise UnknownMLModelTypeError("model_type is missing / empty")
    if s not in MODEL_DIR_TO_API:
        raise UnknownMLModelTypeError(
            f"model_type {s!r} is not a known ML variant; "
            f"expected one of {sorted(MODEL_DIR_TO_API)}"
        )
    return MODEL_DIR_TO_API[s]


# ---------------------------------------------------------------------------
# Record building
# ---------------------------------------------------------------------------


def _build_record(
    row: dict[str, str],
    *,
    preserve_legacy_horizons: bool,
) -> dict | None:
    """Build the per-row API payload for an ML forecast record.

    Default behavior (user-lock L6): the payload ``horizon_type`` is ALWAYS
    ``"day"`` regardless of any source-CSV ``horizon_type`` cell. Day-of-year
    is computed from the target date for ``horizon_value``/``horizon_in_year``.

    When ``preserve_legacy_horizons`` is True AND the source row has a
    ``horizon_type`` of ``pentad`` / ``decade``, the payload preserves that
    horizon_type and sets ``horizon_value=0`` / ``horizon_in_year=0`` per
    the legacy ``ForecastDataMigrator`` convention. Any other source
    ``horizon_type`` (including ``day``) reverts to the default day behavior.

    Args:
        row: CSV row as ``{column_name: value_str}``. Expected columns:
            ``code``, ``model_type``, ``date`` (issue / forecast date),
            ``target`` (target / valid date), optional ``flag``, optional
            quantile columns (accept both ``Q5/Q25/Q50/Q75/Q95`` and
            ``q05/q25/q50/q75/q95``), optional ``forecasted_discharge``,
            optional ``horizon_type`` (only consulted under the legacy flag).
        preserve_legacy_horizons: when True, preserve PENTAD/DECADE source
            ``horizon_type`` instead of defaulting to ``day``.

    Returns:
        A payload dict (only non-NULL fields), or ``None`` if the row cannot
        satisfy required fields (caller counts as parse skip).

    Raises:
        UnknownMLModelTypeError: when ``model_type`` cannot be resolved (no
        silent fallback — surfaces typos / forward-compat ambiguities).
    """
    code = (row.get("code") or "").strip()
    if not code:
        return None

    date_str = _normalize_iso_date(row.get("date"))
    target_str = _normalize_iso_date(row.get("target"))
    if not date_str or not target_str:
        return None

    # Resolve model_type (raises UnknownMLModelTypeError on bad input).
    model_type = resolve_model_type(row.get("model_type"))

    # --- Horizon-type selection (default = 'day'; opt-in legacy) ---
    src_horizon = (row.get("horizon_type") or "").strip().lower()
    target_date = datetime.date.fromisoformat(target_str)
    if preserve_legacy_horizons and src_horizon in _LEGACY_HORIZON_TYPES:
        horizon_type = src_horizon
        horizon_value = 0
        horizon_in_year = 0
    else:
        horizon_type = "day"
        horizon_value = target_date.timetuple().tm_yday
        horizon_in_year = horizon_value

    rec: dict = {
        "horizon_type": horizon_type,
        "code": code,
        "model_type": model_type,
        "date": date_str,
        "target": target_str,
        "horizon_value": horizon_value,
        "horizon_in_year": horizon_in_year,
    }

    # --- Optional integer flag ---
    flag = _parse_int(row.get("flag"))
    if flag is not None:
        rec["flag"] = flag

    # --- Optional float quantiles (accept Q5/Q25/Q50/Q75/Q95 OR q05/q25/q50/q75/q95) ---
    # The legacy ForecastDataMigrator (data_migrator.py:350-354) uses Q5/Q25/...
    # The modern operational writer (_write_ml_forecast_to_api) uses the same.
    # Some psql exports may normalize columns to lower-case — accept both.
    q05 = _parse_float(row.get("Q5"))
    if q05 is None:
        q05 = _parse_float(row.get("q05"))
    if q05 is not None:
        rec["q05"] = q05

    q25 = _parse_float(row.get("Q25"))
    if q25 is None:
        q25 = _parse_float(row.get("q25"))
    if q25 is not None:
        rec["q25"] = q25

    q50 = _parse_float(row.get("Q50"))
    if q50 is None:
        q50 = _parse_float(row.get("q50"))
    # Q50 maps to forecasted_discharge per the legacy ForecastDataMigrator
    # convention. The API schema separates these into two fields, but the
    # CSV-side does not have a separate forecasted_discharge column — Q50
    # is the median quantile AND the central estimate.
    fd = _parse_float(row.get("forecasted_discharge"))
    if fd is None:
        # Fall back to Q50 if no explicit forecasted_discharge column.
        fd = q50
    if fd is not None:
        rec["forecasted_discharge"] = fd

    q75 = _parse_float(row.get("Q75"))
    if q75 is None:
        q75 = _parse_float(row.get("q75"))
    if q75 is not None:
        rec["q75"] = q75

    q95 = _parse_float(row.get("Q95"))
    if q95 is None:
        q95 = _parse_float(row.get("q95"))
    if q95 is not None:
        rec["q95"] = q95

    return rec


def _read_filtered_records(
    csv_path: Path,
    *,
    cutoff: str | None,
    station_filter: str | None,
    model_filter: str | None,
    preserve_legacy_horizons: bool,
) -> tuple[
    list[dict],
    dict[str, int],
    set[str],
    str | None,
    str | None,
    dict[str, int],
]:
    """Read CSV and return (records, counters, distinct_codes, date_min,
    date_max, per_model_counts).

    Args:
        csv_path: path to the ML forecast CSV (export from
            ``sapphire-postprocessing-db.forecasts``).
        cutoff: optional ISO date; rows with ``date >= cutoff`` are dropped.
            None means full-import (no date filter).
        station_filter: optional single station code; rows whose ``code``
            does not match are dropped.
        model_filter: optional model name (any form accepted by
            ``resolve_model_type``); rows whose resolved API model_type does
            not match the resolved filter are dropped. None = no filter.
        preserve_legacy_horizons: forwarded to ``_build_record``; also
            governs whether non-``day`` source rows are accepted at all.
            When False (default), source rows whose ``horizon_type`` is
            something other than ``day`` (case-insensitive) are dropped to
            ``skipped_horizon``.

    Returns:
        Tuple of (records, counters, distinct_codes, date_min, date_max,
        per_model_counts) where ``per_model_counts`` maps the canonical API
        model string to the post-filter row count (for the dry-run inventory
        per-model breakdown).

    Raises:
        ValueError: if required columns are missing.
    """
    counters = {
        "source_row_count": 0,
        "filtered_row_count": 0,
        "skipped_parse": 0,
        "skipped_cutoff": 0,
        "skipped_station": 0,
        "skipped_model": 0,
        "skipped_horizon": 0,
        "skipped_unknown_model": 0,
    }
    records: list[dict] = []
    distinct_codes: set[str] = set()
    date_min: str | None = None
    date_max: str | None = None
    per_model_counts: dict[str, int] = {}

    # Resolve model_filter once (validates the operator's flag value).
    resolved_filter: str | None
    if model_filter is None:
        resolved_filter = None
    else:
        # Raises UnknownMLModelTypeError if the operator passed garbage.
        resolved_filter = resolve_model_type(model_filter)

    required = {"code", "date", "target", "model_type"}

    with csv_path.open(newline="") as f:
        reader = csv.DictReader(f)
        header = set(reader.fieldnames or [])
        missing = required - header
        if missing:
            raise ValueError(
                f"CSV {csv_path.name} is missing required column(s): {sorted(missing)}"
            )

        for row in reader:
            counters["source_row_count"] += 1
            code = (row.get("code") or "").strip()
            date_str = _normalize_iso_date(row.get("date"))

            # Track source date range pre-filter (for dry-run inventory).
            if date_str:
                if date_min is None or date_str < date_min:
                    date_min = date_str
                if date_max is None or date_str > date_max:
                    date_max = date_str

            if not code or not date_str:
                counters["skipped_parse"] += 1
                continue

            # Station filter (applied first for counter clarity).
            if station_filter is not None and code != station_filter:
                counters["skipped_station"] += 1
                continue

            # Cutoff filter (strict less-than).
            if cutoff is not None and date_str >= cutoff:
                counters["skipped_cutoff"] += 1
                continue

            # Horizon-type guard: non-day rows are rejected unless the
            # operator opts in via --preserve-legacy-ml-horizons.
            src_horizon = (row.get("horizon_type") or "").strip().lower()
            if not preserve_legacy_horizons and src_horizon and src_horizon != "day":
                counters["skipped_horizon"] += 1
                continue

            # Resolve / filter model_type. Unknown model strings are surfaced
            # as a skip with a clear counter (operator can re-run after fix).
            try:
                api_model = resolve_model_type(row.get("model_type"))
            except UnknownMLModelTypeError:
                counters["skipped_unknown_model"] += 1
                continue

            if resolved_filter is not None and api_model != resolved_filter:
                counters["skipped_model"] += 1
                continue

            try:
                record = _build_record(row, preserve_legacy_horizons=preserve_legacy_horizons)
            except UnknownMLModelTypeError:
                # Should not happen — we just resolved it. Defensive.
                counters["skipped_unknown_model"] += 1
                continue

            if record is None:
                counters["skipped_parse"] += 1
                continue

            records.append(record)
            distinct_codes.add(code)
            per_model_counts[api_model] = per_model_counts.get(api_model, 0) + 1
            counters["filtered_row_count"] += 1

    return records, counters, distinct_codes, date_min, date_max, per_model_counts


# ---------------------------------------------------------------------------
# HTTP POST
# ---------------------------------------------------------------------------


def _post_batch(
    batch: list[dict],
    url: str,
    *,
    timeout: float = 120.0,
) -> tuple[bool, str]:
    """POST a single batch envelope to the postprocessing API ``/forecast/``.

    The endpoint expects ``{"data": [<record>, ...]}`` per the
    ``ForecastBulkCreate`` schema. Returns ``(ok, message)``.
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
        prog="python3 -m migration_py.ml_forecast",
        description="Push ML forecast CSV rows (TFT/TiDE/TSMixer) to the postprocessing API.",
    )
    p.add_argument(
        "--csv-path",
        required=True,
        type=Path,
        help="Path to the ML forecast export CSV inside the container.",
    )
    p.add_argument(
        "--api-url",
        required=True,
        help="Postprocessing API endpoint URL (e.g. http://localhost:8003/forecast/).",
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
        help="Filter to a single station code (binding P0 interface contract).",
    )
    p.add_argument(
        "--model",
        default=None,
        help=(
            "Restrict to a single ML model variant. Accepts any of: "
            "TFT, TIDE, TiDE, TSMIXER, TSMixer. Resolved to the canonical "
            "API form via MODEL_DIR_TO_API."
        ),
    )
    p.add_argument(
        "--preserve-legacy-ml-horizons",
        action="store_true",
        help=(
            "WARNING: preserve source PENTAD/DECADE horizon_type instead of "
            "the default user-lock L6 'day' storage. Used only to migrate the "
            "pre-1cb3495 legacy rows. Modern ML writes use 'day'."
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
    counters: dict[str, int],
    distinct_codes: set[str],
    date_min: str | None,
    date_max: str | None,
    mode: str,
    cutoff: str | None,
    per_model_counts: dict[str, int],
    preserve_legacy_horizons: bool,
) -> None:
    """Emit the §4.4 runbook dry-run inventory block.

    Lines are printed on stdout (the wrapper tees them into its log file).
    Station codes are NEVER printed individually — only the redacted count
    via ``log_redacted_station_count``.

    Adds the P4b-specific per-model breakdown line:

        ``ML_PER_MODEL_COUNTS={TFT: <n>, TiDE: <n>, TSMixer: <n>}``

    And — when ``--preserve-legacy-ml-horizons`` is active — a prominent
    WARNING line so the operator sees the lock-override in the run log.
    """
    print(f"MODE={mode}" + (f" (cutoff={cutoff})" if cutoff else " (target empty)"))
    print("TARGET_TABLE=forecasts")
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
        f"SKIPPED_MODEL={counters['skipped_model']} "
        f"SKIPPED_HORIZON={counters['skipped_horizon']} "
        f"SKIPPED_UNKNOWN_MODEL={counters['skipped_unknown_model']}"
    )
    # Per-model breakdown — always print all three keys for stable output
    # shape, fill in 0 for any model missing post-filter.
    formatted = ", ".join(f"{m}: {per_model_counts.get(m, 0)}" for m in ("TFT", "TiDE", "TSMixer"))
    print(f"ML_PER_MODEL_COUNTS={{{formatted}}}")
    if preserve_legacy_horizons:
        print(
            "WARNING: --preserve-legacy-ml-horizons active; source PENTAD/DECADE "
            "horizon_type values are preserved (user-lock L6 'day' default is bypassed)."
        )
    # Redacted log line (count only).
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

    cutoff: str | None = args.cutoff
    mode = "pre-cutoff" if cutoff else "full-import"

    # WARNING is emitted up-front so it shows in the wrapper log even if a
    # parse error stops the run before the dry-run inventory.
    if args.preserve_legacy_ml_horizons:
        logger.warning(
            "--preserve-legacy-ml-horizons active. Migrating PENTAD/DECADE "
            "horizon_type rows; modern ML writes store as horizon_type='day' "
            "per user-lock L6."
        )

    try:
        (
            records,
            counters,
            distinct_codes,
            date_min,
            date_max,
            per_model_counts,
        ) = _read_filtered_records(
            csv_path,
            cutoff=cutoff,
            station_filter=args.station_filter,
            model_filter=args.model,
            preserve_legacy_horizons=args.preserve_legacy_ml_horizons,
        )
    except (ValueError, UnknownMLModelTypeError) as exc:
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
        per_model_counts=per_model_counts,
        preserve_legacy_horizons=args.preserve_legacy_ml_horizons,
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
