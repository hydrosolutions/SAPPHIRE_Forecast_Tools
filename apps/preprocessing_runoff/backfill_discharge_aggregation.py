"""Milestone M4 historical backfill orchestrator for discharge-aggregation hydrograph rows.

Reuses the M2/M3 writers (``sync_short_horizon_hydrograph.write_short_horizon_hydrograph``
and ``sync_long_horizon_hydrograph.write_long_horizon_hydrograph``) unmodified by routing
them through a write-capturing client wrapper, so the exact records they would have written
for a past year can be computed, diffed against whatever is already stored, snapshotted to
disk, and only then written for real -- with a post-write re-read verification step that
raises loudly on any mismatch instead of silently leaving a partial or wrong backfill.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import math
import os
import sys
from collections.abc import Iterable
from typing import Any

import pandas as pd

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_IEHF_DIR = os.path.join(_SCRIPT_DIR, "..", "iEasyHydroForecast")
if _IEHF_DIR not in sys.path:
    sys.path.insert(0, _IEHF_DIR)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

import setup_library as sl
import sync_long_horizon_hydrograph as sync_lhh
import sync_short_horizon_hydrograph as sync_shh
from ieasyhydro_sdk.sdk import IEasyHydroHFSDK

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

_DEFAULT_SNAPSHOT_DIR = os.path.join(
    _SCRIPT_DIR, "..", "..", "logs", "discharge_aggregation_backfill"
)

_COMPARE_FIELDS = (
    "current",
    "previous",
    "norm",
    "mean",
    "min",
    "max",
    "q05",
    "q25",
    "q75",
    "q95",
)
_EXAMPLES_CAP = 25


class _CapturingClient:
    """Write-blocking client wrapper that captures records instead of persisting them.

    Reads (``readiness_check``, ``read_hydrograph``, ``read_runoff``) pass straight
    through to the wrapped real client so the M2/M3 writers still see live data.
    ``write_hydrograph`` never reaches the real client -- it only appends the records
    to ``captured``, letting the writers be reused purely to COMPUTE what they would
    have written, without touching storage.
    """

    def __init__(self, real_client: Any) -> None:
        self._real_client = real_client
        self.captured: list[dict[str, Any]] = []

    def readiness_check(self, *args: Any, **kwargs: Any) -> Any:
        return self._real_client.readiness_check(*args, **kwargs)

    def read_hydrograph(self, *args: Any, **kwargs: Any) -> Any:
        return self._real_client.read_hydrograph(*args, **kwargs)

    def read_runoff(self, *args: Any, **kwargs: Any) -> Any:
        return self._real_client.read_runoff(*args, **kwargs)

    def write_hydrograph(self, records: list[dict[str, Any]]) -> int:
        self.captured.extend(dict(record) for record in records)
        return len(records)


def compute_backfill_records(
    codes: Iterable[str],
    iehhf_sdk: Any,
    real_client: Any,
    target_year: int,
    today: dt.date,
) -> list[dict[str, Any]]:
    """Run the M2/M3 writers against a capturing client to compute what they'd write.

    Both writers receive the SAME capturing client so any cross-writer reads (e.g. the
    monthly writer's internal decadal-actuals lookup) see a consistent read surface, and
    no write from either writer ever reaches ``real_client``. The capturing client's
    ``captured`` list -- not the writers' own return values -- is the source of truth,
    since it is exactly what would have been sent to ``write_hydrograph``.
    """
    codes = list(codes)
    capturing_client = _CapturingClient(real_client)
    sync_shh.write_short_horizon_hydrograph(codes, iehhf_sdk, capturing_client, target_year, today)
    sync_lhh.write_long_horizon_hydrograph(codes, iehhf_sdk, capturing_client, target_year, today)
    return list(capturing_client.captured)


def _affected_keys(records: list[dict[str, Any]]) -> set[tuple[str, str, str]]:
    """Return the set of (horizon_type, code, date[:10]) keys touched by ``records``."""
    return {(str(r["horizon_type"]), str(r["code"]), str(r["date"])[:10]) for r in records}


def _normalize_hydrograph_rows(rows: Any) -> list[dict[str, Any]]:
    """Normalize a ``read_hydrograph`` result (DataFrame, list-of-dict, or None) to a list."""
    if rows is None:
        return []
    if isinstance(rows, pd.DataFrame):
        if rows.empty:
            return []
        return rows.to_dict("records")
    return list(rows)


def snapshot_existing(real_client: Any, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Read back whatever currently exists at every key ``records`` would touch.

    Groups the affected keys by (horizon_type, code) and issues one ``read_hydrograph``
    call per group over the group's [min_date, max_date] span. A read failure for one
    group is logged and treated as "no existing rows" for that group rather than
    aborting the whole snapshot.
    """
    keys = _affected_keys(records)
    dates_by_group: dict[tuple[str, str], list[str]] = {}
    for horizon_type, code, date_str in keys:
        dates_by_group.setdefault((horizon_type, code), []).append(date_str)

    existing: list[dict[str, Any]] = []
    for (horizon_type, code), dates in dates_by_group.items():
        start_date = min(dates)
        end_date = max(dates)
        try:
            rows = real_client.read_hydrograph(
                horizon=horizon_type, code=code, start_date=start_date, end_date=end_date
            )
        except sync_lhh._API_READ_WRITE_ERRORS as exc:
            logger.warning(
                "snapshot_existing: read failed for horizon=%s code=%s (%s: %s); "
                "treating as no existing rows for this group.",
                horizon_type,
                code,
                type(exc).__name__,
                exc,
            )
            rows = None
        except Exception as exc:  # deliberate generic read-failure fallback
            logger.warning(
                "snapshot_existing: unexpected read failure for horizon=%s code=%s (%s: %s); "
                "treating as no existing rows for this group.",
                horizon_type,
                code,
                type(exc).__name__,
                exc,
            )
            rows = None
        existing.extend(_normalize_hydrograph_rows(rows))
    return existing


def _key_of(record: dict[str, Any]) -> tuple[str, str, str]:
    return (str(record["horizon_type"]), str(record["code"]), str(record["date"])[:10])


def _index_by_key(
    records: list[dict[str, Any]], label: str
) -> dict[tuple[str, str, str], dict[str, Any]]:
    """Key ``records`` by (horizon_type, code, date[:10]); last-write-wins on duplicates."""
    indexed: dict[tuple[str, str, str], dict[str, Any]] = {}
    duplicate_count = 0
    for record in records:
        key = _key_of(record)
        if key in indexed:
            duplicate_count += 1
        indexed[key] = record
    if duplicate_count:
        logger.warning(
            "%s records contained %d duplicate key(s); keeping the last occurrence of each.",
            label,
            duplicate_count,
        )
    return indexed


def _is_missing(value: Any) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def _values_equal(old: Any, new: Any) -> bool:
    """None/NaN on either side are treated as equal; numerics compare with tolerance."""
    old_missing = _is_missing(old)
    new_missing = _is_missing(new)
    if old_missing or new_missing:
        return old_missing and new_missing
    try:
        return math.isclose(float(old), float(new), abs_tol=1e-6)
    except (TypeError, ValueError):
        return old == new


def diff_records(existing: list[dict[str, Any]], new: list[dict[str, Any]]) -> dict[str, Any]:
    """Classify each ``new`` record vs. ``existing`` as added / unchanged / changed."""
    existing_by_key = _index_by_key(existing, "existing")
    new_by_key = _index_by_key(new, "new")

    added = 0
    unchanged = 0
    changed = 0
    examples: list[dict[str, Any]] = []
    examples_omitted = 0

    for key, new_record in new_by_key.items():
        old_record = existing_by_key.get(key)
        if old_record is None:
            added += 1
            if len(examples) < _EXAMPLES_CAP:
                examples.append({"key": key, "kind": "added"})
            else:
                examples_omitted += 1
            continue

        changed_fields: dict[str, dict[str, Any]] = {}
        for field in _COMPARE_FIELDS:
            if field not in old_record and field not in new_record:
                continue
            old_value = old_record.get(field)
            new_value = new_record.get(field)
            if not _values_equal(old_value, new_value):
                changed_fields[field] = {"old": old_value, "new": new_value}

        if changed_fields:
            changed += 1
            if len(examples) < _EXAMPLES_CAP:
                examples.append({"key": key, "kind": "changed", "fields": changed_fields})
            else:
                examples_omitted += 1
        else:
            unchanged += 1

    if examples_omitted:
        logger.info(
            "diff_records: %d example(s) omitted beyond the %d-example cap "
            "(added=%d, unchanged=%d, changed=%d).",
            examples_omitted,
            _EXAMPLES_CAP,
            added,
            unchanged,
            changed,
        )
    else:
        logger.info(
            "diff_records: added=%d unchanged=%d changed=%d (no examples omitted).",
            added,
            unchanged,
            changed,
        )

    return {
        "added": added,
        "unchanged": unchanged,
        "changed": changed,
        "examples": examples,
        "examples_omitted": examples_omitted,
    }


def verify_written(real_client: Any, written: list[dict[str, Any]]) -> list[str]:
    """Re-read after a live write and report every mismatch vs. what was written.

    An empty return means every written record round-tripped correctly.
    """
    written_by_key = _index_by_key(written, "written")
    reread = snapshot_existing(real_client, written)
    reread_by_key = _index_by_key(reread, "reread")

    discrepancies: list[str] = []
    for key, expected in written_by_key.items():
        actual = reread_by_key.get(key)
        if actual is None:
            discrepancies.append(f"missing after write: key={key}")
            continue
        expected_horizon = str(expected.get("horizon_type"))
        actual_horizon = str(actual.get("horizon_type"))
        if expected_horizon != actual_horizon:
            discrepancies.append(
                f"horizon_type mismatch: key={key} expected={expected_horizon!r} "
                f"actual={actual_horizon!r}"
            )
        for field in _COMPARE_FIELDS:
            if field not in expected:
                continue
            expected_value = expected.get(field)
            actual_value = actual.get(field)
            if not _values_equal(expected_value, actual_value):
                discrepancies.append(
                    f"field mismatch: key={key} field={field} expected={expected_value!r} "
                    f"actual={actual_value!r}"
                )
    return discrepancies


def _json_safe_deep(value: Any) -> Any:
    """Recursively apply ``sync_lhh._json_safe`` through dicts/lists/tuples."""
    if isinstance(value, dict):
        return {k: _json_safe_deep(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe_deep(v) for v in value]
    return sync_lhh._json_safe(value)


def backfill(
    codes: Iterable[str],
    iehhf_sdk: Any,
    real_client: Any,
    target_years: Iterable[int],
    today: dt.date,
    dry_run: bool,
    snapshot_dir: str,
) -> dict[str, Any]:
    """Backfill discharge-aggregation hydrograph rows for each year in ``target_years``.

    Safety rails, per year:
      * dry-run: compute + diff + write a JSON diff report under ``snapshot_dir``;
        ``real_client.write_hydrograph`` is NEVER called for this year.
      * live: persist the pre-write ``existing`` snapshot to ``snapshot_dir`` FIRST,
        THEN write the new records (batched per horizon_type), THEN re-read and verify.
        A verification mismatch raises ``RuntimeError`` -- it is never swallowed or
        logged-and-continued -- so a caller cannot mistake a broken write for success.
    """
    os.makedirs(snapshot_dir, exist_ok=True)
    codes = list(codes)
    year_summaries: list[dict[str, Any]] = []

    for year in sorted(set(target_years)):
        logger.info("backfill: computing records for target_year=%d (dry_run=%s)", year, dry_run)
        records = compute_backfill_records(codes, iehhf_sdk, real_client, year, today)
        existing = snapshot_existing(real_client, records)
        diff = diff_records(existing, records)
        timestamp = dt.datetime.now().strftime("%Y%m%dT%H%M%S%f")

        snapshot_file: str | None = None
        verified: bool | None = None

        if dry_run:
            logger.info(
                "backfill[dry-run]: year=%d records=%d added=%d unchanged=%d changed=%d",
                year,
                len(records),
                diff["added"],
                diff["unchanged"],
                diff["changed"],
            )
            snapshot_file = os.path.join(
                snapshot_dir, f"backfill_dryrun_diff_{year}_{timestamp}.json"
            )
            with open(snapshot_file, "w") as handle:
                json.dump(
                    _json_safe_deep({"year": year, "diff": diff}), handle, indent=2, default=str
                )
            # dry-run: real_client.write_hydrograph is deliberately never called here.
        else:
            snapshot_file = os.path.join(snapshot_dir, f"backfill_snapshot_{year}_{timestamp}.json")
            # Snapshot the pre-write state BEFORE any live write for this year.
            with open(snapshot_file, "w") as handle:
                json.dump(
                    _json_safe_deep({"year": year, "existing": existing}),
                    handle,
                    indent=2,
                    default=str,
                )
            logger.info("backfill: pre-write snapshot for year=%d saved to %s", year, snapshot_file)

            records_by_horizon: dict[str, list[dict[str, Any]]] = {}
            for record in records:
                records_by_horizon.setdefault(str(record["horizon_type"]), []).append(record)
            for horizon_type, horizon_records in records_by_horizon.items():
                real_client.write_hydrograph(horizon_records)
                logger.info(
                    "backfill: wrote %d %s record(s) for year=%d",
                    len(horizon_records),
                    horizon_type,
                    year,
                )

            discrepancies = verify_written(real_client, records)
            if discrepancies:
                logger.error(
                    "backfill: verification FAILED for year=%d - %d discrepancy(ies); no "
                    "further years were processed automatically. Discrepancies: %s",
                    year,
                    len(discrepancies),
                    discrepancies,
                )
                raise RuntimeError(f"backfill verification failed for year={year}: {discrepancies}")
            verified = True
            logger.info("backfill: verification OK for year=%d (%d records)", year, len(records))

        year_summaries.append(
            {
                "year": year,
                "record_count": len(records),
                "diff": diff,
                "dry_run": dry_run,
                "snapshot_file": snapshot_file,
                "verified": verified,
            }
        )

    return {"years": year_summaries, "dry_run": dry_run}


def _resolve_target_years(
    today: dt.date, years: int = 3, target_year: int | None = None
) -> list[int]:
    """Resolve the list of target years for a backfill run.

    ``target_year`` (single explicit year) takes precedence over ``years``; otherwise
    returns the ``years`` most-recent complete calendar years ending at ``today.year - 1``.
    """
    if target_year is not None:
        return [target_year]
    return list(range(today.year - years, today.year))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="backfill_discharge_aggregation.py",
        description=(
            "Safety-railed historical backfill of pentad/decad/month/quarter/season "
            "discharge-aggregation hydrograph rows, reusing the M2/M3 writers via a "
            "write-capturing client wrapper."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--years",
        type=int,
        default=3,
        metavar="N",
        help="Number of most-recent complete years to backfill (ignored if --target-year is set).",
    )
    parser.add_argument(
        "--target-year",
        type=int,
        default=None,
        metavar="YEAR",
        help="Backfill exactly this single year instead of the --years window.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Compute and diff records without writing; still reads existing data for the diff.",
    )
    parser.add_argument(
        "--snapshot-dir",
        default=_DEFAULT_SNAPSHOT_DIR,
        metavar="PATH",
        help="Directory for pre-write snapshots and dry-run diff reports.",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    today = dt.date.today()

    try:
        sl.load_environment()
        sdk = IEasyHydroHFSDK()
        codes = sync_lhh.resolve_sdk_station_codes(sdk)
        target_years = _resolve_target_years(today, args.years, args.target_year)
        logger.info(
            "backfill_discharge_aggregation: target_years=%s dry_run=%s snapshot_dir=%s",
            target_years,
            args.dry_run,
            args.snapshot_dir,
        )

        if not codes:
            logger.error("No SDK sites remain after filtering - nothing to backfill.")
            sys.exit(2)

        # Even in dry-run mode a real client is needed: the diff step reads existing
        # data, it just never writes.
        client = sync_lhh._get_preprocessing_client()
        summary = backfill(
            codes=codes,
            iehhf_sdk=sdk,
            real_client=client,
            target_years=target_years,
            today=today,
            dry_run=args.dry_run,
            snapshot_dir=args.snapshot_dir,
        )

        for year_summary in summary["years"]:
            diff = year_summary["diff"]
            logger.info(
                "backfill summary: year=%s dry_run=%s records=%d added=%d unchanged=%d "
                "changed=%d verified=%s snapshot_file=%s",
                year_summary["year"],
                year_summary["dry_run"],
                year_summary["record_count"],
                diff["added"],
                diff["unchanged"],
                diff["changed"],
                year_summary["verified"],
                year_summary["snapshot_file"],
            )
        sys.exit(0)

    except RuntimeError as exc:
        logger.error(
            "backfill_discharge_aggregation: verification failed or an API error occurred; "
            "no further action was taken automatically. Error: %s",
            exc,
        )
        sys.exit(1)
    except SystemExit:
        raise
    except Exception as exc:
        logger.exception("Unexpected error during discharge-aggregation backfill: %s", exc)
        sys.exit(3)


if __name__ == "__main__":
    main()
