"""Raw on-server ML forecast CSV reshaper.

Reads local-branch raw files under ``predictions/<MODEL>/`` and writes one
export-format CSV plus ``.manifest`` for ``migration_py.ml_forecast``. This
helper performs no API or DB writes.

Stdlib-only. Verified by ``migration_py._audit.audit_stdlib_only``.
"""

from __future__ import annotations

import argparse
import csv
import datetime
import sys
from pathlib import Path

from . import _common, ml_forecast

FIELDNAMES: list[str] = [
    "code",
    "model_type",
    "horizon_type",
    "date",
    "target",
    "flag",
    "Q5",
    "Q25",
    "Q50",
    "Q75",
    "Q95",
    "forecasted_discharge",
]

RAW_REQUIRED_COLUMNS: frozenset[str] = frozenset({"code", "date", "forecast_date", "Q50"})
RAW_QUANTILE_COLUMNS: tuple[str, ...] = ("Q5", "Q25", "Q50", "Q75", "Q95")
API_MODELS: tuple[str, ...] = ("TFT", "TiDE", "TSMixer")


def _normalize_iso_date(raw: str | None) -> str | None:
    s = (raw or "").strip()
    if not s:
        return None
    head = s[:10]
    try:
        datetime.date.fromisoformat(head)
    except ValueError:
        return None
    return head


def _parse_int_text(raw: str | None) -> str:
    s = (raw or "").strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return ""
    try:
        return str(int(float(s)))
    except (ValueError, OverflowError):
        return ""


def _prediction_root(data_ref: Path) -> Path:
    candidate = data_ref / "predictions"
    if candidate.is_dir():
        return candidate
    return data_ref


def _iter_model_files(pred_root: Path) -> list[tuple[Path, str]]:
    if not pred_root.is_dir():
        raise ValueError(f"raw predictions directory not found: {pred_root}")

    files: list[tuple[Path, str]] = []
    for model_dir in sorted(p for p in pred_root.iterdir() if p.is_dir()):
        raw_model = model_dir.name
        try:
            ml_forecast.resolve_model_type(raw_model)
        except ml_forecast.UnknownMLModelTypeError as exc:
            if any(model_dir.glob("*_forecast.csv")):
                raise ValueError(f"unknown ML model directory: {raw_model}") from exc
            continue

        for horizon in ("pentad", "decad"):
            csv_path = model_dir / f"{horizon}_{raw_model}_forecast.csv"
            if csv_path.is_file():
                files.append((csv_path, raw_model))
    return files


def _empty_counters() -> dict[str, int]:
    return {
        "source_row_count": 0,
        "filtered_row_count": 0,
        "skipped_bad_date": 0,
        "skipped_missing_required": 0,
        "skipped_station": 0,
        "skipped_model": 0,
        "skipped_unknown_model": 0,
    }


def _minmax(values: list[str]) -> tuple[str | None, str | None]:
    if not values:
        return None, None
    return min(values), max(values)


def _read_reshaped_rows(
    data_ref: Path,
    *,
    station_filter: str | None,
    model_filter: str | None,
) -> tuple[
    list[dict[str, str]],
    dict[str, int],
    dict[str, int],
    str | None,
    str | None,
    str | None,
    str | None,
]:
    counters = _empty_counters()
    per_model_counts = {model: 0 for model in API_MODELS}
    out_rows: list[dict[str, str]] = []
    issue_dates: list[str] = []
    target_dates: list[str] = []

    resolved_filter = (
        ml_forecast.resolve_model_type(model_filter) if model_filter is not None else None
    )

    for csv_path, raw_model in _iter_model_files(_prediction_root(data_ref)):
        try:
            api_model = ml_forecast.resolve_model_type(raw_model)
        except ml_forecast.UnknownMLModelTypeError as exc:
            raise ValueError(f"unknown ML model directory: {raw_model}") from exc

        with csv_path.open(newline="") as f:
            reader = csv.DictReader(f)
            header = set(reader.fieldnames or [])
            missing = RAW_REQUIRED_COLUMNS - header
            if missing:
                raise ValueError(
                    f"raw CSV {csv_path.name} is missing required column(s): {sorted(missing)}"
                )

            for row in reader:
                counters["source_row_count"] += 1

                code = (row.get("code") or "").strip()
                target_date = _normalize_iso_date(row.get("date"))
                issue_date = _normalize_iso_date(row.get("forecast_date"))
                q50 = (row.get("Q50") or "").strip()

                if not code or not q50:
                    counters["skipped_missing_required"] += 1
                    continue
                if target_date is None or issue_date is None:
                    counters["skipped_bad_date"] += 1
                    continue
                if station_filter is not None and code != station_filter:
                    counters["skipped_station"] += 1
                    continue
                if resolved_filter is not None and api_model != resolved_filter:
                    counters["skipped_model"] += 1
                    continue

                out_row = {
                    "code": code,
                    "model_type": api_model,
                    "horizon_type": "day",
                    "date": issue_date,
                    "target": target_date,
                    "flag": _parse_int_text(row.get("flag")),
                    "forecasted_discharge": q50,
                }
                for col in RAW_QUANTILE_COLUMNS:
                    out_row[col] = (row.get(col) or "").strip()
                out_rows.append(out_row)
                issue_dates.append(issue_date)
                target_dates.append(target_date)
                counters["filtered_row_count"] += 1
                per_model_counts[api_model] += 1

    issue_min, issue_max = _minmax(issue_dates)
    target_min, target_max = _minmax(target_dates)
    return out_rows, counters, per_model_counts, issue_min, issue_max, target_min, target_max


def _write_csv_and_manifest(out_csv: Path, rows: list[dict[str, str]]) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    codes = {(r.get("code") or "").strip() for r in rows if r.get("code")}
    dates = [(r.get("date") or "").strip() for r in rows if r.get("date")]
    manifest_path = out_csv.with_name(out_csv.name + ".manifest")
    manifest_path.write_text(
        "\n".join(
            [
                "export_type=ml_forecast",
                f"row_count={len(rows)}",
                f"station_count={len(codes)}",
                f"date_min={min(dates)}",
                f"date_max={max(dates)}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    _common.validate_manifest(out_csv, "ml_forecast")


def _print_inventory(
    *,
    data_ref: Path,
    out_csv: Path,
    counters: dict[str, int],
    per_model_counts: dict[str, int],
    issue_min: str | None,
    issue_max: str | None,
    target_min: str | None,
    target_max: str | None,
    dry_run: bool,
) -> None:
    print(f"DATA_REF={data_ref}")
    print(f"OUT={out_csv}")
    print(f"SOURCE_ROW_COUNT={counters['source_row_count']}")
    print(f"FILTERED_ROW_COUNT={counters['filtered_row_count']}")
    print(f"ISSUE_DATE_MIN={issue_min if issue_min else 'none'}")
    print(f"ISSUE_DATE_MAX={issue_max if issue_max else 'none'}")
    print(f"TARGET_DATE_MIN={target_min if target_min else 'none'}")
    print(f"TARGET_DATE_MAX={target_max if target_max else 'none'}")
    print(
        f"SKIPPED_BAD_DATE={counters['skipped_bad_date']} "
        f"SKIPPED_MISSING_REQUIRED={counters['skipped_missing_required']} "
        f"SKIPPED_STATION={counters['skipped_station']} "
        f"SKIPPED_MODEL={counters['skipped_model']} "
        f"SKIPPED_UNKNOWN_MODEL={counters['skipped_unknown_model']}"
    )
    formatted = ", ".join(f"{m}: {per_model_counts.get(m, 0)}" for m in API_MODELS)
    print(f"RAW_ML_PER_MODEL_COUNTS={{{formatted}}}")
    if dry_run:
        print("DRY RUN: no export CSV or manifest written.")


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python3 -m migration_py.ml_raw_to_export",
        description="Reshape raw predictions/<MODEL> ML CSVs to one ml_forecast export CSV.",
    )
    p.add_argument("--data-ref", required=True, type=Path)
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--station-filter", default=None)
    p.add_argument(
        "--model",
        default=None,
        help="Restrict to one ML model. Accepts TFT, TIDE/TiDE, TSMIXER/TSMixer.",
    )
    p.add_argument("--dry-run", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    try:
        rows, counters, per_model_counts, issue_min, issue_max, target_min, target_max = (
            _read_reshaped_rows(
                args.data_ref,
                station_filter=args.station_filter,
                model_filter=args.model,
            )
        )
    except (ValueError, ml_forecast.UnknownMLModelTypeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    _print_inventory(
        data_ref=args.data_ref,
        out_csv=args.out,
        counters=counters,
        per_model_counts=per_model_counts,
        issue_min=issue_min,
        issue_max=issue_max,
        target_min=target_min,
        target_max=target_max,
        dry_run=args.dry_run,
    )

    if not rows:
        print("ERROR: no rows to export after filtering/parsing", file=sys.stderr)
        return 1
    if args.dry_run:
        return 0

    try:
        _write_csv_and_manifest(args.out, rows)
    except _common.ManifestError as exc:
        print(f"ERROR: generated manifest failed validation: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
