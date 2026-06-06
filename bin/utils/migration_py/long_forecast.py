"""Long-term forecasts CSV-to-API push helper (P5).

Called from ``bin/initialize_long_forecast_history.sh`` as
``python3 -m migration_py.long_forecast``. The wrapper passes a configured
data root + config root path; this module walks
``<config_root>/long_term_configs/*.json``, parses each mode's config, and for
every model in ``models_to_use`` looks for a matching hindcast CSV at
``<data_root>/long_term_predictions/<mode>/<model>/<model>_hindcast.csv``.
Modes whose JSON config is missing are HARD-SKIPPED (architecture §Q3 lock —
no synthetic configs). Models whose hindcast CSV is missing are reported and
skipped. The ``monthly`` mode is ALWAYS skipped (non-operational; see
``apps/long_term_forecasting/lt_schedule_query.py:54-91``).

UZB no-op acceptance (Stage E item #12): if zero modes are discovered (e.g.
demo profile with no configured long-term modes), the module exits 0 with a
``no source data for this deployment`` log message — NOT an error.

Multi-model payload variance:
    The ``long_forecasts`` table has many sparse model-specific quantile and
    ensemble fields. Different model families populate different subsets:
        - LR family: ``q`` + ``q05/q10/q25/q50/q75/q90/q95`` where present
        - GBT family: above + ``q_xgb`` / ``q_lgbm`` / ``q_catboost``
        - MC_ALD: above + ``q_loc``
    The CSV column names are ``Q_<model_name>`` (point forecast),
    ``Q5/Q10/Q25/Q50/Q75/Q90/Q95`` (quantiles), and
    ``Q_<model_name>_xgb`` / ``..._lgbm`` / ``..._catboost`` (ensembles) plus
    ``Q_loc`` (uncertainty). This module builds the per-row payload
    dynamically: only fields actually present (and non-NULL) in the source
    CSV land in the payload. The universal safe-write rule (architecture
    §Q2 layer 2) forbids sending ``null`` for absent fields.

Forecast key: ``(horizon_type, horizon_value, code, date, model_type,
valid_from, valid_to)``. ``horizon_type`` and ``horizon_value`` come from the
mode's JSON config; ``model_type`` is the per-model loop variable (mixed-case
per Stage A.2 audit — e.g. ``LR_Base``, ``GBT``, ``MC_ALD``); the rest come
from the CSV.

Idempotency: the postprocessing service upserts on the full natural key
above; reruns are safe.

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

logger = logging.getLogger("migration_py.long_forecast")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Modes that are always skipped, even when a JSON config is present.
# ``monthly`` is non-operational per
# ``apps/long_term_forecasting/lt_schedule_query.py:54-91``.
_ALWAYS_SKIP_MODES: frozenset[str] = frozenset({"monthly"})

# Standalone quantile columns (model-agnostic).
_QUANTILE_CSV_TO_PAYLOAD: dict[str, str] = {
    "Q5": "q05",
    "Q10": "q10",
    "Q25": "q25",
    "Q50": "q50",
    "Q75": "q75",
    "Q90": "q90",
    "Q95": "q95",
}

# Model-specific ensemble suffixes -> payload key.
# CSV columns: ``Q_<model_name>_xgb`` etc. The ``Q_loc`` column has no suffix
# (it is the literal column name, not derived from the model name).
_ENSEMBLE_SUFFIXES: dict[str, str] = {
    "_xgb": "q_xgb",
    "_lgbm": "q_lgbm",
    "_catboost": "q_catboost",
}

# Required columns in every source row (parse fails otherwise).
_REQUIRED_COLUMNS: frozenset[str] = frozenset({"code", "date", "valid_from", "valid_to"})


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
        return int(float(s))
    except (ValueError, OverflowError):
        return None


def _parse_iso_date(raw: str | None) -> str | None:
    """Return an ISO ``YYYY-MM-DD`` string, or None if unparseable.

    Tolerates trailing time fragments (``YYYY-MM-DD HH:MM:SS`` -> ``YYYY-MM-DD``)
    and ``YYYY-MM-DDTHH:MM:SS`` variants — the source CSVs sometimes carry
    pandas-style timestamps.
    """
    s = (raw or "").strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    # Trim time component if present (after 'T' or whitespace).
    head = s.split("T", 1)[0].split(" ", 1)[0]
    if len(head) != 10:
        return None
    try:
        datetime.date.fromisoformat(head)
    except ValueError:
        return None
    return head


def _parse_code(raw: str | None) -> str | None:
    """Return a normalized code string, or None.

    The source CSV may carry codes as floats (``15013.0``) due to pandas
    coercion. Strip a trailing ``.0`` if present.
    """
    s = (raw or "").strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    # Float-coerced ints: "15013.0" -> "15013"
    if "." in s:
        try:
            f = float(s)
        except ValueError:
            return s
        if f.is_integer():
            return str(int(f))
    return s


# ---------------------------------------------------------------------------
# Mode discovery / config loading
# ---------------------------------------------------------------------------


def _discover_modes(config_dir: Path) -> list[str]:
    """Return all mode names that have a JSON config in ``config_dir``.

    The ``monthly`` mode is always excluded from the returned list (non-
    operational; tracked via ``_ALWAYS_SKIP_MODES``). Other modes whose
    config JSON is absent are hard-skipped at the discovery step — there is
    no synthesis of configs from directory layout (architecture §Q3).

    Args:
        config_dir: path to the deployment's
            ``<config_root>/long_term_configs`` directory.

    Returns:
        Sorted list of mode names (e.g. ``["month_1", "month_2", "quarter",
        "seasonal_april"]``). Empty list means no source data for this
        deployment (UZB no-op acceptance, Stage E item #12).
    """
    if not config_dir.is_dir():
        return []
    modes: list[str] = []
    for entry in config_dir.glob("*.json"):
        if not entry.is_file():
            continue
        mode_name = entry.stem
        if mode_name in _ALWAYS_SKIP_MODES:
            continue
        modes.append(mode_name)
    return sorted(modes)


def _load_mode_config(config_dir: Path, mode_name: str) -> dict:
    """Parse ``<config_dir>/<mode_name>.json`` and extract model + horizon.

    Args:
        config_dir: path to ``long_term_configs`` directory.
        mode_name: mode identifier (file stem, e.g. ``month_1``).

    Returns:
        Dict with keys:
            - ``models`` (list[str]): flattened model list across all
              families in ``models_to_use``.
            - ``horizon_value`` (int): from ``operational_month_lead_time``.
            - ``horizon_type`` (str): from ``horizon_type`` if present, else
              ``"month"`` (default per the legacy migrator).

    Raises:
        FileNotFoundError: if the config JSON does not exist.
        ValueError: if the JSON is malformed or required keys are absent.
    """
    config_path = config_dir / f"{mode_name}.json"
    if not config_path.is_file():
        raise FileNotFoundError(f"long-term config not found: {config_path}")
    try:
        with config_path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except json.JSONDecodeError as exc:
        raise ValueError(f"long-term config {config_path.name}: invalid JSON: {exc}") from exc

    models_to_use = raw.get("models_to_use")
    if not isinstance(models_to_use, dict):
        raise ValueError(
            f"long-term config {config_path.name}: 'models_to_use' missing or not a dict"
        )
    models: list[str] = []
    for family, model_list in models_to_use.items():
        if not isinstance(model_list, list):
            raise ValueError(
                f"long-term config {config_path.name}: family {family!r} "
                f"value must be a list of model names"
            )
        for m in model_list:
            if isinstance(m, str) and m:
                models.append(m)

    horizon_value_raw = raw.get("operational_month_lead_time")
    if not isinstance(horizon_value_raw, int):
        raise ValueError(
            f"long-term config {config_path.name}: "
            f"'operational_month_lead_time' missing or not an int"
        )

    horizon_type = raw.get("horizon_type", "month")
    if not isinstance(horizon_type, str) or not horizon_type:
        horizon_type = "month"
    horizon_type = horizon_type.lower()
    _ALLOWED_HORIZON_TYPES = {"month"}
    if horizon_type not in _ALLOWED_HORIZON_TYPES:
        raise ValueError(
            f"long-term config {config_path.name}: "
            f"'horizon_type' must be one of {sorted(_ALLOWED_HORIZON_TYPES)!r}, "
            f"got {horizon_type!r}"
        )

    return {
        "models": models,
        "horizon_value": horizon_value_raw,
        "horizon_type": horizon_type,
    }


def _discover_hindcast_csvs(
    data_root: Path,
    mode: str,
    models: list[str],
) -> dict[str, Path]:
    """Return only those models whose hindcast CSV exists on disk.

    Hindcast path convention:
        ``<data_root>/long_term_predictions/<mode>/<model>/<model>_hindcast.csv``

    Args:
        data_root: path to the deployment's ``intermediate_data`` directory.
        mode: mode identifier (e.g. ``month_1``).
        models: list of model names from the mode's config.

    Returns:
        Dict mapping each model name to its hindcast CSV ``Path``. Models
        whose CSV is absent are silently omitted (logged at the caller).
    """
    found: dict[str, Path] = {}
    base = data_root / "long_term_predictions" / mode
    for model in models:
        csv_path = base / model / f"{model}_hindcast.csv"
        if csv_path.is_file():
            found[model] = csv_path
    return found


# ---------------------------------------------------------------------------
# Record building
# ---------------------------------------------------------------------------


def _build_record(
    row: dict[str, str],
    model_name: str,
    mode_config: dict,
) -> dict | None:
    """Build the per-row API payload for a long-term forecast record.

    Implements the universal safe-write rule: only non-NULL fields are
    included. The required key fields (``horizon_type``, ``horizon_value``,
    ``code``, ``date``, ``model_type``, ``valid_from``, ``valid_to``) must
    all be present; otherwise ``None`` is returned (caller treats this as a
    parse skip).

    Args:
        row: CSV row as ``{column_name: value_str}``.
        model_name: the model identifier (used for ``model_type`` plus
            ``Q_<model_name>`` and ``Q_<model_name>_<ensemble>`` lookups).
        mode_config: dict with ``horizon_value`` (int) and ``horizon_type``
            (str) from ``_load_mode_config``.

    Returns:
        Payload dict, or ``None`` if a required key field is missing /
        unparseable.
    """
    code = _parse_code(row.get("code"))
    date_str = _parse_iso_date(row.get("date"))
    valid_from = _parse_iso_date(row.get("valid_from"))
    valid_to = _parse_iso_date(row.get("valid_to"))
    if not code or not date_str or not valid_from or not valid_to:
        return None

    rec: dict = {
        "horizon_type": mode_config["horizon_type"],
        "horizon_value": mode_config["horizon_value"],
        "code": code,
        "date": date_str,
        "model_type": model_name,
        "valid_from": valid_from,
        "valid_to": valid_to,
    }

    # --- Optional flag (int) ---
    flag = _parse_int(row.get("flag"))
    if flag is not None:
        rec["flag"] = flag

    # --- Optional composition (string) ---
    composition = (row.get("composition") or "").strip()
    if composition and composition.lower() not in {"nan", "none", "null"}:
        rec["composition"] = composition

    # --- Main model point forecast (Q_<model_name>) ---
    q_col = f"Q_{model_name}"
    q_val = _parse_float(row.get(q_col))
    if q_val is not None:
        rec["q"] = q_val

    # --- Observed (q_obs) ---
    q_obs = _parse_float(row.get("Q_obs"))
    if q_obs is not None:
        rec["q_obs"] = q_obs

    # --- Standalone quantiles (Q5..Q95) ---
    for csv_name, payload_key in _QUANTILE_CSV_TO_PAYLOAD.items():
        v = _parse_float(row.get(csv_name))
        if v is not None:
            rec[payload_key] = v

    # --- Model-specific ensemble columns (Q_<model>_xgb, _lgbm, _catboost) ---
    for suffix, payload_key in _ENSEMBLE_SUFFIXES.items():
        csv_name = f"Q_{model_name}{suffix}"
        v = _parse_float(row.get(csv_name))
        if v is not None:
            rec[payload_key] = v

    # --- Q_loc (literal column name; not derived from model_name) ---
    q_loc = _parse_float(row.get("Q_loc"))
    if q_loc is not None:
        rec["q_loc"] = q_loc

    return rec


def _read_filtered_records(
    csv_path: Path,
    model_name: str,
    mode_config: dict,
    *,
    cutoff: str | None,
    station_filter: str | None,
) -> tuple[list[dict], dict[str, int], set[str], str | None, str | None]:
    """Read CSV and return (records, counters, distinct_codes, date_min, date_max).

    Args:
        csv_path: path to a ``<model>_hindcast.csv`` file.
        model_name: model identifier; used by ``_build_record`` for the
            ``Q_<model>`` and ensemble column lookups.
        mode_config: dict with ``horizon_value`` and ``horizon_type``.
        cutoff: optional ISO date; rows with ``date >= cutoff`` are dropped.
        station_filter: optional single station code; rows whose ``code``
            does not match are dropped.

    Returns:
        Tuple ``(records, counters, distinct_codes, date_min, date_max)``.

    Raises:
        ValueError: if required columns (``code``, ``date``, ``valid_from``,
            ``valid_to``) are missing from the CSV header.
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

        for row in reader:
            counters["source_row_count"] += 1
            code_raw = _parse_code(row.get("code"))
            date_str = _parse_iso_date(row.get("date"))

            # Track source date range pre-filter (for dry-run inventory).
            if date_str:
                if date_min is None or date_str < date_min:
                    date_min = date_str
                if date_max is None or date_str > date_max:
                    date_max = date_str

            if not code_raw or not date_str:
                counters["skipped_parse"] += 1
                continue

            # Station filter (applied before cutoff for clarity in counters).
            if station_filter is not None and code_raw != station_filter:
                counters["skipped_station"] += 1
                continue

            # Cutoff filter (strictly less than cutoff).
            if cutoff is not None and date_str >= cutoff:
                counters["skipped_cutoff"] += 1
                continue

            record = _build_record(row, model_name, mode_config)
            if record is None:
                counters["skipped_parse"] += 1
                continue

            records.append(record)
            distinct_codes.add(code_raw)
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
    ``LongForecastBulkCreate`` schema. Returns ``(ok, message)``.
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
        prog="python3 -m migration_py.long_forecast",
        description="Push long-term forecast CSV rows to the postprocessing API.",
    )
    p.add_argument(
        "--config-dir",
        required=True,
        type=Path,
        help=(
            "Path to the deployment's long_term_configs directory inside the "
            "container (e.g. /config/long_term_configs)."
        ),
    )
    p.add_argument(
        "--data-dir",
        required=True,
        type=Path,
        help=(
            "Path to the deployment's intermediate_data directory inside the "
            "container (e.g. /intermediate_data). Hindcast CSVs are looked up "
            "under <data_dir>/long_term_predictions/<mode>/<model>/."
        ),
    )
    p.add_argument(
        "--api-url",
        required=True,
        help="Postprocessing API endpoint URL (e.g. http://localhost:8003/long-forecast/).",
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
        help="Filter to a single station code (binding contract from P0).",
    )
    p.add_argument(
        "--mode",
        default=None,
        help="Restrict the run to a single mode (e.g. month_1).",
    )
    p.add_argument(
        "--model",
        default=None,
        help="Restrict the run to a single model name (e.g. LR_Base).",
    )
    p.add_argument(
        "--skip-mode",
        default=None,
        help="Comma-separated list of mode names to skip (in addition to monthly).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Read + filter only; do NOT POST.",
    )
    return p


def _print_dry_run_inventory(
    *,
    mode: str,
    target_mode: str,
    cutoff: str | None,
    discovered_modes: list[str],
    per_mode_summary: list[dict],
    distinct_codes_total: set[str],
) -> None:
    """Emit the §4.4 runbook dry-run inventory block.

    Lines are printed on stdout (the wrapper tees them into its log file).
    Station codes are NEVER printed individually — only the redacted count.

    The P5-specific shape adds per-mode and per-model inventory lines so the
    operator validates which (mode, model) pairs will be written before the
    real run.

    Args:
        mode: 'full-import' or 'pre-cutoff' (decided by the wrapper from the
            target-DB query).
        target_mode: passed through verbatim for display.
        cutoff: ISO date string for pre-cutoff mode, else None.
        discovered_modes: list of mode names whose JSON config was found
            (post mode-filter, pre skip-mode filter for visibility).
        per_mode_summary: one dict per (mode, model) with keys
            ``mode``, ``model``, ``hindcast_present`` (bool), ``source_row_count``,
            ``filtered_row_count``, ``date_min``, ``date_max``, ``distinct_code_count``.
        distinct_codes_total: union of distinct codes across all
            (mode, model) pairs.
    """
    print(f"MODE={target_mode}" + (f" (cutoff={cutoff})" if cutoff else " (target empty)"))
    print("TARGET_TABLE=long_forecasts")
    print(f"CUTOFF={cutoff if cutoff else 'none'}")
    print(f"DISCOVERED_MODE_COUNT={len(discovered_modes)}")
    if discovered_modes:
        print(f"DISCOVERED_MODES={discovered_modes}")
    else:
        print("DISCOVERED_MODES=[]")
    total_src = sum(item["source_row_count"] for item in per_mode_summary)
    total_filtered = sum(item["filtered_row_count"] for item in per_mode_summary)
    print(f"SOURCE_ROW_COUNT={total_src}")
    print(f"FILTERED_ROW_COUNT={total_filtered}")
    print(f"DISTINCT_STATION_COUNT_REDACTED={len(distinct_codes_total)}")

    # One line per (mode, model) pair so the operator validates each pairing.
    for item in per_mode_summary:
        flag = "ok" if item["hindcast_present"] else "MISSING_HINDCAST"
        print(
            f"MODE_INVENTORY mode={item['mode']} model={item['model']} "
            f"status={flag} "
            f"source_rows={item['source_row_count']} "
            f"filtered_rows={item['filtered_row_count']} "
            f"date_min={item['date_min'] or 'none'} "
            f"date_max={item['date_max'] or 'none'} "
            f"distinct_codes={item['distinct_code_count']}"
        )

    # Redacted log line (count only, never the actual codes).
    _common.log_redacted_station_count(
        logger, sorted(distinct_codes_total), message_prefix="post_filter_stations"
    )


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    config_dir: Path = args.config_dir
    data_dir: Path = args.data_dir

    if not config_dir.is_dir():
        # Configured-mode absence is the UZB no-op acceptance path: exit 0
        # with a logged "no source data" message (Stage E item #12).
        print(f"no source data for this deployment: config dir {config_dir} not found")
        return 0

    # MODE is decided by the wrapper from the psql query; we just receive
    # the cutoff (or None) and pass it through.
    cutoff: str | None = args.cutoff
    target_mode = "pre-cutoff" if cutoff else "full-import"

    # ---- Mode discovery + filter ----
    all_modes = _discover_modes(config_dir)
    skip_modes: set[str] = set()
    if args.skip_mode:
        skip_modes = {m.strip() for m in args.skip_mode.split(",") if m.strip()}

    filtered_modes: list[str] = []
    for mode_name in all_modes:
        if args.mode is not None and mode_name != args.mode:
            continue
        if mode_name in skip_modes:
            continue
        filtered_modes.append(mode_name)

    if not filtered_modes:
        # No configured modes at all (UZB no-op) OR filter eliminated all of
        # them. Either way: exit 0 with the no-source message.
        print(
            "no source data for this deployment: no configured long-term "
            f"modes found under {config_dir} (after filters: --mode={args.mode!r} "
            f"--skip-mode={sorted(skip_modes)!r})"
        )
        return 0

    # ---- Walk each (mode, model) pair, build records ----
    per_mode_summary: list[dict] = []
    all_records: list[tuple[dict, str, str]] = []  # (record, mode, model)
    distinct_codes_total: set[str] = set()

    for mode_name in filtered_modes:
        try:
            mode_config = _load_mode_config(config_dir, mode_name)
        except (FileNotFoundError, ValueError) as exc:
            print(f"ERROR: cannot load config for mode {mode_name!r}: {exc}", file=sys.stderr)
            # Hard-skip this mode but continue with siblings.
            continue

        models = list(mode_config["models"])
        if args.model is not None:
            models = [m for m in models if m == args.model]

        hindcast_paths = _discover_hindcast_csvs(data_dir, mode_name, models)

        for model in models:
            csv_path = hindcast_paths.get(model)
            if csv_path is None:
                per_mode_summary.append(
                    {
                        "mode": mode_name,
                        "model": model,
                        "hindcast_present": False,
                        "source_row_count": 0,
                        "filtered_row_count": 0,
                        "date_min": None,
                        "date_max": None,
                        "distinct_code_count": 0,
                    }
                )
                print(
                    f"no hindcast for mode={mode_name} model={model} "
                    f"(expected at {data_dir}/long_term_predictions/{mode_name}/{model}/"
                    f"{model}_hindcast.csv); skipping"
                )
                continue

            try:
                records, counters, distinct_codes, date_min, date_max = _read_filtered_records(
                    csv_path,
                    model,
                    mode_config,
                    cutoff=cutoff,
                    station_filter=args.station_filter,
                )
            except ValueError as exc:
                print(
                    f"ERROR reading {csv_path.name} (mode={mode_name} model={model}): {exc}",
                    file=sys.stderr,
                )
                continue

            per_mode_summary.append(
                {
                    "mode": mode_name,
                    "model": model,
                    "hindcast_present": True,
                    "source_row_count": counters["source_row_count"],
                    "filtered_row_count": counters["filtered_row_count"],
                    "date_min": date_min,
                    "date_max": date_max,
                    "distinct_code_count": len(distinct_codes),
                }
            )
            distinct_codes_total |= distinct_codes
            for record in records:
                all_records.append((record, mode_name, model))

    _print_dry_run_inventory(
        mode=target_mode,
        target_mode=target_mode,
        cutoff=cutoff,
        discovered_modes=filtered_modes,
        per_mode_summary=per_mode_summary,
        distinct_codes_total=distinct_codes_total,
    )

    if args.dry_run:
        print("DRY RUN: no POSTs attempted.")
        return 0

    if not all_records:
        print("No records to POST after filtering; exiting 0.")
        return 0

    # ---- POST in batches ----
    records_only = [r for (r, _m, _model) in all_records]
    n = len(records_only)
    batch_size = max(1, args.batch_size)
    n_batches = (n + batch_size - 1) // batch_size
    sent = 0
    failed = 0
    for i in range(0, n, batch_size):
        batch = records_only[i : i + batch_size]
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
