# postprocessing_maintenance.py
# Nightly gap-fill entry point: detects missing ensemble forecasts
# within a lookback window, creates them from pre-calculated skill metrics,
# and saves the results.
#
# Usage:
#   ieasyhydroforecast_env_file_path=/path/to/.env \
#   SAPPHIRE_PREDICTION_MODE=BOTH \
#   POSTPROCESSING_GAPFILL_MAX_MONTHS=13 \
#   python postprocessing_maintenance.py

import contextlib
import datetime as dt
import json
import logging
import os
import sys
import time
import warnings
from dataclasses import dataclass, field
from logging.handlers import TimedRotatingFileHandler
from typing import Any

import pandas as pd

# Local libraries
script_dir = os.path.dirname(os.path.abspath(__file__))
forecast_dir = os.path.join(script_dir, "..", "iEasyHydroForecast")
sys.path.append(forecast_dir)

import setup_library as sl
import tag_library as tl
from src import api_writer, data_reader, ensemble_calculator, file_writer, gap_detector
from src import postprocessing_tools as pt
from src.horizon_config import ShortTermHorizonConfig
from src.postprocessing_tools import TimingStats, timer

# region Logging
logging.basicConfig(level=logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

if not os.path.exists("logs"):
    os.makedirs("logs")

file_handler = TimedRotatingFileHandler(
    "logs/log_maintenance", when="midnight", interval=1, backupCount=30
)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.handlers = []
logger.addHandler(file_handler)
logger.addHandler(console_handler)
# endregion

timing_stats = TimingStats()

PENTAD = ShortTermHorizonConfig(
    name="pentad",
    period_col="pentad_in_year",
    period_in_month_col="pentad_in_month",
    get_period_func=tl.get_pentad,
    combined_csv_env="ieasyforecast_combined_forecast_pentad_file",
    skill_csv_env="ieasyforecast_pentadal_skill_metrics_file",
    api_horizon_type="pentad",
    neural_ensemble_func=sl.calculate_neural_ensemble_forecast,
    station_selection_env="ieasyforecast_config_file_station_selection",
)
DECAD = ShortTermHorizonConfig(
    name="decad",
    period_col="decad_in_year",
    period_in_month_col="decad_in_month",
    get_period_func=tl.get_decad_in_month,
    combined_csv_env="ieasyforecast_combined_forecast_decad_file",
    skill_csv_env="ieasyforecast_decadal_skill_metrics_file",
    api_horizon_type="decad",
    neural_ensemble_func=sl.calculate_neural_ensemble_forecast_decade,
    station_selection_env="ieasyforecast_config_file_station_selection_decad",
)


# ======================================================================
# P2: Recovery preview data structures
# ======================================================================

_RECOVERY_PREVIEW_DEADLINE_SECONDS = 300.0
_EM_BLOCKED_REASON_SKILL = "skill_vintage_unresolved"

# Canonical model keys used in authorized_keys and plan; API call names differ.
_CANONICAL_MODEL_KEY: dict[str, str] = {
    "TFT": "TFT",
    "TiDE": "TIDE",
    "TIDE": "TIDE",
    "TSMixer": "TSMIXER",
    "TSMIXER": "TSMIXER",
    "NE": "NE",
    "ENSEMBLE_MEAN": "ENSEMBLE_MEAN",
    "EM": "ENSEMBLE_MEAN",
}


def _canonical_model_key(model_str: str) -> str:
    """Return canonical planner authorization key for a model string.

    API request names (TiDE, TSMixer) map to their canonical keys (TIDE, TSMIXER).
    Unknown strings are uppercased as a fallback.
    """
    return _CANONICAL_MODEL_KEY.get(model_str, model_str.upper())


@dataclass
class HorizonPreviewResult:
    """Structured result for one horizon's recovery preview.

    All fields are diagnostic/advisory only; no data has been written.
    """

    plan: Any  # PeriodRefreshPlan | None
    source_statuses: dict  # {(code, model_str): status_str}
    candidate_count: int
    missing_count: int
    protected_count: int
    em_blocked_reason: str
    diagnostics: list
    blocked_reason: str | None = None
    blocked_units: dict = field(default_factory=dict)  # {(code,model): reason}
    candidate_payloads: list = field(default_factory=list)  # actual payloads, for inspection
    unusable_units: dict = field(
        default_factory=dict
    )  # {(horizon,code,model,issue,target): reason}
    planner_authorized_keys: set = field(default_factory=set)  # authorized_keys passed to planner


@dataclass
class RecoveryPreviewResult:
    """Top-level result for a recovery preview run."""

    forecast_date: dt.date
    start_date: dt.date
    end_date: dt.date
    horizons: list
    horizon_results: dict  # {horizon_name: HorizonPreviewResult}
    blocked_reason: str | None = None
    destination_base_url: str | None = None  # native client base URL; None for injected
    started_at: float | None = None  # monotonic time before client construction
    preparation_deadline: float | None = None  # shared preview deadline (started_at+300s)
    execution_eligible: bool = False  # True only for native-transport previews


# ======================================================================
# P2: Recovery helpers
# ======================================================================


def _capture_forecast_date() -> dt.date:
    """Capture forecast_date once from SAPPHIRE_FORECAST_DATE or today.

    Fallback to today ONLY when the env var is unset or empty.

    Raises:
        ValueError: If SAPPHIRE_FORECAST_DATE is set but not a valid YYYY-MM-DD date
            (compact YYYYMMDD, ISO week YYYY-Www, and unparseable values are all rejected).
    """
    fd_str = os.getenv("SAPPHIRE_FORECAST_DATE", "")
    if not fd_str:
        return dt.date.today()
    # Reject compact (no dashes) and ISO week (contains W) formats before parsing.
    if "W" in fd_str.upper() or "-" not in fd_str:
        raise ValueError(
            f"SAPPHIRE_FORECAST_DATE={fd_str!r}: must be YYYY-MM-DD "
            "(compact and week formats are not accepted)"
        )
    try:
        return dt.date.fromisoformat(fd_str)
    except ValueError as exc:
        raise ValueError(f"SAPPHIRE_FORECAST_DATE={fd_str!r}: not a valid ISO date") from exc


def _get_enabled_ml_models() -> list[str]:
    """Return list of enabled ML model names (canonical API form) from env.

    Returns TFT/TiDE/TSMixer forms accepted by read_day_forecasts_for_recovery.
    Returns empty list when run_ML_models != "true" or no models configured.
    """
    run_ml = os.getenv("ieasyhydroforecast_run_ML_models", "false").lower()
    if run_ml != "true":
        return []
    available_str = os.getenv("ieasyhydroforecast_available_ML_models", "")
    if not available_str:
        return []
    alias_map = {"TIDE": "TiDE", "TSMIXER": "TSMixer", "TFT": "TFT"}
    models = []
    seen: set[str] = set()
    for m_raw in available_str.split(","):
        m = m_raw.strip()
        if not m:
            continue
        mu = m.upper()
        if mu not in alias_map:
            logger.warning(
                "Unsupported ML model name %r in ieasyhydroforecast_available_ML_models; "
                "skipping (supported: TFT, TiDE, TSMixer)",
                m,
            )
            continue
        api_name = alias_map[mu]
        if api_name not in seen:
            models.append(api_name)
            seen.add(api_name)
    return models


def _get_virtual_config_path() -> str:
    """Return absolute path to virtual station config file, or '' if not configured."""
    vf = os.getenv("ieasyforecast_virtual_stations", "")
    if not vf:
        return ""
    return os.path.join(os.getenv("ieasyforecast_configuration_path", ""), vf)


def _validate_one_virtual_instruction(
    station: str,
    instruction: object,
    all_virtual: set[str],
) -> str | None:
    """Validate one virtual station instruction dict.

    Returns a reason string if invalid, else None.
    Checks: function==sum, non-empty weights, finite non-negative numeric (not bool),
    no self-reference, no nested virtual contributors.
    """
    import math as _math

    if not isinstance(instruction, dict):
        return f"instruction is not a dict: {type(instruction).__name__}"

    if "combinationFunction" not in instruction:
        return "missing combinationFunction"
    func = instruction["combinationFunction"]
    if str(func) != "sum":
        return f"unsupported combinationFunction {func!r} (only 'sum' supported)"

    weights = instruction.get("weightByStation", {})
    if not isinstance(weights, dict) or not weights:
        return "empty or non-dict weightByStation"

    for contributor, w in weights.items():
        if isinstance(w, bool):
            return f"weight for {contributor!r} is bool, not numeric"
        if not isinstance(w, (int, float)):
            return f"weight for {contributor!r} is not numeric: {type(w).__name__}"
        if not _math.isfinite(float(w)):
            return f"weight for {contributor!r} is not finite: {w}"
        if w < 0:
            return f"weight for {contributor!r} is negative: {w}"

    if station in weights:
        return "self-reference in weightByStation"

    nested = [c for c in weights if c in all_virtual and c != station]
    if nested:
        return f"nested virtual contributor(s): {nested}"

    return None


def _preflight_virtual_instructions(
    config_path: str,
) -> tuple[bool, dict[str, str]]:
    """Read and validate ALL configured virtual station instructions.

    Args:
        config_path: Absolute path to the virtual station JSON config file.
            Empty string means not configured (returns (True, {})).

    Returns:
        (file_ok, blocked): file_ok=False means file unreadable/malformed (UNKNOWN);
        blocked maps station_code → reason for stations with invalid instructions.
        When file_ok=False, blocked is always empty — the whole file is UNKNOWN.
    """
    if not config_path:
        return True, {}
    try:
        with open(config_path) as f:
            data = json.load(f)
    except Exception as exc:
        logger.warning("Virtual station config unreadable (UNKNOWN): %s", exc)
        return False, {}

    if not isinstance(data, dict):
        logger.warning("Virtual station config is not a JSON object (UNKNOWN): %s", config_path)
        return False, {}

    virtual_stations = data.get("virtualStations", {})
    if not isinstance(virtual_stations, dict):
        logger.warning(
            "Virtual station config 'virtualStations' is not a dict (UNKNOWN): %s",
            config_path,
        )
        return False, {}
    all_virtual = set(virtual_stations.keys())
    blocked: dict[str, str] = {}
    for station, instruction in virtual_stations.items():
        reason = _validate_one_virtual_instruction(station, instruction, all_virtual)
        if reason:
            blocked[station] = reason

    return True, blocked


def _read_virtual_station_config() -> dict[str, list[str]]:
    """Read virtual station config and return {virtual_code: [contributor_codes]}.

    Returns empty dict if not configured (env var unset or empty).

    Raises:
        OSError: If the configured file cannot be opened.
        ValueError: If the configured file is not valid JSON or has unexpected structure.
    """
    config_path = _get_virtual_config_path()
    if not config_path:
        return {}
    with open(config_path) as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Virtual station config is not a JSON object: {config_path}")
    result: dict[str, list[str]] = {}
    for station, instruction in data.get("virtualStations", {}).items():
        if isinstance(instruction, dict):
            contributors = list(instruction.get("weightByStation", {}).keys())
        else:
            contributors = []
        result[station] = contributors
    return result


def _build_candidate_payload(
    row: "pd.Series",
    config: ShortTermHorizonConfig,
) -> dict[str, Any] | None:
    """Build a canonical planner payload dict from a normalized ML output row.

    Returns None if any required field is missing or non-finite.
    """
    try:
        import math

        import pandas as _pd

        code = str(row["code"])
        if not code:
            return None

        date_val = _pd.Timestamp(row["date"])
        issue_date = date_val.date()
        target_date = (date_val + _pd.Timedelta(days=1)).date()

        model_type = str(row["model_short"])

        def _to_float(v: Any) -> float | None:
            if v is None:
                return None
            try:
                f = float(v)
                return f if math.isfinite(f) else None
            except (TypeError, ValueError):
                return None

        fd = _to_float(row.get("forecasted_discharge"))
        q05 = _to_float(row.get("q05"))
        q25 = _to_float(row.get("q25"))
        q75 = _to_float(row.get("q75"))
        q95 = _to_float(row.get("q95"))

        if any(v is None for v in (fd, q05, q25, q75, q95)):
            return None

        period_in_month_col = config.period_in_month_col
        period_col = config.period_col

        hv_raw = row.get(period_in_month_col)
        hiy_raw = row.get(period_col)

        def _to_int(v: Any) -> int | None:
            """Convert to int without float roundtrip. Rejects bool, NaN, fractions."""
            if v is None:
                return None
            if isinstance(v, bool):
                return None  # bool subclasses int; reject explicitly
            if isinstance(v, int):
                return v
            # numpy integer types: check by type name to avoid hard numpy import
            _tn = type(v).__name__
            if _tn in ("int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64"):
                return int(v)
            # Floating types: allow only when exactly integer-valued
            try:
                f = float(v)
                if not math.isfinite(f):
                    return None
                i = int(f)
                if f != i:  # reject fractions — don't truncate
                    return None
                return i
            except (TypeError, ValueError):
                return None

        horizon_value = _to_int(hv_raw)
        horizon_in_year = _to_int(hiy_raw)

        if horizon_value is None or horizon_in_year is None:
            return None

        flag_raw = row.get("flag")
        flag: int | None
        if flag_raw is None or isinstance(flag_raw, float) and math.isnan(flag_raw):
            flag = None
        else:
            flag = _to_int(flag_raw)

        # composition: take from row if a valid string; NaN → None; never fabricate
        composition_raw = row.get("composition")
        if composition_raw is None:
            composition: str | None = None
        elif isinstance(composition_raw, float) and math.isnan(composition_raw):
            composition = None
        elif isinstance(composition_raw, str):
            composition = composition_raw
        else:
            composition = None

        return {
            "horizon_type": config.name,
            "code": code,
            "model_type": model_type,
            "date": issue_date,
            "target": target_date,
            "forecasted_discharge": fd,
            "q05": q05,
            "q25": q25,
            "q75": q75,
            "q95": q95,
            "flag": flag,
            "composition": composition,
            "horizon_value": horizon_value,
            "horizon_in_year": horizon_in_year,
        }
    except Exception:
        return None


def _run_recovery_preview_for_horizon(
    config: ShortTermHorizonConfig,
    codes: list[str],
    forecast_date: dt.date,
    start_date: dt.date,
    end_date: dt.date,
    deadline: float,
    *,
    _client: Any = None,
    _virtual_sources: dict[str, list[str]] | None = None,
) -> HorizonPreviewResult:
    """Run recovery preview for one horizon.

    Args:
        config: ShortTermHorizonConfig for this horizon.
        codes: Selected station codes (output allowlist).
        forecast_date: Captured once at entry.
        start_date: Inclusive lower bound for issue dates.
        end_date: Inclusive upper bound for issue dates.
        deadline: Absolute monotonic deadline (time.monotonic()).
        _client: Injected SapphirePostprocessingClient (for tests).
        _virtual_sources: Injected virtual→contributors mapping (for tests).
    """
    import pandas as _pd

    COMPLETE = data_reader.RECOVERY_STATUS_COMPLETE
    EMPTY = data_reader.RECOVERY_STATUS_EMPTY

    source_statuses: dict = {}
    blocked_units: dict = {}
    diagnostics: list = []

    ml_models = _get_enabled_ml_models()

    # Virtual station source expansion: for virtual codes, also read contributors.
    # Preflight all configured instructions before calling the helper (which loops all).
    virtual_config_unknown = False
    virtual_helper_blocked = False
    if _virtual_sources is None:
        _vpath = _get_virtual_config_path()
        if _vpath:
            _vfile_ok, _vinvalid = _preflight_virtual_instructions(_vpath)
            if not _vfile_ok:
                virtual_config_unknown = True
                virtual_helper_blocked = True
                virtual_config = {}
                diagnostics.append(
                    "Virtual station config unreadable (UNKNOWN): "
                    "virtual construction blocked for all selected codes"
                )
            elif _vinvalid:
                # Any invalid configured station may cause helper to exit() for all.
                virtual_helper_blocked = True
                # Read config for dependency reporting (NE blocking) even though helper cannot run.
                try:
                    virtual_config = _read_virtual_station_config()
                except Exception as _vce:
                    virtual_config_unknown = True
                    virtual_config = {}
                    diagnostics.append(
                        f"Virtual config read for dependency reporting failed: {_vce}"
                    )
                for _vc, _vr in _vinvalid.items():
                    diagnostics.append(f"virtual station {_vc!r} invalid config: {_vr}")
                diagnostics.append(
                    "Virtual construction blocked: invalid configured station(s) "
                    "prevent safe helper call (helper loops all configured stations)"
                )
            else:
                try:
                    virtual_config = _read_virtual_station_config()
                except Exception as exc:
                    virtual_config_unknown = True
                    virtual_helper_blocked = True
                    virtual_config = {}
                    diagnostics.append(f"Virtual config read failed (UNKNOWN): {exc}")
        else:
            virtual_config = {}
    else:
        virtual_config = _virtual_sources

    source_codes: set[str] = set()
    for code in codes:
        if code in virtual_config:
            source_codes.update(virtual_config[code])
        else:
            source_codes.add(code)
    # Always ensure selected codes are included as potential sources
    source_codes.update(codes)

    output_allowlist = set(codes)

    # ---------- DAY reads per source code per ML model ----------
    all_normalized: list = []

    for code in sorted(source_codes):
        for model_str in ml_models:
            if time.monotonic() >= deadline:
                msg = f"deadline exceeded before reading {code}/{model_str}"
                blocked_units[(code, model_str)] = msg
                diagnostics.append(msg)
                continue

            status, raw, reason = data_reader.read_day_forecasts_for_recovery(
                _client,
                [code],
                model_str,
                start_date,
                end_date,
                forecast_date,
                total_deadline=deadline,
            )
            source_statuses[(code, model_str)] = status

            if status == COMPLETE and raw is not None and not raw.empty:
                normed = data_reader._normalize_ml_forecasts(raw, model_str, config.name)
                if not normed.empty:
                    all_normalized.append(normed)
            elif status not in (COMPLETE, EMPTY):
                blocked_units[(code, model_str)] = f"source {status}: {reason}"
                diagnostics.append(f"DAY read {code}/{model_str}: {status} — {reason}")

    # ---------- LR read (source-only, no period output) ----------
    lr_status: str
    if time.monotonic() < deadline:
        lr_status, lr_raw, lr_reason = data_reader.read_lr_forecasts_for_recovery(
            _client,
            list(source_codes),
            config.name,
            start_date,
            end_date,
            forecast_date,
            total_deadline=deadline,
        )
        source_statuses[("ALL", "LR")] = lr_status
        if lr_status not in (COMPLETE, EMPTY):
            diagnostics.append(f"LR read: {lr_status} — {lr_reason}")
    else:
        lr_status = data_reader.RECOVERY_STATUS_FAILED
        source_statuses[("ALL", "LR")] = lr_status
        diagnostics.append("LR read skipped: deadline exceeded")

    # EM blocked because skill vintage is unresolved (finding 1 / map §5)
    em_blocked_reason = _EM_BLOCKED_REASON_SKILL
    if lr_status not in (COMPLETE, EMPTY):
        em_blocked_reason = f"LR_{lr_status}+{_EM_BLOCKED_REASON_SKILL}"

    # ---------- Build individual normalized frame ----------
    if not all_normalized:
        # Propagate source failures to NE before enumeration (mirrors normal-path NE blocking).
        _unknown_src_early = {c for (c, _) in blocked_units}
        for _oc in output_allowlist:
            if (_oc, "NE") not in blocked_units:
                if _oc in _unknown_src_early:
                    blocked_units[(_oc, "NE")] = f"NE blocked: unknown source read for code {_oc}"
                elif virtual_config_unknown:
                    blocked_units[(_oc, "NE")] = (
                        f"NE blocked: virtual config unknown for code {_oc}"
                    )
                elif _oc in virtual_config and (
                    virtual_helper_blocked
                    or any(c in _unknown_src_early for c in virtual_config[_oc])
                ):
                    blocked_units[(_oc, "NE")] = (
                        f"NE blocked: virtual construction failed for code {_oc}"
                    )

        _unusable_early: dict = {}
        _enum_deadline_hit_early = False
        if ml_models:
            for _iss in _iter_issue_dates(start_date, end_date):
                if not gap_detector._plan_is_valid_boundary(_iss, config.name):
                    continue
                if time.monotonic() >= deadline:
                    _enum_deadline_hit_early = True
                    diagnostics.append(
                        f"unusable-early diagnostics blocked: deadline exceeded at {_iss}"
                    )
                    break
                _tgt = _iss + dt.timedelta(days=1)
                for _oc in sorted(output_allowlist):
                    for _em in list(ml_models) + ["NE"]:
                        _can = _canonical_model_key(_em)
                        _fk = (config.name, _oc, _can, _iss, _tgt)
                        if (_oc, _em) in blocked_units or (_oc, _can) in blocked_units:
                            continue
                        if _oc in virtual_config and virtual_helper_blocked:
                            if _em != "NE" and (_oc, _can) not in blocked_units:
                                blocked_units[(_oc, _can)] = (
                                    f"unknown: virtual construction blocked for code {_oc}"
                                )
                            continue
                        if (
                            _em != "NE"
                            and _oc in virtual_config
                            and any(c in _unknown_src_early for c in virtual_config[_oc])
                        ):
                            if (_oc, _can) not in blocked_units:
                                blocked_units[(_oc, _can)] = (
                                    f"unknown: virtual contributor source failed for code {_oc}"
                                )
                            continue
                        if _em == "NE":
                            _urea = "NE row absent: no eligible ML source data"
                            _unusable_early[_fk] = _urea
                        else:
                            if virtual_config_unknown:
                                if (_oc, _can) not in blocked_units:
                                    blocked_units[(_oc, _can)] = (
                                        f"unknown: virtual config unreadable, cannot classify absence for code {_oc}"
                                    )
                                continue
                            _sst = source_statuses.get((_oc, _em))
                            if _sst == EMPTY:
                                _urea = "confirmed empty DAY"
                            elif _sst == COMPLETE:
                                _urea = (
                                    "complete DAY read but normalizer produced no eligible output"
                                )
                            elif _sst is None:
                                _urea = (
                                    "virtual-derived output absent"
                                    if _oc in virtual_config
                                    else "source not read within bounds"
                                )
                            else:
                                _urea = f"source status: {_sst}"
                            _unusable_early[_fk] = _urea
        if _unusable_early:
            diagnostics.append(
                f"{len(_unusable_early)} unusable unit(s): no eligible candidate output"
            )
        return HorizonPreviewResult(
            plan=None,
            source_statuses=source_statuses,
            candidate_count=0,
            missing_count=0,
            protected_count=0,
            em_blocked_reason=em_blocked_reason,
            diagnostics=diagnostics,
            blocked_reason=(
                "deadline exceeded during unusable-output enumeration (incomplete)"
                if _enum_deadline_hit_early
                else None
            ),
            blocked_units=blocked_units,
            candidate_payloads=[],
            unusable_units=_unusable_early,
        )

    frame = _pd.concat(all_normalized, ignore_index=True)

    # ---------- Virtual stations ----------
    # Save frame so direct ML virtual twins survive a helper failure or skip.
    _frame_before_virtual = frame.copy()
    if not virtual_helper_blocked:
        try:
            frame = sl.calculate_virtual_stations_data(frame)
        except SystemExit as exc:
            # Contain helper exit() as failure; do not let it terminate the preview.
            frame = _frame_before_virtual
            virtual_helper_blocked = True
            diagnostics.append(f"virtual station helper exited (contained as failure): {exc}")
        except Exception as exc:
            frame = _frame_before_virtual
            virtual_helper_blocked = True
            diagnostics.append(f"virtual station calculation failed: {exc}")
    # When helper blocked or failed: frame retains direct ML rows for virtual codes.

    # ---------- NE ----------
    try:
        frame = config.neural_ensemble_func(frame)
    except Exception as exc:
        diagnostics.append(f"NE calculation failed: {exc}")

    # Block NE for output codes where ANY enabled model read was unknown/failed.
    # Known-EMPTY reads are fine (excluded by helper, not unknown transport).
    _unknown_src = {c for (c, _) in blocked_units}
    _ne_blocked: set[str] = set()
    for _oc in output_allowlist:
        if (
            _oc in _unknown_src
            or virtual_config_unknown
            or _oc in virtual_config
            and (
                virtual_helper_blocked
                or any(contrib in _unknown_src for contrib in virtual_config[_oc])
            )
        ):
            _ne_blocked.add(_oc)
    if _ne_blocked:
        _ne_block_mask = (frame["model_short"] == "NE") & frame["code"].isin(_ne_blocked)
        frame = frame[~_ne_block_mask].copy()
        for _nbc in sorted(_ne_blocked):
            _nb_reason = f"NE blocked: unknown source read for code {_nbc}"
            blocked_units[(_nbc, "NE")] = _nb_reason
            diagnostics.append(_nb_reason)

    # ---------- Filter to output allowlist ----------
    # Exclude LR, DAY lead, LT; include ML individual models + NE
    allowed_output_models = set(ml_models) | {"NE"}
    # Normalizer may emit "TiDE"/"TSMixer" as model_short; keep as-is for planner alias
    frame = frame[
        frame["code"].isin(output_allowlist) & frame["model_short"].isin(allowed_output_models)
    ].copy()

    # ---------- Build candidate payloads ----------
    candidate_payloads: list[dict] = []
    candidate_by_unit: dict[tuple, list[dict]] = {}

    for _, row in frame.iterrows():
        payload = _build_candidate_payload(row, config)
        if payload is None:
            diagnostics.append(
                f"skipped row with missing/non-finite fields: code={row.get('code')}"
            )
            continue
        candidate_payloads.append(payload)
        unit_key = (payload["code"], payload["model_type"])
        candidate_by_unit.setdefault(unit_key, []).append(payload)

    # ---------- Read existing combined period records ----------
    # Per output unit (code, model): read only for units not already blocked.
    existing_payloads_all: list[dict] = []

    for code, model_str in list(candidate_by_unit.keys()):
        if (code, model_str) in blocked_units:
            continue
        if time.monotonic() >= deadline:
            msg = f"deadline exceeded before existing read {code}/{model_str}"
            blocked_units[(code, model_str)] = msg
            diagnostics.append(msg)
            continue

        ex_status, ex_raw, ex_reason = data_reader.read_combined_period_forecasts_for_recovery(
            _client,
            [code],
            model_str,
            config.name,
            start_date,
            end_date,
            forecast_date,
            total_deadline=deadline,
        )
        if ex_status in (COMPLETE, EMPTY):
            if ex_status == COMPLETE and ex_raw is not None and not ex_raw.empty:
                existing_payloads_all.extend(ex_raw.to_dict("records"))
        else:
            blocked_units[(code, model_str)] = f"existing read {ex_status}: {ex_reason}"
            diagnostics.append(
                f"existing combined read {code}/{model_str}: {ex_status} — {ex_reason}"
            )

    # Build authorized_keys from candidates whose units are not blocked
    authorized_keys: set[tuple] = set()
    final_candidate_payloads: list[dict] = []
    for (code, model_str), payloads in candidate_by_unit.items():
        if (code, model_str) in blocked_units:
            for p in payloads:
                diagnostics.append(
                    f"candidate ({code}, {model_str}, {p['date']}) blocked: "
                    f"{blocked_units[(code, model_str)]}"
                )
            continue
        for p in payloads:
            key = (
                p["horizon_type"],
                p["code"],
                _canonical_model_key(p["model_type"]),
                p["date"],
                p["target"],
            )
            authorized_keys.add(key)
            final_candidate_payloads.append(p)

    # ---------- Unusable-output diagnostics (normal path) ----------
    _candidate_five_keys: set[tuple] = {
        (
            p["horizon_type"],
            p["code"],
            _canonical_model_key(p["model_type"]),
            p["date"],
            p["target"],
        )
        for p in final_candidate_payloads
    }
    _unusable_normal: dict = {}
    _enum_deadline_hit_normal = False
    if ml_models:
        for _iss in _iter_issue_dates(start_date, end_date):
            if not gap_detector._plan_is_valid_boundary(_iss, config.name):
                continue
            if time.monotonic() >= deadline:
                _enum_deadline_hit_normal = True
                diagnostics.append(
                    f"unusable-normal diagnostics blocked: deadline exceeded at {_iss}"
                )
                break
            _tgt = _iss + dt.timedelta(days=1)
            for _oc in sorted(output_allowlist):
                for _em in list(ml_models) + ["NE"]:
                    _can = _canonical_model_key(_em)
                    _fk = (config.name, _oc, _can, _iss, _tgt)
                    if _fk in _candidate_five_keys:
                        continue
                    if (_oc, _em) in blocked_units or (_oc, _can) in blocked_units:
                        continue
                    if _oc in virtual_config and virtual_helper_blocked:
                        if _em != "NE" and (_oc, _can) not in blocked_units:
                            blocked_units[(_oc, _can)] = (
                                f"unknown: virtual construction blocked for code {_oc}"
                            )
                        continue
                    if (
                        _em != "NE"
                        and _oc in virtual_config
                        and any(c in _unknown_src for c in virtual_config[_oc])
                    ):
                        if (_oc, _can) not in blocked_units:
                            blocked_units[(_oc, _can)] = (
                                f"unknown: virtual contributor source failed for code {_oc}"
                            )
                        continue
                    if _em == "NE":
                        _urea = "NE row absent from normalizer output"
                        _unusable_normal[_fk] = _urea
                    else:
                        if virtual_config_unknown:
                            if (_oc, _can) not in blocked_units:
                                blocked_units[(_oc, _can)] = (
                                    f"unknown: virtual config unreadable, cannot classify absence for code {_oc}"
                                )
                            continue
                        _sst = source_statuses.get((_oc, _em))
                        if _sst == EMPTY:
                            _urea = "confirmed empty DAY"
                        elif _sst == COMPLETE:
                            _urea = "complete DAY read but normalizer produced no eligible output"
                        elif _sst is None:
                            _urea = (
                                "virtual-derived output absent"
                                if _oc in virtual_config
                                else "source not read within bounds"
                            )
                        else:
                            _urea = f"source status: {_sst}"
                        _unusable_normal[_fk] = _urea
    if _unusable_normal:
        diagnostics.append(
            f"{len(_unusable_normal)} unusable unit(s): expected outputs absent from candidates"
        )

    # ---------- Call planner ----------
    # existing_read_status describes only the included (non-blocked) units.
    # Failed existing reads cause the unit to enter blocked_units and be excluded
    # from final_candidate_payloads, so the planner sees only successfully-read units.
    existing_read_status: str
    if existing_payloads_all:
        existing_read_status = "COMPLETE"
    else:
        existing_read_status = "CONFIRMED_EMPTY"

    plan = gap_detector.plan_period_refresh(
        existing_payloads_all,
        final_candidate_payloads,
        authorized_keys,
        set(),  # explicit_refresh_keys: P2 has no coordinated-refresh evidence
        start_date=start_date,
        end_date=end_date,
        forecast_date=forecast_date,
        existing_read_status=existing_read_status,
    )

    # Blocked units are kept in HorizonPreviewResult.blocked_units, not injected
    # into plan.blocked (which requires full 5-part keys with known issue/target dates).

    missing_count = len(plan.missing) if plan.plan_error is None else 0
    protected_count = len(plan.protected) if plan.plan_error is None else 0

    return HorizonPreviewResult(
        plan=plan,
        source_statuses=source_statuses,
        candidate_count=len(final_candidate_payloads),
        missing_count=missing_count,
        protected_count=protected_count,
        em_blocked_reason=em_blocked_reason,
        diagnostics=diagnostics,
        blocked_reason=(
            "deadline exceeded during unusable-output enumeration (incomplete)"
            if _enum_deadline_hit_normal
            else None
        ),
        blocked_units=blocked_units,
        candidate_payloads=final_candidate_payloads,
        unusable_units=_unusable_normal,
        planner_authorized_keys=set(authorized_keys),
    )


def _run_recovery_preview(
    prediction_mode: str,
    forecast_date: dt.date,
    start_date: dt.date,
    end_date: dt.date,
    *,
    _client: Any = None,
    _virtual_sources: dict[str, list[str]] | None = None,
) -> RecoveryPreviewResult:
    """Run read-only recovery preview for one or both horizons.

    Args:
        prediction_mode: "PENTAD", "DECAD", or "BOTH".
        forecast_date: Captured once at entry.
        start_date: Inclusive lower bound for issue dates.
        end_date: Inclusive upper bound for issue dates.
        _client: Injected SapphirePostprocessingClient (for tests).
        _virtual_sources: Injected virtual→contributors mapping (for tests).
    """
    # Input validation and API-disabled guard run before any client calls,
    # even when _client is injected, so tests cannot accidentally bypass them.
    if (
        not isinstance(forecast_date, dt.date)
        or not isinstance(start_date, dt.date)
        or not isinstance(end_date, dt.date)
    ):
        return RecoveryPreviewResult(
            forecast_date=forecast_date,
            start_date=start_date,
            end_date=end_date,
            horizons=[],
            horizon_results={},
            blocked_reason="invalid date arguments: forecast_date, start_date, end_date must be date objects",
        )
    if prediction_mode not in ("PENTAD", "DECAD", "BOTH"):
        return RecoveryPreviewResult(
            forecast_date=forecast_date,
            start_date=start_date,
            end_date=end_date,
            horizons=[],
            horizon_results={},
            blocked_reason=f"invalid prediction_mode {prediction_mode!r}: must be PENTAD, DECAD, or BOTH",
        )
    if start_date > end_date:
        return RecoveryPreviewResult(
            forecast_date=forecast_date,
            start_date=start_date,
            end_date=end_date,
            horizons=[],
            horizon_results={},
            blocked_reason=f"start_date {start_date} > end_date {end_date}",
        )
    if end_date > forecast_date:
        return RecoveryPreviewResult(
            forecast_date=forecast_date,
            start_date=start_date,
            end_date=end_date,
            horizons=[],
            horizon_results={},
            blocked_reason=f"end_date {end_date} > forecast_date {forecast_date}",
        )
    if os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "false":
        return RecoveryPreviewResult(
            forecast_date=forecast_date,
            start_date=start_date,
            end_date=end_date,
            horizons=[],
            horizon_results={},
            blocked_reason="SAPPHIRE_API_ENABLED=false",
        )

    # Capture start before any client construction; used for execution artifact.
    _started_at = time.monotonic()
    _destination_base_url: str | None = None
    _execution_eligible: bool = False

    client = _client
    client_owned = None

    if client is None:
        # Check before construction (map §5)
        if not data_reader.SAPPHIRE_API_AVAILABLE:
            return RecoveryPreviewResult(
                forecast_date=forecast_date,
                start_date=start_date,
                end_date=end_date,
                horizons=[],
                horizon_results={},
                blocked_reason="API client unavailable (sapphire-api-client not installed)",
            )
        api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
        try:
            from sapphire_api_client.postprocessing import (
                SapphirePostprocessingClient as _Client,
            )

            client = _Client(base_url=api_url, max_retries=1, timeout=30)
            # Disable redirects before any reads (execution-eligible transport).
            client.session.max_redirects = 0
            client_owned = client
            _destination_base_url = client.base_url
            _execution_eligible = True
        except Exception as exc:
            return RecoveryPreviewResult(
                forecast_date=forecast_date,
                start_date=start_date,
                end_date=end_date,
                horizons=[],
                horizon_results={},
                blocked_reason=f"client construction failed: {exc}",
            )

    try:
        deadline = _started_at + _RECOVERY_PREVIEW_DEADLINE_SECONDS

        horizons_to_run: list[ShortTermHorizonConfig] = []
        if prediction_mode in ("PENTAD", "BOTH"):
            horizons_to_run.append(PENTAD)
        if prediction_mode in ("DECAD", "BOTH"):
            horizons_to_run.append(DECAD)

        horizon_results: dict = {}
        for config in horizons_to_run:
            try:
                codes = _read_station_codes(config)
            except Exception as exc:
                horizon_results[config.name] = HorizonPreviewResult(
                    plan=None,
                    source_statuses={},
                    candidate_count=0,
                    missing_count=0,
                    protected_count=0,
                    em_blocked_reason=_EM_BLOCKED_REASON_SKILL,
                    diagnostics=[f"station codes read failed: {exc}"],
                    blocked_reason=str(exc),
                    blocked_units={},
                )
                continue

            hr = _run_recovery_preview_for_horizon(
                config,
                codes,
                forecast_date,
                start_date,
                end_date,
                deadline,
                _client=client,
                _virtual_sources=_virtual_sources,
            )
            horizon_results[config.name] = hr

        return RecoveryPreviewResult(
            forecast_date=forecast_date,
            start_date=start_date,
            end_date=end_date,
            horizons=[c.name for c in horizons_to_run],
            horizon_results=horizon_results,
            blocked_reason=None,
            destination_base_url=_destination_base_url,
            started_at=_started_at,
            preparation_deadline=deadline,
            execution_eligible=_execution_eligible,
        )
    finally:
        if client_owned is not None:
            with contextlib.suppress(Exception):
                client_owned.session.close()


def _execute_recovery_preview(
    preview: "RecoveryPreviewResult",
    *,
    approved_missing_keys: set,
    base_url: str,
    exclusion_confirmed: bool,
    batch_limit: int,
    write_cutoff: float,
    deadline: float,
    timeout: int,
    evidence_dir: str,
    _clock=None,
) -> dict:
    """Execute writes for an explicitly approved missing-only subset of a recovery preview.

    Validates provenance, approval scope, and timing before any writes.
    Performs fresh P1 reads + replanning per horizon before submitting.
    Reconciles all outcomes exactly once in a finally block.

    Returns a plain dict with keys:
      success, error, approved_keys, attempted_keys, verified_keys,
      unverified_keys, not_attempted_keys, blocked_diagnostics.

    Defaults must not authorize writes (no implicit approval, no normalization).
    exclusion_confirmed is an administrative attestation, not proof or lock.
    """
    import math as _math
    import os as _os
    import time as _time
    import uuid as _uuid

    if _clock is None:
        _clock = _time.monotonic

    try:
        from src.api_writer import (
            reconcile_recovery_batches as _p3_reconcile,
        )
        from src.api_writer import (
            select_recovery_records as _p3_select,
        )
        from src.api_writer import (
            submit_recovery_batch as _p3_submit,
        )
        from src.data_reader import (
            RECOVERY_STATUS_COMPLETE as _ST_COMPLETE,
        )
        from src.data_reader import (
            RECOVERY_STATUS_EMPTY as _ST_EMPTY,
        )
        from src.data_reader import (
            read_combined_period_forecasts_for_recovery as _p1_read,
        )
        from src.gap_detector import (
            plan_period_refresh as _plan_refresh,
        )
    except ImportError:
        from api_writer import (
            reconcile_recovery_batches as _p3_reconcile,
        )
        from api_writer import (  # type: ignore[no-redef]
            select_recovery_records as _p3_select,
        )
        from api_writer import (
            submit_recovery_batch as _p3_submit,
        )
        from data_reader import (
            RECOVERY_STATUS_COMPLETE as _ST_COMPLETE,
        )
        from data_reader import (
            RECOVERY_STATUS_EMPTY as _ST_EMPTY,
        )
        from data_reader import (  # type: ignore[no-redef]
            read_combined_period_forecasts_for_recovery as _p1_read,
        )
        from gap_detector import (  # type: ignore[no-redef]
            plan_period_refresh as _plan_refresh,
        )

    def _err(msg: str) -> dict:
        return {
            "success": False,
            "error": msg,
            "approved_keys": [],
            "attempted_keys": [],
            "verified_keys": [],
            "unverified_keys": [],
            "not_attempted_keys": [],
            "blocked_diagnostics": {},
        }

    # Wire model → canonical planner form (for key reconstruction from wire records).
    _WIRE_TO_CANON: dict[str, str] = {
        "TFT": "TFT",
        "TiDE": "TIDE",
        "TSMixer": "TSMIXER",
        "NE": "NE",
        "EM": "ENSEMBLE_MEAN",
    }
    # Canonical → API form accepted by read_combined_period_forecasts_for_recovery
    # (_resolve_recovery_model maps TIDE→TiDE, TSMIXER→TSMixer, etc.)
    _EXEC_MODELS: frozenset[str] = frozenset({"TFT", "TIDE", "TSMIXER", "NE"})
    _EXEC_HORIZONS: frozenset[str] = frozenset({"pentad", "decad"})

    # --- Validate exclusion attestation (exact bool True) ---
    if exclusion_confirmed is not True:
        return _err(f"exclusion_confirmed must be exactly True (bool), got {exclusion_confirmed!r}")

    # --- Validate preview type and provenance ---
    if not isinstance(preview, RecoveryPreviewResult):
        return _err("preview must be a RecoveryPreviewResult")
    if preview.blocked_reason is not None:
        return _err(f"preview is blocked: {preview.blocked_reason}")
    if not preview.execution_eligible:
        return _err(
            "preview is not execution_eligible: injected or non-native transport "
            "cannot be verified as a known destination"
        )

    # --- Validate base_url matches preview destination (exact, no normalization) ---
    if base_url != preview.destination_base_url:
        return _err(
            f"base_url {base_url!r} does not match "
            f"preview.destination_base_url {preview.destination_base_url!r}"
        )

    # --- Validate preview date types and bounds: start <= end <= forecast_date ---
    if isinstance(preview.forecast_date, dt.datetime) or not isinstance(
        preview.forecast_date, dt.date
    ):
        return _err("preview.forecast_date must be a date, not datetime")
    if isinstance(preview.start_date, dt.datetime) or not isinstance(preview.start_date, dt.date):
        return _err("preview.start_date must be a date, not datetime")
    if isinstance(preview.end_date, dt.datetime) or not isinstance(preview.end_date, dt.date):
        return _err("preview.end_date must be a date, not datetime")
    if preview.start_date > preview.end_date:
        return _err(f"preview.start_date {preview.start_date} > end_date {preview.end_date}")
    if preview.end_date > preview.forecast_date:
        return _err(f"preview.end_date {preview.end_date} > forecast_date {preview.forecast_date}")

    # --- Validate scalar parameters (ints not bools, positive) ---
    if not isinstance(batch_limit, int) or isinstance(batch_limit, bool) or batch_limit <= 0:
        return _err(f"batch_limit must be a positive int, got {batch_limit!r}")
    if not isinstance(timeout, int) or isinstance(timeout, bool) or timeout <= 0:
        return _err(f"timeout must be a positive int, got {timeout!r}")
    if (
        not isinstance(write_cutoff, (int, float))
        or isinstance(write_cutoff, bool)
        or not _math.isfinite(write_cutoff)
    ):
        return _err("write_cutoff must be a finite real number")
    if (
        not isinstance(deadline, (int, float))
        or isinstance(deadline, bool)
        or not _math.isfinite(deadline)
    ):
        return _err("deadline must be a finite real number")

    # --- Validate timing artifacts from preview ---
    if (
        preview.started_at is None
        or isinstance(preview.started_at, bool)
        or not isinstance(preview.started_at, (int, float))
        or not _math.isfinite(preview.started_at)
    ):
        return _err("preview.started_at is missing or not a finite real number")
    if (
        preview.preparation_deadline is None
        or isinstance(preview.preparation_deadline, bool)
        or not isinstance(preview.preparation_deadline, (int, float))
        or not _math.isfinite(preview.preparation_deadline)
    ):
        return _err("preview.preparation_deadline is missing or not a finite real number")
    if preview.started_at >= write_cutoff:
        return _err(
            f"preview.started_at {preview.started_at} must be < write_cutoff {write_cutoff}"
        )
    if write_cutoff >= deadline:
        return _err(f"write_cutoff {write_cutoff} must be < deadline {deadline}")
    if deadline > preview.preparation_deadline:
        return _err(
            f"deadline {deadline} exceeds preview.preparation_deadline "
            f"{preview.preparation_deadline}"
        )
    reserve = deadline - write_cutoff
    if reserve < timeout:
        return _err(
            f"reserve {reserve:.1f}s < timeout {timeout}s: "
            "write window too narrow; increase deadline or decrease timeout"
        )

    # --- Validate evidence_dir: not symlink BEFORE realpath, then must be a dir ---
    if not isinstance(evidence_dir, str) or not evidence_dir:
        return _err("evidence_dir must be a non-empty string")
    # Strip trailing separators so islink sees the final path component, not its
    # target; preserve filesystem root (e.g. "/" stays "/").
    _ev_sep = _os.sep + (_os.altsep or "")
    _ev_check = evidence_dir.rstrip(_ev_sep) or evidence_dir
    if _os.path.islink(_ev_check):
        return _err(f"evidence_dir {evidence_dir!r} must not be a symlink")
    if not _os.path.isdir(_ev_check):
        return _err(f"evidence_dir {evidence_dir!r} does not exist or is not a directory")
    evidence_dir_real = _os.path.realpath(_ev_check)

    # --- Validate approved_missing_keys type ---
    if not isinstance(approved_missing_keys, (set, frozenset)):
        return _err("approved_missing_keys must be a set")

    # --- Validate each key: exact canonical 5-tuple, no EM/LR, date not datetime ---
    for k in approved_missing_keys:
        if not isinstance(k, tuple) or len(k) != 5:
            return _err(f"approved key is not a 5-tuple: {k!r}")
        ht, code, model, date_v, target_v = k
        if ht not in _EXEC_HORIZONS:
            return _err(f"approved key horizon {ht!r} not in (pentad, decad)")
        if model not in _EXEC_MODELS:
            return _err(
                f"approved key model {model!r} not in (TFT, TIDE, TSMIXER, NE); "
                "EM and LR are blocked for missing-only execution"
            )
        if not isinstance(code, str) or not code:
            return _err(f"approved key code must be a non-empty str in key {k!r}")
        if code != code.strip():
            return _err(f"approved key code has leading/trailing whitespace: {code!r} in key {k!r}")
        if isinstance(date_v, dt.datetime) or not isinstance(date_v, dt.date):
            return _err(f"approved key date must be date not datetime: {date_v!r}")
        if isinstance(target_v, dt.datetime) or not isinstance(target_v, dt.date):
            return _err(f"approved key target must be date not datetime: {target_v!r}")

    # --- Empty approved set → zero effects, immediate success ---
    if not approved_missing_keys:
        _bd = {k: v for hr in preview.horizon_results.values() for k, v in hr.blocked_units.items()}
        return {
            "success": True,
            "error": None,
            "approved_keys": [],
            "attempted_keys": [],
            "verified_keys": [],
            "unverified_keys": [],
            "not_attempted_keys": [],
            "blocked_diagnostics": _bd,
        }

    # --- Pre-loop horizon integrity checks ---
    if len(preview.horizons) != len(set(preview.horizons)):
        return _err("preview.horizons contains duplicates")
    for k in approved_missing_keys:
        _kh = k[0]
        if _kh not in preview.horizons:
            return _err(f"approved key {k!r} horizon {_kh!r} is not in preview.horizons")
        if preview.horizon_results.get(_kh) is None:
            return _err(
                f"approved key {k!r} horizon {_kh!r} has no corresponding "
                "HorizonPreviewResult in preview.horizon_results"
            )

    # --- Validate each approved key against planner dispositions ---
    all_missing_union: set[tuple] = set()
    all_blocked_in_plan: set[tuple] = set()
    all_protected_union: set[tuple] = set()
    all_refresh_union: set[tuple] = set()

    for hr in preview.horizon_results.values():
        if hr.plan is not None and hr.plan.plan_error is None:
            all_missing_union |= hr.plan.missing
            all_blocked_in_plan |= set(hr.plan.blocked.keys())
            all_protected_union |= hr.plan.protected
            all_refresh_union |= {k for k, _ in hr.plan.authorized_refresh}

    for k in approved_missing_keys:
        if k in all_refresh_union:
            return _err(
                f"approved key {k!r} is in plan.authorized_refresh — "
                "not allowed for missing-only execution; "
                "authorized_refresh keys require a separate refresh workflow"
            )
        if k not in all_missing_union:
            return _err(
                f"approved key {k!r} is not in any horizon plan.missing; "
                "only missing-disposition keys may be approved for execution"
            )
        if k in all_blocked_in_plan:
            return _err(f"approved key {k!r} is in plan.blocked")
        if k in all_protected_union:
            return _err(f"approved key {k!r} is in plan.protected")
        # Per-HR checks: key must be in planner_authorized_keys; HR must not be
        # blocked; the specific (code, model) must not appear in blocked_units.
        # Unrelated blocked units (other codes/models) remain in diagnostics only.
        _hr_for_k = preview.horizon_results.get(k[0])
        if _hr_for_k is not None:
            if k not in (_hr_for_k.planner_authorized_keys or set()):
                return _err(
                    f"approved key {k!r} not in hr.planner_authorized_keys for horizon {k[0]!r}"
                )
            if _hr_for_k.plan is None or _hr_for_k.plan.plan_error is not None:
                return _err(f"approved key {k!r}: horizon {k[0]!r} plan is unavailable")
            if k not in _hr_for_k.plan.missing:
                return _err(f"approved key {k!r} is not in hr.plan.missing for horizon {k[0]!r}")
            if _hr_for_k.blocked_reason is not None:
                return _err(
                    f"approved key {k!r}: horizon {k[0]!r} has "
                    f"blocked_reason {_hr_for_k.blocked_reason!r}"
                )
            _bu = _hr_for_k.blocked_units or {}
            if any(
                bc == k[1] and (bm == k[2] or _canonical_model_key(bm) == k[2]) for bc, bm in _bu
            ):
                return _err(f"approved key {k!r}: (code, model) is in hr.blocked_units")

    # Collect unrelated blocked diagnostics (preserved independently).
    blocked_diagnostics = {
        k: v for hr in preview.horizon_results.values() for k, v in hr.blocked_units.items()
    }

    # --- Pre-execution clock guard: reject before any effects if write window expired ---
    if _clock() >= write_cutoff:
        return _err(
            f"write_cutoff already expired at execution entry "
            f"(clock={_clock():.3f} >= write_cutoff={write_cutoff})"
        )

    # --- Group approved keys by horizon ---
    keys_by_horizon: dict[str, set[tuple]] = {}
    for k in approved_missing_keys:
        keys_by_horizon.setdefault(k[0], set()).add(k)

    # Structured error for post-validation failures (approved set known; nothing attempted).
    def _scope_err(msg: str, attempted: "set | None" = None) -> dict:
        _att = set() if attempted is None else attempted
        _not_att = set(approved_missing_keys) - _att
        return {
            "success": False,
            "error": msg,
            "approved_keys": sorted(approved_missing_keys, key=str),
            "attempted_keys": sorted(_att, key=str),
            "verified_keys": [],
            "unverified_keys": sorted(_att, key=str),
            "not_attempted_keys": sorted(_not_att, key=str),
            "blocked_diagnostics": blocked_diagnostics,
        }

    # =========================================================================
    # PASS 1: Preflight — select + freeze + partition ALL horizons before any POST.
    # If any horizon's preflight fails, zero POSTs are made.
    # =========================================================================
    frozen_batches: list[dict] = []
    _preflight_stop: str | None = None

    for hn in preview.horizons:
        if hn not in keys_by_horizon:
            continue

        horizon_approved = keys_by_horizon[hn]
        hr = preview.horizon_results.get(hn)
        if hr is None or hr.plan is None or hr.plan.plan_error is not None:
            _preflight_stop = f"horizon {hn} plan unavailable for execution"
            break

        # Select wire records using the full original planner allowlist.
        try:
            _all_wire = _p3_select(
                hr.candidate_payloads,
                hr.plan,
                hr.planner_authorized_keys,
                start_date=preview.start_date,
                end_date=preview.end_date,
                forecast_date=preview.forecast_date,
            )
        except Exception as _sel_exc:
            _preflight_stop = f"select_recovery_records (original) failed for {hn}: {_sel_exc}"
            break

        # Filter to approved subset; verify every record's key.
        _frozen_wire: list[dict] = []
        _selected_keys: set[tuple] = set()
        _wire_ok = True
        for r in _all_wire:
            try:
                _r_ht = "decad" if r.get("horizon_type") == "decade" else r.get("horizon_type", "")
                _r_model = _WIRE_TO_CANON.get(r.get("model_type", ""), r.get("model_type", ""))
                _r_date = dt.date.fromisoformat(r["date"])
                _r_target = dt.date.fromisoformat(r["target"])
                _r_key = (_r_ht, r.get("code", ""), _r_model, _r_date, _r_target)
            except Exception as _kex:
                _preflight_stop = f"key reconstruction from wire record failed for {hn}: {_kex}"
                _wire_ok = False
                break
            if _r_key in horizon_approved:
                _frozen_wire.append(r)
                _selected_keys.add(_r_key)
            elif _r_key not in (hr.planner_authorized_keys or set()):
                _preflight_stop = (
                    f"select returned unapproved/malformed candidate for {hn}: {_r_key!r}"
                )
                _wire_ok = False
                break
        if not _wire_ok:
            break

        # Selected keys must equal approved exactly (no missing, no extra).
        if _selected_keys != horizon_approved:
            _preflight_stop = (
                f"selected keys for {hn} ({len(_selected_keys)}) do not equal "
                f"approved keys ({len(horizon_approved)}); "
                "expected candidate absent from original plan — aborting"
            )
            break

        # Partition frozen_wire into stable chunks of ≤batch_limit.
        _sorted_wire = sorted(
            _frozen_wire,
            key=lambda r: (r.get("code", ""), r.get("model_type", ""), r.get("date", "")),
        )
        _part_ok = True
        for _bi in range(0, len(_sorted_wire), batch_limit):
            _chunk = _sorted_wire[_bi : _bi + batch_limit]
            _chunk_keys: set[tuple] = set()
            for r in _chunk:
                try:
                    _r_ht = (
                        "decad" if r.get("horizon_type") == "decade" else r.get("horizon_type", "")
                    )
                    _r_model = _WIRE_TO_CANON.get(r.get("model_type", ""), r.get("model_type", ""))
                    _r_date = dt.date.fromisoformat(r["date"])
                    _r_target = dt.date.fromisoformat(r["target"])
                    _chunk_keys.add((_r_ht, r.get("code", ""), _r_model, _r_date, _r_target))
                except Exception as _ck_exc:
                    _preflight_stop = f"key reconstruction in partition failed for {hn}: {_ck_exc}"
                    _part_ok = False
                    break
            if not _part_ok:
                break

            _batch_issue_dates = [k[3] for k in _chunk_keys]
            _batch_start = min(_batch_issue_dates)
            _batch_end = max(_batch_issue_dates)
            _combos: dict[tuple, tuple[dt.date, dt.date]] = {}
            for _k in _chunk_keys:
                _km = (_k[1], _k[2])
                if _km not in _combos:
                    _combos[_km] = (_k[3], _k[3])
                else:
                    _old_min, _old_max = _combos[_km]
                    _combos[_km] = (min(_old_min, _k[3]), max(_old_max, _k[3]))

            frozen_batches.append(
                {
                    "hn": hn,
                    "frozen_wire": _chunk,
                    "batch_keys": _chunk_keys,
                    "batch_start": _batch_start,
                    "batch_end": _batch_end,
                    "combos": _combos,
                }
            )

        if not _part_ok:
            break

    if _preflight_stop:
        return _scope_err(f"preflight failed: {_preflight_stop}")

    # --- Batch-keys union must equal approved_missing_keys exactly. ---
    _all_batch_keys: set[tuple] = set()
    for _fb in frozen_batches:
        _all_batch_keys |= _fb["batch_keys"]
    if _all_batch_keys != set(approved_missing_keys):
        return _scope_err(
            f"batch_keys union ({len(_all_batch_keys)}) does not equal "
            f"approved_missing_keys ({len(set(approved_missing_keys))}): "
            "preflight scope mismatch — not attempted"
        )

    # =========================================================================
    # PASS 2: Execute each frozen batch with a fresh P1 read per batch.
    # Real try/except/finally guarantees reconcile runs for all accumulated outcomes.
    # =========================================================================
    all_outcomes: list[dict] = []
    stop_reason: str | None = None
    _reconcile_result: dict | None = None
    _reconcile_error: str | None = None

    try:
        for _batch in frozen_batches:
            if stop_reason is not None:
                break

            hn = _batch["hn"]
            _batch_keys: set[tuple] = _batch["batch_keys"]
            _batch_start: dt.date = _batch["batch_start"]
            _batch_end: dt.date = _batch["batch_end"]
            _combos = _batch["combos"]

            # Clock guard before each batch's fresh reads.
            if _clock() >= write_cutoff:
                stop_reason = (
                    f"write_cutoff exceeded before fresh reads for batch {hn}/{_batch_start}"
                )
                break

            # --- Fresh P1 reads: isolated client, retry=1, redirect=0. ---
            _fresh_client = None
            _fresh_existing: list[dict] = []
            _fresh_read_ok = True

            try:
                try:
                    from sapphire_api_client.postprocessing import (
                        SapphirePostprocessingClient as _FreshClientCls,
                    )

                    _fresh_client = _FreshClientCls(
                        base_url=base_url,
                        auth_token=None,
                        max_retries=1,
                        timeout=timeout,
                    )
                    _fresh_client.session.max_redirects = 0
                except Exception as _fc_exc:
                    stop_reason = f"fresh read client construction failed for {hn}: {_fc_exc}"
                    _fresh_read_ok = False

                if _fresh_read_ok:
                    for (_fc_code, _fc_canon), (_fc_dmin, _fc_dmax) in sorted(_combos.items()):
                        if _clock() >= write_cutoff:
                            stop_reason = (
                                f"write_cutoff exceeded during fresh reads for {hn}/{_batch_start}"
                            )
                            _fresh_read_ok = False
                            break
                        _fc_status, _fc_raw, _fc_reason = _p1_read(
                            _fresh_client,
                            [_fc_code],
                            _fc_canon,
                            hn,
                            _fc_dmin,
                            _fc_dmax,
                            preview.forecast_date,
                            total_deadline=write_cutoff,
                            _clock=_clock,
                        )
                        if _fc_status not in (_ST_COMPLETE, _ST_EMPTY):
                            stop_reason = (
                                f"fresh read {hn}/{_fc_code}/{_fc_canon} "
                                f"returned {_fc_status}: {_fc_reason}"
                            )
                            _fresh_read_ok = False
                            break
                        if _fc_status == _ST_COMPLETE and _fc_raw is not None and not _fc_raw.empty:
                            _fresh_existing.extend(_fc_raw.to_dict("records"))
            finally:
                if _fresh_client is not None:
                    with contextlib.suppress(Exception):
                        _fresh_client.session.close()

            if not _fresh_read_ok:
                break

            # Replan frozen batch against fresh records (explicit_refresh_keys=∅).
            _fresh_read_status = "COMPLETE" if _fresh_existing else "CONFIRMED_EMPTY"
            try:
                _fresh_plan = _plan_refresh(
                    _fresh_existing,
                    list(_batch["frozen_wire"]),
                    _batch_keys,
                    set(),
                    start_date=_batch_start,
                    end_date=_batch_end,
                    forecast_date=preview.forecast_date,
                    existing_read_status=_fresh_read_status,
                )
            except Exception as _rp_exc:
                stop_reason = f"fresh replan failed for {hn}: {_rp_exc}"
                break

            if _fresh_plan.plan_error is not None:
                stop_reason = f"fresh replan plan_error for {hn}: {_fresh_plan.plan_error}"
                break

            # All batch keys must STILL be missing (existing/identical/conflict stops).
            _not_still_missing = _batch_keys - _fresh_plan.missing
            if _not_still_missing:
                stop_reason = (
                    f"{len(_not_still_missing)} approved key(s) for {hn} are no longer "
                    "missing after fresh read (existing, identical, or conflict) — stop"
                )
                break

            # Fresh private evidence filename (P3 enforces O_CREAT|O_EXCL).
            _ev_name = f"recovery_{hn}_{_uuid.uuid4().hex}.json"
            _ev_path = _os.path.join(evidence_dir_real, _ev_name)

            # Conservative pending before P3: ensures every possible attempt is
            # accounted for even when submission raises or is interrupted.
            # Ordinary exception leaves pending in place; finally reconciles it.
            # KeyboardInterrupt/SystemExit propagate naturally through finally.
            _pending_outcome = {
                "attempted": True,
                "transport_status": "uncertain",
                "acknowledged_count": None,
                "attempted_records": list(_batch["frozen_wire"]),
                "evidence_path": _ev_path,
                "error": "submission outcome pending/unknown",
            }
            _pending_idx = len(all_outcomes)
            all_outcomes.append(_pending_outcome)

            _outcome = _p3_submit(
                list(_batch["frozen_wire"]),
                authorized_keys=_batch_keys,
                start_date=_batch_start,
                end_date=_batch_end,
                forecast_date=preview.forecast_date,
                base_url=base_url,
                batch_limit=batch_limit,
                write_cutoff=write_cutoff,
                deadline=deadline,
                evidence_path=_ev_path,
                timeout=timeout,
                _clock=_clock,
            )
            all_outcomes[_pending_idx] = _outcome

            # Stop on non-acknowledged transport (no resend).
            _ts = _outcome.get("transport_status", "unknown")
            if _ts != "acknowledged":
                stop_reason = (
                    f"batch for {hn}/{_batch_start} not acknowledged: "
                    f"transport_status={_ts!r}, error={_outcome.get('error')!r}"
                )

    except Exception as _loop_exc:
        # Unexpected error after prior outcomes; retain them for reconcile.
        stop_reason = (
            f"unexpected error during batch execution "
            f"(prior outcomes retained, outcome of current batch uncertain): {_loop_exc}"
        )

    finally:
        # --- Reconcile ALL accumulated outcomes exactly once. ---
        if all_outcomes:
            try:
                _reconcile_result = _p3_reconcile(
                    all_outcomes,
                    authorized_keys=set(approved_missing_keys),
                    start_date=preview.start_date,
                    end_date=preview.end_date,
                    forecast_date=preview.forecast_date,
                    base_url=base_url,
                    deadline=deadline,
                    timeout=timeout,
                    _clock=_clock,
                )
            except Exception as _rec_exc:
                _reconcile_error = str(_rec_exc)

    # --- Derive attempted keys independently from outcomes (not from plan). ---
    _attempted_keys: set[tuple] = set()
    for _oc in all_outcomes:
        if not isinstance(_oc, dict) or not _oc.get("attempted"):
            continue
        for _wr in _oc.get("attempted_records", []):
            try:
                _wr_ht = (
                    "decad" if _wr.get("horizon_type") == "decade" else _wr.get("horizon_type", "")
                )
                _wr_model = _WIRE_TO_CANON.get(_wr.get("model_type", ""), _wr.get("model_type", ""))
                _wr_date = dt.date.fromisoformat(_wr["date"])
                _wr_target = dt.date.fromisoformat(_wr["target"])
                _attempted_keys.add((_wr_ht, _wr.get("code", ""), _wr_model, _wr_date, _wr_target))
            except Exception:
                pass

    # --- Derive transport uncertainty from outcomes as well as readback. ---
    _outcome_uncertain = any(
        isinstance(_oc, dict) and _oc.get("transport_status") == "uncertain" for _oc in all_outcomes
    )

    # --- Build verified/unverified partition of attempted keys. ---
    _verified_keys: set[tuple] = set()
    _unverified_keys: set[tuple] = set()

    if _reconcile_error is not None:
        # Reconcile exception → all attempted keys unverified.
        _unverified_keys = set(_attempted_keys)
    elif _reconcile_result is not None:
        _r_error = _reconcile_result.get("error")
        _r_verified_raw = _reconcile_result.get("verified_keys") or []
        _r_unverified_raw = _reconcile_result.get("unverified_keys") or []
        _r_verified_set = set(_r_verified_raw)
        _r_unverified_set = set(_r_unverified_raw)
        if _r_error and not _r_verified_raw and not _r_unverified_raw:
            # Structured error with empty sets → all attempted unverified; retain reason.
            _unverified_keys = set(_attempted_keys)
        else:
            # Use per-key sets from readback even when overall status is "unverified".
            _verified_keys = _r_verified_set & _attempted_keys
            _r_accounted = _r_verified_set | _r_unverified_set
            _omitted = _attempted_keys - _r_accounted
            _unverified_keys = (_r_unverified_set & _attempted_keys) | _omitted
            # Enforce disjoint partition; overlap moves to unverified.
            _overlap = _verified_keys & _unverified_keys
            if _overlap:
                _verified_keys -= _overlap
                _unverified_keys |= _overlap

    _not_attempted_keys = set(approved_missing_keys) - _attempted_keys

    # Partition invariant: verified ∪ unverified == attempted (disjoint).
    assert _verified_keys.isdisjoint(_unverified_keys), "partition invariant violated"
    assert (_verified_keys | _unverified_keys) == _attempted_keys, "partition invariant violated"

    # --- Determine success. ---
    _has_uncertainty = _outcome_uncertain or bool(
        _reconcile_result is not None and _reconcile_result.get("original_transport_uncertain")
    )
    _success = (
        stop_reason is None
        and not _not_attempted_keys
        and _verified_keys == set(approved_missing_keys)
        and not _unverified_keys
        and not _has_uncertainty
        and _reconcile_error is None
        and _reconcile_result is not None
        and _reconcile_result.get("reconciliation_status") == "verified"
        and not _reconcile_result.get("error")
    )

    _final_error: str | None = None
    if not _success:
        _parts: list[str] = []
        if stop_reason:
            _parts.append(stop_reason)
        if _not_attempted_keys:
            _parts.append(f"{len(_not_attempted_keys)} key(s) not attempted")
        if _unverified_keys:
            _parts.append(f"{len(_unverified_keys)} key(s) unverified after readback")
        if _has_uncertainty:
            _parts.append(
                "original transport uncertainty requires operator settlement "
                "(readback alone is insufficient)"
            )
        if _reconcile_error:
            _parts.append(f"reconcile error: {_reconcile_error}")
        if _reconcile_result is None and not _reconcile_error:
            _parts.append("reconcile result absent")
        if _reconcile_result is not None:
            _r_rec_status = _reconcile_result.get("reconciliation_status")
            if _r_rec_status != "verified":
                _parts.append(f"reconciliation_status={_r_rec_status!r} (not verified)")
            _r_rec_err = _reconcile_result.get("error")
            if _r_rec_err and not _reconcile_error:
                _parts.append(f"reconcile result error: {_r_rec_err}")
        _final_error = "; ".join(_parts) if _parts else "unknown failure"

    return {
        "success": _success,
        "error": _final_error,
        "approved_keys": sorted(approved_missing_keys, key=str),
        "attempted_keys": sorted(_attempted_keys, key=str),
        "verified_keys": sorted(_verified_keys, key=str),
        "unverified_keys": sorted(_unverified_keys, key=str),
        "not_attempted_keys": sorted(_not_attempted_keys, key=str),
        "blocked_diagnostics": blocked_diagnostics,
    }


def _iter_issue_dates(start: dt.date, end: dt.date):
    """Yield each date in [start, end] for unusable-unit enumeration."""
    d = start
    while d <= end:
        yield d
        if d >= end:
            break
        d += dt.timedelta(days=1)


def _read_station_codes(config):
    """Read station codes from the horizon's station selection config file.

    Args:
        config: ShortTermHorizonConfig with station_selection_env field.

    Returns:
        List of station code strings.
    """
    config_path = os.path.join(
        os.getenv("ieasyforecast_configuration_path", ""),
        os.getenv(config.station_selection_env, ""),
    )
    with open(config_path) as f:
        station_config = json.load(f)
    codes = [str(c) for c in station_config.get("stationsID", [])]
    logger.info("Read %d station codes for %s", len(codes), config.name)
    return codes


def postprocessing_maintenance():
    global timing_stats

    logger.info("\n\n====== Post-processing forecasts (MAINTENANCE / GAP-FILL) =====")
    logger.debug(f"Script started at {dt.datetime.now()}.")

    errors = []

    # Deprecation: warn if the old env var is still set
    if os.getenv("POSTPROCESSING_GAPFILL_WINDOW_DAYS"):
        logger.warning(
            "POSTPROCESSING_GAPFILL_WINDOW_DAYS is deprecated. "
            "Use POSTPROCESSING_GAPFILL_MAX_MONTHS instead."
        )

    max_lookback_months = int(os.getenv("POSTPROCESSING_GAPFILL_MAX_MONTHS", "13"))
    logger.info(f"Gap-fill lookback window: {max_lookback_months} months")

    with timer(timing_stats, "total execution"):
        with timer(timing_stats, "setup"):
            logger.info("\n\n------ Setting up --------------------------------")
            sl.load_environment()

        # ---- Recovery mode routing (must return/exit on ALL paths) ----
        _recovery_mode = (os.getenv("POSTPROCESSING_RECOVERY_MODE", "") or "off").lower()
        if _recovery_mode != "off":
            if _recovery_mode == "execute":
                logger.error(
                    "POSTPROCESSING_RECOVERY_MODE=execute is not available in this "
                    "release. Use POSTPROCESSING_RECOVERY_MODE=preview first."
                )
                sys.exit(1)
            if _recovery_mode != "preview":
                logger.error(
                    "Unknown POSTPROCESSING_RECOVERY_MODE=%r. Valid values: off, preview, execute.",
                    _recovery_mode,
                )
                sys.exit(1)

            _preview_mode = (os.getenv("SAPPHIRE_PREDICTION_MODE", "") or "BOTH").upper()
            if _preview_mode not in ("PENTAD", "DECAD", "BOTH"):
                logger.error(
                    "Invalid SAPPHIRE_PREDICTION_MODE=%r for recovery preview.",
                    _preview_mode,
                )
                sys.exit(1)

            _start_str = os.getenv("POSTPROCESSING_RECOVERY_START_DATE", "")
            _end_str = os.getenv("POSTPROCESSING_RECOVERY_END_DATE", "")
            if not _start_str or not _end_str:
                logger.error(
                    "POSTPROCESSING_RECOVERY_START_DATE and "
                    "POSTPROCESSING_RECOVERY_END_DATE are required for recovery preview."
                )
                sys.exit(1)

            for _ds, _dv in (
                ("POSTPROCESSING_RECOVERY_START_DATE", _start_str),
                ("POSTPROCESSING_RECOVERY_END_DATE", _end_str),
            ):
                if "W" in _dv.upper() or "-" not in _dv:
                    logger.error(
                        "%s=%r: must be YYYY-MM-DD (compact and ISO week formats are not accepted)",
                        _ds,
                        _dv,
                    )
                    sys.exit(1)
            try:
                _start_date = dt.date.fromisoformat(_start_str)
                _end_date = dt.date.fromisoformat(_end_str)
            except ValueError as exc:
                logger.error("Invalid recovery date bounds: %s", exc)
                sys.exit(1)

            try:
                _forecast_date = _capture_forecast_date()
            except ValueError as exc:
                logger.error("Invalid SAPPHIRE_FORECAST_DATE: %s", exc)
                sys.exit(1)

            if _start_date > _end_date:
                logger.error(
                    "POSTPROCESSING_RECOVERY_START_DATE %s > END_DATE %s",
                    _start_date,
                    _end_date,
                )
                sys.exit(1)
            if _end_date > _forecast_date:
                logger.error(
                    "POSTPROCESSING_RECOVERY_END_DATE %s > forecast_date %s",
                    _end_date,
                    _forecast_date,
                )
                sys.exit(1)

            logger.info(
                "Recovery preview: mode=%s, start=%s, end=%s, forecast_date=%s",
                _preview_mode,
                _start_date,
                _end_date,
                _forecast_date,
            )

            try:
                _result = _run_recovery_preview(
                    _preview_mode, _forecast_date, _start_date, _end_date
                )
            except Exception as _preview_exc:
                logger.error("Recovery preview unexpected failure: %s", _preview_exc)
                sys.exit(1)

            if _result.blocked_reason:
                logger.error("Recovery preview blocked: %s", _result.blocked_reason)
                sys.exit(1)

            for _hn, _hr in _result.horizon_results.items():
                _hlabel = _hn.upper()
                if _hr.blocked_reason:
                    logger.warning("Recovery preview %s blocked: %s", _hlabel, _hr.blocked_reason)
                else:
                    _plan = _hr.plan
                    if _plan is not None and _plan.plan_error is None:
                        logger.info(
                            "Recovery preview %s: %d candidates, %d missing, "
                            "%d protected, %d authorized_refresh, "
                            "%d excluded, %d planner-blocked, %d source-blocked",
                            _hlabel,
                            _hr.candidate_count,
                            _hr.missing_count,
                            _hr.protected_count,
                            len(_plan.authorized_refresh),
                            len(_plan.excluded),
                            len(_plan.blocked),
                            len(_hr.blocked_units),
                        )
                    else:
                        logger.warning(
                            "Recovery preview %s: %d candidates (plan error: %s)",
                            _hlabel,
                            _hr.candidate_count,
                            _plan.plan_error if _plan else "no plan",
                        )
                if _hr.em_blocked_reason:
                    logger.info(
                        "Recovery preview %s EM: blocked (%s)",
                        _hlabel,
                        _hr.em_blocked_reason,
                    )
                if _hr.diagnostics:
                    logger.info(
                        "Recovery preview %s: %d diagnostic(s)",
                        _hlabel,
                        len(_hr.diagnostics),
                    )
                if _hr.unusable_units:
                    logger.info(
                        "Recovery preview %s: %d unusable unit(s) "
                        "(expected outputs absent, not source-blocked)",
                        _hlabel,
                        len(_hr.unusable_units),
                    )

            _has_issues = any(
                hr.blocked_reason
                or hr.blocked_units
                or hr.unusable_units
                or (hr.plan is not None and hr.plan.plan_error is not None)
                for hr in _result.horizon_results.values()
            )
            if _has_issues:
                _n_incomplete = sum(
                    1
                    for hr in _result.horizon_results.values()
                    if hr.blocked_reason
                    or hr.blocked_units
                    or hr.unusable_units
                    or (hr.plan is not None and hr.plan.plan_error is not None)
                )
                logger.warning(
                    "Recovery preview incomplete: %d horizon(s) with blocked units "
                    "or plan errors. Advisory only — no data written.",
                    _n_incomplete,
                )
                sys.exit(1)
            logger.info("Recovery preview complete. Advisory only — no data written.")
            sys.exit(0)
        # ---- end recovery mode routing ----

        prediction_mode = os.getenv("SAPPHIRE_PREDICTION_MODE", "") or "BOTH"
        if prediction_mode not in ["PENTAD", "DECAD", "BOTH"]:
            logger.error(
                f"Invalid SAPPHIRE_PREDICTION_MODE: {prediction_mode}. "
                f"Expected 'PENTAD', 'DECAD', or 'BOTH'."
            )
            sys.exit(1)
        logger.info(f"Running maintenance postprocessing for mode: {prediction_mode}")

        if prediction_mode in ["PENTAD", "BOTH"]:
            codes = _read_station_codes(PENTAD)
            _fill_gaps_for_horizon(PENTAD, max_lookback_months, errors, codes=codes)

        if prediction_mode in ["DECAD", "BOTH"]:
            codes = _read_station_codes(DECAD)
            _fill_gaps_for_horizon(DECAD, max_lookback_months, errors, codes=codes)

    # Print timing summary
    summary, total = timing_stats.summary()
    logger.info("\n\n")
    logger.info("Timing summary for postprocessing_maintenance:")
    logger.info(f"Total execution time: {total:.2f} seconds")
    logger.info("Breakdown by section:")
    for entry in summary:
        logger.info(f"{entry['section']}:")
        logger.info(f"  Total time: {entry['total_time']:.2f} seconds ({entry['percentage']:.1f}%)")
        logger.info(f"  Average time per call: {entry['avg_time']:.2f} seconds")
        logger.info(f"  Number of calls: {entry['calls']}")

    if errors:
        logger.error(f"Script finished with {len(errors)} error(s):")
        for error in errors:
            logger.error(f"  - {error}")
        sys.exit(1)
    else:
        logger.info(f"Script finished successfully at {dt.datetime.now()}.")
        sys.exit(0)


def _fill_gaps_for_horizon(config, max_lookback_months, errors, codes: list[str] | None = None):
    """Detect and fill ensemble gaps for one horizon type.

    New flow (PP-021):
    1. Read combined forecasts (cheap).
    2. Detect EM gaps using combined only — no expensive modelled read.
    3. Detect stale individual-model / NE records (q05 IS NULL).
    4. Detect stale EM records (q05 IS NULL for EM rows).
    5. Early exit if nothing to do.
    6. Read modelled data scoped to affected dates only.
    7. Create NE + EM rows with quantiles.
    8. Merge and save.
    """
    global timing_stats

    label = config.name.upper()

    # Step 1: read what we already have (cheap)
    with timer(timing_stats, f"reading {label} combined forecasts"):
        logger.info(f"\n\n------ Reading {label} combined forecasts for gap detection ----")
        combined = data_reader.read_combined_forecasts(config.name, codes=codes)

    if combined.empty:
        logger.info(f"No {label} combined data found. Skipping gap detection.")
        return

    # Step 2–4: detect gaps and stale records (all cheap, in-memory)
    with timer(timing_stats, f"detecting {label} gaps"):
        # EM + NE gaps (missing rows)
        all_gaps = gap_detector.detect_missing_ensembles(
            combined,
            max_lookback_months=max_lookback_months,
            ensemble_models={"EM", "NE"},
            horizon_type=config.name,
        )
        ne_gaps = all_gaps[all_gaps["model_short"] == "NE"]
        if not ne_gaps.empty:
            logger.info(
                "Found %d NE gaps within lookback window. "
                "NE rows will be re-created from individual-model data.",
                len(ne_gaps),
            )
        em_gaps = all_gaps[all_gaps["model_short"] == "EM"].reset_index(drop=True)

        # Stale individual-model / NE rows (have discharge, no quantiles)
        stale = gap_detector.detect_stale_quantiles(
            combined,
            max_lookback_months=max_lookback_months,
            horizon_type=config.name,
        )

        # Stale EM rows (have discharge, no quantiles)
        stale_em = pd.DataFrame(columns=["date", "code"])
        if "q05" in combined.columns:
            stale_em = (
                combined[
                    (combined["model_short"] == "EM")
                    & combined["forecasted_discharge"].notna()
                    & combined["q05"].isna()
                ][["date", "code"]]
                .drop_duplicates()
                .reset_index(drop=True)
            )

    # Step 5: early exit if nothing to do
    em_gap_dates = set(pd.to_datetime(em_gaps["date"]).unique()) if not em_gaps.empty else set()
    stale_dates = set(pd.to_datetime(stale["date"]).unique()) if not stale.empty else set()
    stale_em_dates = set(pd.to_datetime(stale_em["date"]).unique()) if not stale_em.empty else set()
    ne_gap_dates = set(pd.to_datetime(ne_gaps["date"]).unique()) if not ne_gaps.empty else set()
    all_affected = em_gap_dates | stale_dates | stale_em_dates | ne_gap_dates

    logger.info(
        "%s: %d EM gaps, %d NE gaps, %d stale individual/NE records, "
        "%d stale EM records → %d total affected dates",
        label,
        len(em_gaps),
        len(ne_gaps),
        len(stale),
        len(stale_em),
        len(all_affected),
    )

    if not all_affected:
        logger.info(f"No {label} gaps or stale records found. Nothing to do.")
        return

    # Compute gap codes BEFORE data read so we can scope the API query
    gap_codes: set[str] = set()
    for df_check in [em_gaps, ne_gaps, stale, stale_em]:
        if not df_check.empty and "code" in df_check.columns:
            gap_codes.update(df_check["code"].unique())

    # Step 6: read modelled data scoped to affected dates and codes
    affected_dates = sorted(all_affected)
    with timer(timing_stats, f"reading {label} data for gap-fill"):
        logger.info(
            "\n\n------ Reading %s modelled data for %d affected date(s) ----",
            label,
            len(affected_dates),
        )
        modelled, _ = data_reader.read_individual_model_forecasts_for_dates(
            config.name,
            affected_dates,
            codes=list(gap_codes) if gap_codes else None,
        )
        if modelled.empty:
            logger.warning(
                "No %s modelled data for %d affected date(s) and %d gap code(s). Cannot fill gaps.",
                label,
                len(affected_dates),
                len(gap_codes),
            )
            return
        modelled = sl.calculate_virtual_stations_data(modelled)
        modelled = config.neural_ensemble_func(modelled)

    if modelled.empty:
        logger.warning(f"No {label} modelled data for affected dates. Cannot fill gaps.")
        return

    modelled_filtered = modelled[
        modelled["date"].isin(all_affected) & modelled["code"].isin(gap_codes)
    ].copy()

    if modelled_filtered.empty:
        logger.warning(f"No {label} forecast data for affected dates/codes. Cannot fill gaps.")
        return

    with timer(timing_stats, f"reading {label} skill metrics"):
        skill_stats = data_reader.read_skill_metrics(config.name, codes=codes)

    # Step 7: build the set of refreshed rows to merge back into combined.
    # Only include rows that are actually stale/gap-fill — not all of
    # modelled_filtered, which would duplicate non-stale individual rows.
    refresh_parts: list[pd.DataFrame] = []
    stale_keys: set[tuple] = set()

    # 7a: Refreshed stale individual/NE rows (from freshly-read modelled data)
    if not stale.empty:
        stale_keys = set(
            zip(
                pd.to_datetime(stale["date"]).dt.normalize(),
                stale["code"],
                stale["model_short"],
                strict=True,
            )
        )
        stale_mask = modelled_filtered.apply(
            lambda r: (
                pd.Timestamp(r["date"]).normalize(),
                r["code"],
                r["model_short"],
            )
            in stale_keys,
            axis=1,
        )
        refreshed_stale = modelled_filtered[stale_mask]
        if not refreshed_stale.empty:
            refresh_parts.append(refreshed_stale)

    # 7b: New NE rows for NE gaps (created by neural_ensemble_func above).
    # Skip NE rows already captured as stale in 7a to avoid double-counting.
    if not ne_gaps.empty:
        ne_gap_keys = set(
            zip(
                pd.to_datetime(ne_gaps["date"]).dt.normalize(),
                ne_gaps["code"],
                strict=True,
            )
        )
        ne_mask = modelled_filtered.apply(
            lambda r: r["model_short"] == "NE"
            and (pd.Timestamp(r["date"]).normalize(), r["code"]) in ne_gap_keys
            and (
                pd.Timestamp(r["date"]).normalize(),
                r["code"],
                "NE",
            )
            not in stale_keys,
            axis=1,
        )
        new_ne = modelled_filtered[ne_mask]
        if not new_ne.empty:
            refresh_parts.append(new_ne)

    # 7c: EM rows (gap-fill + stale EM refresh) — requires skill metrics
    if skill_stats.empty:
        logger.warning(
            f"No {label} skill metrics available. "
            "Refreshing individual/NE rows but skipping EM creation."
        )
    else:
        with timer(timing_stats, f"creating {label} gap-fill ensembles"):
            ensemble_out, _ = ensemble_calculator.create_ensemble_forecasts(
                forecasts=modelled_filtered,
                skill_stats=skill_stats,
                period_col=config.period_col,
                period_in_month_col=config.period_in_month_col,
                get_period_in_month_func=config.get_period_func,
            )
        new_em = ensemble_out[ensemble_out["model_short"] == "EM"]
        if not new_em.empty:
            refresh_parts.append(new_em)

    if not refresh_parts:
        logger.info(f"No new {label} rows created from gap-fill data. Nothing to save.")
        return

    joint = pd.concat(refresh_parts, ignore_index=True)

    # Step 8: merge refreshed rows into combined.
    # Drop all-NA columns from joint to avoid introducing empty columns
    # into the merge. Suppress the remaining FutureWarning from pandas
    # >= 2.1 about concat dtype inference with all-NA entries — the
    # current behavior is correct for our use case.
    joint = joint.dropna(axis=1, how="all")

    # Write refreshed rows directly to API (bypasses get_latest_forecasts
    # filter so that historical gap-fills reach the database).
    if not joint.empty:
        try:
            ok = api_writer._write_combined_forecast_to_api(joint, config.name)
            if ok:
                logger.info(
                    "%s: submitted %d refreshed rows to API",
                    label,
                    len(joint),
                )
            else:
                logger.warning(
                    "%s: direct API write returned False (API may be unavailable or data filtered)",
                    label,
                )
        except Exception:
            logger.exception(
                "%s: direct API write of refreshed rows failed",
                label,
            )
            errors.append(f"{label} direct API write failed")

    # concat puts combined first, joint last → keep="last" replaces stale entries
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="The behavior of DataFrame concatenation with empty or all-NA",
            category=FutureWarning,
        )
        merged = pd.concat([combined, joint], ignore_index=True)
    merged = merged.drop_duplicates(
        subset=["date", "code", config.period_col, "model_short"], keep="last"
    )

    n_em = (joint["model_short"] == "EM").sum()
    n_ne = (joint["model_short"] == "NE").sum()
    n_individual = len(joint) - n_em - n_ne

    logger.info(
        "Merged %d refreshed rows (%d EM, %d NE, %d individual) into %d existing → %d total",
        len(joint),
        n_em,
        n_ne,
        n_individual,
        len(combined),
        len(merged),
    )

    with timer(timing_stats, f"saving {label} gap-fill results"):
        ret = file_writer.save_forecast_data(config, merged)
        if ret is None:
            logger.info(f"{label} gap-fill forecast results saved successfully.")
        else:
            logger.error(f"Error saving {label} gap-fill forecast results: {ret}")
            errors.append(f"{label} gap-fill save failed: {ret}")

    pt.log_most_recent_forecasts(config, merged)

    # Audit trail
    logger.info(
        "AUDIT: %s — filled %d EM gaps, refreshed %d NE, %d individual rows; "
        "%d stale detected (%d NE/individual, %d EM); lookback=%d months",
        label,
        len(em_gaps),
        n_ne,
        n_individual,
        len(stale) + len(stale_em),
        len(stale),
        len(stale_em),
        max_lookback_months,
    )
    for _, gap_row in em_gaps.iterrows():
        logger.info("  Filled EM: date=%s, code=%s", gap_row["date"], gap_row["code"])


if __name__ == "__main__":
    postprocessing_maintenance()
