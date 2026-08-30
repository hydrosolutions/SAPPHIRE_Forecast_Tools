"""Guarded recovery of one missed long-term forecast month.

Stage A of the "long-term missing-month recovery" issue: an operator names
one forecast mode and one issue date, and this module performs

    guard  ->  run  ->  read-back

inside a *single* process, so the whole sequence sits behind one exit code.

Why one process: the Luigi retry helper (``DockerTaskBase.execute_with_retries``
in ``apps/pipeline/pipeline_docker.py``) creates the task's completion marker
immediately after the child container exits 0. A check performed in the parent
task would therefore run *after* the marker exists and could not veto a false
success. The child container already joins ``sapphire_sapphire-network`` and so
already has API access; the Luigi worker does not.

Acceptance is defined on DATABASE rows only. ``run_forecast.py --today`` also
overwrites ``{model}_forecast.csv`` and rewrites the hindcast CSV; that is an
accepted side effect of the recovery, deliberately not guarded here.

Authoritative clock
-------------------
``now_local()`` is the single clock used for the "not in the future" and
"current or previous calendar month" checks. It is deliberately the same naive,
process-local clock that ``lt_utils.check_valid_forecast_issue_date`` uses
(``pd.Timestamp.now()``), because the two must agree: if they disagreed a run
could be admitted here and then skipped or snapped there. The long-term Compose
service sets no timezone, so this is the container's local time.
"""

import calendar
import os
from collections.abc import Callable, Iterable
from datetime import datetime
from typing import Any

import pandas as pd
from __init__ import SAPPHIRE_API_AVAILABLE, logger
from config_forecast import ForecastConfig
from lt_utils import map_model_name_to_model_type, nearest_scheduled_issue_date

try:
    from sapphire_api_client import SapphirePostprocessingClient
except ImportError:  # pragma: no cover - exercised by the dependency-gated path
    SapphirePostprocessingClient = None


# ─────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────

#: Exit code: recovery ran and rows were read back.
EXIT_OK = 0
#: Exit code: the forecast ran but could not be proven to have written rows.
EXIT_FAILED = 1
#: Exit code: refused before anything ran; the database was not touched.
EXIT_REFUSED = 2

#: Flag persisted on rows produced by a recovery run (see API-006).
RECOVERY_FLAG = 1
#: Flag persisted on rows whose value is missing / all-NaN.
MISSING_VALUE_FLAG = 2
#: Flag persisted on ordinary operational rows.
OPERATIONAL_FLAG = 0

#: Ensemble aggregates. They are derived from members by the postprocessing
#: maintenance job, never produced by ``run_forecast.py``, so they are neither
#: guarded on nor read back.
AGGREGATE_MODEL_NAMES = frozenset({"em", "skilled mean", "naive mean"})

#: Mirrors the +-5 day execution gate in ``lt_utils.check_valid_forecast_issue_date``.
#: A member model whose scheduled issue date differs from the requested date by
#: at most this much would be *snapped* to its own scheduled date while the
#: guard checked the requested one — the exact mismatch this module refuses.
#: (The scheduler admits modes up to 10 days out; see LTF-007.)
EXECUTION_WINDOW_DAYS = 5

#: Page size for long-forecast reads.
READ_PAGE_SIZE = 500


# ─────────────────────────────────────────────────────────────────
# Errors
# ─────────────────────────────────────────────────────────────────


class RecoveryError(Exception):
    """Base class for recovery failures."""


class RecoveryRefused(RecoveryError):
    """The recovery was refused; no forecast was run."""


class RecoveryQueryError(RecoveryError):
    """A long-forecast query failed. Always treated as fail-closed."""


# ─────────────────────────────────────────────────────────────────
# Clock
# ─────────────────────────────────────────────────────────────────


def now_local() -> pd.Timestamp:
    """Return the authoritative clock for recovery-window checks.

    Naive, process-local, date-only — deliberately identical to the clock
    ``lt_utils.check_valid_forecast_issue_date`` compares against.

    Returns:
        Today's date as a normalized ``pd.Timestamp``.
    """
    return pd.Timestamp.now().normalize()


# ─────────────────────────────────────────────────────────────────
# Members
# ─────────────────────────────────────────────────────────────────


def _normalize_model_name(name: Any) -> str:
    return str(name).strip().lower().replace("_", " ")


def member_model_names(forecast_config: Any) -> list[str]:
    """Return the configured raw member models for a mode.

    Members are the models ``run_forecast.py`` actually runs. Ensemble
    aggregates (``EM`` / ``Skilled Mean`` / ``Naive Mean``) are excluded.

    Args:
        forecast_config: A loaded ``ForecastConfig`` (or equivalent).

    Returns:
        List of internal model names, in configuration order.
    """
    return [
        name
        for name in forecast_config.get_models_to_run()
        if _normalize_model_name(name) not in AGGREGATE_MODEL_NAMES
    ]


def member_model_types(forecast_config: Any) -> list[str]:
    """Return the API ``model_type`` values for a mode's member models.

    Mapped exactly the way the writer maps them
    (``lt_utils.map_model_name_to_model_type``), de-duplicated, order preserved.

    Args:
        forecast_config: A loaded ``ForecastConfig`` (or equivalent).

    Returns:
        List of API model_type strings.
    """
    seen: set[str] = set()
    ordered: list[str] = []
    for name in member_model_names(forecast_config):
        model_type = map_model_name_to_model_type(name)
        if model_type not in seen:
            seen.add(model_type)
            ordered.append(model_type)
    return ordered


# ─────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────


def parse_issue_date(value: Any) -> pd.Timestamp:
    """Parse an operator-supplied ISO issue date.

    Args:
        value: Date string in strict ``YYYY-MM-DD`` form.

    Returns:
        The parsed date as a normalized ``pd.Timestamp``.

    Raises:
        RecoveryRefused: If the value is missing or not strict ISO.
    """
    text = "" if value is None else str(value).strip()
    if not text:
        raise RecoveryRefused("No issue date supplied. Expected ISO YYYY-MM-DD.")
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise RecoveryRefused(
            f"Issue date {text!r} is not a valid ISO date (expected YYYY-MM-DD)."
        ) from exc
    return pd.Timestamp(parsed)


def _month_key(timestamp: pd.Timestamp) -> tuple[int, int]:
    return (int(timestamp.year), int(timestamp.month))


def _previous_month_key(timestamp: pd.Timestamp) -> tuple[int, int]:
    if timestamp.month == 1:
        return (int(timestamp.year) - 1, 12)
    return (int(timestamp.year), int(timestamp.month) - 1)


def check_recovery_window(effective_date: pd.Timestamp, now: pd.Timestamp) -> None:
    """Refuse dates outside the current or previous calendar month.

    Recent-gap recovery only: anything older belongs to the historical
    backfill tooling, and a future date can never have observations behind it.

    Args:
        effective_date: The requested issue date.
        now: The authoritative clock (see :func:`now_local`).

    Raises:
        RecoveryRefused: If the date is in the future or too old.
    """
    if effective_date > now:
        raise RecoveryRefused(
            f"Issue date {effective_date.date()} is in the future "
            f"(clock says {now.date()}). Refusing."
        )
    allowed = {_month_key(now), _previous_month_key(now)}
    if _month_key(effective_date) not in allowed:
        allowed_text = ", ".join(f"{y:04d}-{m:02d}" for y, m in sorted(allowed))
        raise RecoveryRefused(
            f"Issue date {effective_date.date()} is outside the recovery window. "
            f"Only the current and previous calendar month are recoverable "
            f"({allowed_text}). Use the historical backfill tooling for older gaps."
        )


def resolve_scheduled_models(forecast_config: Any, effective_date: pd.Timestamp) -> list[str]:
    """Require the requested date to *be* the scheduled issue date.

    ``run_forecast.py`` silently snaps a late ``--today`` back to the model's
    scheduled issue date while persistence writes the frame's date. That
    mismatch is exactly what the no-overwrite guard must not fall through, so
    near misses are refused rather than snapped.

    Args:
        forecast_config: A loaded ``ForecastConfig`` (or equivalent).
        effective_date: The requested issue date.

    Returns:
        The member models that are scheduled precisely on ``effective_date``.

    Raises:
        RecoveryRefused: If any member would be snapped, or if no member is
            scheduled on that date at all.
    """
    issue_day = forecast_config.get_operational_issue_day()
    days_in_month = calendar.monthrange(effective_date.year, effective_date.month)[1]
    expected_day = min(int(issue_day), days_in_month)

    scheduled: list[str] = []
    for model_name in member_model_names(forecast_config):
        months = forecast_config.get_forecast_months(model_name=model_name)
        model_issue_date = nearest_scheduled_issue_date(effective_date, issue_day, months)
        day_offset = (effective_date - model_issue_date).days
        if day_offset == 0:
            scheduled.append(model_name)
        elif abs(day_offset) <= EXECUTION_WINDOW_DAYS:
            raise RecoveryRefused(
                f"Issue date {effective_date.date()} is {abs(day_offset)} day(s) off the "
                f"scheduled issue date {model_issue_date.date()} for member model "
                f"{model_name}. The forecast run would snap to "
                f"{model_issue_date.date()} while the guard checked "
                f"{effective_date.date()}. Re-run with "
                f"{model_issue_date.date()} instead."
            )

    if not scheduled:
        raise RecoveryRefused(
            f"Issue date {effective_date.date()} is not a scheduled issue date for any "
            f"member model of this mode. Expected day-of-month {expected_day} "
            f"(operational_issue_day={issue_day}) in a month the models forecast for."
        )
    return scheduled


def check_station_codes(station_codes: Iterable[Any] | None) -> list[str]:
    """Refuse an empty station list before any query is issued.

    An empty list disables organisation scoping in ``DataInterfaceDB`` and in
    the guard query alike, which would read (and count) the whole database.

    Args:
        station_codes: Configured station codes.

    Returns:
        The codes as non-empty strings.

    Raises:
        RecoveryRefused: If no usable code is present.
    """
    codes = [str(code).strip() for code in (station_codes or []) if str(code).strip()]
    if not codes:
        raise RecoveryRefused(
            "Station list is empty. An empty list disables organisation scoping and "
            "would read the whole database, so the guard cannot be trusted. Check "
            "ieasyforecast_config_file_station_selection."
        )
    return codes


# ─────────────────────────────────────────────────────────────────
# Database access
# ─────────────────────────────────────────────────────────────────


def build_postprocessing_client() -> Any:
    """Build a postprocessing API client, failing closed.

    Returns:
        A ready ``SapphirePostprocessingClient``.

    Raises:
        RecoveryQueryError: If the client is unavailable, writing is disabled,
            or the API is not ready. Any of these means the guard and the
            read-back cannot be trusted.
    """
    if not SAPPHIRE_API_AVAILABLE or SapphirePostprocessingClient is None:
        raise RecoveryQueryError(
            "sapphire-api-client is not installed. Recovery is defined on database "
            "rows only and cannot be verified without it."
        )
    if os.getenv("SAPPHIRE_API_ENABLED", "true").lower() != "true":
        raise RecoveryQueryError(
            "SAPPHIRE_API_ENABLED is false, so the run would write no database rows. "
            "Recovery is defined on database rows only."
        )
    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    client = SapphirePostprocessingClient(base_url=api_url)
    try:
        ready = client.readiness_check()
    except Exception as exc:
        raise RecoveryQueryError(f"SAPPHIRE API at {api_url} is unreachable: {exc}") from exc
    if not ready:
        raise RecoveryQueryError(f"SAPPHIRE API at {api_url} is not ready.")
    return client


def _row_flag(record: dict[str, Any]) -> int | None:
    raw = record.get("flag")
    if raw is None:
        return None
    try:
        if pd.isna(raw):
            return None
        return int(raw)
    except (TypeError, ValueError):
        return None


def _count_matching_rows(
    frame: pd.DataFrame,
    wanted_codes: set[str],
    date_text: str,
    flags: set[int] | None,
    require_value: bool,
) -> int:
    target_date = pd.Timestamp(date_text)
    matched = 0
    for record in frame.to_dict("records"):
        if str(record.get("code")) not in wanted_codes:
            continue
        raw_date = record.get("date")
        if raw_date is None:
            continue
        try:
            if pd.Timestamp(raw_date).normalize() != target_date:
                continue
        except (TypeError, ValueError):
            continue
        if flags is not None and _row_flag(record) not in flags:
            continue
        if require_value:
            value = record.get("q")
            if value is None or pd.isna(value):
                continue
        matched += 1
    return matched


def count_member_rows(
    client: Any,
    *,
    horizon_type: str,
    horizon_value: int,
    effective_date: pd.Timestamp,
    model_types: list[str],
    station_codes: list[str],
    flags: set[int] | None = None,
    require_value: bool = False,
    page_size: int = READ_PAGE_SIZE,
) -> int:
    """Count member rows for one ``(horizon_type, horizon_value, date)`` key.

    Args:
        client: Postprocessing API client.
        horizon_type: Horizon type of the mode ("month", "quarter", "season").
        horizon_value: Lead time of the mode.
        effective_date: The forecast issue date.
        model_types: API model_type values of the member models.
        station_codes: Configured station codes; rows for other codes are
            ignored so the count stays organisation-scoped.
        flags: If given, only rows whose ``flag`` is in this set are counted.
        require_value: If True, only rows with a non-null ``q`` are counted.
        page_size: Rows requested per API call.

    Returns:
        Number of matching rows.

    Raises:
        RecoveryQueryError: If any query fails. The caller fails closed.
    """
    wanted_codes = {str(code) for code in station_codes}
    date_text = effective_date.strftime("%Y-%m-%d")
    total = 0

    for model_type in model_types:
        skip = 0
        while True:
            try:
                frame = client.read_long_term_forecasts(
                    horizon_type=horizon_type,
                    horizon_value=horizon_value,
                    model=model_type,
                    start_date=date_text,
                    end_date=date_text,
                    skip=skip,
                    limit=page_size,
                )
            except Exception as exc:
                raise RecoveryQueryError(
                    f"Long-forecast query failed for model {model_type} on {date_text}: {exc}"
                ) from exc

            if frame is None or len(frame) == 0:
                break
            total += _count_matching_rows(frame, wanted_codes, date_text, flags, require_value)
            if len(frame) < page_size:
                break
            skip += page_size

    return total


# ─────────────────────────────────────────────────────────────────
# Flag override
# ─────────────────────────────────────────────────────────────────


def apply_success_flag(
    forecast: pd.DataFrame, main_q_col: str, recovery_flag: int | None = None
) -> pd.DataFrame:
    """Set the ``flag`` column on a forecast frame.

    Rows whose main Q value is NaN always get :data:`MISSING_VALUE_FLAG` — a
    recovery must never dress a missing value up as a recovered one. Rows with
    a value get :data:`OPERATIONAL_FLAG` normally, or ``recovery_flag`` when a
    recovery run supplies one.

    Args:
        forecast: Forecast frame; modified in place.
        main_q_col: Name of the model's main Q column.
        recovery_flag: Flag to persist on rows carrying a value, or None for
            the ordinary operational value.

    Returns:
        The same frame, for convenience.
    """
    value_flag = OPERATIONAL_FLAG if recovery_flag is None else int(recovery_flag)
    nan_mask = forecast[main_q_col].isna()
    forecast.loc[nan_mask, "flag"] = MISSING_VALUE_FLAG
    forecast.loc[~nan_mask, "flag"] = value_flag
    return forecast


# ─────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────


def _default_config_factory(forecast_mode: str) -> ForecastConfig:
    config = ForecastConfig()
    config.load_forecast_config(forecast_mode=forecast_mode)
    return config


def run_recovery(
    *,
    issue_date: Any,
    forecast_mode: str | None,
    run_forecast_fn: Callable[..., Any],
    station_codes_fn: Callable[[], Iterable[Any]],
    config_factory: Callable[[str], Any] | None = None,
    client_factory: Callable[[], Any] | None = None,
    now: pd.Timestamp | None = None,
) -> int:
    """Guard, run and read back one dated long-term recovery.

    The three stages are kept explicit because they carry different exit
    codes: a refusal means the database was never touched, a failure means the
    forecast ran but could not be proven to have written anything.

    Args:
        issue_date: Operator-supplied ISO date (``YYYY-MM-DD``).
        forecast_mode: Long-term mode to recover (e.g. ``month_0``).
        run_forecast_fn: The forecast entry point; called with
            ``forecast_all=True``, ``models_to_run=[]``, ``forecast_mode`` and
            ``recovery_flag``.
        station_codes_fn: Callable returning the configured station codes.
            Called after the configuration is loaded, so the environment is up.
        config_factory: Builds a loaded ``ForecastConfig`` for a mode.
        client_factory: Builds a postprocessing API client.
        now: Override for the authoritative clock (tests only).

    Returns:
        :data:`EXIT_OK`, :data:`EXIT_FAILED` or :data:`EXIT_REFUSED`.
    """
    config_factory = config_factory or _default_config_factory
    client_factory = client_factory or build_postprocessing_client
    clock = now_local() if now is None else pd.Timestamp(now).normalize()

    # ── Stage 1: validate and guard. Nothing has run yet. ───────────────
    try:
        mode = "" if forecast_mode is None else str(forecast_mode).strip()
        if not mode:
            raise RecoveryRefused("No forecast mode supplied. Set lt_forecast_mode (e.g. month_0).")

        effective_date = parse_issue_date(issue_date)
        check_recovery_window(effective_date, clock)

        config = config_factory(mode)

        model_types = member_model_types(config)
        if not model_types:
            raise RecoveryRefused(
                f"Mode {mode} has no member models configured (only ensemble "
                f"aggregates). There is nothing to recover."
            )

        scheduled_models = resolve_scheduled_models(config, effective_date)
        station_codes = check_station_codes(station_codes_fn())

        horizon_type = config.get_horizon_type()
        horizon_value = config.get_operational_month_lead_time()

        logger.info(
            "Long-term recovery requested: mode=%s effective_date=%s "
            "horizon=%s/%s members=%s scheduled=%s stations=%d",
            mode,
            effective_date.date(),
            horizon_type,
            horizon_value,
            model_types,
            scheduled_models,
            len(station_codes),
        )

        client = client_factory()

        pre_count = count_member_rows(
            client,
            horizon_type=horizon_type,
            horizon_value=horizon_value,
            effective_date=effective_date,
            model_types=model_types,
            station_codes=station_codes,
        )
        if pre_count > 0:
            raise RecoveryRefused(
                f"{pre_count} member row(s) already exist for "
                f"({horizon_type}, {horizon_value}, {effective_date.date()}). "
                f"A recovery overwrites every submitted field, so a partially "
                f"populated month is refused as a whole. Delete the existing rows "
                f"first if you really intend to regenerate them."
            )
        logger.info(
            "Guard passed: no member rows exist for (%s, %s, %s).",
            horizon_type,
            horizon_value,
            effective_date.date(),
        )
    except RecoveryError as exc:
        logger.error("Long-term recovery REFUSED (nothing was run): %s", exc)
        return EXIT_REFUSED
    except Exception as exc:
        logger.exception("Long-term recovery REFUSED (nothing was run): %s", exc)
        return EXIT_REFUSED

    # ── Stage 2: run the forecast for the dated issue. ──────────────────
    try:
        run_forecast_fn(
            forecast_all=True,
            models_to_run=[],
            forecast_mode=mode,
            recovery_flag=RECOVERY_FLAG,
        )
    except Exception as exc:
        logger.exception("Long-term recovery FAILED while running the forecast: %s", exc)
        return EXIT_FAILED

    # ── Stage 3: read back. Exit 0 alone proves nothing. ────────────────
    try:
        post_count = count_member_rows(
            client,
            horizon_type=horizon_type,
            horizon_value=horizon_value,
            effective_date=effective_date,
            model_types=model_types,
            station_codes=station_codes,
            flags={RECOVERY_FLAG},
            require_value=True,
        )
    except Exception as exc:
        logger.exception(
            "Long-term recovery FAILED: read-back query failed, failing closed: %s", exc
        )
        return EXIT_FAILED

    if post_count <= 0:
        logger.error(
            "Long-term recovery FAILED: no member row with flag=%d and a usable value "
            "was written for (%s, %s, %s). Rows flagged %d (missing/all-NaN) do not "
            "count. Nothing was recovered.",
            RECOVERY_FLAG,
            horizon_type,
            horizon_value,
            effective_date.date(),
            MISSING_VALUE_FLAG,
        )
        return EXIT_FAILED

    logger.info(
        "Long-term recovery SUCCEEDED: %d member row(s) with flag=%d read back for "
        "(%s, %s, %s). Success criterion is PARTIAL — some rows written, not full "
        "station x model coverage.",
        post_count,
        RECOVERY_FLAG,
        horizon_type,
        horizon_value,
        effective_date.date(),
    )
    return EXIT_OK
