"""Pure helpers for the Bulletin-tab "Publish bulletin" feature (FD-017).

These functions assemble a frozen, Excel-equivalent JSON snapshot of the
bulletin data for a set of selected stations/horizons, and compute the
expiry timestamp (start of the next period of the horizon). They are
intentionally free of any ``panel`` import so they stay importable and
unit-testable without a Panel/Bokeh install.

``assemble_bulletin_snapshot`` reuses the existing bulletin hydration
pipeline (``dashboard.bulletin_manager._load_bulletin_from_api``, which
internally calls ``_populate_forecast_attributes``) rather than
duplicating it. That module imports ``panel`` at module scope, so the
import is deferred to inside the function body — importing
``bulletin_publish`` itself never requires Panel to be installed; only
*calling* ``assemble_bulletin_snapshot`` does.

See ``doc/plans/issues/mid_prio_gi_draft_fd_publish_bulletin.md`` for the
full spec and ``doc/plans/publish_bulletin_api_design.md`` for the
service contract this payload is POSTed to.
"""

from __future__ import annotations

import math
import os
import sys
from datetime import UTC, date, datetime

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# tag_library import (mirrors dashboard.config.import_tag_library, without
# pulling in dashboard.config's own transitive imports)
# ---------------------------------------------------------------------------


def _import_tag_library():
    """Add iEasyHydroForecast to sys.path and import tag_library."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    forecast_dir = os.path.join(script_dir, "..", "..", "iEasyHydroForecast")
    if forecast_dir not in sys.path:
        sys.path.append(forecast_dir)
    import tag_library

    return tag_library


tl = _import_tag_library()

_SHORT_TERM_HORIZONS = ("pentad", "decade")
_LONG_TERM_HORIZONS = ("month", "season", "quarter")


# ---------------------------------------------------------------------------
# JSON-safety helper
# ---------------------------------------------------------------------------


def _json_safe(value):
    """Convert a pandas/numpy scalar to a JSON-safe python value.

    NaN / NaT / Inf all become ``None`` so ``requests.post(json=...)``
    never chokes on a non-finite float.
    """
    if isinstance(value, np.generic):
        value = value.item()
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, float) and math.isinf(value):
        return None
    return value


# ---------------------------------------------------------------------------
# Period-boundary helpers
# ---------------------------------------------------------------------------


def _period_start_date(horizon: str, forecast_horizon: int, forecast_year: int) -> date:
    """Return the calendar date on which the CURRENT period of `horizon` starts."""
    if horizon == "pentad":
        raw = tl.get_date_for_pentad(forecast_horizon, forecast_year)
    elif horizon == "decade":
        raw = tl.get_date_for_decad(forecast_horizon, forecast_year)
    elif horizon == "month":
        return date(forecast_year, forecast_horizon, 1)
    elif horizon == "season":
        # A single annual season, April-September (see
        # dashboard.widgets.format_horizon_info); forecast_horizon is
        # always 1 (dm.get_bulletin_metadata) and does not disambiguate.
        return date(forecast_year, 4, 1)
    elif horizon == "quarter":
        start_month = (forecast_horizon - 1) * 3 + 1
        return date(forecast_year, start_month, 1)
    else:
        raise ValueError(f"Unsupported horizon: {horizon!r}")

    if raw is None:
        raise ValueError(
            f"Could not resolve start date for horizon={horizon!r} "
            f"forecast_horizon={forecast_horizon!r} forecast_year={forecast_year!r}"
        )
    return date.fromisoformat(raw)


def _next_period_start_date(horizon: str, forecast_horizon: int, forecast_year: int) -> date:
    """Return the calendar date on which the NEXT period of `horizon` begins.

    Handles the pentad-72->1 and decade-36->1 year wraparounds, the
    December->January month wraparound, and (as a documented choice —
    there is no natural "next quarter/season within the calendar year"
    concept for a horizon that only fires once a year) treats "next
    season"/"next quarter" as the same period next year.
    """
    if horizon == "pentad":
        if forecast_horizon >= 72:
            next_value, next_year = 1, forecast_year + 1
        else:
            next_value, next_year = forecast_horizon + 1, forecast_year
        raw = tl.get_date_for_pentad(next_value, next_year)
    elif horizon == "decade":
        if forecast_horizon >= 36:
            next_value, next_year = 1, forecast_year + 1
        else:
            next_value, next_year = forecast_horizon + 1, forecast_year
        raw = tl.get_date_for_decad(next_value, next_year)
    elif horizon == "month":
        if forecast_horizon >= 12:
            return date(forecast_year + 1, 1, 1)
        return date(forecast_year, forecast_horizon + 1, 1)
    elif horizon == "season":
        # One season per year (April-September) -> "next period" is next
        # year's season start.
        return date(forecast_year + 1, 4, 1)
    elif horizon == "quarter":
        next_value = forecast_horizon + 1
        next_year = forecast_year
        if next_value > 4:
            next_value = 1
            next_year += 1
        start_month = (next_value - 1) * 3 + 1
        return date(next_year, start_month, 1)
    else:
        raise ValueError(f"Unsupported horizon: {horizon!r}")

    if raw is None:
        raise ValueError(
            f"Could not resolve next-period start for horizon={horizon!r} "
            f"forecast_horizon={forecast_horizon!r} forecast_year={forecast_year!r}"
        )
    return date.fromisoformat(raw)


def _period_end_date(horizon: str, forecast_horizon: int, forecast_year: int) -> date:
    """Return the last calendar date of the CURRENT period of `horizon`."""
    if horizon == "season":
        # Single annual season, April-September.
        return date(forecast_year, 9, 30)
    next_start = _next_period_start_date(horizon, forecast_horizon, forecast_year)
    return next_start - pd.Timedelta(days=1)


def compute_next_period_start(
    horizon: str,
    forecast_horizon: int,
    forecast_year: int,
    forecast_date,
) -> datetime:
    """Return the UTC datetime at which a link for this horizon expires.

    This is the first instant of the NEXT period of `horizon`
    (pentad/decade/month rolling into the next year at 72/36/December;
    season/quarter rolling into next year's same period — see
    ``_next_period_start_date``).

    ``forecast_date`` is accepted (and never defaulted to
    ``date.today()``/``datetime.now()``) per the Forecast Date Rule: the
    period boundaries here are fully determined by
    ``forecast_horizon``/``forecast_year`` (already resolved by the
    caller from the forecast date), so it is not itself used in the
    arithmetic, but the parameter keeps every caller explicit rather than
    letting this function reach for wall-clock time.

    Returns a timezone-aware UTC ``datetime`` (midnight of the next
    period's first day).
    """
    del forecast_date  # not needed for the arithmetic; see docstring
    next_date = _next_period_start_date(horizon, forecast_horizon, forecast_year)
    return datetime(next_date.year, next_date.month, next_date.day, tzinfo=UTC)


def _forecast_date_to_utc_datetime(forecast_date) -> datetime:
    """Normalize a `date`/`datetime` forecast_date to a UTC-aware datetime."""
    if isinstance(forecast_date, datetime):
        if forecast_date.tzinfo is None:
            return forecast_date.replace(tzinfo=UTC)
        return forecast_date.astimezone(UTC)
    return datetime(forecast_date.year, forecast_date.month, forecast_date.day, tzinfo=UTC)


def _iso_z(value: datetime | date) -> str:
    """Format a datetime as an ISO-8601 UTC string with a trailing 'Z'."""
    if isinstance(value, datetime):
        value = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
    return value.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Site serialization
# ---------------------------------------------------------------------------


def serialize_site(site, horizon: str) -> dict:
    """Serialize one hydrated SapphireSite into a JSON-safe bulletin row.

    The field set mirrors the Excel bulletin and differs by horizon:
    short-term (pentad/decade) sites carry forecast bounds + skill
    metrics; month/season/quarter sites additionally carry volumes and
    the norm (there is no skill-metric column set for long-term
    horizons in the Excel bulletin).
    """
    base = {
        "code": _json_safe(getattr(site, "code", None)),
        "station_label": _json_safe(getattr(site, "station_label", None)),
        "basin": _json_safe(getattr(site, "basin_ru", "")),
        "river": _json_safe(getattr(site, "river_name_ru", "")),
        "model": _json_safe(getattr(site, "forecast_model", "")),
    }
    if horizon in _LONG_TERM_HORIZONS:
        base.update(
            forecasted_discharge=_json_safe(getattr(site, "forecast_expected", None)),
            q_min=_json_safe(getattr(site, "forecast_q_min", None)),
            q_max=_json_safe(getattr(site, "forecast_q_max", None)),
            v_min=_json_safe(getattr(site, "forecast_v_min", None)),
            v_max=_json_safe(getattr(site, "forecast_v_max", None)),
            norm=_json_safe(getattr(site, "forecast_norm", None)),
            perc_norm=_json_safe(getattr(site, "perc_norm", None)),
        )
    else:
        base.update(
            forecasted_discharge=_json_safe(getattr(site, "forecast_expected", None)),
            fc_lower=_json_safe(getattr(site, "forecast_lower_bound", None)),
            fc_upper=_json_safe(getattr(site, "forecast_upper_bound", None)),
            delta=_json_safe(getattr(site, "forecast_delta", None)),
            sdivsigma=_json_safe(getattr(site, "forecast_sdivsigma", None)),
            mae=_json_safe(getattr(site, "forecast_mae", None)),
            accuracy=_json_safe(getattr(site, "forecast_accuracy", None)),
            perc_norm=_json_safe(getattr(site, "perc_norm", None)),
        )
    return base


# ---------------------------------------------------------------------------
# Snapshot assembly
# ---------------------------------------------------------------------------


def assemble_bulletin_snapshot(horizon: str, selected_codes, dm, forecast_date) -> dict:
    """Assemble the frozen, Excel-equivalent JSON snapshot for one horizon.

    Args:
        horizon: One of "pentad", "decade", "month", "season" (or
            "quarter", supported for completeness though not exposed by
            the horizon multiselect).
        selected_codes: Iterable of bare station codes selected by the
            user (e.g. ``["90001", "90002"]``).
        dm: The dashboard's ``DataManager`` — used only for
            ``sites_list``. The ``(forecast_year, forecast_horizon)``
            period is derived from the PERSISTED bulletin records via the
            API (below), never from ``dm.get_bulletin_metadata`` /
            ``dm.forecasts_all``: those only hold data for the
            dashboard's currently ACTIVE horizon (the main
            ``horizon_selector``), so publishing a horizon other than the
            active one would ``KeyError`` on a missing ``*_in_year``
            column.
        forecast_date: The forecast date (Forecast Date Rule) — used to
            derive ``generated_at``; never read via ``date.today()``.

    Returns:
        ``{"payload": {...}, "skipped_codes": [...]}``. ``payload``
        contains ``horizon``, ``year``, ``horizon_value``, ``valid_from``,
        ``valid_to``, ``generated_at``, ``expires_at``, and ``stations``
        (list of ``serialize_site`` rows, one per selected code that has
        bulletin data for this horizon). ``skipped_codes`` lists selected
        codes with no bulletin data for this horizon — they are omitted
        from ``payload["stations"]``.

        If no persisted bulletin record exists for this horizon among the
        selected stations, ``payload["stations"]`` is ``[]``, the other
        payload fields are ``None``, and every selected code is reported
        in ``skipped_codes`` — no exception is raised.
    """
    # Deferred imports: dashboard.bulletin_manager imports `panel` at
    # module scope. Keeping both imports inside the function body means
    # merely importing `bulletin_publish` never requires Panel.
    from src import db

    from dashboard.bulletin_manager import _load_bulletin_from_api

    selected_set = {str(code) for code in selected_codes}

    # Query the bulletin resource for this horizon (no year/horizon_value
    # filter yet — the latest period is determined below from the rows
    # that actually belong to the selected stations).
    bdf = db._read_data("postprocessing", "bulletin", {"horizon": horizon, "limit": 1000})

    # Scope to the selected station codes BEFORE choosing the period: the
    # postprocessing DB is shared across deployments locally, so an
    # unscoped max() could pick a foreign deployment's row.
    if bdf.empty or "code" not in bdf.columns:
        bdf = bdf.iloc[0:0]
    else:
        bdf = bdf[bdf["code"].astype(str).isin(selected_set)]

    if bdf.empty:
        # No persisted bulletin for these stations under this horizon —
        # report all selected codes as skipped rather than raising.
        payload = {
            "horizon": horizon,
            "year": None,
            "horizon_value": None,
            "valid_from": None,
            "valid_to": None,
            "generated_at": None,
            "expires_at": None,
            "stations": [],
        }
        return {"payload": payload, "skipped_codes": sorted(selected_set)}

    # Pick the latest period among the selected stations' persisted rows.
    latest = bdf.sort_values(["year", "horizon_value"]).tail(1)
    forecast_year = int(latest["year"].values[0])
    forecast_horizon = int(latest["horizon_value"].values[0])

    bulletin_sites = _load_bulletin_from_api(
        horizon, forecast_year, forecast_horizon, dm.sites_list
    )

    found_codes: set[str] = set()
    stations = []
    for site in bulletin_sites:
        code = str(getattr(site, "code", ""))
        if code in selected_set:
            stations.append(serialize_site(site, horizon))
            found_codes.add(code)

    skipped_codes = sorted(selected_set - found_codes)

    valid_from = _period_start_date(horizon, forecast_horizon, forecast_year)
    valid_to = _period_end_date(horizon, forecast_horizon, forecast_year)
    expires_at = compute_next_period_start(horizon, forecast_horizon, forecast_year, forecast_date)
    generated_at = _forecast_date_to_utc_datetime(forecast_date)

    payload = {
        "horizon": horizon,
        "year": forecast_year,
        "horizon_value": forecast_horizon,
        "valid_from": _iso_z(valid_from),
        "valid_to": _iso_z(valid_to),
        "generated_at": _iso_z(generated_at),
        "expires_at": _iso_z(expires_at),
        "stations": stations,
    }
    return {"payload": payload, "skipped_codes": skipped_codes}
