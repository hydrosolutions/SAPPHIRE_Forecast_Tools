import calendar
import contextlib

import pandas as pd
import panel as pn
from skill_lead_aware_flag import skill_lead_aware_enabled
from src import db  # _read_data, _save_data, _delete_data live here
from src.gettext_config import _

from dashboard.logger import setup_logger
from dashboard.utils import (
    hydrate_month_hydrograph_stats,
    hydrate_season_hydrograph_stats,
    rehydrate_sites_hydrograph_stats,
)

logger = setup_logger()

_SERVICE  = "postprocessing"
_RESOURCE = "bulletin"

_HYDROGRAPH_DEFAULTS = {
    "hydrograph_mean":         float('nan'),
    "hydrograph_norm":         float('nan'),
    "hydrograph_max":          float('nan'),
    "hydrograph_min":          float('nan'),
    "last_year_q_pentad_mean": float('nan'),
    "linreg_predictor":        float('nan'),
    "act_q_this":              float('nan'),
    "act_q_last":              float('nan'),
    "act_norm":                float('nan'),
    "month_last_year_q":       float('nan'),
    "season_last_year_q":      float('nan'),
}


def resolve_bulletin_header_date(horizon, last_date, forecasts_all):
    """Return the date whose month/year the bulletin header should use.

    Monthly forecasts are issued in the month before their target month,
    so `last_date` (issue date + 1 day) can still fall in the previous
    month. For the month horizon, use the forecast target month start
    (`valid_from` of the latest forecast row) so the bulletin title shows
    the forecasted month, not the issue month. Other horizons are
    unaffected and return `last_date` unchanged.

    Args:
        horizon: Forecast horizon string (e.g. ``"month"``, ``"pentad"``).
        last_date: The date derived from the maximum issue date + 1 day.
        forecasts_all: DataFrame of all loaded forecasts; must contain a
            ``valid_from`` column for the month horizon.

    Returns:
        ``pd.Timestamp`` to use for bulletin header month/year derivation.
    """
    if horizon != "month":
        return last_date

    try:
        if (
            forecasts_all is None
            or forecasts_all.empty
            or "valid_from" not in forecasts_all.columns
        ):
            return last_date
        raw = forecasts_all["valid_from"].tail(1).values[0]
        ts = pd.Timestamp(raw)
        if pd.isna(ts):
            return last_date
        return ts
    except Exception:
        return last_date


def _reshape_long_forecast_for_bulletin(q_df: pd.DataFrame, _) -> pd.DataFrame:
    """Rename raw long-forecast columns into the gettext schema that
    get_{monthly,quarterly}_forecast_attributes_for_site reads."""
    if q_df is None or q_df.empty:
        return pd.DataFrame()
    out = q_df.copy()
    renames = {
        "model_short":          _('Model'),
        "forecasted_discharge": _('Forecasted discharge'),
        "Q25":                  _('Forecast lower bound'),
        "Q75":                  _('Forecast upper bound'),
    }
    out = out.rename(columns={k: v for k, v in renames.items() if k in out.columns})
    return out


def _resolve_month_target_period(source_df, site, date_bound=None):
    """Resolve a site's own (month, year) monthly target period from its data.

    FD-018: a bulletin site carries no target period of its own unless one is
    captured here. `valid_from` on the site's OWN forecast frame (the frame
    the site was actually added from — the main `month_1` panel or the m0
    card) is the target period, by definition: it is the first day of the
    month the forecast is FOR. This must never be derived from lead
    arithmetic or from whichever panel happens to be on screen later.

    FD-018 review #2: `source_df` (`get_long_forecasts()`-shaped) is a WIDE,
    undeduplicated history — many issue dates, many models, for this
    station. Scanning it unfiltered and taking `sorted(valid_from)[-1]`
    picks the globally-latest row, which is not necessarily the row the
    operator actually selected (e.g. a different model in `source_df` with
    a newer issue date would win, even though the operator picked an older
    row for a different model). `site.forecasts` — set by `_on_add` /
    `_on_add_m0` to the operator's own selected tabulator row(s) BEFORE this
    is called — carries the model(s) actually picked, so we narrow
    `source_df` to those models first.

    FD-018 review #5: narrowing by model is not enough — the operator's
    tabulator selection was ALSO bounded by the date picker
    (`create_forecast_summary_table`, vizualization.py: `date <=
    date_picker + 1 day`, then the max date WITHIN that bounded subset).
    Without reproducing that bound here, a later, unrelated issue-date row
    for the SAME model elsewhere in this wide `source_df` outranks the row
    the operator actually saw and picked when they wound the date picker
    back to an earlier issue date. `date_bound` must be the exact value the
    tabulator that produced `site.forecasts` was built with: `wm.date_
    picker.value` for the main panel (`_on_add`), or the m0 frame's own max
    date for the m0 card (`_on_add_m0`, mirroring `PlotManager.update_
    forecast_tabulator_m0`, which passes `m0['date'].max()` instead of the
    shared date picker). `site.forecasts` never carries `valid_from` itself
    (the tabulator's summary table drops it), so the period still has to
    come from `source_df`; narrowing by model, then by the date-picker
    bound, then by the latest issue date within that bounded/model-narrowed
    subset, ties the resolved period to the operator's actual selection
    instead of the whole frame.

    FD-018 review #5 (kill switch, not silent fallback): if `site.forecasts`
    IS set but the selected model(s) cannot be determined from it (e.g. a
    missing/renamed `Model` column) or none of them match a row for this
    site, that is a broken invariant — not a legacy caller. Returning the
    whole (model-un-narrowed) station frame in that case would silently
    resolve to a possibly-wrong model/date; this function returns `None`
    instead, so the caller falls back to the bulletin-wide period AND the
    operator sees the existing "could not be confirmed" warning (see
    `_on_add` / `_on_add_m0` / `_on_write`). This only applies when `site.
    forecasts` is actually present — a caller that never sets it at all
    (the pre-FD-018 legacy shape) still gets the whole-frame resolution,
    unchanged.

    FD-018 review #5 (multi-model disagreement): if the operator's
    selection spans more than one model and, after date-bound + max-date
    narrowing, those models resolve to DIFFERENT (month, year) pairs, this
    is treated as unresolved (`None`) rather than silently picking one.
    Structurally this should not happen — every row in one issue-date batch
    is produced by the same monthly forecast run and targets the same
    calendar month — so reaching this branch indicates a data anomaly, not
    normal operation. (It is also not reachable via the tabulator itself:
    `create_forecast_summary_table` already collapses to a single `date ==
    max(date)` batch before the operator ever sees a row to select.)

    Args:
        source_df: A `get_long_forecasts()`-shaped DataFrame (must carry
            `station_labels` and `valid_from`) — pass the site's own source
            frame (`dm.forecasts_all` for the main panel, `dm.long_forecasts_m0`
            for the m0 card).
        site: The site being added to the bulletin; matched by
            `site.station_label`. If `site.forecasts` is already set (the
            operator's selected row(s)), it is used to narrow `source_df` to
            the selected model(s) before resolving.
        date_bound: The date-picker bound the tabulator that produced
            `site.forecasts` was built with (see above). `None` skips bound
            filtering entirely — only correct for legacy/test callers whose
            `source_df` carries no `date` column to bound in the first
            place.

    Returns:
        `(month, year)` tuple, or `None` if the frame is missing/empty, has
        no rows for this site, the selected model(s) could not be resolved
        to a row, the selected models disagree on target period, or the
        resolved `valid_from` is NaT/invalid. Callers must treat `None` as
        "fall back to the bulletin-wide period" — this function never
        raises (data-shape problems are handled defensively; it does not
        swallow programming errors).
    """
    if (
        source_df is None
        or source_df.empty
        or "valid_from" not in source_df.columns
        or "station_labels" not in source_df.columns
    ):
        return None
    try:
        site_rows = source_df[source_df["station_labels"] == site.station_label]
    except (KeyError, TypeError):
        return None
    if site_rows.empty:
        return None

    # Narrow to the operator's own selected model(s). `site.forecasts`
    # absent entirely means a legacy caller that never captured a selection
    # — fall through and resolve from the whole (station-scoped) frame, as
    # always. `site.forecasts` PRESENT but unusable (no Model column, or no
    # matching rows) means the selection can't be confirmed — return None
    # rather than silently reusing the un-narrowed, possibly-wrong-model
    # frame (FD-018 review #5).
    selected_forecasts = getattr(site, "forecasts", None)
    model_col = _('Model')
    if selected_forecasts is not None and not selected_forecasts.empty:
        if model_col not in selected_forecasts.columns or "model_short" not in site_rows.columns:
            return None
        selected_models = set(selected_forecasts[model_col].dropna().unique())
        if not selected_models:
            return None
        site_rows = site_rows[site_rows["model_short"].isin(selected_models)]
        if site_rows.empty:
            return None

    # Mirror the tabulator's date-picker bound EXACTLY (vizualization.py
    # create_forecast_summary_table: `date <= date_picker + 1 day`) before
    # taking the max date — otherwise a later, unrelated issue-date row for
    # the same (narrowed) model(s) outranks the row the operator actually
    # saw and picked while the date picker was wound back (FD-018 review
    # #5, the surviving defect in the original add-time-capture fix).
    if "date" in site_rows.columns and date_bound is not None:
        try:
            dates = pd.to_datetime(site_rows["date"])
            bound = pd.Timestamp(date_bound) + pd.Timedelta(days=1)
        except (TypeError, ValueError):
            return None
        site_rows = site_rows[dates <= bound]
        if site_rows.empty:
            return None

    # Narrow to the latest ISSUE date within the (bounded, possibly
    # model-narrowed) subset — the same `date == max(date)` batch the
    # tabulator itself drew the operator's selection from (see
    # create_forecast_summary_table). This stops an unrelated, differently
    # -dated row elsewhere in the wide history from outranking the row the
    # operator actually picked.
    if "date" in site_rows.columns:
        max_date = site_rows["date"].max()
        if pd.notna(max_date):
            site_rows = site_rows[site_rows["date"] == max_date]

    raw = site_rows["valid_from"].dropna()
    if raw.empty:
        return None
    try:
        parsed = [pd.Timestamp(v) for v in raw]
    except (TypeError, ValueError):
        return None
    parsed = [ts for ts in parsed if pd.notna(ts)]
    if not parsed:
        return None
    periods = {(int(ts.month), int(ts.year)) for ts in parsed}
    if len(periods) > 1:
        # Selected models disagree on target month/year within the same
        # batch -- see docstring: structurally shouldn't happen, so treat
        # as unresolved rather than silently picking one.
        logger.warning(
            "_resolve_month_target_period: selected model(s) for site "
            "'%s' resolve to different target periods within the same "
            "batch (%s); treating as unresolved.",
            getattr(site, "code", "?"), sorted(periods),
        )
        return None
    return next(iter(periods))


def _ensure_site_defaults(site) -> None:
    """Set hydrograph/predictor attributes to None if not yet hydrated.

    `get_forecast_attributes_for_site` reads these; at bulletin-load time
    the site objects have not yet been through the statistics update, so
    we guard against AttributeError by initialising any missing ones here.
    """
    for attr, default in _HYDROGRAPH_DEFAULTS.items():
        if not hasattr(site, attr):
            setattr(site, attr, default)

# ---------------------------------------------------------------------------
# Bulletin-specific API helpers  (thin wrappers around db primitives)
# ---------------------------------------------------------------------------

def _site_to_records(horizon_type: str, year: int, horizon_value: int, site) -> list[dict]:
    """Flatten one site's forecasts DataFrame into API-ready bulletin dicts."""
    records = []
    for _idx, row in site.forecasts.iterrows():
        model = row.get(_('Model'), '')
        if not model:
            continue
        records.append({
            "horizon_type":        horizon_type,
            "year":                year,
            "horizon_value":       horizon_value,
            "code":                site.code,
            "model_type":          model,
            "basin_name":          getattr(site, 'basin_ru', ''),
            "station_label":       site.station_label,
            "forecasted_discharge": row.get(_('Forecasted discharge')),
            "fc_lower":            row.get(_('Forecast lower bound')),
            "fc_upper":            row.get(_('Forecast upper bound')),
            "delta":               row.get(_('δ')),
            "sdivsigma":           row.get(_('s/σ')),
            "mae":                 row.get(_('MAE')),
            "accuracy":            row.get(_('Accuracy')),
        })
    return records


def _populate_forecast_attributes(
    site, horizon_type, forecast_year, forecast_horizon, target_period=None,
):
    """Populate a site's forecast attributes from its site.forecasts DataFrame.

    Runs the hydrograph-stat hydration and get_*_forecast_attributes_for_site
    calls for the given horizon. Extracted from _load_bulletin_from_api so it
    can also be re-invoked at bulletin-write time after in-cell edits.

    Args:
        target_period: FD-018 — optional `(month, year)` override for the
            'month' horizon, used instead of `(forecast_horizon, forecast_year)`
            when a bulletin site has its own resolved target period (e.g. an
            m0-card site whose target month differs from the main panel's).
            `None` (the default) preserves the original bulletin-wide
            behavior exactly — required for existing callers and for the
            `SAPPHIRE_SKILL_LEAD_AWARE` flag-OFF kill switch.
    """
    _ensure_site_defaults(site)
    if horizon_type == 'month':
        month_for_calc, year_for_calc = (
            target_period if target_period is not None
            else (forecast_horizon, forecast_year)
        )
        days_in_month = calendar.monthrange(year_for_calc, month_for_calc)[1]
        hydrate_month_hydrograph_stats(site, month_for_calc, db)
        site.get_monthly_forecast_attributes_for_site(_, site.forecasts, days_in_month)
        if 'вдхр' in (site.punkt_name_ru or ''):
            q_df = db.get_long_forecasts_quarter(site.code)
            if "code" in q_df.columns and "date" in q_df.columns and not q_df.empty:
                filtered_q = q_df[q_df["code"] == site.code]
                if not filtered_q.empty:
                    filtered_q = filtered_q.sort_values("date", ascending=False).head(1)
            else:
                filtered_q = pd.DataFrame()
            if not filtered_q.empty and "valid_from" in filtered_q.columns and "valid_to" in filtered_q.columns:
                vf = pd.to_datetime(filtered_q["valid_from"].values[0])
                vt = pd.to_datetime(filtered_q["valid_to"].values[0])
                seconds_in_quarter = int((vt - vf + pd.Timedelta(days=1)).total_seconds())
            else:
                seconds_in_quarter = 0
            filtered_q = _reshape_long_forecast_for_bulletin(filtered_q, _)
            site.get_quarterly_forecast_attributes_for_site(_, filtered_q, seconds_in_quarter)
        else:
            site.get_quarterly_forecast_attributes_for_site(_, pd.DataFrame(), 0)
    elif horizon_type == 'season':
        # The frozen bulletin record (site.forecasts) holds the same
        # forecast bounds the API and UI show. Use it for the seasonal
        # Q_MIN/Q_MAX (mirrors the month branch) so the Excel matches.
        # get_long_forecasts_season is re-fetched ONLY to derive the
        # season window (valid_from/valid_to -> seconds_in_season),
        # because the bulletin record does not carry those. Passing the
        # bulletin's horizon_value as the forecast issue-lead would
        # otherwise resolve to a stale (older-lead) forecast.
        s_df = db.get_long_forecasts_season(site.code, horizon_value=forecast_horizon)
        if (
            not s_df.empty
            and "code" in s_df.columns
            and "date" in s_df.columns
        ):
            filtered_s = s_df[s_df["code"] == site.code]
            if not filtered_s.empty:
                filtered_s = filtered_s.sort_values("date", ascending=False).head(1)
        else:
            filtered_s = pd.DataFrame()
        vf = vt = None
        if (
            not filtered_s.empty
            and "valid_from" in filtered_s.columns
            and "valid_to" in filtered_s.columns
        ):
            vf = pd.to_datetime(filtered_s["valid_from"].values[0])
            vt = pd.to_datetime(filtered_s["valid_to"].values[0])
            seconds_in_season = int((vt - vf + pd.Timedelta(days=1)).total_seconds())
        else:
            seconds_in_season = 0
        season_df = site.forecasts.copy()
        if vf is not None:
            season_df["valid_from"] = vf
            season_df["valid_to"] = vt
        hydrate_season_hydrograph_stats(site, db)
        site.get_seasonal_forecast_attributes_for_site(_, season_df, seconds_in_season)
    else:
        site.get_forecast_attributes_for_site(_, site.forecasts)


def _load_bulletin_from_api(
    horizon_type: str, forecast_year: int, forecast_horizon: int, sites_list,
) -> list:
    """Fetch bulletin records from the API and reconstruct site objects.

    FD-018 review #3: an earlier draft of this function tried to guess a
    reloaded site's own monthly target period (`_resolve_reload_month_target_
    period`, since deleted) by matching its persisted `(model,
    forecasted_discharge)` against both the main and m0 source frames. That
    heuristic was worse than doing nothing: it could confidently resolve to
    the WRONG frame (an edited discharge coinciding with the other frame's
    value), and a malformed `valid_from` in a matched row could raise and
    (via the broad `except Exception` below) silently discard the entire
    saved bulletin. `Bulletin` (sapphire/services/postprocessing/app/
    models.py), unlike `LongForecast`, has no `valid_from`/`valid_to` and no
    per-record marker for which card a row was added from, so there is no
    sound way to reconstruct this from the persisted record alone — see
    PP-040 (filed for the service owner) for the schema change a correct fix
    needs.

    Until PP-040 lands, reload is intentionally IDENTICAL to trunk: every
    reloaded site is hydrated from the bulletin-wide `(forecast_horizon,
    forecast_year)`, regardless of the `SAPPHIRE_SKILL_LEAD_AWARE` flag.

    Stale-cache note: `site` objects come from `dm.sites_list`, which is
    reused across station/horizon/date switches (`DataManager.load_station`
    replaces `_data` but not `_sites_list`). A site touched here may still
    carry a `bulletin_target_period` cached by an earlier in-session
    `_on_add`/`_on_add_m0` call for a *different* bulletin (e.g. an m0 add
    for July, followed by navigating to a MAIN-panel August bulletin for the
    same station code). That cached value must never leak into this reload,
    so it is unconditionally cleared below before repopulating the site.
    """
    try:
        df = db._read_data(_SERVICE, _RESOURCE, {
            "horizon":       horizon_type,
            "year":          forecast_year,
            "horizon_value": forecast_horizon,
            "limit":         1000,
        })

        if df.empty:
            logger.info("No bulletin records found for %s %s value=%s",
                        horizon_type, forecast_year, forecast_horizon)
            return []

        bulletin_sites = []
        for code in df["code"].unique():
            site_df = df[df["code"] == code]
            site = next((s for s in sites_list if s.code == str(code)), None)
            if site is None:
                logger.warning("Bulletin record references unknown site code '%s', skipping.", code)
                continue

            site.forecasts = pd.DataFrame([
                {
                    _('Model'):                row["model_type"],
                    _('Forecasted discharge'): row.get("forecasted_discharge"),
                    _('Forecast lower bound'): row.get("fc_lower"),
                    _('Forecast upper bound'): row.get("fc_upper"),
                    _('δ'):                    row.get("delta"),
                    _('s/σ'):                  row.get("sdivsigma"),
                    _('MAE'):                  row.get("mae"),
                    _('Accuracy'):             row.get("accuracy"),
                }
                for _idx, row in site_df.iterrows()
            ])
            site.forecasts = site.forecasts.where(site.forecasts.notna(), other=float('nan'))
            # Kill the stale-cache hazard: never trust a `bulletin_target_period`
            # left on this (possibly reused) site object from an earlier
            # in-session add/reload — see the docstring above. Reload always
            # uses the bulletin-wide (forecast_horizon, forecast_year), same
            # as trunk, for both flag states.
            if hasattr(site, "bulletin_target_period"):
                del site.bulletin_target_period
            _populate_forecast_attributes(site, horizon_type, forecast_year, forecast_horizon)
            bulletin_sites.append(site)

        logger.info("Loaded %d bulletin sites from API", len(bulletin_sites))
        return bulletin_sites

    except Exception as e:
        logger.error("Error loading bulletin from API: %s", e)
        return []


def _save_bulletin_to_api(horizon_type: str, forecast_year: int, forecast_horizon: int, bulletin_sites: list) -> None:
    """Upsert bulletin site records to the API."""
    records = []
    for site in bulletin_sites:
        records.extend(_site_to_records(horizon_type, forecast_year, forecast_horizon, site))

    if not records:
        logger.info("No bulletin records to save.")
        return

    db._save_data(_SERVICE, _RESOURCE, records)


def _delete_site_from_api(horizon_type: str, forecast_year: int, forecast_horizon: int, site) -> None:
    """Delete the bulletin record for one site from the API bulletin table."""
    db._delete_data(_SERVICE, _RESOURCE, {
        "horizon":       horizon_type,
        "year":          forecast_year,
        "horizon_value": forecast_horizon,
        "code":          site.code,
    })


# ---------------------------------------------------------------------------
# BulletinManager
# ---------------------------------------------------------------------------

class BulletinManager:
    """Encapsulates all bulletin state and wiring for the forecast dashboard."""

    def __init__(self, *, wm, cfg, dm, processing, write_to_excel):
        self.wm = wm
        self.cfg = cfg
        self.dm = dm
        self._processing = processing
        self._write_to_excel = write_to_excel

        # --- Load persisted bulletin sites ---
        self.bulletin_sites = _load_bulletin_from_api(
            wm.horizon_selector.value,
            wm.forecast_year,
            wm.forecast_horizon,
            dm.sites_list,
        )

        # --- Disable "Add to Bulletin" while pipeline runs ---
        # Set the initial state of the button based on whether the pipeline is running
        wm.add_to_bulletin_button.disabled = cfg.viz.app_state.pipeline_running
        wm.add_to_bulletin_m0_button.disabled = cfg.viz.app_state.pipeline_running
        # Watch for changes in pipeline_running and update the add_to_bulletin_button
        cfg.viz.app_state.param.watch(self._sync_add_button_to_pipeline, 'pipeline_running')

        # --- Initial table render & basin filter watcher ---
        self._update_bulletin_table()
        wm.basin_selector.param.watch(lambda event: self._update_bulletin_table(), 'value')
        wm.register_post_load_callback(self._on_horizon_change)

        # --- Button callbacks ---
        wm.add_to_bulletin_button.on_click(self._on_add)
        wm.add_to_bulletin_m0_button.on_click(self._on_add_m0)
        wm.remove_bulletin_button.on_click(self._on_remove)
        wm.write_bulletin_button.on_click(self._on_write)
        wm.bulletin_tabulator.on_edit(self._on_bulletin_edit)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _horizon_context(self) -> tuple[str, int, int]:
        return (
            self.wm.horizon_selector.value,
            self.wm.forecast_year,
            self.wm.forecast_horizon,
        )

    def _on_horizon_change(self) -> None:
        """Reload bulletin after station/period data has finished loading.

        Called via wm._post_load_callbacks so it always runs AFTER
        dm.load_station() and dm.get_bulletin_metadata() have both
        completed with the current horizon.
        """
        horizon = self.wm.horizon_selector.value
        try:
            _last_date, forecast_horizon, forecast_year = (
                self.dm.get_bulletin_metadata(horizon)
            )
        except (KeyError, IndexError, TypeError, ValueError):
            logger.info("No %s data available yet, clearing bulletin.", horizon)
            self.bulletin_sites = []
            self._update_bulletin_table()
            return

        self.wm.forecast_horizon = forecast_horizon
        self.wm.forecast_year = forecast_year

        self.bulletin_sites = _load_bulletin_from_api(
            horizon, forecast_year, forecast_horizon,
            self.dm.sites_list,
        )
        self._update_bulletin_table()

    def _update_bulletin_table(self) -> None:
        # Function to update the bulletin table
        create_bulletin_table(
            self.bulletin_sites, self.wm.basin_selector, self.wm.bulletin_tabulator
        )
    
    # Adding the watcher logic for disabling the "Add to Bulletin" button
    def _sync_add_button_to_pipeline(self, event) -> None:
        """Disable 'Add to Bulletin' while the pipeline is running."""
        # Update the state of 'Add to Bulletin' button based on pipeline_running status.
        self.wm.add_to_bulletin_button.disabled = event.new
        self.wm.add_to_bulletin_m0_button.disabled = event.new

    def _show_popup(self, message: str, alert_type: str = "success") -> None:
        self.wm.add_to_bulletin_popup.object = message
        self.wm.add_to_bulletin_popup.alert_type = alert_type
        self.wm.add_to_bulletin_popup.visible = True
        pn.state.add_periodic_callback(
            lambda: setattr(self.wm.add_to_bulletin_popup, 'visible', False),
            2000, count=1,
        )

    def _show_popup_m0(self, message: str, alert_type: str = "success") -> None:
        self.wm.add_to_bulletin_m0_popup.object = message
        self.wm.add_to_bulletin_m0_popup.alert_type = alert_type
        self.wm.add_to_bulletin_m0_popup.visible = True
        pn.state.add_periodic_callback(
            lambda: setattr(self.wm.add_to_bulletin_m0_popup, 'visible', False),
            2000, count=1,
        )

    def _show_write_popup(self, message: str, alert_type: str = "success") -> None:
        self.wm.write_bulletin_popup.object = message
        self.wm.write_bulletin_popup.alert_type = alert_type
        self.wm.write_bulletin_popup.visible = True
        pn.state.add_periodic_callback(
            lambda: setattr(self.wm.write_bulletin_popup, 'visible', False),
            3000, count=1,
        )

    # ------------------------------------------------------------------
    # Button handlers
    # ------------------------------------------------------------------
    def _month_hydration_params(self):
        """Target (month_in_year, year, days_in_month) for the monthly bulletin.

        The monthly forecast targets the month AFTER it is issued, so the norm
        and month length must be resolved for the target month/year (the
        month-in-year of the latest forecast row), not the current calendar
        month. Mirrors the loader in _load_bulletin_from_api.
        """
        _last_date, target_month, target_year = self.dm.get_bulletin_metadata("month")
        days_in_month = calendar.monthrange(target_year, target_month)[1]
        return target_month, target_year, days_in_month

    # Function to handle adding the current selection to the bulletin
    def _on_add(self, event=None) -> None:
        if self.cfg.viz.app_state.pipeline_running:
            print("Cannot add to bulletin while containers are running.")
            return

        forecast_df = self.wm.forecast_tabulator.value
        if forecast_df is None or forecast_df.empty:
            print("Forecast summary table is empty.")
            return

        selected_indices = self.wm.forecast_tabulator.selection or ([0] if len(forecast_df) > 0 else [])
        selected_rows = forecast_df.iloc[selected_indices]
        selected_station = self.wm.station_selector.value
        selected_site = next(
            (s for s in self.dm.sites_list if s.station_label == selected_station), None
        )
        if selected_site is None:
            print(f"Site '{selected_station}' not found in sites_list.")
            return
        
        # Assign forecasts to selected site object
        selected_site.forecasts = selected_rows.reset_index(drop=True)
        # Add forecast attributes to site object
        horizon = self.wm.horizon_selector.value
        if horizon == "month":
            target_month, _target_year, days_in_month = self._month_hydration_params()
            hydrate_month_hydrograph_stats(selected_site, target_month, db)
            selected_site.get_monthly_forecast_attributes_for_site(
                _, selected_rows, days_in_month,
            )
            # FD-018: capture this site's OWN target period (from the main
            # panel's own forecast data, via valid_from) so a later
            # same-session bulletin WRITE can honour it instead of
            # re-deriving a single bulletin-wide period from whatever
            # station happens to be loaded at that later time. A RELOAD
            # never honours this — see `_load_bulletin_from_api`, which
            # always clears it and uses the bulletin-wide period (identical
            # to trunk; a correct per-site reload needs a schema change,
            # PP-040). Gated on the flag; a resolution failure (missing/NaT
            # valid_from) is not fatal — it just leaves the site without an
            # override, so write falls back to the bulletin-wide period as
            # before.
            #
            # FD-018 review #5: the date-picker bound must be the SAME one
            # `create_forecast_summary_tabulator` built `wm.forecast_tabulator`
            # with for the main panel — `wm.date_picker.value`
            # (`PlotManager.update_forecast_tabulator` passes `self._wm.
            # date_picker`). Without it, a later, unrelated issue-date row
            # for the same model elsewhere in `dm.forecasts_all` outranks
            # the row the operator actually saw and picked while the date
            # picker was wound back to an earlier issue date.
            if skill_lead_aware_enabled():
                period = _resolve_month_target_period(
                    self.dm.forecasts_all, selected_site, self.wm.date_picker.value,
                )
                selected_site.bulletin_target_period = period
                if period is None:
                    logger.warning(
                        "_on_add: could not resolve site '%s' own target "
                        "month from valid_from; write will fall back to the "
                        "bulletin-wide target month.", selected_site.code,
                    )
            # Populate quarterly attributes for reservoir sites
            if 'вдхр' in (selected_site.punkt_name_ru or ''):
                q_df = db.get_long_forecasts_quarter(selected_site.code)
                if "code" in q_df.columns and "date" in q_df.columns and not q_df.empty:
                    filtered_q = q_df[q_df["code"] == selected_site.code]
                    if not filtered_q.empty:
                        filtered_q = filtered_q.sort_values("date", ascending=False).head(1)
                else:
                    filtered_q = pd.DataFrame()
                if not filtered_q.empty and "valid_from" in filtered_q.columns and "valid_to" in filtered_q.columns:
                    vf = pd.to_datetime(filtered_q["valid_from"].values[0])
                    vt = pd.to_datetime(filtered_q["valid_to"].values[0])
                    seconds_in_quarter = int((vt - vf + pd.Timedelta(days=1)).total_seconds())
                else:
                    seconds_in_quarter = 0
                filtered_q = _reshape_long_forecast_for_bulletin(filtered_q, _)
                selected_site.get_quarterly_forecast_attributes_for_site(_, filtered_q, seconds_in_quarter)
            else:
                selected_site.get_quarterly_forecast_attributes_for_site(_, pd.DataFrame(), 0)
        elif horizon == "season":
            s_df = db.get_long_forecasts_season(selected_site.code)
            model_short = None
            if not selected_rows.empty and _("Model") in selected_rows.columns:
                model_short = selected_rows[_("Model")].values[0]
            if (
                not s_df.empty
                and "code" in s_df.columns
                and "date" in s_df.columns
                and "model_short" in s_df.columns
                and model_short is not None
            ):
                filtered_s = s_df[
                    (s_df["code"] == selected_site.code)
                    & (s_df["model_short"] == model_short)
                ]
                if not filtered_s.empty:
                    filtered_s = filtered_s.sort_values("date", ascending=False).head(1)
            else:
                filtered_s = pd.DataFrame()
            if (
                not filtered_s.empty
                and "valid_from" in filtered_s.columns
                and "valid_to" in filtered_s.columns
            ):
                vf = pd.to_datetime(filtered_s["valid_from"].values[0])
                vt = pd.to_datetime(filtered_s["valid_to"].values[0])
                seconds_in_season = int((vt - vf + pd.Timedelta(days=1)).total_seconds())
            else:
                seconds_in_season = 0
            filtered_s = _reshape_long_forecast_for_bulletin(filtered_s, _)
            hydrate_season_hydrograph_stats(selected_site, db)
            selected_site.get_seasonal_forecast_attributes_for_site(_, filtered_s, seconds_in_season)
        else:
            selected_site.get_forecast_attributes_for_site(_, selected_rows)
        # Debugging: Print site details
        print(f"DEBUG: Adding site '{selected_site.code}' to bulletin: {selected_site.forecasts}")

        existing = next((s for s in self.bulletin_sites if s.code == selected_site.code), None)
        if existing is None:
            self.bulletin_sites.append(selected_site)
            print(f"DEBUG: Added new site '{selected_site.station_label}' to bulletin_sites.")
        else:
            self.bulletin_sites[self.bulletin_sites.index(existing)] = selected_site
            print(f"DEBUG: Updated existing site '{selected_site.station_label}' in bulletin_sites.")

        _save_bulletin_to_api(*self._horizon_context(), [selected_site])
        # Update bulletin table
        self._update_bulletin_table()
        self._show_popup(_("Added to bulletin table"))

    def _on_add_m0(self, event=None) -> None:
        """Handle adding the m0 forecast selection to the bulletin."""
        if self.cfg.viz.app_state.pipeline_running:
            print("Cannot add to bulletin while containers are running.")
            return

        forecast_df = self.wm.forecast_tabulator_m0.value
        if forecast_df is None or forecast_df.empty:
            print("Forecast m0 summary table is empty.")
            return

        selected_indices = self.wm.forecast_tabulator_m0.selection or ([0] if len(forecast_df) > 0 else [])
        selected_rows = forecast_df.iloc[selected_indices]
        selected_station = self.wm.station_selector.value
        selected_site = next(
            (s for s in self.dm.sites_list if s.station_label == selected_station), None
        )
        if selected_site is None:
            print(f"Site '{selected_station}' not found in sites_list.")
            return

        selected_site.forecasts = selected_rows.reset_index(drop=True)
        horizon = self.wm.horizon_selector.value
        if horizon == "month":
            target_month, _target_year, days_in_month = self._month_hydration_params()
            hydrate_month_hydrograph_stats(selected_site, target_month, db)
            selected_site.get_monthly_forecast_attributes_for_site(
                _, selected_rows, days_in_month,
            )
            # FD-018: capture this site's OWN target period from the m0
            # card's own forecast data (dm.long_forecasts_m0), NOT from the
            # main panel — the m0 target month can and does diverge from the
            # main panel's target month once the calendar rolls over. See
            # _on_add for the symmetric main-panel capture, and its comment
            # above about the write-only (not reload) scope of this
            # override.
            #
            # FD-018 review #5: the m0 card has no shared date-picker bound
            # of its own — `PlotManager.update_forecast_tabulator_m0` builds
            # `wm.forecast_tabulator_m0` using the m0 frame's OWN max date
            # (`m0['date'].max()`) instead of `wm.date_picker`, precisely
            # because the m0 card always shows the latest available m0
            # data. Mirror that here rather than reusing `wm.date_picker`
            # (the main panel's bound), which would be the wrong source for
            # this frame.
            if skill_lead_aware_enabled():
                m0_df = self.dm.long_forecasts_m0
                m0_date_bound = None
                if m0_df is not None and not m0_df.empty and "date" in m0_df.columns:
                    m0_date_bound = m0_df["date"].max()
                period = _resolve_month_target_period(
                    m0_df, selected_site, m0_date_bound,
                )
                selected_site.bulletin_target_period = period
                if period is None:
                    logger.warning(
                        "_on_add_m0: could not resolve site '%s' own target "
                        "month from valid_from; write will fall back to the "
                        "bulletin-wide target month.", selected_site.code,
                    )
            # Populate quarterly attributes for reservoir sites
            if 'вдхр' in (selected_site.punkt_name_ru or ''):
                q_df = db.get_long_forecasts_quarter(selected_site.code)
                if "code" in q_df.columns and "date" in q_df.columns and not q_df.empty:
                    filtered_q = q_df[q_df["code"] == selected_site.code]
                    if not filtered_q.empty:
                        filtered_q = filtered_q.sort_values("date", ascending=False).head(1)
                else:
                    filtered_q = pd.DataFrame()
                if not filtered_q.empty and "valid_from" in filtered_q.columns and "valid_to" in filtered_q.columns:
                    vf = pd.to_datetime(filtered_q["valid_from"].values[0])
                    vt = pd.to_datetime(filtered_q["valid_to"].values[0])
                    seconds_in_quarter = int((vt - vf + pd.Timedelta(days=1)).total_seconds())
                else:
                    seconds_in_quarter = 0
                filtered_q = _reshape_long_forecast_for_bulletin(filtered_q, _)
                selected_site.get_quarterly_forecast_attributes_for_site(_, filtered_q, seconds_in_quarter)
            else:
                selected_site.get_quarterly_forecast_attributes_for_site(_, pd.DataFrame(), 0)
        else:
            selected_site.get_forecast_attributes_for_site(_, selected_rows)

        existing = next((s for s in self.bulletin_sites if s.code == selected_site.code), None)
        if existing is None:
            self.bulletin_sites.append(selected_site)
        else:
            self.bulletin_sites[self.bulletin_sites.index(existing)] = selected_site

        _save_bulletin_to_api(*self._horizon_context(), [selected_site])
        self._update_bulletin_table()
        self._show_popup_m0(_("Added to bulletin table"))

    # Function to remove selected forecasts from the bulletin
    def _on_remove(self, event=None) -> None:
        """Handle removing selected forecasts from the bulletin."""
        # List of selected row indices
        selected = self.wm.bulletin_tabulator.selection
        if not selected:
            print("No forecasts selected for removal.")
            logger.warning("Remove action triggered, but no forecasts were selected.")
            return
        
        # Get the bulletin DataFrame from the tabulator
        bulletin_df = self.wm.bulletin_tabulator.value
        # Get the hydroposts of the selected rows
        selected_hydroposts = bulletin_df.iloc[selected][_('Hydropost')].unique()
        horizon_ctx = self._horizon_context()

        # Remove the selected sites from bulletin_sites and API
        for hydropost in selected_hydroposts:
            site = next((s for s in self.bulletin_sites if s.station_label == hydropost), None)
            if site is None:
                continue
            _delete_site_from_api(*horizon_ctx, site)
            self.bulletin_sites.remove(site)
            logger.info("Removed site from bulletin: %s", hydropost)
        
        # Update the bulletin table to reflect the changes
        self._update_bulletin_table()

        # Show a success message
        print("Selected forecasts have been removed from the bulletin.")
        self._show_popup(_("Selected forecasts have been removed from the bulletin."))
    
    def _on_bulletin_edit(self, event) -> None:
        """Live-save an in-cell edit of the Forecast bulletin table.

        Writes the edited value straight into the matching site.forecasts row
        and upserts to the API so it survives reloads and is used verbatim by
        'Write bulletin'. Identity columns (Hydropost/Model/Basin) are ignored.
        """
        field_to_display = {
            'station_label':        _('Hydropost'),
            'model_short':          _('Model'),
            'basin_ru':             _('Basin'),
            'forecasted_discharge': _('Forecasted discharge'),
            'fc_lower':             _('Forecast lower bound'),
            'fc_upper':             _('Forecast upper bound'),
            'delta':                _('δ'),
            'sdivsigma':            _('s/σ'),
            'mae':                  _('MAE'),
            'accuracy':             _('Accuracy'),
        }
        column_display = field_to_display.get(event.column, event.column)

        identity_columns = {_('Hydropost'), _('Model'), _('Basin')}
        if column_display in identity_columns:
            return

        try:
            df = self.wm.bulletin_tabulator.value
            row = df.iloc[event.row]
            station_label = row[_('Hydropost')]
            model = row[_('Model')]
        except Exception as exc:  # noqa: BLE001
            logger.error("_on_bulletin_edit: failed to resolve edited row (%s)", exc)
            return

        site = next((s for s in self.bulletin_sites if s.station_label == station_label), None)
        if site is None or not hasattr(site, 'forecasts') or site.forecasts is None:
            logger.warning(
                "_on_bulletin_edit: no bulletin site/forecasts found for '%s', edit dropped.",
                station_label,
            )
            return

        mask = site.forecasts[_('Model')] == model
        if not mask.any():
            logger.warning(
                "_on_bulletin_edit: no forecasts row for model '%s' on site '%s', edit dropped.",
                model, station_label,
            )
            return

        value = event.value
        with contextlib.suppress(TypeError, ValueError):
            value = float(value)

        site.forecasts.loc[mask, column_display] = value

        try:
            _save_bulletin_to_api(*self._horizon_context(), [site])
        except Exception as exc:  # noqa: BLE001
            logger.error("_on_bulletin_edit: failed to save edit to API (%s)", exc)

    # Function to handle writing bulletin to Excel
    def _on_write(self, event=None) -> None:
        """Handle writing the bulletin to Excel."""
        try:
            if not self.bulletin_sites:
                print("DEBUG: No sites in bulletin to write.")
                return

            selected_basin = self.wm.basin_selector.value
            filtered = (
                self.bulletin_sites.copy()
                if selected_basin == _("All basins")
                else [s for s in self.bulletin_sites if getattr(s, 'basin_ru', '') == selected_basin]
            )

            if not filtered:
                print("DEBUG: No sites in bulletin for the selected basin.")
                return

            # Debugging: print the site details being written            
            for site in filtered:
                print(f"DEBUG: Writing site '{site.code}' with forecasts: {site.forecasts}")

            horizon = self.wm.horizon_selector.value
            last_date, forecast_horizon, forecast_year = self.dm.get_bulletin_metadata(
                horizon
            )
            legacy_horizon = "decad" if horizon == "decade" else horizon
            header_date = resolve_bulletin_header_date(horizon, last_date, self.dm.forecasts_all)
            bulletin_header_info = self._processing.get_bulletin_header_info(header_date, legacy_horizon)

            # Re-hydrate hydrograph stats for every bulletin site so that
            # columns like last_year_q_pentad_mean / hydrograph_min/max fill
            # correctly even when a station was never opened interactively.
            # Wrapped in its own try/except so a stats-fetch failure never
            # prevents the bulletin from being written.
            try:
                rehydrate_sites_hydrograph_stats(filtered, horizon, forecast_horizon, db)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "_on_write: re-hydration step failed (%s); proceeding "
                    "without updated hydrograph stats.", exc,
                )

            # Refresh each site's forecast attributes from its (possibly edited)
            # site.forecasts so in-cell edits are reflected in the Excel output.
            # FD-018 review #4: `unresolved_codes` collects sites whose OWN
            # add-time target-period resolution genuinely failed — i.e.
            # `site.bulletin_target_period is None`, set explicitly by
            # `_on_add`/`_on_add_m0` when `_resolve_month_target_period`
            # couldn't find a usable `valid_from` (see there). A site that
            # never went through an add this session (or was reloaded — see
            # `_load_bulletin_from_api`, which always clears any cached
            # `bulletin_target_period`) has no such attribute at all and is
            # NOT reported here: reload has nothing to confirm or fail, it
            # always uses the bulletin-wide period, same as trunk. The write
            # still proceeds either way (never block on this), but the
            # operator is told which station(s) may carry the wrong month's
            # norm/day-count instead of silently seeing "Bulletin saved
            # successfully".
            unresolved_codes = []
            for site in filtered:
                try:
                    # FD-018: honour this site's own captured target period
                    # (set at add-time by _on_add/_on_add_m0, in THIS
                    # session) instead of the single bulletin-wide period
                    # derived above from whichever station/panel is
                    # currently loaded. Gated on the flag; flag-OFF keeps
                    # the exact trunk behavior (always the bulletin-wide
                    # period).
                    target_period = None
                    if (
                        skill_lead_aware_enabled()
                        and horizon == "month"
                        and hasattr(site, "bulletin_target_period")
                    ):
                        target_period = site.bulletin_target_period
                        if target_period is None:
                            unresolved_codes.append(getattr(site, "code", "?"))
                    _populate_forecast_attributes(
                        site, horizon, forecast_year, forecast_horizon, target_period,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "_on_write: attribute refresh failed for %s (%s); "
                        "writing with existing attributes.", getattr(site, 'code', '?'), exc,
                    )

            self._write_to_excel(
                self.dm.sites_list, filtered, bulletin_header_info,
                self.cfg.env_file_path, horizon=legacy_horizon,
            )
            print("DEBUG: Bulletin written to Excel successfully.")
            # Refresh the file downloader panel
            self.wm.downloader.refresh_file_list()
            if unresolved_codes:
                logger.warning(
                    "_on_write: wrote bulletin with unresolved target "
                    "month for site(s): %s", unresolved_codes,
                )
                self._show_write_popup(
                    _("Bulletin saved, but the target month could not be "
                      "confirmed for: ") + ", ".join(str(c) for c in unresolved_codes),
                    alert_type="warning",
                )
            else:
                self._show_write_popup(_("Bulletin saved successfully"))
        except Exception as e:
            logger.error("Error writing bulletin to Excel: %s", e, exc_info=True)
            self._show_write_popup(_("Failed to write bulletin"), alert_type="danger")


# ---------------------------------------------------------------------------
# Table renderer
# ---------------------------------------------------------------------------

# Function to create the bulletin table
def create_bulletin_table(bulletin_sites, select_basin_widget, bulletin_tabulator):
    print("Creating/updating bulletin table...")

    if bulletin_sites:
        data = []
        for site in bulletin_sites:
            for _idx, forecast_row in site.forecasts.iterrows():
                data.append({
                    _('Hydropost'):            site.station_label,
                    _('Model'):                forecast_row.get(_('Model'), ''),
                    _('Basin'):                getattr(site, 'basin_ru', ''),
                    _('Forecasted discharge'): forecast_row.get(_('Forecasted discharge'), ''),
                    _('Forecast lower bound'): forecast_row.get(_('Forecast lower bound'), ''),
                    _('Forecast upper bound'): forecast_row.get(_('Forecast upper bound'), ''),
                    _('δ'):                    forecast_row.get('δ', ''),
                    _('s/σ'):                  forecast_row.get('s/σ', ''),
                    _('MAE'):                  forecast_row.get('MAE', ''),
                    _('Accuracy'):             forecast_row.get(_('Accuracy'), ''),
                })
        bulletin_df = pd.DataFrame(data)

        # Apply 'Select Basin' filter if applicable
        selected_basin = select_basin_widget.value
        if selected_basin != _("All basins"):
            bulletin_df = bulletin_df[bulletin_df[_('Basin')] == selected_basin]

        bulletin_tabulator.value = bulletin_df
    else:
        # Empty DataFrame with predefined columns
        bulletin_tabulator.value = pd.DataFrame(columns=[
            _('Hydropost'), _('Model'), _('Basin'),
            _('Forecasted discharge'), _('Forecast lower bound'), _('Forecast upper bound'),
            _('δ'), _('s/σ'), _('MAE'), _('Accuracy'),
        ])

    print("Bulletin table updated.")
