import calendar
import pandas as pd
import panel as pn

from src.gettext_config import _
from dashboard.logger import setup_logger
from dashboard.utils import hydrate_month_hydrograph_stats, hydrate_season_hydrograph_stats, rehydrate_sites_hydrograph_stats
from src import db  # _read_data, _save_data, _delete_data live here

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


def _load_bulletin_from_api(horizon_type: str, forecast_year: int, forecast_horizon: int, sites_list) -> list:
    """Fetch bulletin records from the API and reconstruct site objects."""
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
            _ensure_site_defaults(site)
            if horizon_type == 'month':
                days_in_month = calendar.monthrange(forecast_year, forecast_horizon)[1]
                hydrate_month_hydrograph_stats(site, forecast_horizon, db)
                site.get_monthly_forecast_attributes_for_site(_, site.forecasts, days_in_month)
                if 'вдхр' in (site.punkt_name_ru or ''):
                    q_df = db.get_long_forecasts_quarter(site.code, horizon_value=1)
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
                s_df = db.get_long_forecasts_season(site.code)
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
                hydrate_season_hydrograph_stats(site, db)
                site.get_seasonal_forecast_attributes_for_site(_, filtered_s, seconds_in_season)
            else:
                site.get_forecast_attributes_for_site(_, site.forecasts)
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

    # ------------------------------------------------------------------
    # Button handlers
    # ------------------------------------------------------------------
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
            import calendar
            from datetime import date as _date
            now = _date.today()
            days_in_month = calendar.monthrange(now.year, now.month)[1]
            hydrate_month_hydrograph_stats(selected_site, now.month, db)
            selected_site.get_monthly_forecast_attributes_for_site(
                _, selected_rows, days_in_month,
            )
            # Populate quarterly attributes for reservoir sites
            if 'вдхр' in (selected_site.punkt_name_ru or ''):
                q_df = db.get_long_forecasts_quarter(selected_site.code, horizon_value=1)
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
            import calendar
            from datetime import date as _date
            now = _date.today()
            days_in_month = calendar.monthrange(now.year, now.month)[1]
            hydrate_month_hydrograph_stats(selected_site, now.month, db)
            selected_site.get_monthly_forecast_attributes_for_site(
                _, selected_rows, days_in_month,
            )
            # Populate quarterly attributes for reservoir sites
            if 'вдхр' in (selected_site.punkt_name_ru or ''):
                q_df = db.get_long_forecasts_quarter(selected_site.code, horizon_value=1)
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

            self._write_to_excel(
                self.dm.sites_list, filtered, bulletin_header_info,
                self.cfg.env_file_path, horizon=legacy_horizon,
            )
            print("DEBUG: Bulletin written to Excel successfully.")
            # Refresh the file downloader panel
            self.wm.downloader.refresh_file_list()
        except Exception as e:
            logger.error("Error writing bulletin to Excel: %s", e, exc_info=True)


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
