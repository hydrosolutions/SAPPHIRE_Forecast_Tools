import calendar
import datetime as dt
import os

import pandas as pd
import panel as pn
from skill_lead_aware_flag import skill_lead_aware_enabled
from src.file_downloader import FileDownloader
from src.gettext_config import _, p_

from dashboard.config import import_tag_library

tl = import_tag_library()

# Not used as widget
def create_date_picker(forecast_df):
    """Create date picker widget."""
    # Widget for date selection, always visible
    # Dates here refer to the forecast issue day, i.e. 1 day before the first day of the forecast pentad.
    if 'date' in forecast_df.columns and not forecast_df.empty:
        max_val = forecast_df['date'].max()
    else:
        max_val = None
    if max_val is not None and not pd.isna(max_val) and hasattr(max_val, 'date'):
        forecast_date = max_val.date()
    else:
        forecast_date = dt.datetime.now().date()

    date_picker = pn.widgets.DatePicker(
        name=_("Select date:"),
        start=dt.datetime(forecast_date.year - 1, 1, 5).date(),
        end=forecast_date,
        value=forecast_date
    )
    return date_picker


# Used inside pentad card
def create_pentad_selector(last_date):
    """Create pentad selection widget."""
    # Determine the corresponding pentad
    current_pentad = tl.get_pentad_for_date(last_date)
    print(f"   dbg: current_pentad: {current_pentad}")

    # Create a dictionary mapping each pentad description to its pentad_in_year value
    pentad_options = {
        f"{i+1}{_('st pentad of')} {calendar.month_name[month]}" if i == 0 else
        f"{i+1}{_('nd pentad of')} {calendar.month_name[month]}" if i == 1 else
        f"{i+1}{_('rd pentad of')} {calendar.month_name[month]}" if i == 2 else
        f"{i+1}{_('th pentad of')} {calendar.month_name[month]}": i + (month-1)*6 + 1
        for month in range(1, 13) for i in range(6)
    }

    # Create the dropdown widget for pentad selection
    pentad_selector = pn.widgets.Select(
        name=_("Select Pentad"),
        options=pentad_options,
        value=current_pentad,
        margin=(0, 0, 0, 0)
    )
    return pentad_selector


# Not used as widget
def create_decad_selector(last_date):
    """Create decad selection widget."""
    current_decad = tl.get_decad_for_date(last_date)
    print(f"   dbg: current_decad: {current_decad}")

    # Create a dictionary mapping each decade description to its decad_in_year value
    decad_options = {
        f"{i+1}{_('st decade of')} {calendar.month_name[month]}" if i == 0 else
        f"{i+1}{_('nd decade of')} {calendar.month_name[month]}" if i == 1 else
        f"{i+1}{_('rd decade of')} {calendar.month_name[month]}" if i == 2 else
        f"{i+1}{_('th decade of')} {calendar.month_name[month]}": i + (month - 1) * 3 + 1
        for month in range(1, 13) for i in range(3)
    }

    # Create the dropdown widget for decad selection
    decad_selector = pn.widgets.Select(
        name=_("Select Decad"),
        options=decad_options,
        value=current_decad,
        margin=(0, 0, 0, 0)
    )
    return decad_selector


# ============================== Widgets for sidebar content ==============================
def create_horizon_selector(display_ML_forecasts: bool = True):
    """Create forecast horizon selection widget.

    When display_ML_forecasts is False, only short-term horizons (pentad,
    decade) are exposed; month and season are hidden because they require
    ML-driven long-term forecasts.
    """
    horizon_types = {
        _("pentad"): "pentad",
        _("decade"): "decade",
    }
    if display_ML_forecasts:
        horizon_types[_("month")] = "month"
        horizon_types[_("season")] = "season"
    horizon_selector = pn.widgets.Select(
        name=_("Select forecast horizon:"),
        options=horizon_types,
        value="pentad",
        margin=(0, 0, 0, 0)
    )
    return horizon_selector


def create_horizon_card(horizon_selector, width):
    """Create forecast horizon selection card widget."""
    horizon_card = pn.Card(
        pn.Column(horizon_selector),
        title=_('Horizon:'),
        width_policy='fit', width=width,
        collapsed=False
    )
    horizon_card.visible = True

    return horizon_card


# Used inside station_card
def create_station_selector(station_dict):
    """Create station (hydropost) selector widget."""

    # Widget for station selection, always visible
    _default_station_value = None
    if station_dict:
        try:
            _default_station_value = station_dict[next(iter(station_dict))][0] # "15189 - Аламедин  -  у.р.Чункурчак"
        except Exception:
            _default_station_value = None

    station_selector = pn.widgets.Select(
        name=_("Select discharge station:"),
        groups=station_dict if station_dict else {_("No stations available"): []},
        value=_default_station_value,
        margin=(0, 0, 0, 0)
    )
    return station_selector


# Used inside sidebar_content
def create_station_card(station_selector):
    """Create station selection card widget."""
    station_card = pn.Card(
        pn.Column(station_selector),
        title=_('Hydropost:'),
        width_policy='fit',
        width=station_selector.width,
        collapsed=False
    )
    station_card.visible = True

    return station_card


# Used inside forecast_card
def create_model_checkbox(model_dict):
    """Create forecast model selection checkbox widget."""
    # Widget for forecast model selection, only visible in forecast tab
    # a given hydropost/station.
    model_checkbox = pn.widgets.CheckBoxGroup(
        name=_("Select forecast model:"),
        options=model_dict,
        value=[],
        #value=[model_dict[model] for model in current_model_pre_selection],
        #width=200,  # 280
        margin=(0, 0, 0, 0),
        sizing_mode='stretch_width',
        css_classes=['checkbox-label']
    )
    return model_checkbox


# Used inside forecast_card
def create_range_widgets():
    """Create forecast range selection widgets."""

    range_selector = pn.widgets.Select(
        options=[_("delta"), _("Manual range, select value below"), _("min[delta, %]")],
        value=_("delta"),
        margin=(0, 0, 0, 0)
    )

    range_slider = pn.widgets.IntSlider(
        name=_("Manual range (%)"),
        start=0,
        end=100,
        value=20,
        step=1,
        margin=(20, 0, 0, 0)  # martin=(top, right, bottom, left)
    )
    range_slider.visible = False

    range_radiobutton =pn.widgets.RadioButtonGroup(
        name=_("Show ranges in figure:"),
        options=[_("Yes"), _("No")],
        value=_("No")
    )

    return range_selector, range_slider, range_radiobutton


# Used inside forecast_card
def create_apply_changes_button():
    """Create apply changes button."""
    apply_changes_button = pn.widgets.Button(name=_("Apply changes"), button_type="success")
    return apply_changes_button


# Used inside sidebar_content
def create_forecast_card(range_selector, range_slider, model_checkbox, range_radiobutton, apply_changes_button, width):
    """Create forecast configuration card widget."""

    # Forecast card for sidepanel
    range_label = pn.pane.Markdown(
        _("Select forecast range (for both summary table and figures):"),
        styles={"white-space": "normal"},  # force wrapping
        margin=(0, 0, -5, 0)
    )

    model_title = pn.pane.Markdown(
        _("Select forecast model (for figures only):"), margin=(0, 0, -15, 0)
    )  # margin=(top, right, bottom, left)

    range_selection_title = pn.pane.Markdown(_("Show ranges (for figures only):"), margin=(0, 0, -15, 0))

    forecast_card = pn.Card(
        pn.Column(
            range_label,
            range_selector,
            range_slider,
            pn.layout.Divider(),
            model_title,
            model_checkbox,
            range_selection_title,
            range_radiobutton,
            apply_changes_button
        ),
        title=_('Forecast configuration:'),
        width_policy='fit', width=width,
        collapsed=False
    )
    # Initially hide the card
    forecast_card.visible = False

    return forecast_card


# Used inside basin_card
def create_basin_selector(station_dict):
    """Create basin selection widget."""
    basin_names = list(station_dict.keys())
    basin_names.insert(0, _("All basins"))  # Add 'Select all basins' as the first option

    # Create the 'Select Basin' widget
    basin_selector = pn.widgets.Select(
        name=_("Select basin:"),
        options=basin_names,
        value=_("All basins"),  # Default value
        margin=(0, 0, 0, 0)
    )
    return basin_selector


# Used inside sidebar_content
def create_basin_card(basin_selector, width):
    """Create basin selection card widget."""
    # Basin card
    basin_card = pn.Card(
        pn.Column(
            basin_selector
            ),
        title=_('Basin:'),
        width_policy='fit', width=width,
        collapsed=False
    )
    basin_card.visible = False

    return basin_card


# Used inside sidebar_content
def create_message_pane(data):
    stations_iehhf = None

    # Test if we have sites in stations_iehhf which are not present in forecasts_all
    # Placeholder for a message pane
    message_pane = pn.pane.Markdown("", width=300)
    if stations_iehhf is not None:
        missing_sites = set(stations_iehhf) - set(data["forecasts_all"]['code'].unique())
        if missing_sites:
            missing_sites_message = f"_('WARNING: The following sites are missing from the forecast results:') {missing_sites}. _('No forecasts are currently available for these sites. Please make sure your forecast models are configured to produce results for these sites, re-run hindcasts manually and re-run the forecast.')"
            message_pane.object = missing_sites_message

    # Add message to message_pane, depending on the status of recent data availability
    latest_data_is_current_year = True
    if not latest_data_is_current_year:
        message_pane.object += "\n\n" + _("WARNING: The latest data available is not for the current year. Forecast Tools may not have access to iEasyHydro. Please contact the system administrator.")
    
    return message_pane


# ============================== Generic functions ==============================

def get_pane_alert(msg):
    return pn.pane.Alert(
        "⚠️ " + _("Warning:") + " " + msg,
        alert_type="warning",
        sizing_mode="stretch_width"
    )


def refresh_predictors_warning(warning_col, station, data):
    warning_col.objects = []
    warning = get_predictors_warning(station, data)
    if warning:
        warning_col.append(warning)

def get_period_warning(horizon, forecast_period, forecast_year, today=None):
    """Warn when the displayed forecast's target period differs from the
    current period for this horizon (e.g. a pentad-31 forecast still shown
    once we're in pentad 32). Returns an alert pane or None.
    """
    if forecast_period is None or forecast_year is None:
        return None
    if today is None:
        today = dt.datetime.now().date()
    if horizon == "pentad":
        current = tl.get_pentad_in_year(today)
    elif horizon == "decade":
        current = tl.get_decad_in_year(today)
    elif horizon == "month":
        current = today.month
    elif horizon == "quarter":
        current = (today.month - 1) // 3 + 1
    elif horizon == "season":
        current = 1  # one season per year — only the year distinguishes periods
    else:
        return None
    try:
        outdated = (int(forecast_year), int(forecast_period)) < (today.year, int(current))
    except (TypeError, ValueError):
        return None
    if not outdated:
        return None
    return get_pane_alert(
        _(
            "The displayed %(horizon)s forecast is for %(horizon)s %(period)s of "
            "%(year)s, but the current %(horizon)s is %(current)s of %(today_year)s. "
            "This forecast may be outdated."
        ) % {
            "horizon": _(horizon),
            "period": forecast_period,
            "year": forecast_year,
            "current": current,
            "today_year": today.year,
        }
    )


def refresh_forecast_warning(warning_col, station, data, date_value,
                             horizon=None, forecast_period=None, forecast_year=None):
    warning_col.objects = []
    w_models = get_forecast_warning(station, data, date_value)
    if w_models:
        warning_col.append(w_models)
    w_period = get_period_warning(horizon, forecast_period, forecast_year)
    if w_period:
        warning_col.append(w_period)

# ============================== Widgets for Predictors Tab ==============================

def get_predictors_warning(station, data):
    # predictors_warning.objects = []  # clear old content
    # today_date = today.date()
    today_date = dt.datetime.now().date()
    year_col = str(today_date.year)
    filtered = data["hydrograph_day_all"][
        (data["hydrograph_day_all"]["station_labels"] == station.value) &
        (data["hydrograph_day_all"]["date"] == pd.to_datetime(today_date))
    ]

    if not filtered.empty:
        if year_col in filtered.columns and pd.notna(filtered[year_col].iloc[0]):
            print(f"{year_col} has a value:", filtered[year_col].iloc[0])
            return
        else:
            print(f"{year_col} is NaN/empty")
            return get_pane_alert(
                _("No discharge record available today for %(station)s")
                % {"station": station.value}
            )
    else:
        print("No record for today and given station")
        return get_pane_alert(
            _("No discharge record available today for %(station)s")
            % {"station": station.value}
        )

def create_predictors_warning(station, data):
    col = pn.Column()
    warning = get_predictors_warning(station, data)
    if warning:
        col.append(warning)
    return col

# ============================== Widgets for Forecast Tab ==============================

def get_forecast_warning(station, data, date_picker_value):
    forecasts_all = data.get("forecasts_all")
    if (
        forecasts_all is None
        or forecasts_all.empty
        or "station_labels" not in forecasts_all.columns
    ):
        return get_pane_alert(
            _("No forecast data available for %(station)s on %(date)s.")
            % {"station": station.value, "date": date_picker_value}
        )

    station_rows = forecasts_all[forecasts_all["station_labels"] == station.value]
    if station_rows.empty:
        return get_pane_alert(
            _("No forecast data available for %(station)s on %(date)s.")
            % {"station": station.value, "date": date_picker_value}
        )

    expected_models = set(station_rows["model_short"].dropna().unique())

    on_date = station_rows[
        station_rows["date"] == pd.to_datetime(date_picker_value)
    ]
    present_models = set(
        on_date.loc[on_date["forecasted_discharge"].notna(), "model_short"].dropna()
    )

    missing_models = sorted(expected_models - present_models)
    if missing_models:
        if not present_models:
            # No model has a forecast for this date — don't enumerate every model.
            return get_pane_alert(
                _("No forecast data available for %(station)s on %(date)s.")
                % {"station": station.value, "date": date_picker_value}
            )
        return get_pane_alert(
            _("No forecast data available for models %(models)s at %(station)s on %(date)s.")
            % {
                "models": ", ".join(missing_models),
                "station": station.value,
                "date": date_picker_value,
            }
        )
    return


def create_forecast_warning(station, data, date_value):
    col = pn.Column()
    warning = get_forecast_warning(station, data, date_value)
    if warning:
        col.append(warning)
    return col


# Used inside Summary table Plot (Forecast Tab)
def create_add_to_bulletin_button():
    add_to_bulletin_button = pn.widgets.Button(
        name=_("Add to bulletin"), 
        button_type="primary"
    )
    return add_to_bulletin_button


# Used inside forecast_summary_table
def create_forecast_tabulator():
    # Create a single Tabulator instance
    forecast_tabulator = pn.widgets.Tabulator(
        theme='bootstrap',
        show_index=False,
        selection=[],
        selectable='checkbox-single',
        sizing_mode='stretch_both',
        height=None
    )
    return forecast_tabulator


# Used for Summary table Plot (Forecast Tab)
def create_forecast_summary_table(forecast_tabulator):
    # Same Tabulator in both tabs
    forecast_summary_table = pn.panel(
        forecast_tabulator,
        sizing_mode='stretch_width'
    )
    return forecast_summary_table


# Used for Hydrograph Plot (Forecast Tab)
def create_aggregate_radiobutton():
    aggregate_radiobutton = pn.widgets.RadioButtonGroup(
        name=_("Show daily data:"),
        options=[_("Yes"), _("No")],
        value=_("No")
    )
    return aggregate_radiobutton

# ============================== Widgets for Bulletin Tab ==============================

# Used inside bulletin_table
def create_bulletin_buttons():
    """Create bulletin action buttons."""
    
    # Button to remove selected forecasts from the bulletin
    remove_bulletin_button = pn.widgets.Button(
        name=_("Remove Selected"),
        button_type='danger',
        margin=(10, 0, 0, 0)  # top, right, bottom, left
    )

    # Write bulletin button
    write_bulletin_button = pn.widgets.Button(
        name=_("Write bulletin"),
        button_type='primary',
        description=_("Write bulletin to Excel")
    )

    return remove_bulletin_button, write_bulletin_button


# Used inside bulletin_table
def create_bulletin_tabulator():
    # Initialize the bulletin_tabulator as a global Tabulator with predefined columns and grouping
    bulletin_tabulator = pn.widgets.Tabulator(
        value=pd.DataFrame(columns=[
            _('Hydropost'), _('Model'), _('Basin'),
            _('Forecasted discharge'), _('Forecast lower bound'), _('Forecast upper bound'),
            _('δ'), _('s/σ'), _('MAE'), _('Accuracy')
        ]),
        theme='bootstrap',
        configuration={
            'columns': [
                {'field': 'station_label', 'title': _('Hydropost')},
                {'field': 'model_short', 'title': _('Model')},
                {'field': 'basin_ru', 'title': _('Basin')},
                {'field': 'forecasted_discharge', 'title': _('Forecasted discharge')},
                {'field': 'fc_lower', 'title': _('Forecast lower bound')},
                {'field': 'fc_upper', 'title': _('Forecast upper bound')},
                {'field': 'delta', 'title': _('δ')},
                {'field': 'sdivsigma', 'title': _('s/σ')},
                {'field': 'mae', 'title': _('MAE')},
                {'field': 'accuracy', 'title': _('Accuracy')},
            ],
            'columnFilters': True  # Enable column filtering if needed
        },
        show_index=False,
        height=300,
        selectable='checkbox',  # Allow multiple selections for removal
        sizing_mode='stretch_width',
        groupby=[_('Basin')],  # Enable grouping by 'Basin'
        layout='fit_columns',
        editors={
            _('Hydropost'): None,
            _('Model'): None,
            _('Basin'): None,
        },
    )
    return bulletin_tabulator


# Used inside bulletin_table
def create_add_to_bulletin_popup():
    # Create the pop-up notification pane (initially hidden)
    add_to_bulletin_popup = pn.pane.Alert(_("Added to bulletin"), alert_type="success", visible=False)
    return add_to_bulletin_popup


def create_forecast_info_pane():
    """Create a Markdown pane for displaying forecast info text."""
    return pn.pane.Markdown("", sizing_mode="stretch_width")


def format_horizon_info(
    horizon, forecast_horizon, forecast_year, last_date, metadata_is_current=True,
):
    """Build the header label describing the active forecast horizon.

    Args:
        metadata_is_current: Whether (forecast_horizon, forecast_year) were
            actually resolved for `horizon` (vs. left stale from a
            previously-selected horizon after a failed metadata refresh —
            see widget_manager._on_change). Only consulted by the
            flag-gated "month" branch, where trusting a stale cross-horizon
            period number as a month index can crash or silently mislabel
            the month; defaults to True so all other callers (and every
            other horizon branch) are unaffected.

    See widget_manager._refresh_horizon_info_pane for the call site.
    """
    import datetime as _dt
    if last_date is None:
        return ""

    # Stable English month names used as gettext msgids (independent of any
    # system locale set on the `calendar` module). pgettext resolves the
    # correctly-cased translation per locale; English falls back to these.
    months_en = (
        "", "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
    )

    def month_name(n, case):
        # case is "genitive" (pentad/decade and the produced-at date) or
        # "nominative" (the month horizon target month).
        return p_(case, months_en[n])

    production_date = last_date - _dt.timedelta(days=1)

    if horizon == "pentad":
        from forecast_library import get_pentad_from_pentad_in_year
        pim = get_pentad_from_pentad_in_year(forecast_horizon)
        body = _("pentad: %(pim)s of %(month)s %(year)s (%(num)s)") % {
            "pim": pim,
            "month": month_name(last_date.month, "genitive"),
            "year": last_date.year,
            "num": forecast_horizon,
        }
    elif horizon == "decade":
        from forecast_library import get_decad_from_decad_in_year
        dim = get_decad_from_decad_in_year(forecast_horizon)
        body = _("decade: %(dim)s of %(month)s %(year)s (%(num)s)") % {
            "dim": dim,
            "month": month_name(last_date.month, "genitive"),
            "year": last_date.year,
            "num": forecast_horizon,
        }
    elif horizon == "month":
        # Defect J: use the resolved target month/year passed in (lead-aware);
        # the legacy kill-switch recomputes them from the production date.
        if skill_lead_aware_enabled():
            if not metadata_is_current:
                # forecast_horizon/forecast_year were resolved for a
                # DIFFERENT horizon (the refresh for this one failed and the
                # cache was left stale) — do not render a month from them.
                return ""
            target_month_num = forecast_horizon
            target_year = forecast_year
        else:
            target_month_num = (production_date.month % 12) + 1
            target_year = production_date.year
        body = _("month: %(month)s %(year)s") % {
            "month": month_name(target_month_num, "nominative"),
            "year": target_year,
        }
    elif horizon == "season":
        body = _("season: April–September")
    else:
        return ""

    produced = _(", produced on %(prod)s") % {
        "prod": production_date.strftime("%b %-d, %Y"),
        "day": production_date.day,
        "month": month_name(production_date.month, "genitive"),
        "year": production_date.year,
    }
    return body + produced


def create_horizon_info_pane():
    """Header pane for the per-horizon info, vertically centered next to the title."""
    return pn.pane.HTML(
        "",
        margin=0,
        align=("start", "center"),
        sizing_mode="stretch_width",
        styles={
            "color": "white",
            "font-size": "0.8rem",
            "line-height": "1",
            "padding-left": "12px",
        },
    )


# Used for Forecast bulletin Plot (Bulletin Tab)
def create_bulletin_table(bulletin_tabulator, remove_bulletin_button, add_to_bulletin_popup):
    bulletin_table = pn.Column(
        bulletin_tabulator,  # Add the global Tabulator directly
        pn.Row(remove_bulletin_button, sizing_mode='stretch_width'),
        add_to_bulletin_popup  # Include the popup for success messages
    )
    return bulletin_table


def _bulletin_folder_for_horizon(horizon: str) -> str:
    """Resolve the on-disk bulletin folder for a given horizon value.

    The horizon_selector exposes 'decade' but bulletin files are written
    to 'bulletins/decad/...' (legacy convention; see bulletins.py).
    """
    legacy = "decad" if horizon == "decade" else horizon
    return os.path.join(
        os.getenv('ieasyreports_report_output_path'),
        'bulletins', legacy,
    )


def create_downloader_and_panel(horizon):
    bulletin_folder = _bulletin_folder_for_horizon(horizon)
    downloader = FileDownloader(bulletin_folder)
    bulletin_download_panel = downloader.panel()
    return downloader, bulletin_download_panel


# Used inside publish_bulletin_card (see widget_manager.py)
def create_publish_horizon_multiselect(display_ML_forecasts: bool = True):
    """Create the multi-horizon selector for the Publish bulletin card.

    Mirrors create_horizon_selector's option set (pentad/decade always
    available; month/season gated on display_ML_forecasts), but as a
    CheckBoxGroup so more than one horizon can be selected at once.
    """
    horizon_types = {
        _("pentad"): "pentad",
        _("decade"): "decade",
    }
    if display_ML_forecasts:
        horizon_types[_("month")] = "month"
        horizon_types[_("season")] = "season"
    publish_horizon_multiselect = pn.widgets.CheckBoxGroup(
        name=_("Select horizons to publish:"),
        options=horizon_types,
        value=[],
        margin=(0, 0, 0, 0),
    )
    return publish_horizon_multiselect


# Used inside publish_bulletin_card (see widget_manager.py)
def create_publish_station_multiselect(station_dict):
    """Create the multi-station selector for the Publish bulletin card.

    Panel's MultiSelect (unlike Select, used by create_station_selector)
    has no native optgroup/`groups` support, so the basin-grouped
    station_dict is flattened into one flat options list here. Stations
    are still drawn from the same basin-ordered pool as station_selector;
    only the visual basin grouping is not preserved by this widget type.
    """
    flat_options = {
        label: label
        for labels in (station_dict or {}).values()
        for label in labels
    }
    publish_station_multiselect = pn.widgets.MultiSelect(
        name=_("Select stations to publish:"),
        options=flat_options,
        size=8,
        margin=(0, 0, 0, 0),
        sizing_mode="stretch_width",
    )
    return publish_station_multiselect


# Used inside publish_bulletin_card (see widget_manager.py)
def create_generate_links_button():
    """Create the 'Generate links' button for the Publish bulletin card."""
    generate_links_button = pn.widgets.Button(
        name=_("Generate links"),
        button_type="primary",
    )
    return generate_links_button


# Used inside publish_bulletin_card (see widget_manager.py)
def create_publish_results_pane():
    """Create the results pane showing generated links / warnings / errors."""
    return pn.pane.Markdown("", sizing_mode="stretch_width")


# ============================== Widgets for Language and Auth ==============================

# Used in dashboard header
def create_language_buttons():
    # Create language selection buttons as links that reload the page with the selected language
    buttons = []
    for lang_name, lang_code in {'English': 'en_CH', 'Русский': 'ru_KG'}.items():
        # Create a hyperlink styled as a button
        href = pn.state.location.pathname + f'?lang={lang_code}'

        # current_user = check_current_user()

        # if current_user:
        #     # Log language change before redirecting
        #     log_user_activity(current_user, 'language_change')

        link = f'<a href="{href}" style="margin-right: 10px; padding: 5px 10px; background-color: white; color: #307086; text-decoration: none; border-radius: 4px;">{lang_name}</a>'
        buttons.append(link)
    # Combine the links into a single Markdown pane
    language_buttons = pn.pane.Markdown(' '.join(buttons))
    language_buttons.visible = False  # Initially hidden

    return language_buttons

# Used in login_form
def create_login_widgets():
    # Create widgets for login
    username_input = pn.widgets.TextInput(name=_('Username'), placeholder=_('Enter your username'))
    password_input = pn.widgets.PasswordInput(name=_('Password'), placeholder=_('Enter your password'))
    login_submit_button = pn.widgets.Button(name=_('Login'), button_type='primary')
    login_feedback = pn.pane.Markdown("", visible=False)

    return username_input, password_input, login_submit_button, login_feedback

# Used in dashboard main
def create_login_form(username_input, password_input, login_submit_button, login_feedback):
    # Create layout components
    login_form = pn.Column(
        pn.pane.Markdown(f"# {_('Login')}"),
        username_input,
        password_input,
        login_submit_button,
        login_feedback
    )
    return login_form

# Used in dashboard header
def create_logout_button():
    logout_button = pn.widgets.Button(name="Logout", button_type="danger")
    return logout_button

# Used in logout_panel
def create_logout_confirm_widgets():
    # Create logout confirmation widgets
    logout_confirm = pn.pane.Markdown("**Are you sure you want to log out?**", visible=False)
    logout_yes = pn.widgets.Button(name="Yes", button_type="success", visible=False)
    logout_no = pn.widgets.Button(name="No", button_type="danger", visible=False)
    return logout_confirm, logout_yes, logout_no

# Used in dashboard header
def create_logout_panel(logout_confirm, logout_yes, logout_no):
    logout_panel = pn.Column(
        logout_confirm,
        pn.Row(logout_yes, logout_no)
    )
    return logout_panel