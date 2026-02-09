import os
import panel as pn
import pandas as pd
import datetime as dt
import calendar

from src.file_downloader import FileDownloader
from src.gettext_config import _
from src.auth_utils import log_user_activity, check_current_user
from dashboard.config import import_tag_library


tl = import_tag_library()

# Not used as widget
def create_date_picker(forecast_df):
    """Create date picker widget."""
    # Widget for date selection, always visible
    # Dates here refer to the forecast issue day, i.e. 1 day before the first day of the forecast pentad.
    forecast_date = forecast_df['date'].max().date()

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
def create_pentad_card(pentad_selector, station):
    """Create pentad selection card widget."""

    # Pentad card
    pentad_card = pn.Card(
        pn.Column(pentad_selector),
        title=_('Pentad:'),
        width_policy='fit', 
        width=station.width,
        collapsed=False
    )
    pentad_card.visible = False

    return pentad_card


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

# Used inside station_card
def create_station(station_dict):
    """Create station (hydropost) selector widget."""

    # Widget for station selection, always visible
    _default_station_value = None
    if station_dict:
        try:
            _default_station_value = "15189 - Аламедин  -  у.р.Чункурчак" #station_dict[next(iter(station_dict))][0]
        except Exception:
            _default_station_value = None

    station = pn.widgets.Select(
        name=_("Select discharge station:"),
        groups=station_dict if station_dict else {_("No stations available"): []},
        value=_default_station_value,
        margin=(0, 0, 0, 0)
    )
    return station


# Used inside sidebar_content
def create_station_card(station):
    """Create station selection card widget."""
    station_card = pn.Card(
        pn.Column(station),
        title=_('Hydropost:'),
        width_policy='fit',
        width=station.width,
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

    allowable_range_selection = pn.widgets.Select(
        options=[_("delta"), _("Manual range, select value below"), _("min[delta, %]")],
        value=_("delta"),
        margin=(0, 0, 0, 0)
    )

    manual_range = pn.widgets.IntSlider(
        name=_("Manual range (%)"),
        start=0,
        end=100,
        value=20,
        step=1,
        margin=(20, 0, 0, 0)  # martin=(top, right, bottom, left)
    )
    manual_range.visible = False

    show_range_button =pn.widgets.RadioButtonGroup(
        name=_("Show ranges in figure:"),
        options=[_("Yes"), _("No")],
        value=_("No")
    )

    return allowable_range_selection, manual_range, show_range_button


# Used inside forecast_card
def create_update_forecast_button():
    """Create update forecast button."""
    update_forecast_button = pn.widgets.Button(name=_("Apply changes"), button_type="success")
    return update_forecast_button


# Used inside sidebar_content
def create_forecast_card(allowable_range_selection, manual_range, model_checkbox, show_range_button, update_forecast_button, station):
    """Create forecast configuration card widget."""

    # Forecast card for sidepanel
    allowable_range_label = pn.pane.Markdown(
        _("Select forecast range (for both summary table and figures):"),
        styles={"white-space": "normal"},  # force wrapping
        margin=(0, 0, -5, 0)
    )

    forecast_model_title = pn.pane.Markdown(
        _("Select forecast model (for figures only):"), margin=(0, 0, -15, 0)
    )  # margin=(top, right, bottom, left)

    range_selection_title = pn.pane.Markdown(_("Show ranges (for figures only):"), margin=(0, 0, -15, 0))

    forecast_card = pn.Card(
        pn.Column(
            allowable_range_label,
            allowable_range_selection,
            manual_range,
            pn.layout.Divider(),
            forecast_model_title,
            model_checkbox,
            range_selection_title,
            show_range_button,
            update_forecast_button
        ),
        title=_('Forecast configuration:'),
        width_policy='fit', width=station.width,
        collapsed=False
    )
    # Initially hide the card
    forecast_card.visible = False

    return forecast_card


# Used inside basin_card
def create_select_basin_widget(station_dict):
    """Create basin selection widget."""
    basin_names = list(station_dict.keys())
    basin_names.insert(0, _("All basins"))  # Add 'Select all basins' as the first option

    # Create the 'Select Basin' widget
    select_basin_widget = pn.widgets.Select(
        name=_("Select basin:"),
        options=basin_names,
        value=_("All basins"),  # Default value
        margin=(0, 0, 0, 0)
    )
    return select_basin_widget


# Used inside sidebar_content
def create_basin_card(select_basin_widget, station):
    """Create basin selection card widget."""
    # Basin card
    basin_card = pn.Card(
        pn.Column(
            select_basin_widget
            ),
        title=_('Select basin:'),
        width_policy='fit', width=station.width,
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
        "⚠️ Warning: " + msg,
        alert_type="warning",
        sizing_mode="stretch_width"
    )

# ============================== Widgets for Predictors Tab ==============================

def get_predictors_warning(station, data):
    # predictors_warning.objects = []  # clear old content
    # today_date = today.date()
    today_date = dt.datetime.now().date()
    filtered = data["hydrograph_day_all"][
        (data["hydrograph_day_all"]["station_labels"] == station.value) &
        (data["hydrograph_day_all"]["date"] == pd.to_datetime(today_date))
    ]

    if not filtered.empty:
        if pd.notna(filtered["2025"].iloc[0]):
            print("2025 has a value:", filtered["2025"].iloc[0])
            return
        else:
            print("2025 is NaN/empty")
            # predictors_warning.append(get_pane_alert(f"No discharge record available today for {station.value}"))
            return get_pane_alert(f"No discharge record available today for {station.value}")
    else:
        print("No record for today and given station")
        # predictors_warning.append(get_pane_alert(f"No discharge record available today for {station.value}"))
        return get_pane_alert(f"No discharge record available today for {station.value}")


# ============================== Widgets for Forecast Tab ==============================


def get_forecast_warning(station, data, date_picker_value):
    # forecast_warning.objects = []  # clear old content
    filtered = data["forecasts_all"][
        (data["forecasts_all"]["station_labels"] == station.value) &
        (data["forecasts_all"]["date"] == pd.to_datetime(date_picker_value))
    ]
    if not filtered.empty:
        # filter rows where forecasted_discharge is NaN
        missing_forecasts = filtered[filtered["forecasted_discharge"].isna()]

        # collect the model_short values into a list
        missing_models = missing_forecasts["model_short"].tolist()

        if missing_models:
            print("Missing forecasts for models:", missing_models)
            # forecast_warning.append(get_pane_alert(
            #     f"No forecast data available for models {', '.join(missing_models)} at {station.value} on {date_picker.value}."))
            return get_pane_alert(
                f"No forecast data available for models {', '.join(missing_models)} at {station.value} on {date_picker_value}.")
        else:
            print("All models have forecast data.")
            return
    else:
        # forecast_warning.append(get_pane_alert(f"No forecast data available for {station.value} on {date_picker.value}."))
        return get_pane_alert(f"No forecast data available for {station.value} on {date_picker_value}.")


# Used for Summary table Plot (Forecast Tab)
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
def create_show_daily_data_widget():
    show_daily_data_widget = pn.widgets.RadioButtonGroup(
        name=_("Show daily data:"),
        options=[_("Yes"), _("No")],
        value=_("No")
    )
    return show_daily_data_widget

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
        layout='fit_columns'
    )
    return bulletin_tabulator


# Used inside bulletin_table
def create_add_to_bulletin_popup():
    # Create the pop-up notification pane (initially hidden)
    add_to_bulletin_popup = pn.pane.Alert(_("Added to bulletin"), alert_type="success", visible=False)
    return add_to_bulletin_popup


# Used for Forecast bulletin Plot (Bulletin Tab)
def create_bulletin_table(bulletin_tabulator, remove_bulletin_button, add_to_bulletin_popup):
    bulletin_table = pn.Column(
        bulletin_tabulator,  # Add the global Tabulator directly
        pn.Row(remove_bulletin_button, sizing_mode='stretch_width'),
        add_to_bulletin_popup  # Include the popup for success messages
    )
    return bulletin_table


def create_downloader_and_panel(horizon):
    # Initialize the downloader with a specific directory
    bulletin_folder = os.path.join(
        os.getenv('ieasyreports_report_output_path'),
        'bulletins', horizon)
    downloader = FileDownloader(bulletin_folder)
    bulletin_download_panel = downloader.panel()

    return downloader, bulletin_download_panel

# ============================== Widgets for Language and Auth ==============================

# Used in dashboard header
def create_language_buttons():
    # Create language selection buttons as links that reload the page with the selected language
    buttons = []
    for lang_name, lang_code in {'English': 'en_CH', 'Русский': 'ru_KG', 'Кыргызча': 'ky_KG'}.items():
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