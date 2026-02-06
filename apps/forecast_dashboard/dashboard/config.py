import os
import sys
from src.gettext_config import _
import src.processing as processing
from src.environment import load_configuration
import src.gettext_config as localize


def import_tag_library():
    """
    Adds iEasyHydroForecast to sys.path and imports tag_library.
    Returns the imported module.
    """
    # Local libraries, installed with pip install -e ./iEasyHydroForecast
    # Get the absolute path of the directory containing the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Construct the path to the iEasyHydroForecast directory
    forecast_dir = os.path.join(script_dir, '../..', 'iEasyHydroForecast')
    # Test if the forecast dir exists and print a warning if it does not
    if not os.path.isdir(forecast_dir):
        raise Exception("Directory not found: " + forecast_dir)

    # Add the forecast directory to the Python path
    sys.path.append(forecast_dir)
    # Import the modules from the forecast library
    # import tag_library as tl

    import tag_library
    return tag_library


def setup_panel(pn):
    # Set primary color to be consistent with the icon color
    # Set the primary color to be consistent with the icon color
    # Trying to fix the issue with the checkbox label not wrapping
    # Loads the font-awesome icons (used for the language icon)
    pn.extension(
        'tabulator',
        raw_css=[
            ':root { --design-primary-color: #307096; }',
            """
            .checkbox-label {
                white-space: normal !important;
                word-wrap: break-word !important;
                width: 100%; /* Adjust as needed */
            }
            """
        ]
    )

    # CSS for language widget text color
    language_button_css = """
    header .bk.pn-widget-button {
        color: #307086 !important;  /* Change text color of button widget */
    }
    """

    # Inject the custom CSS
    pn.config.raw_css.append(language_button_css)


def setup_directories():
    # Get folder to store visible points for linear regression
    # Test if the path to the configuration folder is set
    if not os.getenv('ieasyforecast_configuration_path'):
        raise ValueError("The path to the configuration folder is not set.")

    # Define the directory to save the data
    save_directory = os.path.join(
        os.getenv('ieasyforecast_configuration_path'),
        os.getenv('ieasyforecast_linreg_point_selection', 'linreg_point_selection')
    )
    os.makedirs(save_directory, exist_ok=True)

    return save_directory


def load_env_and_icons():
    # Get path to .env file from the environment variable
    env_file_path = os.getenv("ieasyhydroforecast_env_file_path")

    # Load .env file
    # Read the environment varialbe IN_DOCKER_CONTAINER to determine which .env
    # file to use
    in_docker_flag = load_configuration(env_file_path)

    # Get icon path from config
    icon_path = processing.get_icon_path(in_docker_flag)

    # The current date is displayed as the title of each visualization.
    # today = dt.datetime.now()
    return env_file_path, in_docker_flag, icon_path


def setup_localization(pn):
    # region localization
    # We'll use pn.state.location to get query parameters from the URL
    # This allows us to pass the selected language via the URL

    # Default language from .env file
    default_locale = os.getenv("ieasyforecast_locale", "en_CH")
    print(f"INFO: Default locale: {default_locale}")

    # Get the language from the URL query parameter
    selected_language = pn.state.location.query_params.get('lang', default_locale)
    print(f"INFO: Selected language: {selected_language}")

    # Localization, translation to different languages.
    localedir = os.getenv("ieasyforecast_locale_dir")
    print(f"DEBUG: Translation directory: {localedir}")
    print(f"DEBUG: Translation directory exists: {os.path.exists(localedir)}")
    print(f"DEBUG: Files in translation directory: {os.listdir(localedir)}")

    # Set the locale directory in the translation manager
    localize.translation_manager.set_locale_dir(locale_dir=localedir)

    # Set the current language
    localize.translation_manager.language = selected_language

    # Load translations globally
    localize.translation_manager.load_translation_forecast_dashboard()
    print(f"DEBUG: Selected language: {selected_language}")
    print(f"DEBUG: Translation manager language: {localize.translation_manager.language}")

    print(f"DEBUG: Translation test - 'Hydropost:' translates to: {_('Hydropost:')}")
    print(f"DEBUG: Translation test - 'Forecast' translates to: {_('Forecast')}")

    # Check for cached translations
    if hasattr(pn.state, 'cache') and 'translations' in pn.state.cache:
        print(f"DEBUG: Cached translations found.")
    
    # Import visualization module after setting up localization
    import src.vizualization as viz
    return viz


def display_weather_and_snow_data():
    # Find out which forecasts have to be displayed
    display_ML_forecasts = os.getenv('ieasyhydroforecast_run_ML_models', 'False').lower() in ('true', 'yes', '1', 't', 'y')
    display_CM_forecasts = os.getenv('ieasyhydroforecast_run_CM_models', 'False').lower() in ('true', 'yes', '1', 't', 'y')

    # If both display_ML_forecasts and display_CM_forecasts are False, we don't need 
    # to display data gateway results. 
    if not display_ML_forecasts and not display_CM_forecasts:
        display_weather_data = False
    if display_ML_forecasts == False and display_CM_forecasts == False:
        display_weather_data = False
    else:
        display_weather_data = True

    print(f"Display ML forecasts: {display_ML_forecasts}")
    print(f"Display CM forecasts: {display_CM_forecasts}")
    print(f"Display weather data: {display_weather_data}")

    # Find out if snow data is configured and can be displayed
    display_snow_data = os.getenv('ieasyhydroforecast_HRU_SNOW_DATA_DASHBOARD', 'False')

    # If display_snow_data is not null or empty, and display_weather_data is True, we display snow data
    if display_snow_data and display_weather_data:
        display_snow_data = True
    else:
        display_snow_data = False
    
    return display_weather_data, display_snow_data


def get_horizon():
    # Print forecast horizon variable from environment
    horizon = os.getenv('sapphire_forecast_horizon')
    if horizon:
        print(f"INFO: Forecast horizon: {horizon}")
    else:
        print("WARNING: Forecast horizon not set. Assuming default 'decad'.")
        horizon = 'decad'
    
    if horizon == "pentad":
        horizon_in_year = "pentad_in_year"
        dashboard_title = _('SAPPHIRE Central Asia - Pentadal forecast dashboard')
    else:
        horizon_in_year = "decad_in_year"
        dashboard_title = _('SAPPHIRE Central Asia - Decadal forecast dashboard')
    
    return horizon, horizon_in_year, dashboard_title
