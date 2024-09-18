import sys
import os
import gettext
import panel as pn

from src.environment import load_configuration
from src.gettext_config import configure_gettext
import src.processing as processing
import src.vizualization as viz

# Get the absolute path of the directory containing the current script
cwd = os.getcwd()

# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')
# Test if the forecast dir exists and print a warning if it does not
if not os.path.isdir(forecast_dir):
    raise Exception("Directory not found: " + forecast_dir)

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)
# Import the modules from the forecast library
import setup_library as sl
import tag_library as tl
import forecast_library as fl


# Load .env file
# Read the environment varialbe IN_DOCKER_CONTAINER to determine which .env
# file to use
in_docker_flag = load_configuration()






# Set up gettext
#locales_dir = 'locales'
locales_dir = os.getenv("ieasyforecast_locale_dir")
print("locales_dir: ", locales_dir)

def load_translation(language):
    gettext.bindtextdomain('pentad_dashboard', locales_dir)
    gettext.textdomain('pentad_dashboard')
    return gettext.translation('pentad_dashboard', locales_dir, languages=[language]).gettext

# Initialize with default language
_ = load_translation('ru_KG')

# Read the locale from the environment file
current_locale = os.getenv("ieasyforecast_locale")

# Load custom CSS for the language icon
pn.extension(
    css_files=['https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css']
)





# Create a language selection widget
language_select = pn.widgets.Select(
    name='',
    options={'en':'en_CH', 'ru':'ru_KG'},
    value='ru_KG',
    width=50
)

# Create an icon using HTML
icon_html = pn.pane.HTML(
    '<i class="fas fa-language"></i>',
    width=20
)





# Function to create the dashboard
def create_dashboard():
    title = _("SAPPHIRE Central Asia - Pentadal forecast dashboard")
    description = _("Disclaimer")
    print("create_dashboard: title: ", title)
    print("create_dashboard: description: ", description)

    return pn.Column(
        pn.pane.Markdown(f"# {title}"),
        pn.pane.Markdown(description),
        #pn.widgets.TextInput(name=_("Hydrograph"), placeholder=_("Last updated on ")),
        #pn.widgets.Button(name=_("Select discharge station:"))
    )

# Function to update the dashboard based on selected language
def update_dashboard(language):
    global _
    _ = load_translation(language)
    # Print the currently selected language
    print(f"Selected language: {language}")
    return create_dashboard()

# Bind the function to the language selection widget
main_panel = pn.bind(update_dashboard, language_select.param.value)

# Create the dashboard
dashboard = pn.template.BootstrapTemplate(
    title=_('SAPPHIRE Central Asia - Pentadal forecast dashboard'),
    #header=[pn.Row(pn.layout.HSpacer(), icon_html, language_select)],
    header=[pn.Row(pn.layout.HSpacer(), language_select)],
    main=main_panel
)
# Serve the dashboard with the language selection widget
pn.serve(dashboard) #, dashboard))
# run with: python test_localization.py
#dashboard.servable()