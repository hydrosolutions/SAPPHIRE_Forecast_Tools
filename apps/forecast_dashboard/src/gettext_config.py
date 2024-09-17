# gettext_config.py

import os
import gettext


def load_translation(language, locale_dir):
    if hasattr(language, 'new'):
        language = language.new
    gettext.bindtextdomain('pentad_dashboard', locale_dir)
    gettext.textdomain('pentad_dashboard')
    return gettext.translation('pentad_dashboard', locale_dir, languages=[language]).gettext


def configure_gettext(language, locale_dir):
    """
    Configures the gettext translation object for the application.

    Args:
        locale (str): The locale to use for the translation.
        locale_dir (str): The directory where the .mo files are stored.

    Returns:
        function: The translation function.

    Raises:
        Exception: If the directory does not exist.

    Usage:
        # Read the locale from the environment file
        current_locale = os.getenv("ieasyforecast_locale")

        # Localization, translation to different languages.
        localedir = os.getenv("ieasyforecast_locale_dir")

        _ = configure_gettext(current_locale, localedir)

    """
    # Test if the directory exists
    if not os.path.isdir(locale_dir):
        raise Exception("Directory not found: " + locale_dir)
    # Create a translation object
    try:
        my_translation = load_translation(language, locale_dir)
    except FileNotFoundError:
        # Fallback to the default language if the .mo file is not found
        my_translation = load_translation('ru_KG', locale_dir)
    return my_translation


# How to update the translation file:
# 1. Extract translatable strings from the source code
# xgettext -o ../config/locale/messages.pot pentad_dashboard.py
# 2. Create a new translation file, make sure you have a backup of the old one
# to avoid having to translate everything again.
# msginit -i ../config/locale/messages.pot -o ../config/locale/ru_KG/LC_MESSAGES/pentad_dashboard.po -l ru_KG
# msginit -i ../config/locale/messages.pot -o ../config/locale/en_CH/LC_MESSAGES/pentad_dashboard.po -l en_CH
# 3. Translate the strings in the .po file and make sure that charset is set to
# UTF-8 (charset=UTF-8)
# 4. Compile the .po file to a .mo file
# msgfmt -o ../config/locale/ru_KG/LC_MESSAGES/pentad_dashboard.mo ../config/locale/ru_KG/LC_MESSAGES/pentad_dashboard.po
# msgfmt -o ../config/locale/en_CH/LC_MESSAGES/pentad_dashboard.mo ../config/locale/en_CH/LC_MESSAGES/pentad_dashboard.po
