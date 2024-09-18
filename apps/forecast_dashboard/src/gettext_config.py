# gettext_config.py

import os
import gettext
import param

class TranslationManager(param.Parameterized):
    language = param.String(default='en')

    def __init__(self, **params):
        super().__init__(**params)
        self._ = lambda x: x  # Default to no translation

    @param.depends('language', watch=True)
    def load_translation(self):
        print(f"\n\ndebug Translation manager: load_translation")
        print(f"self.language: {self.language}")
        try:
            trans = gettext.translation('pentad_dashboard', localedir='locale', languages=[self.language])
            trans.install()
            self._ = trans.gettext
        except FileNotFoundError:
            # Fallback to default language if translation file not found
            self._ = lambda x: x

# Create and export an instance
translation_manager = TranslationManager()



def load_translation(language, locale_dir):
    if hasattr(language, 'new'):
        language = language.new
    print(f"debug load_translation: language: {language}")
    print(f"debug load_translation: locale_dir: {locale_dir}")
    gettext.bindtextdomain('pentad_dashboard', locale_dir)
    gettext.textdomain('pentad_dashboard')
    # Print the path to the .mo file
    print(f"debug load_translation: path to .mo file: {gettext.find('pentad_dashboard', locale_dir, languages=[language])}")
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
        print(f"debug configure_gettext: my_translation: {my_translation}")
    except FileNotFoundError:
        # Fallback to the default language if the .mo file is not found
        my_translation = load_translation('ru_KG', locale_dir)
        print(f"debug configure_gettext: failed to load_translation. falling back to my_translation: {my_translation}")
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
