import os
import time
import re
import requests
import pandas as pd
from playwright.sync_api import Page, expect
import tag_library as tl
import datetime as dt


TEST_PENTAD = False
TEST_DECAD = False
TEST_LOCAL = False
LOCAL_URL = "http://localhost:5055/forecast_dashboard"
PENTAD_URL = "https://kyg.fc.pentad.ieasyhydro.org/forecast_dashboard"
DECAD_URL = "https://demo.fc.decade.ieasyhydro.org/forecast_dashboard"
SLEEP = 1
API_BASE = "http://localhost:8000/api"
API_TIMEOUT = 30
horizon = "pentad"  # pentad or decad

today = dt.datetime.now()
today = today + dt.timedelta(days=1)
year = today.year
date_str = today.strftime("%Y-%m-%d")
print("Today's date:", date_str)
month_str = today.strftime("%m") + "_" + tl.get_month_str_case1(date_str)
if horizon == "pentad":
    horizon_value = tl.get_pentad_for_date(today)
    print("Pentad in year:", horizon_value)
    horizon_value_in_month = tl.get_pentad(today)
    print("Pentad in month:", horizon_value_in_month)
    # horizon_value_in_month = "2"
    sheet_name = f"{horizon_value_in_month} пентада"
else:
    horizon_value = tl.get_decad_for_date(today)
    print("Decad in year:", horizon_value)
    horizon_value_in_month = tl.get_decad_in_month(today)
    print("Decad in month:", horizon_value_in_month)
    sheet_name = f"{horizon_value_in_month} декада"

if len(str(horizon_value)) == 1:
    horizon_value = "0" + str(horizon_value)
# horizon_value = "14"

def normalize_spaces(s):
    return re.sub(r'\s+', ' ', s).strip()


def normalize_comma(s):
    return s.replace(",", "")


def _fetch_bulletin_from_api() -> list[dict]:
    """Fetch bulletin records from the backend API for the current horizon/year."""
    print("horizon, year, horizon_value:", horizon, year, horizon_value)
    resp = requests.get(
        f"{API_BASE}/postprocessing/bulletin/",
        params={
            "horizon":       horizon,
            "year":          year,
            "horizon_value": horizon_value,
            "limit":         1000,
        },
        timeout=API_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def _get_float(value) -> float | str:
    try:
        if isinstance(value, str) and "," in value:
            value = value.replace(",", ".")
        return float(value)
    except (ValueError, TypeError):
        return "nan"


def _compare_numeric(api_val, ui_str: str, tolerance: float = 0.05) -> None:
    """Assert that an API numeric value matches a UI string value within tolerance."""
    ui_float = _get_float(ui_str)
    if api_val is None or (isinstance(api_val, float) and pd.isna(api_val)):
        assert ui_str in ('-', '', 'nan'), f"Expected empty UI value, got '{ui_str}'"
    elif isinstance(api_val, str):
        assert _get_float(api_val.strip()) == ui_float, f"{api_val!r} != {ui_str!r}"
    else:
        assert abs(api_val - ui_float) <= tolerance * abs(api_val), (
            f"API {api_val} vs UI '{ui_str}' exceeds {tolerance:.0%} tolerance"
        )


def test_pentad(page: Page):
    if not TEST_PENTAD:
        print("#### Skipping PENTAD test...")
        return

    page.goto(PENTAD_URL)

    print("#### Testing PENTAD started...")

    # Testing the page title
    expect(page).to_have_title(re.compile("SAPPHIRE Central Asia - Pentadal forecast dashboard"))
    print("#### Page title is correct.")

    # Testing Pentad.png being loaded
    content = page.content()
    assert 'DINppRCxDAAEEalfg/wLZeXf9HTaUOAAAAABJRU5ErkJggg==' in content
    print("#### Pentad.png is shown.")
    time.sleep(SLEEP)

    # # Testing the page is in Russian
    # expect(page).to_have_title(re.compile("SAPPHIRE Central Asia - Панель управления пентадными прогнозами"))
    # expect(page.get_by_text("Войти")).to_be_visible()
    # expect(page.get_by_text("Имя пользователя")).to_be_visible()
    # expect(page.get_by_text("Введите имя пользователя")).to_be_visible()
    # print("#### Page is in Russian.")
    # time.sleep(SLEEP)


def test_decad(page: Page):
    if not TEST_DECAD:
        print("#### Skipping DECAD test...")
        return

    page.goto(DECAD_URL)

    print("#### Testing DECAD started...")

    # Testing the page title
    expect(page).to_have_title(re.compile("SAPPHIRE Central Asia - Decadal forecast dashboard"))
    print("#### Page title is correct.")

    # Testing Decad.png being loaded
    content = page.content()
    assert '8tYYd0q55fCZAgMBYAv8DTUYpzxgsaeEAAAAASUVORK5CYII=' in content
    print("#### Decad.png is shown.")
    time.sleep(SLEEP)

    # Testing the page is in Russian
    expect(page).to_have_title(re.compile("SAPPHIRE Central Asia - Панель управления декадными прогнозами"))
    expect(page.get_by_text("Войти")).to_be_visible()
    expect(page.get_by_text("Имя пользователя")).to_be_visible()
    expect(page.get_by_text("Введите имя пользователя")).to_be_visible()
    print("#### Page is in Russian.")
    time.sleep(SLEEP)


def test_local(page: Page):
    if not TEST_LOCAL:
        print("#### Skipping LOCAL test...")
        return

    # Set default timeouts at the start of the test
    page.set_default_timeout(60000)  # 60 seconds for all actions
    page.set_default_navigation_timeout(60000)  # 60 seconds for navigation

    page.goto(LOCAL_URL)

    print("#### Testing LOCAL started...")

    # Testing the page title
    expect(page).to_have_title(re.compile("SAPPHIRE Central Asia"))
    print("#### Page title is correct.")

    # Testing login failure with incorrect credentials
    page.get_by_label("Username").fill("user1")
    page.get_by_label("Password").fill("user111")
    assert page.get_by_label("Username").input_value() == "user1"
    assert page.get_by_label("Password").input_value() == "user111"
    password_input = page.get_by_label("Password")
    password_input.press("Tab")
    page.get_by_role("button", name="Login").click()

    expect(page.get_by_text("Invalid username or password")).to_be_visible()
    expect(page.get_by_text("Predictors")).not_to_be_visible()
    expect(page.get_by_text("Hydropost")).not_to_be_visible()
    print("#### Login failed as expected.")
    time.sleep(SLEEP)

    # Testing login success with correct credentials
    password_input = page.get_by_label("Password")
    password_input.fill("user1user1")
    password_input.press("Tab")  # Moves focus away from input
    page.get_by_role("button", name="Login").click()

    expect(page.get_by_text("Invalid username or password")).not_to_be_visible()
    expect(page.get_by_text("Predictors")).to_be_visible()
    expect(page.get_by_text("Hydropost")).to_be_visible()
    print("#### Login successful.")
    time.sleep(SLEEP)

    # Testing sign out
    page.get_by_role("button", name="Logout").click()
    page.get_by_role("button", name="Yes").click()
    expect(page.get_by_text("Username")).to_be_visible()
    print("#### Logout successful.")
    time.sleep(SLEEP)

    # Testing login after logout
    page.get_by_label("Username").fill("user1")
    password_input = page.get_by_label("Password")
    password_input.fill("user1user1")
    password_input.press("Tab")  # Moves focus away from input
    page.get_by_role("button", name="Login").click()

    expect(page.get_by_text("Predictors")).to_be_visible()
    expect(page.get_by_text("Hydropost")).to_be_visible()
    print("#### Login after logout successful.")
    time.sleep(SLEEP)

    # Testing language switching
    page.get_by_role("link", name="Русский").click()
    # page.get_by_label("Имя пользователя").fill("user1")
    # password_input = page.get_by_label("Пароль")
    # password_input.fill("user1user1")
    # password_input.press("Tab")  # Moves focus away from input
    # page.get_by_role("button", name="Войти").click()

    expect(page.get_by_text("Предикторы")).to_be_visible()
    expect(page.locator("div.bk-tab", has_text="Прогноз")).to_be_visible()
    expect(page.get_by_text("Бюллетень")).to_be_visible()
    expect(page.get_by_text("Информация об ответственности")).to_be_visible()
    print("#### Login after language change successful.")
    time.sleep(SLEEP)

    ### PREDICTORS TAB ###
    # Select station 16936
    page.locator("select#input").nth(1).select_option(value="16936 - Нарын  -  Приток в Токтогульское вдхр.**)", timeout=60000)
    print("#### Station 16936 selected")
    time.sleep(SLEEP)

    ### FORECAST TAB ###
    page.locator("div.bk-tab", has_text="Прогноз").click()
    print("#### Switch to Forecast tab successful.")
    time.sleep(SLEEP)

    def get_model_values():
        """Find selected models in Summary table"""
        selected_div = page.locator("div.tabulator-selected")
        model_values = []
        for div in ["Модель", "Прогн. расх. воды", "Прогн. нижн. гран.", "Прогн. верхн. гран.", "δ", "s/σ", "Средняя абсолютная ошибка", "Оправдываемость"]:
            model_div = selected_div.locator(f'div[tabulator-field="{div}"]')
            model_values.append(model_div.inner_text())
        return model_values

    summary_table_values = []

    def select_station_and_add_to_bulletin(station):
        page.locator("select#input").nth(1).select_option(value=station, timeout=60000)
        print(f"#### SELECTED station: {station}")
        time.sleep(SLEEP)

        model_values = get_model_values()
        model_values.insert(0, station.split()[0])
        summary_table_values.append(model_values)
        page.get_by_role("button", name="Добавить в бюллетень").click()
        print(f"#### ADDED TO BULLETIN: {station}")
        time.sleep(SLEEP)

    stations = [
        "15013 - Джыргалан-с.Советское",
        # "15016 - Тургень-Ак-Суу - пос.лесозавода",
        "16936 - Нарын  -  Приток в Токтогульское вдхр.**)",
        #"15194 - р.Ала-Арча-у.р.Кашка-Суу",
        "15212 - Ак-Суу - с.Чон-Арык",
        "15256 - Талас -  с.Ак-Таш",
    ]
    for station in stations:
        select_station_and_add_to_bulletin(station)

    print("#### Summary table values added to bulletins:")
    for value in summary_table_values:
        print(value)
    time.sleep(SLEEP)

    ### BULLETIN TAB ###
    page.locator("div.bk-tab", has_text="Бюллетень").click()
    print("#### Switch to Bulletin tab successful.")
    time.sleep(SLEEP)

    # Extract forecast bulletin table values
    forecast_bulletin_values = []
    selectable_divs = page.locator("div.tabulator-selectable")
    for i in range(selectable_divs.count()):
        div = selectable_divs.nth(i)
        values = div.inner_text().split("\n")
        forecast_bulletin_values.append(values)

    print("#### Forecast bulletin values:")
    for value in forecast_bulletin_values:
        print(value)
    time.sleep(SLEEP)

    # Comparing summary table with forecast bulletin
    print("Comparing summary table with forecast bulletin...")
    count = 0
    for s_value in summary_table_values:
        for f_value in forecast_bulletin_values:
            if s_value[0] in f_value[0] and s_value[1] == f_value[1]:
                count += 1
                assert s_value[2] == f_value[3]  # Forecasted discharge
                assert s_value[3] == f_value[4]  # Forecast lower bound
                assert s_value[4] == f_value[5]  # Forecast upper bound
                assert s_value[5] == f_value[6]  # δ
                assert s_value[6] == f_value[7]  # s/σ
                assert s_value[8] == f_value[8]  # Accuracy
    assert count == len(summary_table_values) == len(forecast_bulletin_values)
    print("#### Summary table values are EQUAL to Forecast bulletin values")
    time.sleep(SLEEP)

    # Checking the top checkbox to select all bulletins
    page.locator('input[type="checkbox"][aria-label="Select Row"]').first.check()
    print("#### All bulletins selected")
    time.sleep(SLEEP)

    # Clicking Write bulletin button
    page.get_by_role("button", name="Записать бюллетень").click()
    print("#### Write bulletin button clicked")
    time.sleep(SLEEP)

    # Fetch bulletin records from the API
    print("#### Fetching bulletin from API...")
    api_records = _fetch_bulletin_from_api()
    print(f"#### API returned {len(api_records)} bulletin record(s):")
    for rec in api_records:
        print(rec)
    time.sleep(SLEEP)

    assert api_records, "API returned no bulletin records — was the bulletin written correctly?"

    # Compare API records with UI bulletin table
    print("Comparing API bulletin records with UI forecast bulletin table...")
    count = 0
    for rec in api_records:
        api_station  = normalize_spaces(rec.get("station_label", ""))
        api_basin    = normalize_spaces(rec.get("basin_name", ""))
        api_model    = rec.get("model_type", "")
        for f_value in forecast_bulletin_values:
            ui_station = normalize_spaces(f_value[0])
            ui_basin   = normalize_spaces(f_value[2])
            if api_station == ui_station and api_basin == ui_basin and api_model == f_value[1]:
                count += 1
                _compare_numeric(rec.get("forecasted_discharge"), f_value[3])  # forecasted discharge
                _compare_numeric(rec.get("fc_lower"),            f_value[4])  # lower bound
                _compare_numeric(rec.get("fc_upper"),            normalize_comma(f_value[5]))  # upper bound
                if rec.get("delta") is not None:
                    assert str(rec["delta"]).replace(",", ".") == f_value[6].replace(",", ".")  # δ
                _compare_numeric(rec.get("sdivsigma"),           f_value[7])  # s/σ
                _compare_numeric(rec.get("accuracy"),            f_value[8])  # accuracy
    assert count == len(forecast_bulletin_values) == len(api_records), (
        f"Match count {count} does not equal bulletin rows {len(forecast_bulletin_values)} "
        f"or API records {len(api_records)}"
    )
    print("#### API bulletin records are EQUAL to UI Forecast bulletin values")
    time.sleep(SLEEP)

    # Construct all excel paths
    sensitive_data_forecast_tools = os.getenv('ieasyhydroforecast_data_dir')
    excel_file_paths = []
    basins = set()
    for f_value in forecast_bulletin_values:
        basin = f_value[2]
        if basin not in basins:
            basins.add(basin)
            path = f"{sensitive_data_forecast_tools}reports/bulletins/{horizon}/{year}/{year}_{month_str}_{basin}_short_term_forecast_bulletin.xlsx"
            excel_file_paths.append(path)
    excel_file_paths.append(f"{sensitive_data_forecast_tools}reports/bulletins/{horizon}/{year}/{year}_{month_str}_all_basins_short_term_forecast_bulletin.xlsx")

    print("#### Excel file paths:")
    for path in excel_file_paths:
        print(path)
    time.sleep(SLEEP)

    # Compare Excel values with API records
    print("Comparing Excel values with API bulletin records...")

    count = 0
    for excel_file_path in excel_file_paths:
        df = pd.read_excel(excel_file_path, sheet_name=sheet_name, skiprows=10)
        print(f"Comparing Excel with API: {excel_file_path}")
        for row_index in range(len(df)):
            if pd.isna(df.iloc[row_index, 0]) or df.iloc[row_index, 0] == "":
                continue
            excel_river  = df.iloc[row_index, 0]
            excel_punkt  = df.iloc[row_index, 1]
            excel_model  = df.iloc[row_index, 2]
            excel_delta  = df.iloc[row_index, 5]
            for rec in api_records:
                api_station = normalize_spaces(rec.get("station_label", ""))
                # match when both Excel river and punkt appear in the API station label
                if excel_river in api_station and excel_punkt in api_station:
                    count += 1
                    assert excel_model == rec.get("model_type"), (
                        f"Model mismatch: Excel '{excel_model}' vs API '{rec.get('model_type')}'"
                    )
                    api_delta = rec.get("delta")
                    if not (pd.isna(excel_delta) and api_delta is None):
                        assert str(excel_delta).replace(",", ".") == str(api_delta).replace(",", "."), (
                            f"Delta mismatch: Excel '{excel_delta}' vs API '{api_delta}'"
                        )
        print("#### Excel values are EQUAL to API bulletin records")
    assert count == len(api_records) * 2, (
        f"Expected {len(api_records) * 2} Excel/API matches (2 files), got {count}"
    )

    # Clicking Remove Selected button
    page.get_by_role("button", name="Удалить выбранное").click()
    selectable_divs = page.locator("div.tabulator-selectable")
    assert selectable_divs.count() == 0
    print("#### Remove Selected button clicked")
    time.sleep(SLEEP)

    # Clicking Download button
    page.locator("h3", has_text="Скачать бюллетень").click()
    options = page.locator('select#input.bk-input[multiple="true"][size="10"] option').all()
    for option in options:
        option.click(modifiers=["Meta"])  # Windows: "Control"

    page.get_by_role("button", name="Подготовить загрузку выбранных файлов").click()
    page.get_by_role("button", name="Download selected_files.zip").click()
    time.sleep(SLEEP)

    ### INFO TAB ###
    page.locator("div.bk-tab", has_text="Информация об ответственности").click()
    print("#### Switch to Info tab successful.")
    time.sleep(SLEEP)
