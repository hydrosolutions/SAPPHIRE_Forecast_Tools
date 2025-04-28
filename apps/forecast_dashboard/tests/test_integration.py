import time
import csv
import re
import pandas as pd
from playwright.sync_api import Page, expect
import tag_library as tl
import datetime as dt


TEST_PENTAD = True
TEST_DECAD = True
TEST_LOCAL = True
LOCAL_URL = "http://localhost:5006/forecast_dashboard"
PENTAD_URL = "https://fc.pentad.ieasyhydro.org/forecast_dashboard"
DECAD_URL = "https://fc.decad.ieasyhydro.org/forecast_dashboard"
SLEEP = 1
sensitive_data_forecast_tools = "/SAPPHIRE_Central_Asia_Technical_Work/data/sensitive_data_forecast_tools/"
horizon = "decad"  # pentad or decad

today = dt.datetime.now()
year = today.year
date_str = today.strftime("%Y-%m-%d")
month_str = today.strftime("%m") + "_" + tl.get_month_str_case1(date_str)
if horizon == "pentad":
    horizon_value = tl.get_pentad_for_date(today)
    print("Pentad in year:", horizon_value)
    horizon_value_in_month = tl.get_pentad(today)
    print("Pentad in month:", horizon_value_in_month)
    sheet_name = f"{horizon_value_in_month} пентада"
else:
    horizon_value = tl.get_decad_for_date(today)
    print("Decad in year:", horizon_value)
    horizon_value_in_month = tl.get_decad_in_month(today)
    print("Decad in month:", horizon_value_in_month)
    sheet_name = f"{horizon_value_in_month} декада"


def normalize_spaces(s):
    return re.sub(r'\s+', ' ', s).strip()


def test_pentad(page: Page):
    if not TEST_PENTAD:
        print("#### Skipping PENTAD test...")
        return

    page.goto(PENTAD_URL)

    print("#### Testing PENTAD started...")

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

    # Testing Decad.png being loaded
    content = page.content()
    assert '8tYYd0q55fCZAgMBYAv8DTUYpzxgsaeEAAAAASUVORK5CYII=' in content
    print("#### Decad.png is shown.")
    time.sleep(SLEEP)

    # # Testing the page is in Russian
    # expect(page).to_have_title(re.compile("SAPPHIRE Central Asia - Панель управления декадными прогнозами"))
    # expect(page.get_by_text("Войти")).to_be_visible()
    # expect(page.get_by_text("Имя пользователя")).to_be_visible()
    # expect(page.get_by_text("Введите имя пользователя")).to_be_visible()
    # print("#### Page is in Russian.")
    # time.sleep(SLEEP)


def test_local(page: Page):
    if not TEST_LOCAL:
        print("#### Skipping LOCAL test...")
        return

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
    page.get_by_role("button", name="Login").click()

    expect(page.get_by_text("Invalid username or password")).to_be_visible()
    expect(page.get_by_text("Predictors")).not_to_be_visible()
    expect(page.get_by_text("Hydropost")).not_to_be_visible()
    print("#### Login failed as expected.")
    time.sleep(SLEEP)

    # Testing login success with correct credentials
    password_input = page.get_by_label("Password")
    password_input.fill("user1")
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
    password_input.fill("user1")
    password_input.press("Tab")  # Moves focus away from input
    page.get_by_role("button", name="Login").click()

    expect(page.get_by_text("Predictors")).to_be_visible()
    expect(page.get_by_text("Hydropost")).to_be_visible()
    print("#### Login after logout successful.")
    time.sleep(SLEEP)

    # Testing language switching
    page.get_by_role("link", name="Русский").click()
    page.get_by_label("Имя пользователя").fill("user1")
    password_input = page.get_by_label("Пароль")
    password_input.fill("user1")
    password_input.press("Tab")  # Moves focus away from input
    page.get_by_role("button", name="Войти").click()

    expect(page.get_by_text("Предикторы")).to_be_visible()
    expect(page.locator("div.bk-tab", has_text="Прогноз")).to_be_visible()
    expect(page.get_by_text("Бюллетень")).to_be_visible()
    expect(page.get_by_text("Информация об ответственности")).to_be_visible()
    print("#### Login after language change successful.")
    time.sleep(SLEEP)

    ### PREDICTORS TAB ###
    # Select station 16936
    page.select_option("select#input", value="16936 - Нарын  -  Приток в Токтогульское вдхр.**)")
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
        page.select_option("select#input", value=station)
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
        "16936 - Нарын  -  Приток в Токтогульское вдхр.**)",
        "15194 - р.Ала-Арча-у.р.Кашка-Суу",
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

    # Extract CSV values
    csv_file_path = f"{sensitive_data_forecast_tools}config/linreg_point_selection/bulletin_{horizon}_{year}_{horizon_value}.csv"
    with open(csv_file_path, mode='r', newline='') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        csv_data = [row for row in reader]

    print("#### CSV values:")
    for row in csv_data:
        print(row)
    time.sleep(SLEEP)

    # Compare csv file with bulletin values
    print("Comparing Forecast bulletin with CSV...")
    count = 0
    for row in csv_data:
        for i, r in enumerate(row):
            if r == '':
                row[i] = '-'
        row[-1] = normalize_spaces(row[-1])  # basin_ru
        row[-2] = normalize_spaces(row[-2])  # station_label
        for f_value in forecast_bulletin_values:
            if row[-1] == f_value[2] and row[-2] == f_value[0]:
                count += 1
                assert row[0] == f_value[1]  # model_short
                assert row[1] == f_value[3]  # forecasted_discharge
                assert row[2] == f_value[4]  # fc_lower
                assert row[3] == f_value[5]  # fc_upper
                assert row[4] == f_value[6]  # delta
                assert row[5] == f_value[7]  # sdivsigma
                assert row[7] == f_value[8]  # accuracy
    assert count == len(forecast_bulletin_values) == len(csv_data)
    print("#### Forecast bulletin values are EQUAL to CSV file values")
    time.sleep(SLEEP)

    # Construct all excel paths
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

    # Extract Excel values
    def get_float(value):
        try:
            if isinstance(value, str) and "," in value:
                value = value.replace(",", ".")
            return round(float(value))
        except ValueError:
            return "nan"

    def compare(excel, csv, tolerance=0.05):
        csv_value = get_float(csv)
        if isinstance(excel, str):
            excel_value = get_float(excel.strip())
            assert excel_value == csv_value
            # assert abs(excel_value - csv_value) <= tolerance * abs(excel_value)
        elif pd.isna(excel):
            assert "nan" == csv_value
        else:
            # assert excel == csv_value
            assert abs(excel - csv_value) <= tolerance * abs(excel)
        print(excel, "<=>", csv_value)

    count = 0
    for excel_file_path in excel_file_paths:
        df = pd.read_excel(excel_file_path, sheet_name=sheet_name, skiprows=10)
        print(f"Comparing CSV with: {excel_file_path}")
        for row_index in range(len(df)):
            if pd.isna(df.iloc[row_index, 0]) or df.iloc[row_index, 0] == "":
                continue
            for row in csv_data:
                # print(df.iloc[row_index, 0], "#", df.iloc[row_index, 1], "#", row[-2])
                # check if `река` and `пункт` of Excel are in station_label of CSV
                if df.iloc[row_index, 0] in row[-2] and df.iloc[row_index, 1] in row[-2]:
                    count += 1
                    assert df.iloc[row_index, 2] == row[0]  # model_short
                    compare(df.iloc[row_index, 4], row[1])  # forecasted_discharge
                    compare(df.iloc[row_index, 15], row[2])  # fc_lower
                    compare(df.iloc[row_index, 17], row[3])  # fc_upper
                    assert df.iloc[row_index, 5].replace(',', '.') == row[4]  # delta
                    assert df.iloc[row_index, 10].replace(',', '.') == row[5]  # sdivsigma
        print("#### CSV values are EQUAL to Excel values")
    assert count == len(csv_data) * 2

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
