import re
import time
from playwright.sync_api import Page, expect

TEST_PENTAD = False
TEST_DECAD = False
TEST_LOCAL = True
LOCAL_URL = "http://localhost:5006/forecast_dashboard"
PENTAD_URL = "https://fc.pentad.ieasyhydro.org/forecast_dashboard"
DECAD_URL = "https://fc.decad.ieasyhydro.org/forecast_dashboard"
SLEEP = 1


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
            # print(model_div.inner_text())
            model_values.append(model_div.inner_text())
        return model_values
    values = []

    # Adding station 16936 to bulletins
    values_16936 = get_model_values()
    values_16936.insert(0, "16936")
    del values_16936[-2]
    values.append(values_16936)
    page.get_by_role("button", name="Добавить в бюллетень").click()
    print("#### 16936 - Нарын  -  Приток в Токтогульское вдхр.**) added to bulletin")
    print(values_16936)
    time.sleep(SLEEP)

    # Select station 15194
    page.select_option("select#input", value="15194 - р.Ала-Арча-у.р.Кашка-Суу")
    print("#### Station 15194 selected")
    time.sleep(SLEEP)

    # Adding station 15194 to bulletins
    values_15194 = get_model_values()
    values_15194.insert(0, "15194")
    del values_15194[-2]
    values.append(values_15194)
    page.get_by_role("button", name="Добавить в бюллетень").click()
    print("#### 15194 - р.Ала-Арча-у.р.Кашка-Суу added to bulletin")
    print(values_15194)
    time.sleep(SLEEP)

    page.select_option("select#input", value="15256 - Талас -  с.Ак-Таш")
    print("#### Station 15256 selected")
    time.sleep(SLEEP)

    page.select_option("select#input", value="15013 - Джыргалан-с.Советское")
    print("#### Station 15013 selected")
    time.sleep(SLEEP)

    ### BULLETIN TAB ###
    page.locator("div.bk-tab", has_text="Бюллетень").click()
    print("#### Switch to Bulletin tab successful.")
    time.sleep(SLEEP)

    # Extract bulletin table values
    bulletin_values = []
    selectable_divs = page.locator("div.tabulator-selectable")
    for i in range(selectable_divs.count()):
        div = selectable_divs.nth(i)
        temp = div.inner_text().split("\n")
        temp[0] = temp[0].split()[0]
        del temp[2]
        print(temp)
        bulletin_values.append(temp)

    ### INFO TAB ###
    page.locator("div.bk-tab", has_text="Информация об ответственности").click()
    print("#### Switch to Info tab successful.")
    time.sleep(SLEEP)
