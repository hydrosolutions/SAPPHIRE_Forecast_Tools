import os
import time
import re
import requests
import pandas as pd
import pytest
pytest.importorskip("playwright.sync_api")
from playwright.sync_api import Page, expect
import tag_library as tl
import datetime as dt


TEST_PENTAD = False
TEST_DECAD = False
TEST_LOCAL = os.getenv("TEST_LOCAL", "").lower() == "true"
LOCAL_URL = "http://localhost:5055/forecast_dashboard"
PENTAD_URL = "https://kyg.fc.pentad.ieasyhydro.org/forecast_dashboard"
DECAD_URL = "https://demo.fc.decade.ieasyhydro.org/forecast_dashboard"
SLEEP = 1
API_BASE = "http://localhost:8000/api"
API_TIMEOUT = 30
horizon = "pentad"  # pentad or decad

def normalize_spaces(s):
    return re.sub(r'\s+', ' ', s).strip()


def normalize_comma(s):
    return s.replace(",", "")


def _get_latest_forecast_metadata(horizon: str) -> tuple[dt.date, int, int]:
    """Mirror the dashboard's get_bulletin_metadata.

    Returns (last_date, horizon_value, year) where last_date is the latest
    forecast production date in the postprocessing DB plus one day,
    horizon_value is the horizon_in_year of that latest record (e.g. pentad
    1-72), and year is last_date.year.

    Pages through both /forecast/ and /lr-forecast/ for the current and
    previous year so we are not silently truncated by the API's default limit.
    """
    cur_year = dt.datetime.now().year
    page_size = 1000
    base_params = {
        "horizon":    horizon,
        "start_date": f"{cur_year - 1}-12-20",
        "end_date":   f"{cur_year + 1}-12-31",
    }
    records = []
    for endpoint in ("forecast", "lr-forecast"):
        skip = 0
        while True:
            resp = requests.get(
                f"{API_BASE}/postprocessing/{endpoint}/",
                params={**base_params, "skip": skip, "limit": page_size},
                timeout=API_TIMEOUT,
            )
            resp.raise_for_status()
            page = resp.json()
            records.extend(page)
            if len(page) < page_size:
                break
            skip += page_size
    if not records:
        raise RuntimeError(f"No forecast records found for horizon {horizon}")
    latest = max(records, key=lambda r: r["date"])
    max_date = dt.datetime.strptime(latest["date"], "%Y-%m-%d").date()
    last_date = max_date + dt.timedelta(days=1)
    return last_date, int(latest["horizon_in_year"]), last_date.year


def _extract_bokeh_data_sources(page) -> list[dict]:
    """Dump every Bokeh ColumnDataSource the active document holds.

    Returns a list of {id, name, data} dicts where `data` is the column dict
    Bokeh actually renders (TypedArrays converted to plain lists so we can
    compare values from Python).
    """
    return page.evaluate(
        """
() => {
  const out = [];
  const docs = (window.Bokeh && window.Bokeh.documents) ? window.Bokeh.documents : [];
  for (const doc of docs) {
    let models;
    try { models = Array.from(doc._all_models.values()); }
    catch (e) { continue; }
    for (const m of models) {
      if (m.type !== 'ColumnDataSource') continue;
      const data = {};
      for (const [k, v] of Object.entries(m.data)) {
        try { data[k] = Array.from(v); }
        catch (e) { data[k] = v; }
      }
      out.push({id: m.id, name: m.name || null, data: data});
    }
  }
  return out;
}
"""
    )


def _series_from_db_rows(
    rows: list[dict], field: str, scale: float = 1.0
) -> list[float]:
    """Extract a date-sorted list of non-null numeric values from API rows.

    `scale` mirrors any unit conversion the dashboard applies before plotting
    (e.g. HS is multiplied by 100 to render in cm — see vizualization.py).
    """
    pairs: list[tuple[str, float]] = []
    for r in rows:
        v = r.get(field)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        pairs.append((str(r.get("date", ""))[:10], float(v) * scale))
    pairs.sort(key=lambda x: x[0])
    return [v for _, v in pairs]


def _is_nan(v) -> bool:
    return isinstance(v, float) and v != v


def _values_close(nums: list[float], expected: list[float]) -> bool:
    """Per-point comparison within max(0.5%, 1e-3) absolute tolerance."""
    if len(nums) != len(expected):
        return False
    return all(
        abs(a - b) <= max(0.005 * abs(b), 1e-3)
        for a, b in zip(nums, expected)
    )


def _is_contiguous_subseq(short: list[float], long_: list[float]) -> bool:
    """True if `short` matches some contiguous slice of `long_` within tol."""
    n = len(short)
    if n == 0 or n > len(long_):
        return False
    for start in range(len(long_) - n + 1):
        if _values_close(long_[start:start + n], short):
            return True
    return False


def _find_matching_canvas_source(
    sources: list[dict], expected_values: list[float]
) -> dict | None:
    """Return the first {id, col, shape} whose non-null numeric values match
    `expected_values` per-point within 0.5% tolerance.

    Tolerates two transforms the dashboard applies before plotting:
      - HoloViews `interpolation='steps-mid'`: each input point becomes two
        consecutive output points → try `nums[::2]`.
      - Display-window trimming (e.g. snow from Sep 1 → Aug 31): the rendered
        series is a contiguous slice of the DB series → try subsequence match.
    """
    expected_count = len(expected_values)
    for src in sources:
        for col, values in src["data"].items():
            if not isinstance(values, list):
                continue
            try:
                nums = [
                    float(v) for v in values
                    if v is not None and not _is_nan(v)
                ]
            except (TypeError, ValueError):
                continue
            if not nums:
                continue
            # 1. Direct equal-length match
            if len(nums) == expected_count and _values_close(nums, expected_values):
                return {"id": src["id"], "col": col, "shape": "direct"}
            # 2. steps-mid doubles each point: [y0, y0, y1, y1, ...]
            if len(nums) == 2 * expected_count and _values_close(nums[::2], expected_values):
                return {"id": src["id"], "col": col, "shape": "steps-mid"}
            # 3. Canvas is a windowed (contiguous) subset of the DB series
            if len(nums) < expected_count and _is_contiguous_subseq(nums, expected_values):
                return {"id": src["id"], "col": col, "shape": "subseq"}
            # 4. steps-mid + windowed: collapse doubling, then subsequence
            if (
                len(nums) % 2 == 0
                and len(nums) // 2 < expected_count
                and _is_contiguous_subseq(nums[::2], expected_values)
            ):
                return {
                    "id": src["id"], "col": col, "shape": "steps-mid+subseq",
                }
    return None


def _dump_numeric_columns_near(sources: list[dict], target_len: int, window: int = 5) -> str:
    """Format a debug list of every numeric column we can extract, with its
    non-null length and first/last/last-expected values for triage. Columns
    whose len is timestamp-like (>=1e10) are skipped — those are date axes."""
    rows = []
    for src in sources:
        for col, values in src["data"].items():
            if not isinstance(values, list):
                continue
            try:
                nums = [
                    float(v) for v in values
                    if v is not None and not _is_nan(v)
                ]
            except (TypeError, ValueError):
                continue
            if not nums:
                continue
            # Skip date axes (large integer timestamps in ms since epoch).
            if abs(nums[0]) > 1e10 and abs(nums[-1]) > 1e10:
                continue
            near = "*" if abs(len(nums) - target_len) <= window else " "
            rows.append(
                f" {near} src={src['id']:>6} col={col!r:<42} len={len(nums):>4} "
                f"first={nums[0]:.4g} last={nums[-1]:.4g}"
            )
    return "\n".join(rows) if rows else "  (no numeric columns)"


def _assert_canvas_matches_db(
    page,
    expected_values: list[float],
    pane_label: str,
    sources: list[dict] | None = None,
) -> None:
    """Assert one ColumnDataSource on the page contains `expected_values`.

    `sources` can be passed in to amortise the page.evaluate cost across
    multiple pane checks for the same station.
    """
    if not expected_values:
        return  # DB has no values for this pane; nothing to compare
    if sources is None:
        sources = _extract_bokeh_data_sources(page)
    match = _find_matching_canvas_source(sources, expected_values)
    if match is None:
        debug = _dump_numeric_columns_near(sources, len(expected_values))
        raise AssertionError(
            f"{pane_label}: no Bokeh source matches DB series "
            f"({len(expected_values)} points; last={expected_values[-1]:.3f}).\n"
            f"Numeric columns near len={len(expected_values)}:\n{debug}"
        )
    print(
        f"#### {pane_label} canvas matches DB "
        f"({len(expected_values)} points, src={match['id']}, col={match['col']!r}, "
        f"shape={match['shape']})"
    )


def _fetch_predictor_data_from_db(station_code: str) -> dict[str, list]:
    """Fetch the data that should populate each Predictors-tab graph.

    Mirrors the dashboard's per-station fetches in src/db.py:
      - daily hydrograph: /preprocessing/hydrograph/?horizon=day
      - precipitation:    /preprocessing/meteo/?meteo_type=P
      - temperature:      /preprocessing/meteo/?meteo_type=T
      - snow:             /preprocessing/snow/?snow_type={HS,ROF,SWE}

    Returns a dict mapping logical pane names to the raw API rows so the test
    can assert "what the graph should display" matches what the DB has.
    """
    cur_year = dt.datetime.now().year
    out: dict[str, list] = {}

    out["hydrograph_day"] = requests.get(
        f"{API_BASE}/preprocessing/hydrograph/",
        params={
            "horizon":    "day",
            "code":       station_code,
            "start_date": f"{cur_year}-01-01",
            "end_date":   f"{cur_year}-12-31",
            "limit":      1000,
        },
        timeout=API_TIMEOUT,
    ).json()

    for key, mtype in (("precipitation", "P"), ("temperature", "T")):
        out[key] = requests.get(
            f"{API_BASE}/preprocessing/meteo/",
            params={
                "meteo_type": mtype,
                "code":       station_code,
                "start_date": f"{cur_year}-01-01",
                "end_date":   f"{cur_year}-12-31",
                "limit":      1000,
            },
            timeout=API_TIMEOUT,
        ).json()

    for stype in ("HS", "ROF", "SWE"):
        out[f"snow_{stype}"] = requests.get(
            f"{API_BASE}/preprocessing/snow/",
            params={
                "snow_type":  stype,
                "code":       station_code,
                "start_date": f"{cur_year - 1}-01-01",
                "end_date":   f"{cur_year}-12-31",
                "limit":      10000,
            },
            timeout=API_TIMEOUT,
        ).json()

    return out


def _fetch_bulletin_from_api(horizon: str, year: int, horizon_value: int) -> list[dict]:
    """Fetch bulletin records from the backend API for the given horizon/year."""
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
    print("#### Testing Predictors tab...")

    # Predictors is the default first tab; click for safety so subsequent
    # locator counts only see the Predictors tab DOM (Tabs use dynamic=True).
    page.locator("div.bk-tab", has_text="Предикторы").click()
    time.sleep(SLEEP)

    # Sidebar: only Horizon, Hydropost, and Manual re-run cards are visible.
    # Card titles are pulled from the Russian gettext catalogue (kyg locale);
    # untranslated strings like "Basin:" stay in English.
    expect(page.get_by_text("Горизонт:", exact=True)).to_be_visible()
    expect(page.get_by_text("Гидропост:", exact=True)).to_be_visible()
    expect(page.get_by_text("Запуск расчета прогноза в ручную", exact=True)).to_be_visible()
    expect(page.get_by_text("Конфигурация прогноза:", exact=True)).not_to_be_visible()
    expect(page.get_by_text("Basin:", exact=True)).not_to_be_visible()
    print("#### Sidebar shows only Horizon, Hydropost, Manual re-run cards.")

    # Main area: only Hydrograph, Precipitation, Temperature, Snow Data cards.
    # "Snow Data" stays English (no Russian translation in the catalogue).
    expect(page.get_by_text("Гидрограф", exact=True)).to_be_visible()
    expect(page.get_by_text("Осадки", exact=True)).to_be_visible()
    expect(page.get_by_text("Температура воздуха", exact=True)).to_be_visible()
    # Snow card title is "Snow Data" with plots, "Snow Data (SnowMapper)" without —
    # accept either with a substring match here; canvas count below proves data.
    expect(page.get_by_text("Snow Data")).to_be_visible()
    print("#### Main area shows Hydrograph, Precipitation, Temperature, Snow Data cards.")

    # Switch through the four hydroposts and assert each pane renders data.
    # Each card produces ≥1 Bokeh <canvas> when data is present; missing data
    # falls back to a markdown "No ... data" pane (no canvas). Requiring ≥4
    # canvases proves all four panes (Hydrograph/Precipitation/Temperature/Snow)
    # received and displayed data for the selected station.
    # Rotate the horizon selector through pentad → decade → month → season
    # for the four stations. Predictor data (hydrograph_day_all, rain, temp,
    # snow_data) is fetched the same way for every horizon (see src/db.py
    # _get_data_monthly / _get_data_season etc.), so the per-pane DB↔canvas
    # checks below must hold regardless of which horizon is active.
    #
    # Horizon select uses the Russian visible label (`пентада` / `декада` /
    # `месяц` / `season`) — Panel emits dict-options as `(value, label)` but
    # Bokeh renders the *label* as the <option>'s `value` attribute, so
    # Playwright matches via `label=`.
    predictor_steps = [
        ("15013 - Джыргалан-с.Советское",                       "pentad",  "пентада"),
        ("16936 - Нарын  -  Приток в Токтогульское вдхр.**)",   "decade",  "декада"),
        ("15212 - Ак-Суу - с.Чон-Арык",                         "month",   "месяц"),
        ("15256 - Талас -  с.Ак-Таш",                           "season",  "season"),
    ]
    for station, horizon_value, horizon_label in predictor_steps:
        code = station.split()[0]
        page.locator("select#input").nth(1).select_option(value=station, timeout=60000)
        page.locator("select#input").nth(0).select_option(label=horizon_label, timeout=60000)
        print(f"#### PREDICTORS station={station}, horizon={horizon_value}")
        time.sleep(SLEEP * 3)  # let plots re-render after both changes

        # Compare what the graphs should show against what the DB has by
        # fetching the same backend data the dashboard requests.
        db = _fetch_predictor_data_from_db(code)
        snow_total = sum(len(db[f"snow_{s}"]) for s in ("HS", "ROF", "SWE"))
        print(
            f"#### Station {code} DB rows — hydrograph_day={len(db['hydrograph_day'])}, "
            f"precipitation={len(db['precipitation'])}, "
            f"temperature={len(db['temperature'])}, snow={snow_total}"
        )
        assert db["hydrograph_day"], f"Station {code}: no daily hydrograph data in DB"
        assert db["precipitation"],  f"Station {code}: no precipitation data in DB"
        assert db["temperature"],    f"Station {code}: no temperature data in DB"
        assert snow_total > 0,       f"Station {code}: no snow data in DB"

        # Each pane renders ≥1 Bokeh <canvas> when its data is present; missing
        # data falls back to a markdown "No ... data" pane (no canvas). With
        # the DB asserted non-empty above, ≥4 canvases proves every pane
        # rendered the data the DB holds for this station.
        canvas_count = page.locator("canvas").count()
        assert canvas_count >= 4, (
            f"Station {code}: only {canvas_count} predictor canvas(es) rendered "
            "(expected ≥4 — DB has data for Hydrograph/Precipitation/Temperature/Snow "
            "so all four panes must render canvases)"
        )
        print(f"#### Station {code}: {canvas_count} predictor canvas(es) rendered.")

        # Value-level comparison: each pane's canvas must hold the same
        # values the DB returns for this station. Pull the page's Bokeh
        # ColumnDataSources once and reuse across panes.
        sources = _extract_bokeh_data_sources(page)

        _assert_canvas_matches_db(
            page,
            _series_from_db_rows(db["hydrograph_day"], "current"),
            f"Hydrograph (current year) [{code}]",
            sources=sources,
        )
        _assert_canvas_matches_db(
            page,
            _series_from_db_rows(db["precipitation"], "value"),
            f"Precipitation [{code}]",
            sources=sources,
        )
        _assert_canvas_matches_db(
            page,
            _series_from_db_rows(db["temperature"], "value"),
            f"Temperature [{code}]",
            sources=sources,
        )
        # Snow card may render any subset of HS/RoF/SWE — verify each that
        # the DB has data for is also drawn into the canvas. The dashboard
        # multiplies HS by 100 (metres → centimetres) before plotting, and
        # trims the series to a configured display window (Sep 1 → Aug 31
        # for the kyg locale), so the matcher's subsequence path handles
        # the windowing while the scale handles the unit conversion.
        snow_scales = {"HS": 100.0, "ROF": 1.0, "SWE": 1.0}
        for stype in ("HS", "ROF", "SWE"):
            snow_series = _series_from_db_rows(
                db[f"snow_{stype}"], "value", scale=snow_scales[stype]
            )
            if not snow_series:
                continue
            _assert_canvas_matches_db(
                page,
                snow_series,
                f"Snow {stype} [{code}]",
                sources=sources,
            )

    # Reset horizon to pentad so the existing Forecast/Bulletin flow below —
    # which inspects pentad-horizon summary tables and pentad bulletins — runs
    # against the same horizon it was originally written for.
    page.locator("select#input").nth(0).select_option(label="пентада", timeout=60000)
    print("#### Horizon reset to pentad for Forecast/Bulletin flow.")
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

    # Resolve forecast year/horizon_value/sheet_name/month_str from the latest
    # forecast record in the DB — mirrors the dashboard's get_bulletin_metadata
    # so the test does not depend on today's wall-clock date.
    last_date, horizon_value, year = _get_latest_forecast_metadata(horizon)
    date_str = last_date.strftime("%Y-%m-%d")
    print("Latest forecast (last_date):", date_str)
    month_str = last_date.strftime("%m") + "_" + tl.get_month_str_case1(date_str)
    if horizon == "pentad":
        horizon_value_in_month = tl.get_pentad(date_str)
        print("Pentad in year:", horizon_value)
        print("Pentad in month:", horizon_value_in_month)
        sheet_name = f"{horizon_value_in_month} пентада"
    else:
        horizon_value_in_month = tl.get_decad_in_month(date_str)
        print("Decad in year:", horizon_value)
        print("Decad in month:", horizon_value_in_month)
        sheet_name = f"{horizon_value_in_month} декада"

    # Fetch bulletin records from the API
    print("#### Fetching bulletin from API...")
    api_records = _fetch_bulletin_from_api(horizon, year, horizon_value)
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
