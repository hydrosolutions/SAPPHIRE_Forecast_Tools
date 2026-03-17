# Dashboard Code Observations

Observations found during test development for `apps/forecast_dashboard/`.
These are **read-only notes** — the dashboard `src/` and `dashboard/` code is
owned by a colleague and should not be edited without coordination.

## site.py

1. **`oder_sites_list_according_to_bulletin_order` is not a classmethod** — it
   takes `cls` as first argument but is not decorated with `@classmethod`. It
   works when called on an instance (`site.oder_sites_list_according_to_...`)
   but fails when called as `SapphireSite.oder_sites_list_according_to_...(list)`
   because Python passes the class as `cls` and the actual list as `sites_list`.

2. **`oder_sites_list_according_to_bulletin_order` does not reorder** — the
   implementation uses a list comprehension `[site for site in sites_list if
   site.code in ordered_codes]` which preserves original list order, not the
   sorted order from the DataFrame.

3. **`station_label` construction is inconsistent** — the `__init__` builds it
   as `"code - river punkt"` but only when `station_label` arg is truthy,
   while `get_site_attribues_from_iehhf_dataframe` overwrites it as
   `"code - name_ru"`.

## processing.py

4. **`calculate_forecast_range` uses inconsistent min/max logic** — in the
   `max[delta, %]` mode with a scalar slider, the code uses `np.minimum` for
   `fc_lower` and `np.maximum` for `fc_upper` (opposite of the widget path
   which uses `np.maximum`/`np.minimum`). This means the scalar and widget
   paths produce different results for the same inputs.

5. **`get_bulletin_header_info` requires string date input** — the `tl.*`
   functions expect ISO date strings, but the function signature suggests
   `date` (no type hint). Passing a `datetime.date` object causes a
   `TypeError` in `strptime`.

## db.py

6. **Module-level `horizon` variable** — `db.py` evaluates `os.getenv` at
   import time (line 26–28), which can be stale if the env var changes later.
   The `_get_horizon()` function correctly re-reads the env var each call.

## test_integration.py

7. **Integration tests use early-return instead of `pytest.skip()`** — the
   three Playwright tests (`test_pentad`, `test_decad`, `test_local`) check
   env-var flags and `return` immediately when the flag is false. Pytest
   reports them as PASSED (3 hollow passes), not SKIPPED. This inflates the
   pass count and masks the fact that no integration testing actually ran.
   The recommended fix is to replace the early-return pattern with
   `pytest.skip("TEST_PENTAD not set")` so pytest reports them honestly.
