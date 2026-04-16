# Migrate snow norm computation from CSV to API

**Status**: Implemented — all tests passing (2026-04-16)
**Module**: preprocessing_gateway
**Priority**: Medium
**Labels**: `enhancement`, `snow-data`, `api-migration`, `preprocessing_gateway`

---

## Summary

Migrate the snow norm computation in `recalculate_snow_norms.py` from
reading historical CSV files to reading from the SAPPHIRE preprocessing
API. This is a follow-up to PREPG-001 (yearly snow norm recalculation),
which implemented the infrastructure but used CSV as the data source.

## Context

Snow norms (climatological daily averages for SWE, HS, RoF) are computed
yearly (August 30) by `recalculate_snow_norms.py`. The script calls
`dg_utils.calculate_snow_norms()` which reads CSVs at
`{snow_path}/{variable}/{hru}_{variable}.csv`.

The project is transitioning from CSV-based I/O to API-backed storage.
The preprocessing API already contains all historical snow data (written
by the operational and reanalysis pipelines). The CSV source is
deprecated.

## Problem

`calculate_snow_norms()` reads CSVs that may contain only 1-2 years of
data, producing norms that are nearly identical to individual year values.
The API contains the full historical record (potentially 2000-present from
reanalysis), but the norm computation doesn't use it.

On the dashboard, the "Norm" and "Current year" curves appear identical
because the norm was computed from insufficient historical data.

## Desired Outcome

- Norm computation reads all historical snow data from the preprocessing
  API instead of CSV files
- Norms are computed as the mean across all available years per
  day-of-year — more years of data produce a more meaningful climatological
  reference
- The dashboard's norm curve diverges from the current-year curve,
  giving hydrologists a useful reference
- The output format is unchanged: `DataFrame[snow_type, code, dayofyear, norm]`

---

## Technical Analysis

### Current Implementation

**Norm computation** — `dg_utils.py:304-372`, `calculate_snow_norms()`:
```python
csv_path = os.path.join(path, variable, f"{hru}_{variable}.csv")
df = pd.read_csv(csv_path)
df["dayofyear"] = df["date"].dt.dayofyear
norms = code_df.groupby("dayofyear")[variable].mean().reset_index()
```
Reads CSVs, groups by `dayofyear`, takes mean of the variable column.

**Caller** — `recalculate_snow_norms.py:93`:
```python
norms_df = dg_utils.calculate_snow_norms(snow_path, variables, hru_codes)
```

**API client already available** — `recalculate_snow_norms.py:117`:
```python
client = dg_utils.SapphirePreprocessingClient(base_url=api_url)
```
The client is created after the norm computation. It needs to be created
before it.

**API read method** — `SapphirePreprocessingClient.read_snow()`:
```python
def read_snow(
    self,
    snow_type: Optional[str] = None,
    code: Optional[str] = None,
    start_date: Optional[Union[str, date]] = None,
    end_date: Optional[Union[str, date]] = None,
    skip: int = 0,
    limit: int = 100,
) -> pd.DataFrame:
```
Returns DataFrame with columns: `snow_type, code, date, value, norm,
value1-value14`. All filter params are optional. Default limit is 100 —
must be increased to fetch all historical data.

**Data volume estimate**: ~25 years x 365 days = ~9,000 rows per
station/variable. With no code filter the query returns all stations:
10 stations → ~90,000 rows; 12+ stations → exceeds 100,000 rows and
causes silent truncation. The function **paginates** (batch size 10,000)
to handle any station count safely (see third review).

### Key Files

| File | Role |
|------|------|
| `apps/preprocessing_gateway/dg_utils.py:304-372` | `calculate_snow_norms()` — current CSV-based computation |
| `apps/preprocessing_gateway/recalculate_snow_norms.py:79-218` | `_recalculate_norms_impl()` — orchestration |
| `apps/preprocessing_gateway/test/test_recalculate_snow_norms.py` | **6 existing tests — 4 will break** (see review findings) |
| `apps/preprocessing_gateway/test/test_data_transforms.py:657-870` | `TestCalculateSnowNorms` — 10 tests for CSV function (safe, function kept) |
| `apps/preprocessing_gateway/test/test_api_integration.py` | Existing API integration tests (pattern reference) |

---

## Code review findings (2026-04-16)

An automated review verified the plan against the actual codebase. All
references are accurate. One critical amendment is required.

### CRITICAL: 4 of 6 existing tests in `test_recalculate_snow_norms.py` will break

The tests create CSV files and call `rsn.recalculate_norms()` which
internally calls `dg_utils.calculate_snow_norms()` to read those CSVs.
After migration, the production code calls `calculate_snow_norms_from_api()`
instead, which uses `client.read_snow()`.

The mock setup in the affected tests:
```python
mock_client.read_snow.return_value = pd.DataFrame()  # empty!
```

This was fine because `read_snow` was only used for "read existing records
to preserve values". After migration, `read_snow` is ALSO called for the
norm computation — the empty return means no norms computed → returns False.

| Test | Status after migration |
|------|----------------------|
| `test_happy_path_norms_written_to_api` | **BREAKS** — empty `read_snow` → no norms |
| `test_preserves_existing_values_from_api` | **BREAKS** — same |
| `test_leap_year_includes_day_366` | **BREAKS** — same |
| `test_multiple_variables_and_codes` | **BREAKS** — same |
| `test_no_csv_data_returns_false` | **Semantics change** — CSVs irrelevant |
| `test_api_unavailable_returns_false` | Safe — still returns False |

**Fix**: Mock `dg_utils.calculate_snow_norms_from_api` directly in the
tests that focus on the write logic. This decouples norm computation tests
(in the new test file) from end-to-end write tests. Example:

```python
@patch("dg_utils.calculate_snow_norms_from_api")
@patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
@patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
def test_happy_path_norms_written_to_api(self, mock_client_class,
                                          mock_calc_norms, tmp_path):
    mock_calc_norms.return_value = pd.DataFrame({
        "snow_type": ["SWE"] * 365,
        "code": ["19999"] * 365,
        "dayofyear": range(1, 366),
        "norm": [50.0 + i * 0.1 for i in range(365)],
    })
    # ... rest of test unchanged ...
```

`test_no_csv_data_returns_false` should be renamed and updated to test
that an empty API response returns False.

### Additional verified findings (no action needed)

- `SapphirePreprocessingClient` is imported at module level in `dg_utils.py`
  (line 14) with `SAPPHIRE_API_AVAILABLE` guard — in scope for the new function
- `client.read_snow()` confirmed to return `value` column (from `SnowBase` schema)
- `start_date` without `end_date` works (only `>= start_date` filter applied)
- `limit=100000` is accepted (no server-side cap, already used by
  `_read_existing_norms` at line 380)
- Insertion point after line 372 is clean (before section comment at line 375)
- No naming conflicts with `calculate_snow_norms_from_api`
- `test_data_transforms.py::TestCalculateSnowNorms` (10 tests) tests the CSV
  function directly — safe, function is kept with deprecation comment
- Moving API checks before norm computation: no caller observes the difference
  (all return False paths unchanged)

---

## Second code review findings (2026-04-16)

A second review verified the plan against the actual data flow in the
codebase. Two amendments are required.

### CRITICAL: `hru_codes` vs station codes semantic mismatch

The plan's proposed `calculate_snow_norms_from_api()` function passes
`hru_codes` (e.g., `"HRU_SNOW01"`) to `client.read_snow(code=...)`.
This will **silently return no data** because the API stores **station
codes** (e.g., `"19999"`), not HRU identifiers.

**How the current CSV flow works:**
1. `hru_codes` (from env var `ieasyhydroforecast_HRU_SNOW_DATA`) → used
   only for filename construction: `{path}/SWE/HRU_SNOW01_SWE.csv`
2. Inside the CSV, the `code` column contains station codes (written by
   `transform_snow_data()`)
3. `calculate_snow_norms()` reads the CSV, discovers station codes via
   `df["code"].unique()`, computes norms per station
4. The write loop in `_recalculate_norms_impl()` iterates over these
   station codes — they never come from `hru_codes`

**API stores station codes:** `Snow.code` stores station codes (e.g., `"19999"`).
(`crud.py:295` does exact equality: `Snow.code == code`). Querying with
`"HRU_SNOW01"` matches zero rows.

**Fix (approved by user):** Drop the `codes` parameter from
`calculate_snow_norms_from_api()` entirely. Query per variable without a
`code` filter — the API returns all station codes for that snow type.
Discover codes from the response data and compute norms per
`(code, dayofyear)`. This also means `hru_codes` becomes unused in
`_recalculate_norms_impl()` alongside `snow_path`.

### CORRECTION: Test 4 failure reason

The first review stated: "This test currently returns False because of
`SAPPHIRE_API_ENABLED=false`, NOT because of empty CSVs."

This is **incorrect**. In the current flow:
1. Line 93: `calculate_snow_norms(empty_path, ...)` → CSV not found →
   returns empty DataFrame
2. Lines 95-97: `if norms_df.empty: return False` — **returns here**
3. The `SAPPHIRE_API_ENABLED=false` check at line 114 is never reached

The test returns False because of empty CSVs, exactly as the test name
says. The proposed rewrite to `test_empty_api_data_returns_false` is
still correct — it just tests the API-era equivalent scenario.

---

## Third code review findings (2026-04-16)

A third review verified the plan against the actual data volume and test
consistency. Three amendments are required.

### CRITICAL: Real station code "15013" throughout plan and tests

The plan's code examples and the existing `_make_snow_csv` helper use
`"15013"`, which is a real station code. Per CLAUDE.md sensitive data
rules, real station codes must never be committed to GitHub.

**Fix**: Replace all `"15013"` with `"19999"` in the plan and in the
existing test file lines being touched (lines 48 and 114 of
`test_recalculate_snow_norms.py`).

### CRITICAL: `_make_snow_csv` removal contradicts test 3

The plan says "Remove `_make_snow_csv` helper" (General changes) but
test 3 (`test_api_unavailable_returns_false`) calls it at line 149.
The plan said "No change needed" for test 3.

**Fix**: Explicitly remove the `_make_snow_csv` call from test 3.
The test still passes — `SAPPHIRE_API_ENABLED=false` triggers early
return before any CSV or API interaction.

### HIGH: Silent data truncation with `limit=100000` and no code filter

The plan queries all stations per variable in a single call with
`limit=100000`. Per-station data is ~9,000 rows (25 years × 365 days).
With 12+ stations, total rows exceed 100,000 and the API silently
returns only the first 100,000 — some stations lose data entirely.

The preprocessing API (`crud.py:300`) applies `.limit(limit)` with no
server-side maximum. No error is raised on truncation.

**Fix**: Paginate through results in batches of 10,000 using `skip`
and `limit` parameters, concatenating pages until the API returns
fewer rows than the batch size.

### MEDIUM: Mock DataFrames should match actual API schema

The API returns columns: `id, snow_type, code, date, value, norm,
value1-value14`. Test 2's `read_snow` mock only includes `date, code,
snow_type, value, norm`. While the production code only accesses a
subset, mocks should include `id` for schema fidelity.

**Fix**: Add `"id"` column to the `read_snow` mock in test 2.

---

## Fourth code review findings (2026-04-16)

A fourth review verified the pagination approach against the actual
database query implementation.

### HIGH: `get_snow()` lacks `ORDER BY` — pagination is non-deterministic

The plan paginates through snow data using `skip`/`limit` batches of
10,000. However, `crud.get_snow()` (`crud.py:300`) applies
`.offset(skip).limit(limit)` **without `ORDER BY`**:

```python
results = query.offset(skip).limit(limit).all()  # no order_by!
```

All three other CRUD functions (`get_runoff` line 80, `get_hydrograph`
line 153, `get_meteo` line 227) include `.order_by(Model.code, Model.date)`
before pagination. `get_snow()` is missing this — likely an oversight.

Without deterministic ordering, PostgreSQL may return rows in different
orders between page requests. Rows can be duplicated on two pages or
missed entirely, producing silently incorrect norms.

**Fix**: Add `df.drop_duplicates(subset=["snow_type", "code", "date"])`
after concatenating pages. This makes pagination safe regardless of
query ordering, at zero cost. A separate issue should be filed to add
`order_by` to `get_snow()` in the preprocessing service.

### LOW: Hardcoded `start_date="2000-01-01"` excludes pre-2000 data

The function hardcodes `start_date="2000-01-01"`. If reanalysis data
extends before 2000, those years are silently excluded from norms.
`crud.get_snow()` only applies the date filter `if start_date:` — passing
`None` fetches all available data, which is both simpler and safer.

**Fix**: Remove `start_date` parameter from `read_snow()` calls (pass
`None` / omit it). The API returns all records for the given `snow_type`.

---

## Implementation Plan

### Approach

Add a new function `calculate_snow_norms_from_api()` in `dg_utils.py`
that reads historical snow data from the API and returns the same output
format. Update `recalculate_snow_norms.py` to use it. Keep the old
CSV-based function with a deprecation comment.

### Files to Modify

| File | Changes |
|------|---------|
| `apps/preprocessing_gateway/dg_utils.py` | Add `calculate_snow_norms_from_api()` after the existing `calculate_snow_norms()` (line 372) |
| `apps/preprocessing_gateway/recalculate_snow_norms.py` | Restructure `_recalculate_norms_impl()` to create client first, then compute norms from API |
| `apps/preprocessing_gateway/test/test_recalculate_snow_norms.py` | Update 4 breaking tests to mock `calculate_snow_norms_from_api` instead of relying on CSV files; rename `test_no_csv_data_returns_false` |

### Files to Create

| File | Purpose |
|------|---------|
| `apps/preprocessing_gateway/test/test_snow_norms_from_api.py` | Unit tests for the new function |

---

### Step 1: Add `calculate_snow_norms_from_api()` to `dg_utils.py`

Add after `calculate_snow_norms()` (after line 372), before the
`_read_existing_norms` function (line 380):

```python
def calculate_snow_norms_from_api(
    client,
    variables: list[str],
) -> pd.DataFrame:
    """Calculate climatological daily snow norms from API data.

    Reads all historical snow data from the preprocessing API for each
    variable (no code filter — discovers station codes from the response).
    Groups by ``(code, dayofyear)`` and computes the mean of the
    ``value`` column across all years.

    Args:
        client: SapphirePreprocessingClient instance.
        variables: Snow variable names (e.g., ``["SWE", "HS", "RoF"]``).

    Returns:
        DataFrame with columns ``[snow_type, code, dayofyear, norm]``.
        Returns an empty DataFrame with those columns if no data is
        found.
    """
    result_frames = []
    batch_size = 10000

    for variable in variables:
        # Paginate through all historical data for this variable
        pages = []
        skip = 0
        try:
            while True:
                page = client.read_snow(
                    snow_type=variable.upper(),
                    skip=skip,
                    limit=batch_size,
                )
                if page.empty:
                    break
                pages.append(page)
                if len(page) < batch_size:
                    break
                skip += batch_size
        except Exception as e:
            logger.warning(
                "Could not read snow data for %s: %s", variable, e,
            )
            continue

        if not pages:
            logger.info("No API data for %s, skipping", variable)
            continue

        df = pd.concat(pages, ignore_index=True)
        df = df.drop_duplicates(subset=["snow_type", "code", "date"])
        logger.info(
            "Fetched %d unique rows for %s in %d pages",
            len(df), variable, len(pages),
        )

        if "value" not in df.columns:
            logger.warning(
                "No 'value' column for %s, skipping", variable,
            )
            continue

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date", "value"])

        if df.empty:
            continue

        df["dayofyear"] = df["date"].dt.dayofyear

        # Compute norms per station code, per day of year
        for code in df["code"].unique():
            code_df = df[df["code"] == code]

            n_years = code_df["date"].dt.year.nunique()
            logger.info(
                "Computing norm for %s/%s from %d years of data",
                variable, code, n_years,
            )

            norms = (
                code_df.groupby("dayofyear")["value"]
                .mean()
                .reset_index()
            )
            norms.columns = ["dayofyear", "norm"]
            norms["snow_type"] = variable
            norms["code"] = str(code)
            result_frames.append(
                norms[["snow_type", "code", "dayofyear", "norm"]]
            )

    if result_frames:
        return pd.concat(result_frames, ignore_index=True)

    return pd.DataFrame(columns=["snow_type", "code", "dayofyear", "norm"])
```

Key differences from the CSV version:
- Takes a `client` instead of a filesystem `path`
- **No `codes` parameter** — station codes are discovered from the API
  response (the CSV version used `hru_codes` only for filenames; the
  actual codes came from the CSV `code` column — see second review)
- **Paginates** through API results in batches of 10,000 rows to avoid
  silent truncation when many stations exist (see third review)
- Fetches all station data per variable (no code filter)
- Groups by `(code, dayofyear)` on the `value` column
- No `start_date` filter — fetches all available historical data
- Deduplicates after pagination to guard against non-deterministic
  query ordering in `get_snow()` (see fourth review)
- Logs the number of years used per station/variable and page count
- Returns identical output format: `[snow_type, code, dayofyear, norm]`

Also add a deprecation comment to `calculate_snow_norms()` (line 304):
```python
def calculate_snow_norms(
    path: str,
    variables: list[str],
    hru_codes: list[str],
) -> pd.DataFrame:
    """Calculate climatological daily snow norms from historical CSVs.

    .. deprecated::
        Use ``calculate_snow_norms_from_api()`` instead. CSV-based
        computation is deprecated as part of the CSV-to-API migration.
    ...
```

---

### Step 2: Update `recalculate_snow_norms.py`

Restructure `_recalculate_norms_impl()` (line 79) to:
1. Check API availability and create client **first**
2. Compute norms from API using the new function
3. Write norms (existing code, unchanged)

**Current order** (lines 79-218):
```
1. norms_df = dg_utils.calculate_snow_norms(snow_path, ...)     # CSV
2. if not dg_utils.SAPPHIRE_API_AVAILABLE: return False          # API check
3. client = SapphirePreprocessingClient(...)                      # create client
4. for each variable+code: read existing, build records, write   # write norms
```

**New order**:
```
1. if not dg_utils.SAPPHIRE_API_AVAILABLE: return False          # API check (moved up)
2. client = SapphirePreprocessingClient(...)                      # create client (moved up)
3. if not client.readiness_check(): return False                  # readiness (moved up)
4. norms_df = dg_utils.calculate_snow_norms_from_api(client, ...) # API-based
5. for each variable+code: read existing, build records, write   # write norms (unchanged)
```

Concrete changes to `_recalculate_norms_impl()`:

```python
def _recalculate_norms_impl(
    snow_path: str,
    variables: list[str],
    hru_codes: list[str],
    year: int,
) -> bool:
    """Internal implementation of norm recalculation."""
    # 1. Check API availability and create client
    if not dg_utils.SAPPHIRE_API_AVAILABLE:
        logger.warning("sapphire-api-client not installed, cannot compute or write norms")
        return False

    api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
    if not api_enabled:
        logger.info("API disabled via SAPPHIRE_API_ENABLED=false")
        return False

    api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    client = dg_utils.SapphirePreprocessingClient(base_url=api_url)

    if not client.readiness_check():
        logger.warning("API at %s not ready, skipping norm recalculation", api_url)
        return False

    # 2. Compute norms from API data
    logger.info(
        "Calculating snow norms from API for variables %s",
        variables,
    )
    norms_df = dg_utils.calculate_snow_norms_from_api(client, variables)

    if norms_df.empty:
        logger.warning("No snow norms computed — no historical data found in API")
        return False

    logger.info(
        "Computed %d norm entries across %d variables and %d codes",
        len(norms_df),
        norms_df["snow_type"].nunique(),
        norms_df["code"].nunique(),
    )

    # 3. Build date range for the target year
    is_leap = dg_utils.is_leap_year(year)
    # ... rest of function unchanged from line 124 onward ...
```

The `snow_path` and `hru_codes` parameters to `_recalculate_norms_impl()`
and `recalculate_norms()` become unused. Keep them in the signature for
backward compatibility (the `main()` entry point still passes them) but
add deprecation notes:

```python
def recalculate_norms(
    snow_path: str,          # Deprecated — no longer used (CSV migration)
    variables: list[str],
    hru_codes: list[str],    # Deprecated — codes discovered from API data
    year: int,
    env_overrides: dict | None = None,
) -> bool:
```

---

### Step 3: Write new tests + update existing tests

#### 3a: New tests for `calculate_snow_norms_from_api`

Create `apps/preprocessing_gateway/test/test_snow_norms_from_api.py`
following the pattern in `test_api_integration.py` (mocked API client,
`sys.path.insert`, `sys.modules` mock for `sapphire_dg_client`).

**Class: `TestCalculateSnowNormsFromApi`**

1. `test_single_year_norm_equals_values` — 1 year of data: norm for
   each dayofyear equals the single observation.

2. `test_multi_year_norm_is_mean` — 3 years of data with different
   values on the same dayofyear: norm = mean of the 3 values.

3. `test_multiple_variables` — SWE + HS data: returns norms for both
   with correct `snow_type` column.

4. `test_multiple_codes_discovered_from_response` — API returns data
   for 2 station codes in the same response: returns separate norms per
   code. Verifies the function discovers codes from the data (no `codes`
   parameter).

5. `test_missing_value_rows_excluded` — rows with `value=NaN` are
   excluded from the mean computation.

6. `test_empty_api_response` — `client.read_snow()` returns empty
   DataFrame: returns empty DataFrame with correct columns.

7. `test_api_error_handled_gracefully` — `client.read_snow()` raises
   exception: logs warning and continues to next variable.

8. `test_output_format` — verify output has exactly columns
   `[snow_type, code, dayofyear, norm]`, `dayofyear` is int 1-366,
   `norm` is float.

9. `test_pagination_fetches_all_pages` — mock `client.read_snow()` to
   return full batches on the first two calls and a partial batch on
   the third: verifies all pages are concatenated, **deduplicated**,
   and norms computed from the full dataset. Use `batch_size` as
   internal constant (10,000) but mock with small sizes (e.g., 3 rows
   per page) for test speed.

All test station codes must use `"19999"` (or similar fake codes like
`"29999"` for multi-code tests). Never use real station codes.

#### 3b: Update existing `test_recalculate_snow_norms.py` (CRITICAL)

The file has 6 tests. After migration, `_recalculate_norms_impl` calls
`dg_utils.calculate_snow_norms_from_api(client, ...)` instead of
`dg_utils.calculate_snow_norms(snow_path, ...)`. Four tests create CSV
files and expect the CSV function to read them — these break because the
CSV function is no longer called.

**Strategy**: Mock `dg_utils.calculate_snow_norms_from_api` in the 4
affected tests. This keeps the tests focused on the write logic (their
actual purpose) and decouples them from the norm computation (tested
separately in Step 3a).

**General changes**:
- Update module docstring (lines 1-9): replace "Calls
  dg_utils.calculate_snow_norms() on historical CSVs" with "Calls
  dg_utils.calculate_snow_norms_from_api() to compute norms from API"
- Remove `_make_snow_csv` helper (lines 34-56) — no longer needed.
  Also remove the call in `test_api_unavailable_returns_false` (line 149)
- Replace all occurrences of station code `"15013"` with `"19999"` in
  this file (lines 48 and 114 in the existing code) — real station
  codes must not appear in test code committed to GitHub
- `snow_path` parameter in test calls: keep passing a dummy string
  (parameter still exists but is unused)

**Helper to add** (at class level, replaces `_make_snow_csv`):

```python
@staticmethod
def _make_norms_df(variables, code="19999", n_days=365):
    """Build a norms DataFrame matching calculate_snow_norms_from_api output.

    Uses fake station code "19999" (real codes must never appear in tests).
    """
    frames = []
    for var in variables:
        frames.append(pd.DataFrame({
            "snow_type": [var] * n_days,
            "code": [code] * n_days,
            "dayofyear": range(1, n_days + 1),
            "norm": [50.0 + i * 0.1 for i in range(n_days)],
        }))
    return pd.concat(frames, ignore_index=True)
```

---

**Test 1: `test_happy_path_norms_written_to_api` (line 58)**

Current: Creates CSV, relies on CSV norm computation.

Change:
- Add decorator: `@patch("dg_utils.calculate_snow_norms_from_api")`
- Add parameter: `mock_calc_norms` (note: decorators apply bottom-up,
  so mock_calc_norms is the first positional mock param after self)
- Set mock: `mock_calc_norms.return_value = self._make_norms_df(["SWE"])`
- Remove: `self._make_snow_csv(snow_path, "SWE", "HRU01")` call
- Keep: all `mock_client` setup, all assertions unchanged

```python
@patch("dg_utils.calculate_snow_norms_from_api")
@patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
@patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
def test_happy_path_norms_written_to_api(self, mock_client_class,
                                          mock_calc_norms, tmp_path):
    mock_calc_norms.return_value = self._make_norms_df(["SWE"])

    mock_client = Mock()
    mock_client.readiness_check.return_value = True
    mock_client.read_snow.return_value = pd.DataFrame()
    mock_client.write_snow.return_value = 365
    mock_client_class.return_value = mock_client

    env = {
        "SAPPHIRE_API_ENABLED": "true",
        "SAPPHIRE_API_URL": "http://localhost:8000",
    }

    result = rsn.recalculate_norms(
        snow_path=str(tmp_path),  # unused but required
        variables=["SWE"],
        hru_codes=["HRU01"],
        year=2024,
        env_overrides=env,
    )

    assert result is True
    mock_client.write_snow.assert_called()
    records = mock_client.write_snow.call_args[0][0]
    assert len(records) > 0
    for r in records:
        assert r["norm"] is not None
        assert r["snow_type"] == "SWE"
    dates = sorted(r["date"] for r in records)
    assert dates[0] == "2024-01-01"
    assert dates[-1] == "2024-12-31"
```

All assertions are identical — they verify the write output, not the
norm computation.

---

**Test 2: `test_preserves_existing_values_from_api` (line 100)**

Current: Creates CSV. `mock_client.read_snow` returns one row with
`value=88.8, norm=NaN`. Verifies Jan 15 record preserves `value=88.8`
and gets a computed norm.

Change:
- Add decorator: `@patch("dg_utils.calculate_snow_norms_from_api")`
- Add parameter: `mock_calc_norms`
- Set mock: `mock_calc_norms.return_value = self._make_norms_df(["SWE"])`
- Remove: CSV creation
- Update: `mock_client.read_snow` mock to use `"19999"` instead of
  `"15013"` and include `"id"` column to match actual API schema
- Keep: `mock_client.read_snow` returning existing record — this is now
  ONLY called by the "preserve existing values" step (norm computation
  is mocked out)
- Keep: all assertions unchanged

Mock DataFrame for `read_snow` should match the real API schema:
```python
mock_client.read_snow.return_value = pd.DataFrame({
    "id": [1],
    "date": pd.to_datetime(["2024-01-15"]),
    "code": ["19999"],
    "snow_type": ["SWE"],
    "value": [88.8],
    "norm": [np.nan],
})
```

This test remains meaningful: it verifies that when the API has an
existing record with a value, that value is preserved in the written
record alongside the computed norm.

---

**Test 3: `test_api_unavailable_returns_false` (line 146)**

Current: Creates CSV via `_make_snow_csv`, sets `SAPPHIRE_API_ENABLED=false`.
After migration: API disabled check happens first → returns False.

Change:
- Remove: `self._make_snow_csv(snow_path, "SWE", "HRU01")` call (the
  `_make_snow_csv` helper is deleted — see General changes)
- Keep: `snow_path = str(tmp_path / "snow")` (unused but harmless)
- Keep: all assertions unchanged

The test still returns False because `SAPPHIRE_API_ENABLED=false` triggers
early return. No CSV or API interaction occurs.

---

**Test 4: `test_no_csv_data_returns_false` (line 165)**

Current: No CSV files, sets `SAPPHIRE_API_ENABLED=false` → returns False.
This test returns False because `calculate_snow_norms()` finds no CSVs
and returns an empty DataFrame, triggering the `norms_df.empty` check at
line 97. The `SAPPHIRE_API_ENABLED=false` check at line 114 is never
reached.

Change — rename and restructure to test the actual API-based scenario:
- Rename to `test_empty_api_data_returns_false`
- Add decorators: `@patch("dg_utils.calculate_snow_norms_from_api")`,
  `@patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)`,
  `@patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")`
- Mock `calculate_snow_norms_from_api` to return empty DataFrame
- Mock client with `readiness_check.return_value = True`
- Set `SAPPHIRE_API_ENABLED=true`
- Assert: `result is False` (empty norms → function returns False)

```python
@patch("dg_utils.calculate_snow_norms_from_api")
@patch.object(dg_utils, "SAPPHIRE_API_AVAILABLE", True)
@patch("recalculate_snow_norms.dg_utils.SapphirePreprocessingClient")
def test_empty_api_data_returns_false(self, mock_client_class,
                                       mock_calc_norms, tmp_path):
    mock_calc_norms.return_value = pd.DataFrame(
        columns=["snow_type", "code", "dayofyear", "norm"]
    )
    mock_client = Mock()
    mock_client.readiness_check.return_value = True
    mock_client_class.return_value = mock_client

    result = rsn.recalculate_norms(
        snow_path=str(tmp_path),
        variables=["SWE"],
        hru_codes=["HRU01"],
        year=2024,
        env_overrides={"SAPPHIRE_API_ENABLED": "true",
                       "SAPPHIRE_API_URL": "http://localhost:8000"},
    )

    assert result is False
    mock_client.write_snow.assert_not_called()
```

---

**Test 5: `test_leap_year_includes_day_366` (line 182)**

Current: Creates CSV with 5 years, year=2024 (leap year).

Change:
- Add decorator: `@patch("dg_utils.calculate_snow_norms_from_api")`
- Add parameter: `mock_calc_norms`
- Set mock: `mock_calc_norms.return_value = self._make_norms_df(["SWE"], n_days=366)`
- Remove: CSV creation
- Keep: all assertions (366 records, dates span full leap year)

---

**Test 6: `test_multiple_variables_and_codes` (line 214)**

Current: Creates CSVs for SWE and HS.

Change:
- Add decorator: `@patch("dg_utils.calculate_snow_norms_from_api")`
- Add parameter: `mock_calc_norms`
- Set mock: `mock_calc_norms.return_value = self._make_norms_df(["SWE", "HS"])`
- Remove: CSV creation
- Keep: all assertions (`write_snow.call_count >= 2`)

---

**Summary of what each test verifies after migration**:

| Test | What it actually tests |
|------|----------------------|
| `test_happy_path_norms_written_to_api` | Norms are written to API with correct format and date range |
| `test_preserves_existing_values_from_api` | Existing API values are preserved alongside new norms |
| `test_api_unavailable_returns_false` | Graceful failure when API is disabled |
| `test_empty_api_data_returns_false` | Graceful failure when API has no historical data |
| `test_leap_year_includes_day_366` | Leap year produces 366 records |
| `test_multiple_variables_and_codes` | Multiple variables produce separate write calls |

---

## Testing

### Testing Commands

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway
```

### Manual Verification

1. Run `recalculate_snow_norms.py` against a deployment with historical
   snow data in the API
2. Query the API for a station's snow records and verify `norm` values
   differ from `value` for recent years
3. Check dashboard: the Norm curve should diverge from the Current Year
   curve

---

## Documentation Impact

- [ ] Update PREPG-001 plan status to note CSV source is deprecated
- [ ] No other doc changes needed — the script's docstring and `--help`
      are updated as part of the code change

## Out of Scope

- Backfilling norms for historical years (currently only writes the
  target year). Could be added later by looping over years.
- Minimum-year guard (warning when norm is computed from <5 years).
  Useful but separate concern.
- Removing the old `calculate_snow_norms()` function entirely (keep
  for backward compatibility until CSV removal is complete).
- Adding `order_by(Snow.code, Snow.date)` to `get_snow()` in the
  preprocessing service CRUD layer. This is a pre-existing inconsistency
  (all other CRUD functions have it). Filed as a separate concern since
  `sapphire/services/` is colleague-managed.

## Dependencies

- The preprocessing API must contain historical snow data (from
  reanalysis runs). If the API has no data, norms cannot be computed.
- `sapphire-api-client` must be installed.

## Acceptance Criteria

- [ ] `calculate_snow_norms_from_api(client, variables)` reads from API
      (no `codes` param — discovers station codes from response) and
      returns `DataFrame[snow_type, code, dayofyear, norm]`
- [ ] `recalculate_snow_norms.py` uses the API-based function
- [ ] Norms computed from multi-year API data differ from any single
      year's values
- [ ] All existing tests pass
- [ ] 9 new tests cover the API-based function (including pagination)
- [ ] Code follows project conventions (see `CLAUDE.md`)

## Dependency Graph

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1, "goal": "Add calculate_snow_norms_from_api to dg_utils.py" },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1, "goal": "Update recalculate_snow_norms.py" },
    "P3a": { "depends_on": ["P1"], "parallel_agents": 1, "goal": "Write new tests for calculate_snow_norms_from_api" },
    "P3b": { "depends_on": ["P2"], "parallel_agents": 1, "goal": "Update existing test_recalculate_snow_norms.py" }
  }
}
```

P2 and P3a can run in parallel after P1. P3b depends on P2 (needs to
know the final production code to mock correctly).
