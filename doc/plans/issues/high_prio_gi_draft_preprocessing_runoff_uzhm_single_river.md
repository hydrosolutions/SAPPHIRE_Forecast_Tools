# Dispatch single-river xlsx alongside wide-matrix under uzhm

## Status — draft 2026-04-23 (revised after critical review)

## Problem

Under `_read_runoff_data_by_organization()` (`apps/preprocessing_runoff/src/src.py:3147`), the `uzhm` branch dispatches only to `read_all_runoff_data_from_uzhm_excel()` (`src.py:837`), which scans for files with pure-digit filename stems (e.g. `19001.xlsx`) and invokes the wide-matrix reader only. But on the uzhm deployment the `daily_runoff/` folder contains **two** xlsx formats:

1. **Wide-matrix format** — filename pattern: `<code>.xlsx` (pure digits). One sheet per year, day × month matrix. Read by `read_runoff_data_from_uzhm_wide_xlsx()` (`src.py:728`).
2. **Single-river format** — filename pattern: `<code>_<name>_UZB_SYSTEM.xlsx` (suffix is exactly 16 chars including `.xlsx`). Sheets with `dd.mm.YYYY` date + discharge columns. Format produced by `apps/preprocessing_runoff/dev_code/convert_daily_runoff.py` (see line 229 — suffix length is hardcoded). Reader `read_runoff_data_from_single_river_xlsx()` already exists at `src.py:633` and is used by kghm/tjhm/demo — but is **not** wired into the uzhm dispatch.

The `_UZB_SYSTEM.xlsx` suffix was intentionally chosen so this file would be **excluded** from the wide-matrix filename filter — see the archive note at `doc/plans/issues/archive/high_prio_gi_draft_prepq_uzhm_wide_matrix_adapter.md:226-235`. The exclusion was added; the corresponding dispatch to the single-river reader was not.

**Symptom:** single-river-format stations silently fail to ingest on uzhm. No error is logged; the data is simply absent from the preprocessing DB and therefore absent from the dashboard.

**Mirror exists:** `read_all_runoff_data_from_excel()` (`src.py:1066-1126`, used by kghm/tjhm/demo) already dispatches both patterns in parallel via `parallel_read_excel_files()` and concatenates the results. The same pattern will be applied to the uzhm path, with the refinements listed below.

## Hazards identified during critical review

Before implementation, this plan must address the following real risks:

1. **Date dtype mismatch.** `read_runoff_data_from_uzhm_wide_xlsx()` builds records with `datetime.date(y, m, d)` (`src.py:821`) — producing **object** dtype after `pd.DataFrame(records)`. `read_runoff_data_from_single_river_xlsx()` uses `pd.to_datetime(...).dt.normalize()` (`src.py:714`) — producing **datetime64[ns]** dtype. A naive `pd.concat` coerces the date column to **object** dtype, which will silently break downstream code expecting `datetime64[ns]` (`.dt` accessors, date arithmetic, API payload serialization). This is a latent bug that surfaces only on uzhm because the kghm multi-river reader already uses `pd.read_excel` and returns `datetime64[ns]` — so kghm's concat is type-consistent and ours is not.

2. **`read_runoff_data_from_single_river_xlsx()` has zero existing tests.** The reader is in production for kghm/tjhm/demo and hardcodes `filename[:5]` for station code and `filename[6:-16]` for station name. Any deviation from the 5-digit code + 16-char suffix convention silently corrupts the extracted name or drops the file. We are about to rely on it for uzhm production data.

3. **Restrictive vs permissive filename pattern — with a width constraint matching the reader.** The kghm mirror uses `f[0].isdigit()` (permissive — anything starting with a digit that isn't pure-digit). For uzhm we will use `^\d{5}_.+_SYSTEM$` because `dev_code/convert_daily_runoff.py:229` explicitly produces `_UZB_SYSTEM.xlsx` AND the single-river reader hardcodes `filename[:5]` / `filename[6:-16]` (i.e. exactly 5-digit code, exactly 16-char suffix). Using `\d+` instead of `\d{5}` would let a file like `123456_X_UZB_SYSTEM.xlsx` reach the reader, where `filename[:5]="12345"` silently drops the last digit of the station code. The restrictive width constraint must be documented in a code comment so the constraint and the reader's slicing stay in sync.

4. **Duplicate station code across formats.** If both `19001.xlsx` and `19001_Foo_UZB_SYSTEM.xlsx` exist in the directory (e.g. during a migration), both ingest with the same code and potentially overlapping date ranges. No dedup happens in the uzhm path today; downstream DB write behavior depends on the API. We will emit a `logger.warning` at dispatch time surfacing the collision, without changing ingest behavior. Duplicate detection must extract codes the same way each reader does: `Path(f).stem` for wide-matrix, `Path(f).stem[:5]` for single-river — otherwise the warning could miss real dupes or fire on false positives.

5. **Downstream station filter.** It is not yet verified that `_read_runoff_data_by_organization`'s return value isn't further filtered by organization or station whitelist before the DB write. If it is, single-river-format stations could still be invisible after this fix. This is resolved in **P0**.

6. **Existing negative-control test becomes semantically obsolete.** `test_directory_scan_pure_digits_filter` (`test_src.py:1231-1266`) creates a file with a `<code>_Name_SYSTEM.xlsx` filename in wide-matrix *content* to assert that the wide-matrix directory scan rejects non-pure-digit filenames. Under the new dispatch, that filename now matches `^\d{5}_.+_SYSTEM$` and is routed to the single-river reader, where it's filtered out because its code isn't in the test's `code_list`. The test's assertion still passes, but only by accident. This test must be updated in P2 to reflect the new behavior, or split into two tests that make the new routing explicit.

## Goal

Extend `read_all_runoff_data_from_uzhm_excel()` to scan both filename patterns, dispatch each to the correct reader, normalize date dtype, detect duplicate codes, and concatenate results — mirroring the kghm/demo path with the refinements above. Fix the wide-matrix reader's date dtype at the source so the schema is consistent across readers. Add missing unit tests for the single-river reader.

## Scope

**Files allowed to modify:**
- `apps/preprocessing_runoff/src/src.py` — two functions only:
  - `read_all_runoff_data_from_uzhm_excel()` (`src.py:837`) — add single-river dispatch, summary log, duplicate-code warning.
  - `read_runoff_data_from_uzhm_wide_xlsx()` (`src.py:728`) — narrow change to line 821 only, replacing `date(year, month_idx, day)` with `pd.Timestamp(year, month_idx, day)` so the date column dtype becomes `datetime64[ns]`. No other changes inside this function.
- `apps/preprocessing_runoff/test/test_src.py` — additions only.

**Explicitly out of scope:**
- `_read_runoff_data_by_organization()` — no changes.
- `read_runoff_data_from_single_river_xlsx()` — no signature or behavior changes. New tests for it, but no code edits.
- Any other reader (`read_runoff_data_from_multiple_rivers_xlsx`, `read_all_runoff_data_from_excel`, CSV readers).
- Google Sheets ingestion path.
- `parallel_read_excel_files()` — no changes (per-file success logging is a follow-up).
- Station library config — user is responsible for ensuring station entries exist in `config_all_stations_library.json` on the server.

**Station codes in fixtures:** use placeholder codes `19001`, `19002`, `19999` — **no real operational station codes** in tests, fixtures, or this plan document.

## Phases

### P0 — Downstream trace (research only)

**Goal:** Confirm that data returned from `_read_runoff_data_by_organization(organization="uzhm", ...)` is not filtered by any station-level whitelist before being written to the preprocessing DB. If a filter is found, its criteria must be documented so we can verify station codes from single-river files will survive it.

**Files:** read-only. No modifications.

**Depends on:** none.

**Agents:** 1 Explore agent.

**Agent prompt scope:**
- Start at `_read_runoff_data_by_organization` (`src.py:3119`) and trace every caller. Follow the return value through any post-processing, filtering, dedup, and API write steps until the data either reaches the preprocessing API or is persisted to disk.
- Report: list of transformations applied, any station-code filters encountered, any dtype assumptions on the date column.
- Explicitly answer: **does any downstream step assume `datetime64[ns]` for the date column** (e.g. via `.dt` accessors, Pandas date arithmetic, JSON serialization that hits `pd.Timestamp.isoformat`, SQL type binding)? This is load-bearing for P2's dtype fix.
- Conclude with a pass/fail: "A station code newly returned from the dispatcher (e.g. from a single-river file) WILL / WILL NOT appear in the preprocessing DB, assuming the station exists in `config_all_stations_library.json`."

**Acceptance:**
- Research report with file:line citations.
- If a filter is found, its criteria are explicit (what it allows, what it rejects).
- Date-column dtype assumption downstream is called out explicitly (yes/no, and if yes where).

### P1 — Regression tests (passing) + dispatch tests (failing)

**Goal:** Two distinct sets of tests. (a) Regression/documentation tests for the currently-untested single-river reader — these must pass immediately. (b) Dispatch tests describing the new behavior — these must fail against current `src.py` to confirm the gap.

**Files:** `apps/preprocessing_runoff/test/test_src.py` (additions only).

**Depends on:** P0.

**Agents:** 1 Sonnet 4.6, worktree isolation.

**Agent prompt scope:**

*Fixture helpers (additions):*
- `_build_single_river_xlsx(path, code, river_name, year_data)` — writes a workbook matching what `read_runoff_data_from_single_river_xlsx()` (`src.py:633`) expects. Read that reader first to confirm sheet layout (header row, column names, `dd.mm.YYYY` date format). Filename must follow `{code}_{name}_UZB_SYSTEM.xlsx` convention (exactly 5-digit code, exactly 16-char suffix).
- **Preferred alternative** if feasible: import and use `write_single_river_excel` from `apps/preprocessing_runoff/dev_code/convert_daily_runoff.py` as the fixture generator. That way tests exercise the actual producer's output format, not our interpretation of the reader's contract. If this requires inputs the tests can't easily synthesize, fall back to the manual helper.
- Reuse the in-memory openpyxl fixture pattern from `test_src.py:954-998` (see `_build_uzhm_xlsx`, `_make_uzhm_fixture`).

*New test class `TestSingleRiverReader`* (direct unit tests for `read_runoff_data_from_single_river_xlsx` — this reader has NO existing tests):
- `test_happy_path`: one file, two year sheets, returns expected rows with `datetime64[ns]` date dtype, `int` code, `float` discharge.
- `test_code_list_filter_excludes`: file `19999_Demo_UZB_SYSTEM.xlsx` but `code_list=["19001"]` → empty DataFrame returned, debug-log "not in code_list" emitted.
- `test_code_list_filter_includes`: file `19999_Demo_UZB_SYSTEM.xlsx` with `code_list=["19999"]` → data returned.
- `test_missing_values_handled`: sheet contains `"-"` and empty cells → converted to NaN.
- `test_file_not_found`: nonexistent path → `FileNotFoundError` raised.

*New `TestUzhmExcelDispatch` class (these MUST fail against current `src.py` — they describe the new behavior):*
- `test_uzhm_excel_dispatches_single_river_format`: fixture dir has `19001.xlsx` (wide-matrix) AND `19999_Demo_River_UZB_SYSTEM.xlsx` (single-river). Assert returned DataFrame has rows for both codes `19001` and `19999`.
- `test_uzhm_excel_single_river_only`: fixture dir has only `19999_Demo_River_UZB_SYSTEM.xlsx`. Assert data for `19999` is returned.
- `test_uzhm_excel_skips_unknown_filename_pattern`: fixture dir has `garbage_file.xlsx` (no code extractable). Assert it is skipped — no exception, no data for it.
- `test_uzhm_excel_ignores_excel_temp_files`: fixture dir has `~$19001.xlsx` and `19001.xlsx`. Assert temp file is skipped.
- `test_uzhm_excel_date_dtype_is_datetime64`: fixture dir has one wide-matrix file and one single-river file. Assert the returned DataFrame's `date` column dtype is `datetime64[ns]` (not `object`). **Fails twice today**: once because single-river dispatch doesn't happen (P2 fixes it), and in the mixed case also because wide-matrix returns `datetime.date` objects (P2 Part A fixes this).
- `test_uzhm_excel_column_set`: fixture with both formats. Assert the returned DataFrame's columns are exactly `{"date", "discharge", "code", "name"}` (set equality, not order). Locks the contract against silent schema drift from concat.
- `test_uzhm_excel_warns_on_duplicate_code`: fixture dir has both `19001.xlsx` and `19001_Demo_UZB_SYSTEM.xlsx`. Using `caplog`, assert a warning is emitted mentioning code `19001` appears in both formats. Data ingestion behavior itself is not asserted (both are ingested; downstream is out of scope).
- `test_uzhm_excel_routes_six_digit_stem_to_neither`: fixture dir has `123456_Demo_UZB_SYSTEM.xlsx` (6-digit code — violates the reader's `[:5]` slicing contract). Assert it is skipped (not routed to the single-river reader). This pins the regex width constraint from Hazard #3.

*Do NOT modify `src.py` in this phase.*

Run `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff`. Expected outcomes at end of P1:
- All existing `TestUzhmWideMatrixReader` tests pass (unchanged).
- All new `TestSingleRiverReader` tests pass (they document existing behavior).
- All new `TestUzhmExcelDispatch` tests fail with clear assertion messages (not import/collection errors).

**Acceptance:**
- Existing `TestUzhmWideMatrixReader` tests all pass.
- `TestSingleRiverReader` tests all pass.
- `TestUzhmExcelDispatch` tests all fail with assertion-level errors (confirming the gap and the expected P2 change).
- `_build_single_river_xlsx` helper (or the imported `write_single_river_excel` path) is present and produces files that the single-river reader can parse — verified by `TestSingleRiverReader` tests.

### P2 — Implement dispatch + dtype fix + duplicate warning

**Goal:** Extend `read_all_runoff_data_from_uzhm_excel()` to scan both patterns in parallel, normalize date dtype at the source in the wide-matrix reader, emit a duplicate-code warning, and concatenate results with a consistent schema.

**Files:**
- `apps/preprocessing_runoff/src/src.py` — `read_all_runoff_data_from_uzhm_excel()` (`src.py:837`) and a minimal edit to `read_runoff_data_from_uzhm_wide_xlsx()` line 821.

**Depends on:** P1.

**Agents:** 1 Sonnet 4.6, worktree isolation.

**Agent prompt scope:**

*Part A — narrow dtype fix in `read_runoff_data_from_uzhm_wide_xlsx`:*
- At `src.py:821`, change:
  ```python
  date_col: date(year, month_idx, day),
  ```
  to:
  ```python
  date_col: pd.Timestamp(year, month_idx, day),
  ```
- No other changes to this function. Imports: `pd` is already imported; if `date` import becomes unused, leave it (out of scope to clean up).
- This makes the wide-matrix reader return `datetime64[ns]` for the date column, matching the single-river reader and eliminating the concat-coercion hazard.

*Part B — dispatch in `read_all_runoff_data_from_uzhm_excel`:*
- Mirror the pattern at `read_all_runoff_data_from_excel()` (`src.py:1066-1126`), with these specifics:
  - Partition the directory listing using `re.fullmatch` on `Path(f).stem`:
    - **wide-matrix bucket:** `^\d+$` (e.g. `19001`).
    - **single-river bucket:** `^\d{5}_.+_SYSTEM$` (e.g. `19999_Demo_River_UZB_SYSTEM`) — **5-digit code constraint is load-bearing**, it pins the regex to the single-river reader's hardcoded `filename[:5]` slicing.
    - Temp files (`~$...`) and anything else: skip with `logger.debug`.
  - **Inline comment** above the single-river regex: "Exactly-5-digit constraint matches `read_runoff_data_from_single_river_xlsx`'s hardcoded `filename[:5]` and `filename[6:-16]` slicing. Do NOT relax to `\\d+_...` or to kghm's `f[0].isdigit()` without first generalising the reader's name/code extraction — otherwise 6-digit codes get silently truncated."
  - **Duplicate-code detection (runs BEFORE any file reading, purely from filename metadata):** extract codes using each reader's convention — wide-matrix: `Path(f).stem`; single-river: `Path(f).stem[:5]`. For each code appearing in both buckets, emit `logger.warning(f"uzhm xlsx ingest: station {code} appears in BOTH wide-matrix and single-river files — both will be ingested, downstream dedup is not performed here")`. Ingest proceeds regardless; no filtering.
  - Call `parallel_read_excel_files()` for each non-empty bucket with the appropriate reader. Both readers get the same `code_list` passed through.
  - Empty-safe concat: handle all four cases (both empty → return `None` with warning; one empty → return the other; both non-empty → `pd.concat([df_wide, df_sr], ignore_index=True)`).
  - Summary log at the end: `logger.info(f"uzhm xlsx ingest: {n_wide} wide-matrix + {n_sr} single-river files, {0 if result is None else len(result)} rows")`.
- **Do NOT** change the signature of `read_all_runoff_data_from_uzhm_excel` or of either reader.
- **Do NOT** modify `_read_runoff_data_by_organization` or any other function.

*Part C — update the now-semantically-obsolete negative-control test:*
- `test_directory_scan_pure_digits_filter` (`test_src.py:1231-1266`) was written to verify the wide-matrix filename filter rejects non-pure-digit stems. Under the new dispatch its fixture file `10001_Name_SYSTEM.xlsx` now matches the single-river bucket. The test assertion still passes (via `code_list` filter), but its intent is no longer what the code does. Replace it with two tests:
  1. `test_directory_scan_pure_digits_goes_to_wide_matrix`: fixture with `19001.xlsx`; assert only wide-matrix reader sees it. (Can be asserted indirectly: data parses in wide-matrix shape.)
  2. `test_directory_scan_system_suffix_goes_to_single_river`: fixture with `19002_Name_UZB_SYSTEM.xlsx` in single-river *content*; assert it is routed to the single-river reader and parses successfully when its code is in `code_list`.
- Delete the old `test_directory_scan_pure_digits_filter` after the replacements are added.

Re-run `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff`. All tests (existing + P1 + Part C replacements) must pass.

**Acceptance:**
- All P1 dispatch tests now pass (dtype, column set, duplicate warning, 6-digit-stem skip).
- All pre-existing `TestUzhmWideMatrixReader` tests still pass. **Agent must actually run them and confirm** — do not assume the dtype change is transparent. Pay specific attention to:
  - `test_happy_path` line 1072: `result[result["date"] == date_type(2000, 1, 1)]` (pandas coerces `date` vs `datetime64[ns]` equality, but verify the row is actually returned).
  - `test_none_discharge_excluded` lines 1159-1164: same equality pattern.
  - `test_invalid_dates_excluded` lines 1098-1131: `.apply(lambda d: d.year)` — works for both `date` and `pd.Timestamp`, so should be unaffected.
- Part C test replacements present; old `test_directory_scan_pure_digits_filter` removed.
- Diff in `src.py` is confined to `read_all_runoff_data_from_uzhm_excel` and the single-line dtype fix at line 821. Nothing else in the file touched.
- Signatures of `read_all_runoff_data_from_uzhm_excel`, `read_runoff_data_from_uzhm_wide_xlsx`, `read_runoff_data_from_single_river_xlsx` unchanged.
- No changes to imports that aren't strictly required.

### P3 — Integration test via organization router

**Goal:** Verify end-to-end that `_read_runoff_data_by_organization(organization="uzhm", ...)` returns data from both formats with correct dtype.

**Files:** `apps/preprocessing_runoff/test/test_src.py` — additions only.

**Depends on:** P2.

**Agents:** 1 Sonnet 4.6, worktree isolation.

**Agent prompt scope:**
- Add a sibling to `test_organization_router_uzhm` (`test_src.py:1267-1292`) called `test_organization_router_uzhm_mixed_formats`:
  - Fixture directory contains one wide-matrix xlsx (`19001.xlsx`) and one single-river xlsx (`19999_Demo_River_UZB_SYSTEM.xlsx`).
  - Call `_read_runoff_data_by_organization(organization="uzhm", ...)`.
  - Assert returned DataFrame contains rows for both station codes.
  - Assert date column dtype is `datetime64[ns]`.
- Add `test_uzhm_excel_logs_summary` using `caplog`: both formats present → assert the summary log line from P2 is emitted with both counts and total row count.
- **Do NOT** modify `_read_runoff_data_by_organization` or `src.py`.

**Acceptance:**
- New tests pass.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff` → zero failures, zero unexpected skips.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh` (full suite) → zero failures, zero unexpected skips.

## Final verification (orchestrator)

After P3 completes:

1. `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff` — zero failures, zero unexpected skips.
2. `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — full suite, zero failures, zero unexpected skips.
3. Orchestrator-level deliberation on the combined diff:
   - `apps/preprocessing_runoff/src/src.py`: only the two scoped functions touched.
   - `apps/preprocessing_runoff/test/test_src.py`: additions only.
   - Line 821 dtype change: `date(...)` → `pd.Timestamp(...)`, nothing else in the wide-matrix reader.
   - Summary log, duplicate-code warning, and restrictive-pattern comment all present as specified.
   - No real station codes in any fixture.
4. Hand off to user for server verification:
   - Restore the previously-renamed file on the server to its original `<code>_<Name>_UZB_SYSTEM.xlsx` form (the file that was temporarily renamed to a pure-digit stem during the investigation).
   - Confirm an entry for that station code exists in `config_all_stations_library.json` on the server.
   - Deploy this branch; rerun the preprocessing_runoff init workflow.
   - Confirm the new summary log line appears and lists the single-river file count.
   - Confirm that station's data appears in the dashboard alongside the wide-matrix stations.

## Out of scope / follow-ups

- Per-file success logging in `parallel_read_excel_files()` (would help operators trace silent-skip cases — separate issue).
- Format sniffing (detecting single-river vs wide-matrix from sheet structure rather than filename) — not needed; filename convention is reliable and documented.
- Relaxing the single-river reader's hardcoded `filename[:5]` / `filename[6:-16]` slicing — defer until a second uzhm naming convention actually appears; flagged here as a known fragility.
- Dedup of overlapping (code, date) rows when a station has both formats — out of scope; flagged via warning log only.
- Station library entry validation — user to confirm the station code has an entry in `config_all_stations_library.json` on server before server test.
- `read_runoff_data_from_single_river_xlsx` iterates `xls.sheet_names` without filtering (`src.py:703`) — unlike the wide-matrix reader, it does not skip non-data sheets like "Summary". Safe in practice because `convert_daily_runoff.py` produces year-only sheets, but a hand-edited file with a summary tab would be silently included. Flag as known fragility.

## Dependency graph

```json
{
  "phases": {
    "P0": { "depends_on": [], "parallel_agents": 1 },
    "P1": { "depends_on": ["P0"], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P2"], "parallel_agents": 1 }
  }
}
```
