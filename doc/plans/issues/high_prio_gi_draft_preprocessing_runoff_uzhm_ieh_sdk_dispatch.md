# Add uzhm (and demo) dispatch to `get_runoff_data_for_sites`

## Status — draft 2026-04-23 (revised 2026-04-24 after critical review)

## Problem

`apps/preprocessing_runoff/src/src.py:2547` defines `get_runoff_data_for_sites()` — the iEH SDK path used when `ieasyhydroforecast_connect_to_iEH=True`. Inside this function, organization dispatch is open-coded **three times** (at `src.py:2583`, `src.py:2626`, `src.py:2658`) as:

```python
if organization == "kghm":
    read_data = read_all_runoff_data_from_excel(...)
elif organization == "tjhm":
    read_data = read_all_runoff_data_from_csv(...)
else:
    raise ValueError(
        f"Organization '{organization}' not recognized. "
        f"Please set the environment variable 'ieasyhydroforecast_organization' to 'kghm' or 'tjhm'."
    )
```

Neither `uzhm` nor `demo` is handled. For uzhm, this is a **latent bug**: the function happens to not raise today because the CSV cache is fresh (<50 days old), so the `else` branch at `src.py:2650` trusts the cache without dispatching. But:

- Whenever `should_reprocess_input_files()` returns `True` (source xlsx changed).
- Whenever the cache becomes older than 50 days.
- Whenever the cache file is missing or unreadable.

…the function falls into one of the three organization-dispatch blocks and raises `ValueError` for uzhm. Immediate operational breakage.

This was surfaced during investigation of why station `10001` historical data (2010–2020) was missing from the local preprocessing DB after PR #337 was merged: the dispatcher inside `read_all_runoff_data_from_uzhm_excel` correctly reads 5,908 rows for that station from the xlsx, but the cached `runoff_day.csv` had only 2,918 rows (2020-02-09 onward) and the cache-trust path at `src.py:2650` never exercises the xlsx re-read for uzhm.

## Alternative fixes considered

**Option A — flip `ieasyhydroforecast_connect_to_iEH=False` in the uzhm env.** This routes uzhm to `get_runoff_data_for_sites_HF()`, which already supports uzhm via `_read_runoff_data_by_organization`. It is arguably the more semantically correct setting: uzhm uses iEasyHydro **HF** (high-frequency) as its live data source, not the legacy iEasyHydro SDK. This would be an immediate unblock with no code change.

**Option B (this plan) — add uzhm/demo branches to the iEH SDK path.** This hardens the codebase against accidental misconfiguration and removes a latent ValueError time-bomb that fires whenever the cache expires or an operator sets `connect_to_iEH=True` for an unsupported org.

**Decision:** do both. Option A unblocks the server immediately (env-only change in `uzb_data_forecast_tools/config/`, out-of-repo). Option B (this plan) lands in the codebase so future deployments cannot hit this same bug by accident.

## Goal

Add `uzhm` and `demo` branches to all three organization-dispatch sites inside `get_runoff_data_for_sites()`, so the iEH SDK path returns correct data for those organizations when the cache needs refreshing. Update the `ValueError` messages to list all four supported organizations.

No other behavior changes. No refactoring of the triplicate dispatch blocks (that's a separate, broader concern — see Follow-ups).

## Scope

**Files allowed to modify:**
- `apps/preprocessing_runoff/src/src.py` — only `get_runoff_data_for_sites()` at `src.py:2547`. The three dispatch blocks inside it (`src.py:2583-2604`, `src.py:2626-2649`, `src.py:2658-2681`) and the error messages they raise.
- `apps/preprocessing_runoff/test/test_src.py` — additions only.

**Explicitly out of scope:**
- `get_runoff_data_for_sites_HF()` — already uses `_read_runoff_data_by_organization`, already supports uzhm. No changes.
- `_read_runoff_data_by_organization()` — no changes.
- `read_all_runoff_data_from_uzhm_excel()` — no changes (added in PR #337).
- The CSV cache invalidation logic at `src.py:2622` (50-day threshold) — separate concern; not fixing here.
- `should_reprocess_input_files()` — not touched.
- Refactoring the triplicate dispatch to use `_read_runoff_data_by_organization` — tempting but higher blast radius (could affect kghm/tjhm). Separate PR.

**Station codes in fixtures:** use `19001`, `19002`, `19999` — no real operational codes.

## Phases

### P1 — Failing tests (TDD)

**Goal:** Write tests that exercise `get_runoff_data_for_sites(organization="uzhm", ...)` and `get_runoff_data_for_sites(organization="demo", ...)` through each of the three dispatch sites. They must fail against current `src.py` with `ValueError`.

**Files:** `apps/preprocessing_runoff/test/test_src.py` (additions only).

**Depends on:** none.

**Agents:** 1 Sonnet 4.6, worktree isolation.

**Agent prompt scope:**

Read `apps/preprocessing_runoff/src/src.py:2547-2682` first to understand the three dispatch sites exactly. Then add a new test class `TestGetRunoffDataForSitesOrganizationDispatch` with these tests:

1. `test_uzhm_reprocess_path`: Monkey-patch or mock `should_reprocess_input_files` to return `True`. Set up fixture dir with `19001.xlsx` (wide-matrix). Call `get_runoff_data_for_sites(ieh_sdk=None, organization="uzhm", code_list=["19001"])`. Assert: non-empty DataFrame with `19001` in the `code` column. Today this raises `ValueError`; after P2 it must succeed.

2. `test_uzhm_stale_cache_path`: Create a stale `runoff_day.csv` (latest date >51 days ago, e.g. 2020-01-01) in the intermediate_data path. Mock `should_reprocess_input_files` to return `False`. Call the function. Assert: non-empty DataFrame sourced from xlsx (the cache was stale, so reprocessing kicks in). Today this raises `ValueError`; after P2 it must succeed.

3. `test_uzhm_unreadable_cache_path`: Create a `runoff_day.csv` with a **schema that lacks the `date` column** (content: `foo,bar\n1,2\n`). `pd.read_csv` will succeed, but line 2613 (`pd.to_datetime(read_data["date"])`) raises `KeyError`, which is caught by the except at `src.py:2655` and triggers the fallback reprocess block. Mock `should_reprocess_input_files` to return `False`. Call the function. Assert: non-empty DataFrame sourced from xlsx. Today this raises `ValueError`; after P2 it must succeed. (Do NOT write binary garbage — `pd.read_csv` is lenient and will often parse it into a weird DataFrame instead of raising.)

4. `test_demo_reprocess_path` (OPTIONAL — fixture-gated): Similar to test 1 but with `organization="demo"`. Demo uses `read_all_runoff_data_from_excel` which expects the multi-river xlsx format. **First grep `test_src.py` for existing multi-river/demo fixture helpers** (`_build_multiple_rivers`, `_make_multiple_rivers_fixture`, or similar). If a helper exists, write the test using it. If no helper exists, **skip this test with a clear comment** and move on — we rely on test 5 (unknown-org contract) and test 6 (kghm regression) to guard the dispatch surface. The demo code change in P2 happens regardless.

5. `test_unknown_org_still_raises`: Set `organization="xyz"` and exercise any of the three dispatch sites. Assert `ValueError` is raised with a message listing `kghm`, `tjhm`, `uzhm`, `demo` (order-agnostic substring checks). This locks the error-message contract.

6. `test_kghm_reprocess_path_unchanged` (REGRESSION — fixture-gated): Exercise kghm through the reprocess path with existing multi-river fixtures. **First grep `test_src.py` for kghm fixture helpers**. If they exist, write the regression test. If absent, skip with a comment — test 5 and test 1 (uzhm happy-path) already cover that the if/elif chain hasn't been broken structurally.

**Fixture setup notes for the agent:**
- `get_runoff_data_for_sites` reads env vars `ieasyforecast_intermediate_data_path`, `ieasyforecast_daily_discharge_file`, `ieasyforecast_daily_discharge_path`, `ieasyhydroforecast_organization`. Use `monkeypatch.setenv` per test.
- Pass `ieh_sdk=None` in tests — the function handles this (search for the `if ieh_sdk is None` block). This avoids needing a real SDK.
- Use the existing `_build_uzhm_xlsx` / `_make_uzhm_fixture` helpers for uzhm fixtures.
- `should_reprocess_input_files()` is defined in `src.py`; mock it via `monkeypatch.setattr(src, "should_reprocess_input_files", lambda: True)`.

Do NOT modify `src.py` in this phase.

Expected outcome: tests 1-4 fail with `ValueError`; tests 5-6 pass.

**Acceptance:**
- All 6 tests present.
- Tests 1-4 fail with `ValueError` — assertion-level, not collection error.
- Tests 5 and 6 pass against current `src.py`.

### P2 — Add uzhm + demo branches, update error messages

**Goal:** Extend each of the three dispatch blocks to handle `uzhm` and `demo`. Update the `ValueError` message to list all supported organizations.

**Files:** `apps/preprocessing_runoff/src/src.py` — only `get_runoff_data_for_sites()` at `src.py:2547`.

**Depends on:** P1.

**Agents:** 1 Sonnet 4.6, worktree isolation.

**Agent prompt scope:**

There are three near-identical dispatch blocks in the function. For EACH of them, inside the if/elif chain, add:

```python
elif organization == "uzhm":
    read_data = read_all_runoff_data_from_uzhm_excel(
        date_col=date_col,
        discharge_col=discharge_col,
        name_col=name_col,
        code_col=code_col,
        code_list=code_list,
    )
elif organization == "demo":
    read_data = read_all_runoff_data_from_excel(
        date_col=date_col,
        discharge_col=discharge_col,
        name_col=name_col,
        code_col=code_col,
        code_list=code_list,
    )
```

And update the `ValueError` message (at each of the three sites) from:

```python
"Please set the environment variable 'ieasyhydroforecast_organization' to 'kghm' or 'tjhm'."
```

to:

```python
"Please set the environment variable 'ieasyhydroforecast_organization' to 'kghm', 'tjhm', 'uzhm', or 'demo'."
```

Do NOT change anything outside these dispatch blocks. Do NOT touch the error-handling structure (try/except, if/elif/else control flow). Do NOT rename variables. Do NOT refactor the three triplicate blocks into a single helper — that's a separate PR.

Run `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff`. All P1 tests must pass; all pre-existing tests must still pass.

**Acceptance:**
- All 6 P1 tests pass.
- All pre-existing `preprocessing_runoff` tests pass.
- Diff in `src.py` confined to the three dispatch blocks inside `get_runoff_data_for_sites`. Nothing else touched.
- No changes to function signature or imports.
- No real station codes in any new code.

### P3 — Manual local verification (user)

**Goal:** User verifies the fix unblocks the 10001 historical-data backfill locally.

**Files:** none (manual step).

**Depends on:** P2, plus the user deleting the stale cache.

**Agents:** none (user action).

**User prerequisites (network):** the uzhm env file uses Docker DNS names (`http://preprocessing-api:8002`, etc.) that resolve only inside the Docker network. For a host-side run, override the API URLs to `localhost`:

```bash
export PREPROCESSING_API_URL=http://localhost:8002
export POSTPROCESSING_API_URL=http://localhost:8003
export API_GATEWAY_URL=http://localhost:8000
```

Alternatively, run the pipeline inside the Docker stack (`bash bin/run_preprocessing_runoff.sh <env_file>`) — Docker DNS resolves in that context.

**User steps:**

1. Back up the stale cache:
   ```bash
   mv ~/Documents/GitHub/uzb_data_forecast_tools/intermediate_data/runoff_day.csv \
      ~/Documents/GitHub/uzb_data_forecast_tools/intermediate_data/runoff_day.csv.bak_pre_10001_backfill
   ```
2. Run `preprocessing_runoff.py` locally (host-side with URL overrides, or via `bin/run_preprocessing_runoff.sh` inside Docker). Use `--maintenance` flag for a full backfill.
3. Query the local preprocessing DB for 10001 coverage — should return rows starting ~2010, not 2020.
4. Inspect the dashboard — confirm the 10001 station renders historical data.

**Acceptance:**
- Local preprocessing DB contains ≥~5,900 rows for code 10001, earliest date in 2010s, not 2020.
- Dashboard displays the Zeravshan station with a hydrograph back to 2010s.

## Final verification (orchestrator)

After P2 completes:

1. `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff` — zero failures, zero unexpected skips.
2. `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — full suite, zero failures, zero unexpected skips (service:* skips from missing `.venv` are pre-existing and unrelated).
3. Orchestrator deliberation:
   - Diff limited to `get_runoff_data_for_sites` body (three dispatch blocks) + `test_src.py` additions.
   - No signature changes, no import changes.
   - No real station codes in tests/plans.
4. User runs P3 for server-side confirmation.
5. Open PR against `maxat_sapphire_2` once P3 is satisfactory.

## Out of scope / follow-ups

- **Refactor the triplicate dispatch** to use `_read_runoff_data_by_organization` (one call per site instead of three if/elif/else blocks). Cleaner and DRY, but touches kghm/tjhm behaviour — defer to a dedicated cleanup PR with broader test coverage.
- **Cache invalidation when xlsx source changes.** The 50-day cache-trust window at `src.py:2622` does not detect when the xlsx grows (new historical rows added). The current operational procedure for adding a new xlsx source is: manually rename/delete the cache file. A smarter invalidation (e.g. file-mtime-based or coverage-gap-based) would avoid the manual step. Separate issue.
- **Server-side cache migration.** When this fix is deployed to the uzhm AWS server, the operator must similarly back up `runoff_day.csv` (and other cached CSVs in `intermediate_data/`) before the first post-deploy pipeline run. Flag this in the deployment docs.

**Verified during critical review (not follow-ups):**

- **Sister functions with same bug?** Ran `grep -nE 'organization == "(kghm|tjhm|uzhm|demo)"' apps/preprocessing_runoff/src/src.py`. Exactly 4 dispatch sites exist: 3 inside `get_runoff_data_for_sites` (the buggy ones, fixed by this plan) + 1 inside `_read_runoff_data_by_organization` (already handles all 4 orgs correctly). No other functions affected.
- **Post-dispatch flow for uzhm.** Read `src.py:2682-2767`. Everything after the dispatch blocks is schema-agnostic: virtual-stations handling (skipped when env var unset), `if ieh_sdk is None: return read_data` early return, and the `else` branch that iterates per-code via SDK methods (no uzhm-specific branching). Safe.
- **`should_reprocess_input_files()` mocking.** Defined at `src.py:175` as a bare function with no closure state. `monkeypatch.setattr(src, "should_reprocess_input_files", lambda: True)` will work because line 2576 calls it by bare name via the module namespace.

## Dependency graph

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P2"], "parallel_agents": 0 }
  }
}
```
