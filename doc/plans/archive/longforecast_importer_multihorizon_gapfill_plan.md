# Long-Term From-File Importer Multihorizon + Gap-Fill Plan

Status: Draft for owner review  -  revised per adversarial review 2026-06-21 (APPROVE-WITH-REQUIRED-REVISIONS; must-fix S1-S7 folded in)  
Target branch: implementation lands on a **new feature branch off `maxat_sapphire_2`** (e.g. `fix_migration_long_forecast_multihorizon`) and PRs into `maxat_sapphire_2`  -  do NOT commit to `maxat_sapphire_2` directly (CLAUDE.md git conventions).  
Execution mode: plan only; implementation delegated to Sonnet agents after owner approval  
Sentinel policy: committed examples/tests use non-real sentinel codes only. **Multi-sentinel fixtures are pre-approved** (review process item): the code-scoped gap-fill test needs >=2 distinct codes, so use `19999` (canary / populated) + `29999`/`39999` (zero-row / missing)  -  no further owner gate. Keep that test a **dry-run / unit test on `_read_filtered_records` with an in-memory cutoff map (no POST)** so sentinel codes never reach the API. (The long-forecast `code` field is an unconstrained `str`  -  `schemas.py:50`  -  so sentinels don't trip validation; only `horizon_type`/`model_type` are enum-gated.)

## Problem

The long-term from-file importer is false-done if it only accepts more horizon types. It must also compute target-state cutoffs for the active horizon type and gap-fill stations in a horizon already partially populated by one canary station.

Verified target-branch state:

- `bin/utils/migration_py/long_forecast.py:258` reads `horizon_type` from config and lowercases it.
- `bin/utils/migration_py/long_forecast.py:262-268` currently sets `_ALLOWED_HORIZON_TYPES = {"month"}` and rejects `quarter` / `season`.
- `bin/initialize_long_forecast_history.sh:320` hard-codes `horizon_type::text='MONTH'` in the global target-state query.
- `bin/initialize_long_forecast_history.sh:469` hard-codes `horizon_type::text='MONTH'` in the per-mode target-state query.
- `bin/initialize_long_forecast_history.sh:524-547` has the `--allow-global-cutoff` fail-closed gate for the current single global cutoff path.
- `bin/utils/migration_py/long_forecast.py:462` applies one cutoff globally to every row; it is not scoped by station code.
- `bin/utils/migration_py/long_forecast.py:88-92` maps GBT-family ensemble columns only from model-prefixed names; `:378-383` reads `Q_<model>_xgb` / `Q_<model>_lgbm` / `Q_<model>_catboost`; `:386` reads literal `Q_loc`.
- Service contract is already sufficient: `sapphire/services/postprocessing/app/models.py:15-17` has `MONTH`, `QUARTER`, `SEASON`; `:123-126` has GBT-family payload columns. `schemas.py:49` accepts `HorizonType`; the GBT-family fields are `:62-65` (`q_xgb`/`q_lgbm`/`q_catboost`/`q_loc`; `:61` is `q_obs`). `schemas.py:50` makes `code` an unconstrained `str` (no regex/length validator).
- `doc/prod/backfill_ml_fromfile.md:523-527` documents the ML sibling pre-cutoff trap; `:566` prescribes a `--full-import` override plus horizon-scoped mode detection for that sibling.
- MIG-003 is **MERGED** on `maxat_sapphire_2` (commit `450ad7f` via PR #362)  -  it landed the uppercase `MONTH` enum-label pattern. **Its test surface is THREE functions** (P2 invalidates all three by replacing the `MONTH` literal), not one: `test_initialize_long_forecast.py:792` (`...uses_uppercase_month_pg_enum_label`), `:1048` (`...per_mode_query_uses_horizon_value_filter`), and `:1133` (`...psql_sql_uses_uppercase_horizon_type_pg_enum_labels`, a string-grep guard at `:1178-1183` that asserts every psql block contains the literal `'MONTH'`). This plan must preserve the uppercase enum-label lesson while broadening it to `MONTH|QUARTER|SEASON`, and the `:1133` grep-guard must be rewritten as a behavioral test or relaxed to match `horizon_type::text='${...}'`.

Symptom framing to use in issue/docs: season is not simply "0 rows." The failure class is horizon rejection plus code-blind cutoff gap-fill. A canary-populated horizon can leave the canary present and the remaining intended stations missing.

## Gap-Fill Decision

Recommended design: per-code cutoff map keyed by `(horizon_type, horizon_value, code)`.

Rationale:

- It fixes the real poison-pill case: one populated station no longer causes `FILTERED_ROW_COUNT=0` for every other station in that horizon.
- It keeps existing pre-cutoff semantics for already-populated codes: import rows older than that code's current minimum date.
- It treats a code with zero existing rows as empty and imports its full source span.
- It is safer than a broad `--full-import` override. Full import is simpler, but it is a foot-gun for partially populated targets because it intentionally replays every row for every code and relies on upsert/idempotency to avoid damage. That is acceptable as an emergency operator tool, but it should not be the default repair for this importer.
- It lets multi-mode runs become less dangerous than the current single global cutoff. If cutoff-map generation fails while target rows exist, fail closed instead of falling back to one global cutoff.

**Known residual (S2  -  must also be stated in the issue and Sign-Off).** Per-code cutoff = that code's `MIN(date)` and the filter keeps only rows strictly older than it. So it fully restores **zero-row** codes (the 17 season stations) and **recent-contiguous-block** codes (the canary, populated by a recent operational run -> recent MIN -> full history backfills). It does **NOT** fill: (a) an **interior gap** between two populated spans, or (b) a code whose only existing rows are **old** (single old row -> cutoff=old date -> nothing importable). This fix resolves the reported "season 1/18" symptom but is **not** a general gap-filler  -  say so, do not imply otherwise.

**Cutoff-map key contract (S1  -  CRITICAL; the map silently no-ops without this).** The wrapper builds the map from `psql` which emits **uppercase** `horizon_type::text` (e.g. `MONTH`), **text** `horizon_value`, and **raw** `code`; the Python lookup uses `mode_config["horizon_type"]` lowercased to `month` (`long_forecast.py:261`), **int** `horizon_value` (`:252`), and a `_parse_code`-normalized code. **Keys would never match -> every code hits the "absent -> no cutoff -> full re-import" branch -> the cutoff is a silent no-op** (the exact MIG-003 case-trap re-created at the wrapper->Python boundary). The map artifact MUST be serialized with **normalized keys**: lowercase `horizon_type`, int `horizon_value`, `_parse_code`-normalized `code`. A regression test must assert that a map generated with uppercase/text/raw keys still matches a lowercase/int/normalized month row.

**`--allow-global-cutoff` gate contract under the map (S5).** Map-success path needs **no opt-in** (each code gets its own cutoff; a multi-mode run is now safe). Only **map-generation failure with target rows present** fails closed. `--allow-global-cutoff` applies **only** to the retained legacy single-scalar `--cutoff` fallback path, not the primary map path.

Implementation shape for Sonnet agents:

- Add a Python-side optional `--cutoff-map` argument to `long_forecast.py`. The map should be generated by the wrapper in a temp workspace and passed read-only into the container.
- The map should contain only horizon/type/value/code cutoff data. For row filtering at `long_forecast.py:462`, look up the row's `(horizon_type, horizon_value, code)`. If no cutoff exists for the code, do not cutoff-filter that row.
- Keep the existing single `--cutoff` path only as a compatibility fallback if needed, but the wrapper should prefer the cutoff map for real imports.
- Do not add service changes under `sapphire/services/**`.

## Phases

### P0 - Confirm MIG-003 Baseline And Branch State

**Goal:** Establish the implementation baseline on `maxat_sapphire_2` and prevent regression of the landed uppercase enum-label fix.

**Files:**

- `bin/initialize_long_forecast_history.sh`
- `bin/utils/migration_py/long_forecast.py`
- `apps/iEasyHydroForecast/tests/test_initialize_long_forecast.py`

**Depends-on:** owner approval of this plan.

**Agents:** Sonnet planning/verification agent. No code agent yet.

**Acceptance:**

- Create a **new feature branch off `maxat_sapphire_2`** (e.g. `fix_migration_long_forecast_multihorizon`); implementation commits land there and PR into `maxat_sapphire_2` (S7  -  do not commit to `maxat_sapphire_2` directly). The current worktree is on `develop_forecast_skill_eval` (16 ahead of `maxat_sapphire_2`, none touching the three target files)  -  branch from `maxat_sapphire_2`, not the current HEAD.
- Reconfirm the line numbers above before editing (MIG-003 already shifted some; treat all `:NNN` as approximate).
- Confirm MIG-003 uppercase enum-label expectations are present in ALL THREE test functions: `test_initialize_long_forecast.py:792`, `:1048`, `:1133` (S3)  -  not just `:792-815`.
- Confirm no implementation edits are made in P0.

### P1 - Accept Quarter And Season In The Importer

**Goal:** Make `long_forecast.py` accept `month`, `quarter`, and `season`, while still failing closed for unsupported horizon types.

**Files:**

- `bin/utils/migration_py/long_forecast.py`
- `apps/iEasyHydroForecast/tests/test_initialize_long_forecast.py`

**Depends-on:** P0.

**Agents:** Sonnet implementation agent.

**Acceptance:**

- `_ALLOWED_HORIZON_TYPES` at `long_forecast.py:262` becomes the explicit allow-list for `month`, `quarter`, and `season`.
- `horizon_type` read at `long_forecast.py:258` continues to be normalized and carried into the payload via `_build_record`.
- Unknown values such as `pentad` or `week` still raise before any write.
- Tests cover default-to-month, uppercase input normalization, quarter acceptance, season acceptance, and fail-closed unknowns.
- Existing month behavior remains unchanged.

### P2 - Derive Per-Mode Horizon Enum In Cutoff Queries

**Goal:** Add per-mode horizon-enum derivation to the wrapper (parser + allow-list) and apply it to the **legacy single-scalar `--cutoff` fallback** path only. **The primary `:320`/`:469` SQL sites are rewritten once, in P3** (the grouped cutoff-map query has no hard-coded `horizon_type` at all)  -  do NOT also rewrite them here, to avoid double-churn of the same lines/tests (S4).

**Files:**

- `bin/initialize_long_forecast_history.sh`
- `apps/iEasyHydroForecast/tests/test_initialize_long_forecast.py`

**Depends-on:** P1.

**Agents:** Sonnet implementation agent.

**Acceptance:**

- The wrapper parser that currently reads only `operational_month_lead_time` around `initialize_long_forecast_history.sh:450-456` also reads `horizon_type`, normalizes it (lowercase), validates against an **internal allow-list** `month|quarter|season`, and derives the PG enum label `MONTH|QUARTER|SEASON` **only from that allow-list** (never interpolate a config string into SQL  -  SQL-injection guard). `horizon_value` stays `int()`-cast.
- Apply the derived enum label to the **legacy `--cutoff` scalar fallback** path (and any helper it uses); it must never assume `MONTH`. The primary grouped query at `:320`/`:469` is P3's job (S4).
- `--allow-global-cutoff` (`:524-547`) remains fail-closed but its contract is redefined in P3 (it applies only to the legacy scalar fallback; the map-success path needs no opt-in).
- **MIG-003 test coordination (S3):** P2's enum-derivation/fallback change must keep ALL THREE MIG-003 tests green by updating their assertion shape  -  `:792`, `:1048`, and especially the grep-guard `:1133`/`:1178-1183` (rewrite it to accept `horizon_type::text='${...}'` with a valid derived label, or convert to a behavioral test). Allowing `MONTH|QUARTER|SEASON`, not month-only. (Coordinate with P3, which re-touches the same SQL sites.)

### P3 - Add Code-Scoped Gap-Fill Cutoff Map

**Goal:** Fix the distinct gap-fill bug: a canary-populated station must not cutoff-filter all missing stations in the same horizon.

**Files:**

- `bin/initialize_long_forecast_history.sh`
- `bin/utils/migration_py/long_forecast.py`
- `apps/iEasyHydroForecast/tests/test_initialize_long_forecast.py`

**Depends-on:** P2.

**Agents:** Sonnet implementation agent with reviewer handoff before tests are broadened.

**Acceptance:**

- **P3 owns the primary rewrite of `:320` and `:469`** (S4): replace those month-only scalar queries with a single grouped target-state query `GROUP BY horizon_type, horizon_value, code` for the selected modes (this query has no hard-coded `horizon_type`  -  it inherently solves the enum-label problem for the primary path). `:320`/`:469` are edited once, here.
- The wrapper writes a temp cutoff-map artifact and passes it read-only into the container.
- **Key-normalization (S1  -  CRITICAL):** serialize the map with keys that match the Python lookup  -  **lowercase** `horizon_type`, **int** `horizon_value`, `_parse_code`-normalized `code`. (psql emits uppercase/text/raw; un-normalized keys make the map a silent no-op  -  the MIG-003 case-trap at the wrapper->Python boundary.)
- `long_forecast.py:462` changes from a single scalar cutoff to per-row `cutoff_map.get((horizon_type, horizon_value, code))`; a **missing entry means "no cutoff for that row" (import it), NOT "skip."** The lookup data is already in scope (`code_raw` at `:442`, `horizon_type`/`horizon_value` from `mode_config`); the change is additive (a map param on `_read_filtered_records`).
- A code **absent** from the map -> treated as empty -> imports all matching source rows. A code **present** -> keeps its own `MIN(date)` pre-cutoff behavior. (Residual per the Gap-Fill Decision: this fills zero-row + recent-block codes, not interior gaps / old-only codes.)
- **`--allow-global-cutoff` contract (S5):** map-success path needs no opt-in; only map-generation-failure-with-rows-present fails closed; the flag applies only to the retained legacy scalar fallback.
- **SQL-injection:** codes come **out of the DB via `GROUP BY`** and live in the map artifact  -  never interpolate codes into SQL (no `WHERE code IN (...)` built from the map). Enum labels come only from the P2 allow-list.
- Dry-run inventory reports aggregate counts only; do not print real station codes in committed docs/tests.
- No `--full-import` default path is introduced. If a `--full-import` override is added for operator parity with the ML sibling, it must be explicit, documented as a broad replay, and not used by the default wrapper path.

### P4 - Tests And Validation

**Goal:** Prove multihorizon import and code-scoped gap-fill behavior before owner sign-off.

**Files:**

- `apps/iEasyHydroForecast/tests/test_initialize_long_forecast.py`
- Any existing sentinel fixture files under `apps/iEasyHydroForecast/tests/fixtures/long_forecast/`

**Depends-on:** P1, P2, P3.

**Agents:** Sonnet test agent, then Sonnet reviewer agent.

**Acceptance:**

- **Canary gap-fill test (the anti-half-fix gate):** a dry-run/unit test on `_read_filtered_records` with an **in-memory cutoff map** where a `season` horizon has a cutoff for the canary code `19999` and **no** entry for `29999`/`39999`; assert the missing codes' rows survive filtering (no empty-target false positive). This test cannot even be written without P3's map param, so a P1/P2-only build fails it. **Use multi-sentinel fixtures `19999` + `29999`/`39999` (pre-approved); no POST, so they never reach API validation.**
- **S1 case-mismatch regression test:** build the map with uppercase/text/raw keys (as psql emits) and assert it still matches a lowercase/int/normalized `month` row  -  guards against the silent-no-op trap.
- Add a test that `quarter` mode writes or dry-runs rows with `horizon_type=quarter`, and one for `season`.
- **Update ALL THREE MIG-003 tests (S3):** `:792`, `:1048`, `:1133`  -  assert per-mode cutoff SQL uses the derived enum label for `month`, `quarter`, AND `season` (the `:1133` grep-guard rewritten to accept `horizon_type::text='${...}'` or made behavioral). The suite must be green, not satisfied by a re-hard-coded `'MONTH'`.
- Validation command for the implementing agent:

  ```bash
  cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh
  ```

- Expected focused tests include `apps/iEasyHydroForecast/tests/test_initialize_long_forecast.py`.

### P5 - Issue Draft Renumber And Rewrite

**Goal:** Update the issue draft so the tracker accurately describes the three-part fix and no longer collides with the existing `MIG-006` placeholder.

**Files:**

- `doc/plans/issues/high_prio_gi_draft_migration_long_forecast_importer_multihorizon.md` on the issue branch
- New renamed issue filename, recommended: `doc/plans/issues/high_prio_gi_draft_migration_long_forecast_importer_multihorizon_gapfill.md`
- `doc/plans/module_issues.md`

**Depends-on:** P0; may run in parallel with implementation after owner approves the issue-ID decision.

**Agents:** Sonnet docs agent.

**Acceptance:**

- Renumber this issue to `MIG-007`. Do not reclaim `MIG-006`  -  it is **tentatively reserved** (not a committed row) for scoped reaggregation in `doc/plans/issues/high_prio_gi_draft_migration_ml_hindcast_wrapper.md:274,325`; no `MIG-006` row exists in `module_issues.md`, so `MIG-007` is the next free id and is safe.
- **Per-file branch targets (S6  -  this phase spans two branches):** the issue **draft** currently lives only on the issue branch `fix_long_forecast_importer_multihorizon` (commit `fbedcd4`) and is absent from `maxat_sapphire_2`; edit/rename it **there** (or carry it onto the implementation feature branch). The **`module_issues.md`** row edit lands on the **implementation feature branch / PR** (off `maxat_sapphire_2`). State explicitly which file goes to which branch so the two don't desync.
- Update the issue draft title and in-file ID from `MIG-006` to `MIG-007`; rename the file to include the gap-fill scope (suggested `..._long_forecast_importer_multihorizon_gapfill.md`).
- Add a `MIG-007` row to `doc/plans/module_issues.md` (on the implementation branch).
- Expand the issue body from horizon-acceptance-only to the three required changes:
  1. accept `quarter` / `season`;
  2. derive per-mode horizon enum for both cutoff query sites;
  3. code-scoped gap-fill at the SQL cutoff and Python filter.
- Add `initialize_long_forecast_history.sh:469` explicitly to the root cause.
- State the dependency on landed MIG-003 uppercase enum-label form.
- Fix symptom framing to "canary present, remaining intended stations missing" rather than only "season 0 rows."
- Keep LTF-006 (`GBT_Base` 422), -13 ML-sibling learnings, and Tajik missing monthly lead-3 config/data issues out of scope.

### Sign-Off

**Goal:** Provide owner with implementation evidence and residual risk before production use.

**Files:** no additional files unless owner requests runbook/prod-doc updates.

**Depends-on:** P4 and P5.

**Agents:** Sonnet reviewer agent, then owner.

**Acceptance:**

- Tests green with the command above.
- The plan's three implementation changes are all present; horizon acceptance alone is not accepted.
- No `sapphire/services/**` edits.
- No real station codes or discharge values in committed artifacts.
- Owner receives a short summary of restored behavior, remaining out-of-scope blockers, and rollback notes.

## Risks And Rollback

- **Risk (S1, highest): cutoff-map key case/type mismatch silently no-ops the gap-fill** and re-POSTs all history every run (saved only by idempotency, but a massive over-POST + false "fixed" signal). Mitigation: normalize map keys (lowercase htype / int hv / normalized code) AND ship the S1 case-mismatch regression test; a "passing" zero-row test alone does NOT prove the map matches.
- Risk: cutoff-map logic changes default import breadth. Mitigation: dry-run inventory should show aggregate source, filtered, skipped-cutoff, and per-mode totals before write.
- Risk: dynamic SQL enum labels introduce injection risk if derived from config directly. Mitigation: derive labels only from an internal allow-list mapping after normalization; never interpolate codes into SQL (read them via `GROUP BY`).
- Risk: multi-code gap-fill tests vs sentinel policy. Mitigation (resolved): multi-sentinel labels `19999`+`29999`/`39999` are pre-approved; keep the gap-fill test a dry-run/unit test (no POST) so sentinels never reach API validation.
- Risk: `GBT_Base` seasonal or quarterly models may still 422. Mitigation: explicitly out of scope under LTF-006; do not broaden this importer fix into service `ModelType` work.
- Rollback: this is toolkit-only. Revert the wrapper/Python/test commits. Runtime DB rows produced by a bad import should be rolled back from the operator's pre-run DB dump; service schema is unchanged.

## JSON Dependency Graph

```json
{
  "nodes": {
    "P0": {
      "goal": "Confirm maxat_sapphire_2 baseline, MIG-003 state, and exact line numbers",
      "depends_on": [],
      "agent": "Sonnet verification"
    },
    "P1": {
      "goal": "Accept month, quarter, and season in long_forecast.py while failing closed (Python; disjoint from P2)",
      "depends_on": ["P0"],
      "agent": "Sonnet implementation"
    },
    "P2": {
      "goal": "Wrapper horizon-enum derivation (allow-list) + legacy --cutoff fallback; primary SQL sites deferred to P3 (wrapper; disjoint from P1 -> can run in parallel with P1)",
      "depends_on": ["P0"],
      "agent": "Sonnet implementation"
    },
    "P3": {
      "goal": "Implement code-scoped cutoff-map gap-fill (owns the :320/:469 grouped-query rewrite + normalized map keys + Python per-row filter)",
      "depends_on": ["P1", "P2"],
      "agent": "Sonnet implementation"
    },
    "P4": {
      "goal": "Add multihorizon and canary-populated gap-fill tests; run app test suite",
      "depends_on": ["P1", "P2", "P3"],
      "agent": "Sonnet test + reviewer"
    },
    "P5": {
      "goal": "Renumber/rewrite issue draft and update module issue index",
      "depends_on": ["P0"],
      "agent": "Sonnet docs"
    },
    "SIGNOFF": {
      "goal": "Owner reviews implementation evidence and approves production use",
      "depends_on": ["P4", "P5"],
      "agent": "Owner"
    }
  },
  "recommended_order": ["P0", "P1", "P2", "P3", "P4", "P5", "SIGNOFF"]
}
```
