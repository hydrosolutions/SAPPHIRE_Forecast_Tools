# LTF-021: Optional monthly mode configurations

**Priority:** Medium

**Status:** Review — implemented and verified; owner authorized commit and PR.

## Evidence and scope

Owner confirms KG legitimately omits month_4 through month_9 JSONs while retaining the supported-mode list. Keep that list and private configuration unchanged. No exact duplicate found in the issue index or related LTF/PP plans. FD-031 concerns explicit quarter lookup and stays separate.

`apps/iEasyHydroForecast/long_term_horizon_resolver.py::supported_long_term_modes` returns environment declarations, not existing files. `_load_long_term_config` uses `_long_term_config_path`, fails on absence, and validates JSON. `operational_schedule_for_mode` validates support and required lead/issue-day fields. `ForecastConfig.load_forecast_config` in `apps/long_term_forecasting/config_forecast.py` is separate, loads model dependencies and synchronizes settings: do not use it for read-only discovery.

Before this fix, `apps/postprocessing_forecasts/src/data_reader.py::_operational_schedules_for_horizon_type` loaded every matching declared mode. Under `SAPPHIRE_SKILL_LEAD_AWARE`, missing month_4 consequently blocked these paths:

| Path | Entrypoint → failing shared read |
|---|---|
| Operational | `postprocessing_operational_long_term.py::postprocessing_operational_long_term` → `read_latest_monthly_forecasts` → monthly schedule enumeration |
| Skills | `recalculate_skill_metrics.py::recalculate_skill_metrics` → `_long_term_horizon_supported("month")` and `read_monthly_forecasts` → monthly schedule enumeration |
| Maintenance | `postprocessing_maintenance_long_term.py::postprocessing_maintenance_long_term` → `_run_monthly_gap_fill` → `read_monthly_forecasts` when gaps need filling → monthly schedule enumeration |

`postprocessing_operational.py` delegates monthly work to the LT script. Maintenance reads combined forecasts first and can stop before this fault when no gaps exist; regression fixtures must contain gaps. `apps/long_term_forecasting/lt_schedule_query.py::query_schedule` has separate config-load handling; scheduler/producer maintenance, recovery and retraining stay outside this fix.

## Contract

- Only monthly batch enumeration treats an absent top-level declared `month_<N>.json` as unconfigured, with visible mode/path diagnostics and declaration order preserved. Require valid resolved configuration directories and at least one present declared monthly JSON when monthly candidates exist.
- Declared monthly candidates with every JSON absent must fail with the directory and candidates identified. Missing/non-directory root or LT directory must fail. Missing environment values retain actionable errors. No declared monthly candidates, such as quarter-only deployment, remains a valid empty monthly mapping: return before requiring configuration-directory environment values or inspecting the filesystem.
- Existing JSONs retain existing resolution and field validation: invalid JSON/non-object JSON, missing fields, values already rejected by `_require_int_field`, permission errors, directory entries and dangling symlinks must fail. Do not tighten integer coercion in this fix; the existing `int(value)` behavior accepts some boolean/fractional inputs. Do not blanket-catch `FileNotFoundError`, conceal resolved paths/causes, or confuse a missing dependency with absent top-level optional mode JSON.
- Preserve global `supported_long_term_modes` semantics, explicit `operational_schedule_for_mode`/lead lookups and `ForecastConfig.load_forecast_config`: unsupported requests remain unsupported and explicit supported-but-absent requests still fail. Quarter/season and flag-OFF contracts remain unchanged.

Absence cannot prove intent: this bounded convention accepts the owner's optional-month policy but cannot distinguish intentional omission from accidental deletion of a single monthly JSON. All-missing and broken-existing guards limit ambiguity without adding a new manifest/schema.

## P1 — Narrow discovery and regressions

**Goal:** Fix shared monthly discovery for all three paths.

**Depends on:** None.

**Agents:** One implementation agent under repository orchestration policy.

**Files allowed for implementation:**

- `apps/iEasyHydroForecast/long_term_horizon_resolver.py`: add a narrowly named public monthly batch-discovery helper using existing `_long_term_config_path`/`_required_env` internally. Validate directories, distinguish true absence from broken entries, enforce all-missing guard, and return available declared monthly names. Keep existing signatures intact.
- `apps/postprocessing_forecasts/src/data_reader.py`: use helper only in the monthly branch of `_operational_schedules_for_horizon_type`, then call existing `operational_schedule_for_mode` for remaining names. Preserve API windows, lead/date selection, output/writes, quarter and seasonal paths. These are the only two production files: no second public helper, package exports, build changes or entrypoint production edits are needed.
- `apps/iEasyHydroForecast/tests/test_long_term_horizon_resolver.py`, `apps/postprocessing_forecasts/tests/test_lead_aware_empty_schedules.py`: cover partial absence, all absence, no monthly declarations, missing env/directories, malformed existing config, broken links and strict explicit loads using temporary synthetic configs.
- `apps/postprocessing_forecasts/tests/test_operational_long_term.py`, `test_recalc_supported_modes_gate.py`, `test_maintenance_long_term.py`: add the three entrypoint regressions below with fake API boundaries and synthetic data. These files currently mock `src`/`data_reader`; bind the real monthly reader and enumeration into the harness, mocking only underlying API reads and unrelated boundaries. Each partial-absence regression must fail against the old enumeration and assert that configured leads reach the writer.

**Implementation constraint:** Do NOT change any existing function signatures, data flow logic, or control flow. Your changes must be purely additive or modify only the specific behavior described.

**Acceptance criteria:**

- Declared month_0–9 with only valid month_0–3 files yields exactly four schedules and visible skip diagnostics. Do not mock discovery in tests proving discovery.
- Operational regression reaches latest monthly selection, ensemble output and successful write handling with configured leads; missing month_4 no longer aborts it.
- Skills regression crosses the real monthly support gate and reader into scoring/write boundaries with configured leads; broken existing configuration still raises rather than becoming an unsupported-horizon warning.
- Maintenance regression supplies actual gaps and reaches raw forecast reads and gap-fill write boundaries. Preserve lookback, gap keys, healthy rows, save/error status and existing tier early-return behavior.
- Broken existing configuration remains failure through each shared path; explicit absent-mode lookup still fails. Quarter-only empty-month and flag-OFF behavior remain unchanged. No private data/config, API/schema/services, scheduler policy, retraining, recovery, dependencies or unrelated maintenance edits.

## P2 — Verification and review

**Goal:** Verify the scoped change and independently review it.

**Depends on:** P1.

**Agents:** One fresh read-only reviewer independent of implementer, out-of-loop tool/model where available per `doc/dev/agent_review_workflow.md`.

**Files:** No implementation expansion; factual corrections to this issue/index/changelog only.

**Acceptance criteria:**

- Run `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh iEasyHydroForecast` and the `postprocessing_forecasts` scope, then full `SAPPHIRE_TEST_ENV=True bash run_tests.sh` before any commit. Zero failures/unexpected skips; investigate skips/zero collection under repository policy.
- Trace each path through discovery, strict resolution, existing reader selection and output. Open-ended adversarial review must inspect hidden configuration errors, explicit-loader weakening, global declaration changes and maintenance scope growth; confirm fixes after corrections.
- This shared discovery change affects cross-module processing, so also run `bash apps/run_locally.sh all` and `bash apps/run_docker_tests.sh --skip-ml` as required by the repository. Explicitly verify operational postprocessing, skills and maintenance with the partial monthly configuration; report any unrelated failures without treating them as passes. Record implementation and validation evidence before moving to Review.
- Move to Review after implementation and verification; human review governs completion.

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 }
  }
}
```

## Implementation and verification — 2026-11-19

Implemented `available_monthly_batch_modes()` in the shared resolver and wired
only the monthly branch of `_operational_schedules_for_horizon_type` to it.
Existing explicit loaders and quarterly/seasonal processing remain unchanged.
The implementation touches two production files and five test files.

- Core scope: 975 passed, one accepted existing skip and one existing expected
  failure. Postprocessing scope: 3,244 passed and one existing expected failure.
- Temporarily restoring the old monthly enumeration made all three new
  partial-absence entrypoint regressions fail on month_4; the candidate was
  restored before final validation. Malformed-existing-file cases stayed strict.
- Full 16-scope suite: 9,677 passed, 12 owner-accepted existing skips and two
  existing expected failures; no new failures or skips.
- Prescribed Docker check: eight builds and eight smoke tests passed with
  `--skip-ml`. The Linux postprocessing image exposes the new helper and its
  resolver source hash matches this worktree.
- Fresh independent read-only adversarial review found no actionable defect
  in the seven-file implementation, tests or unchanged processing contracts.
- Live `run_locally.sh all --continue-on-error`: exit 0, 38m 37s; all eleven
  execution stages and all three API validations passed. In particular, LT
  operational postprocessing passed in 3m 14s, skill recalculation in 5m 10s
  and maintenance in 2m 13s. The six absent optional modes emitted warnings
  instead of aborting these paths. The development .env hash was unchanged.
- The parent had no `OMP_THREAD_LIMIT`; the previously implemented macOS
  local-runner guard applied automatically. No native crash occurred.

Private evidence remains outside Git under `/tmp/sapphire-ltf021-*`, including
`full-tests.log`, `docker.log`, `old-enumeration.log`, `live-pipeline.log` and
`live-pipeline-result.json`. The `all` runner's existing DECAD-only ML dispatch
remains unchanged; this run does not claim an additional explicit PENTAD ML
pass. No GBT/deep-learning retraining was performed. Owner authorized commit and PR after this validation.
