# Update-time migration toolkit — P6: regenerate / gap-backfill hooks meta-wrapper

**Status**: draft, ready for review
**Phase**: P6 of the update-time migration toolkit
**Integration branch**: `develop_migration_toolkit`
**Feature branch**: `feature_p6_regenerate_hooks`
**Depends on**: P0 (foundation, helpers), P1a/P1b/P1c (runoff/meteo/snow), P2a/P2b (period rollups), P3 (hydrograph day), P4a/P4b (LR/ML forecasts), P5 (long-term forecasts). All earlier phases merged to `develop_migration_toolkit` (HEAD `ab726d7`).

## Why

After the eight CSV-source migrations (P1a–P5) finish, four residual coverage gaps remain in the target databases. Each gap has an existing standalone shell script in `bin/` that knows how to close it:

| Gap                                       | Closing script                                                | Target rows                                |
|-------------------------------------------|---------------------------------------------------------------|--------------------------------------------|
| Snow per-year norms + stats               | `bin/backfill_snow_stats_history.sh`                          | `snow_data` (mean/min/max/quantiles)       |
| Hydrograph MONTH + SEASON                 | `bin/yearly_runoff_hydrograph_aggregation.sh` (per year)      | `runoff_hydrograph` (norm + previous + current) |
| Pentad + decade skill metrics             | `bin/yearly_skill_metrics_recalculation.sh`                   | `skill_metrics` (pentad, decade)           |
| Month + quarter + season skill metrics    | `bin/bimonthly_long_term_skill_metrics_recalculation.sh`      | `skill_metrics` (month, quarter, season)   |

Running the four scripts back-to-back without orchestration has two operational sharp edges:

1. **Cron stomping.** A short-term forecast cron tick mid-run will race the backfill on the same containers and target tables. Operators historically pause cron by hand before kicking off these scripts; this is brittle (easy to forget, easy to leave paused on a failure).
2. **Late-start trap.** Starting a 3-hour snow-stats backfill 20 minutes before the next short-term cron tick is essentially the same problem dressed differently — even with cron paused, the operator's expectation of the next pentad forecast is broken, and the cron job itself silently disappears for the duration.

P6 closes both gaps by adding a single meta-wrapper, `bin/initialize_regenerate_hooks.sh`, that pauses cron, runs the four hooks under a trap-protected restore, and refuses to start near a cron tick.

## What

A new wrapper script + tests + runbook section. **No edits to the four underlying hook scripts.** P6 is purely orchestration.

### Files

- `bin/initialize_regenerate_hooks.sh` — NEW. The meta-wrapper.
- `apps/iEasyHydroForecast/tests/test_initialize_regenerate_hooks.py` — NEW. 20 tests.
- `doc/prod/update_data_migration_runbook.md` — MODIFY. Fills the §7 placeholder; does NOT touch §5.x or §6.x.
- `doc/plans/issues/high_prio_gi_draft_update_migration_p6_regenerate_hooks.md` — NEW. This document.

### CLI surface

```bash
bash bin/initialize_regenerate_hooks.sh <env_file_path> \
    [--dry-run] \
    [--start-year <YYYY>] \
    [--skip-hook-snow-stats] \
    [--skip-hook-hydrograph-month-season] \
    [--skip-hook-short-term-skill] \
    [--skip-hook-long-term-skill] \
    [--late-start-window-minutes <N>] \
    [--allow-late-start] \
    [--continue-on-error]
```

The four `--skip-hook-<name>` flags are the headline ergonomics. Each hook runs by default (architecture §3.3 L4 user-lock); the operator opts each one out individually. The flag names match the inventory column in the dry-run output one-to-one so a copy-paste from the dry-run report builds the correct opt-out.

### Default-on with opt-out — design rationale

Three approaches were considered:

1. **All-default-off + `--run-hook-<name>` to opt in.** Rejected. The common case is "run all four after a full migration"; making it the verbose case loses to a missed flag.
2. **`--only-hook-<name>` with an exclusive list.** Rejected. Composes poorly with the dry-run inventory (the dry-run output would have to suppress all but one line) and doesn't cleanly handle "run three of four".
3. **All-default-on + `--skip-hook-<name>` to opt out.** Chosen. Matches the architecture §3.3 L4 user-lock. The dry-run output stays a fixed shape (4 lines, one per hook, RUN or SKIP). Operators can comma-tack opt-outs as exceptions, not as the normal path.

### Cron-pause + late-start guard — design rationale

The meta-wrapper handles both safeties inline because:

- `bin/utils/common_functions.sh` does not currently ship a cron-pause helper. Adding one would change a shared file, which is out of P6's file-scope and would also reshape the §10 runbook content for previous phases. A follow-up issue should extract the helper; until then, the P6 wrapper is the ONLY caller that pauses cron — operators invoking the underlying hook scripts directly are still responsible for pausing cron themselves.
- The late-start computation reads `crontab -l`, parses the minute + hour fields (treating day-of-month / month / day-of-week as wildcards for window purposes — intentionally conservative), and computes the offset to the next tick over the next 48 hours. The minute-and-hour-only parse is forgiving enough for the realistic cron schedules in this project (which use `0 H * * *` shapes) without dragging in a brittle full cron parser.

The trap is installed BEFORE `_pause_cron` runs, so even a failure of the pause itself (e.g. `crontab -` fails) triggers `_restore_cron`. The restore is a no-op when `_CRON_WAS_PAUSED=false`, so missing/empty/unsupported crontabs gracefully skip the whole pause/restore cycle (and the wrapper logs the situation).

### Fail-fast vs continue-on-error — design rationale

**Default fail-fast.** The four hooks share dependencies in subtle ways (e.g. the short-term-skill hook reads runoff aggregations the hydrograph hook updates). If a hook fails, continuing to run the next one risks computing skill metrics against partly-stale source rows, which then need to be invalidated and recomputed anyway. Fail-fast keeps the recovery path clean: fix the failing hook, rerun the same wrapper (the underlying scripts are idempotent), it picks back up.

**Opt-in `--continue-on-error`.** The rare case is "snow-stats is broken because of an iEH HF outage, but the other three hooks read from the local DB only and should still run." For that case, `--continue-on-error` keeps the wrapper running through the failure and exits non-zero at the end with a summary of which hook(s) failed.

The hydrograph-month-season hook has an internal per-year loop. The same fail-fast / continue-on-error policy applies inside that loop: by default the first failing year aborts the rest; `--continue-on-error` keeps the loop going.

### Long-term-skill missing-script handling

The architecture document originally referenced `bin/initialize_site_backfill.sh` for the short-term skill hook. That script does NOT exist on the `develop_migration_toolkit` branch — neither does `bimonthly_long_term_skill_metrics_recalculation.sh` for long-term skill if a future branch removes it.

Each hook runner therefore performs a `[[ ! -f "$HOOK_SCRIPT" ]]` check up front. If the script is missing, the runner logs a WARNING and returns 0 (graceful skip), rather than crashing the orchestrator. The dry-run inventory marks the same condition as `[MISS]` so the operator sees what's wrong without having to start a real run.

This behaviour is pinned by the test `test_long_term_skill_hook_handles_missing_script_gracefully`, which deliberately deletes the long-term stub from the bin/ tree and asserts the wrapper still exits 0 with a logged warning.

### Station-filter contract

P0 defined `--station-filter` as the binding flag name for all CSV-source wrappers. P6 deliberately does NOT honor `--station-filter` — every one of the four hooks operates organisation-wide (yearly recalculations across every station; no per-station carve-out is meaningful). The `--help` output explicitly calls this out so operators porting muscle memory from P1a/P1b/P3/P5 don't waste time looking for a typo.

The test `test_wrapper_help_documents_station_filter_contract` pins the help text so a future refactor can't silently drop the explanation.

### Year-range default for hydrograph

The hydrograph MONTH+SEASON hook is invoked once per year. With no `--start-year`, the wrapper uses `current_year - 5` as the start (a conservative recent-window — enough to backfill the last few years' rows). To get the full archive, the operator passes `--start-year 2010` (or whatever's appropriate). The choice of `-5` years matches the operational norm calculation window in the `recalculate_snow_norms.py` and `sync_long_horizon_hydrograph.py` writers, both of which look ~5 years back by default.

## Acceptance criteria

- [x] `shellcheck -x bin/initialize_regenerate_hooks.sh` clean (only SC2329 info-level warnings about traps; informational only).
- [x] `bash bin/initialize_regenerate_hooks.sh --help` exits 0.
- [x] All 20 tests pass: `SAPPHIRE_TEST_ENV=True apps/iEasyHydroForecast/.venv/bin/pytest apps/iEasyHydroForecast/tests/test_initialize_regenerate_hooks.py -v`.
- [x] The full iEasyHydroForecast test suite (668 tests) still passes — P6 is additive and does not regress anything.
- [x] No real station codes anywhere in tests / fixtures / runbook (sentinel `19999` is the only allowed 5-digit numeric).
- [x] No edits to `sapphire/services/`.
- [x] No edits to the four underlying hook scripts EXCEPT the round-2/3 carve-outs on `yearly_skill_metrics_recalculation.sh`:
      (a) container-exit-code propagation (round-2 — `exit "$CONTAINER_EXIT_CODE"` at end of script; required by P6 fail-fast), and
      (b) shellcheck cleanup + dead-var removal (round-3 OQ3 — quote `$1`/`${LOG_DIR}`/`$IMAGE_ID`, replace `$?`-after-pull with direct check, remove unused `MEMORY_LIMIT`/`MEMORY_SWAP` since the active limits live in `bin/utils/run_skill_metrics_recalc.sh`). No behaviour change.
      The other three hook scripts (`backfill_snow_stats_history.sh`, `yearly_runoff_hydrograph_aggregation.sh`, `bimonthly_long_term_skill_metrics_recalculation.sh`) are untouched.
- [x] Runbook §7 replaces the placeholder; §5.x and §6.x untouched.
- [x] PR open against `develop_migration_toolkit`.

## Test plan summary

| # | Test                                                                  | What it pins                                              |
|---|-----------------------------------------------------------------------|-----------------------------------------------------------|
| 1 | `test_wrapper_help_returns_zero`                                      | `--help` exits 0 with usage                                |
| 2 | `test_wrapper_help_documents_each_skip_hook_flag`                     | All 4 `--skip-hook-*` flags documented                     |
| 3 | `test_wrapper_help_documents_late_start_window`                       | Late-start flags documented                                |
| 4 | `test_wrapper_help_documents_station_filter_contract`                 | Help explicitly documents station-filter omission          |
| 5 | `test_wrapper_rejects_missing_env_file`                               | Missing env file -> non-zero exit + error                  |
| 6 | `test_wrapper_rejects_no_args`                                        | No args -> non-zero exit                                   |
| 7 | `test_wrapper_rejects_unknown_flag`                                   | Typo in flag is loud, not silent                           |
| 8 | `test_wrapper_rejects_non_numeric_start_year`                         | `--start-year abc` rejected at parse time                  |
| 9 | `test_dry_run_lists_all_four_hooks_when_no_skip_flags`                | Inventory mentions each hook                               |
| 10| `test_dry_run_marks_skipped_hooks_correctly`                          | `--skip-hook-snow-stats` makes the snow line `[SKIP]`      |
| 11| `test_dry_run_does_not_invoke_hook_scripts`                           | Dry-run never executes any stub                            |
| 12| `test_late_start_guard_aborts_when_within_window`                     | Crontab says 5 min, window=30, no opt-in -> abort          |
| 13| `test_late_start_guard_bypass_with_allow_late_start`                  | `--allow-late-start` overrides the guard                   |
| 14| `test_late_start_guard_disabled_with_zero_window`                     | `--late-start-window-minutes 0` disables                   |
| 15| `test_cron_pause_and_restore_called_on_normal_run`                    | Pause (`crontab -`) + restore (`crontab <backup>`) both fire |
| 16| `test_cron_restore_called_when_hook_fails`                            | Trap restores cron even when a hook exits non-zero         |
| 17| `test_long_term_skill_hook_handles_missing_script_gracefully`         | Missing script -> WARN + skip + exit 0                     |
| 18| `test_wrapper_forwards_start_year_to_snow_stats_in_dry_run`           | `--start-year 2015` -> snow-stats cmd line                  |
| 19| `test_wrapper_forwards_start_year_to_hydrograph_year_range_in_dry_run`| `--start-year 2015` -> hydrograph year=2015 cmd line        |
| 20| `test_module_audit_not_applicable`                                    | P6 ships no Python module; pin that constraint              |

The tests use `crontab(1)` shims on PATH so the real user crontab is never touched. The four underlying hook scripts are stubbed with copy-from-disk fakes that record invocations to a temp file; tests assert on the contents of the temp file rather than mock chains.

## Out of scope (recorded for follow-up)

1. **Extract cron-pause into a shared helper.** Currently lives inline in P6. Should move to `bin/utils/common_functions.sh` so the other regenerate-style scripts can adopt it. Not done here because it expands the P6 file-scope (and reshapes the runbook §4.2 cron-pause content for prior phases).
2. **A `--single-hook <name>` flag.** Convenience for the "only run one" case. Currently expressible via `--skip-hook-A --skip-hook-B --skip-hook-C`. Add only if operators actually complain about the verbosity.
3. **A wrapper for the short-term `initialize_site_backfill.sh`** that the architecture document references but does not exist on this branch. If a future branch reintroduces that script, update the short-term-skill hook target in `bin/initialize_regenerate_hooks.sh` (and §7.1 of the runbook) to call it.
4. **Discovery from a known crontab file** rather than `crontab -l`. The current wrapper assumes the operator's user crontab is the source of truth. Some deployments may ship a system crontab in `/etc/cron.d/`; the late-start guard would miss those. Document as a known limitation.
5. **Flip hook 4 (long-term-skill) to mandatory** once the underlying script lands on `develop_migration_toolkit`. Tracked at `doc/plans/issues/mid_prio_gi_draft_p6_hook4_long_term_skill_mandatory.md`.

## Operational risks (round-2 review)

The round-2 reviewer identified five operator-safety findings; all are now resolved. The lasting operational risks the resolutions introduce:

1. **Four-way cron-pause classification.** `_pause_cron` now distinguishes `crontab` binary missing (hard-fail, NO bypass), `no crontab for user` (INFO + proceed — normal on day-0 servers + dev laptops), real `crontab -l` errors (hard-fail; bypass via `--allow-unpaused-cron`), and `crontab -` write failures (hard-fail; bypass via `--allow-unpaused-cron`). The bypass downgrades to WARNING and proceeds with cron ACTIVE — operator must use it ONLY on verified no-race hosts. The bypass-path log explicitly states the backup is retained as a pre-attempt state reference, NOT as an active-restore artifact.
2. **Separate INT/TERM trap handlers.** `_on_signal` exits with 130 (INT) or 143 (TERM) per POSIX convention. Both `_on_exit` and `_on_signal` guard cleanup with `|| true` so a `_restore_cron` failure cannot skip `_umh_cleanup_tempdirs`. The wrapper's traps overwrite the umh helper's own EXIT trap; the explicit `_umh_cleanup_tempdirs` call from both handlers ensures workspace cleanup still runs.
3. **Preflight runs BEFORE cron pause.** A missing hook 1/2/3 script aborts the run with a hard error before the workspace is acquired or cron is touched. Hook 4 is the only graceful-skip carve-out (see follow-up issue). Operators who genuinely don't want one of the mandatory hooks must pass the corresponding `--skip-hook-<name>` flag — this is the path that bypasses preflight for that specific hook.
4. **Backup-file lifetime (round-3 contract).** The crontab backup lives at `${ieasyhydroforecast_data_root_dir}/logs/regenerate_hooks/crontab_backup_<utc-ts>.txt` — INSIDE the wrapper's LOG directory, OUTSIDE the umh-managed workspace. This placement is what lets the file survive trap-driven cleanup when restore fails or `--allow-unpaused-cron` is set. Lifetime rules: (a) restore success removes the backup AND clears `_CRON_BACKUP_PATH` + `_CRON_WAS_PAUSED`; (b) restore failure LEAVES the backup + logs the manual-recovery command pointing at the surviving file; (c) `--allow-unpaused-cron` write-failure KEEPS the backup as a pre-attempt-state reference (cron was never paused); (d) hard-fail write-failure (no bypass) removes the partial backup before exit; (e) SIGKILL / power loss leaves the backup behind (no trap fires); (f) "no crontab for user" and "crontab binary missing" never write a backup at all. Persisted backups are operator-review artifacts; no automatic age-based cleanup is performed.
5. **Upstream `yearly_skill_metrics_recalculation.sh` exit propagation.** The script now `exit "$CONTAINER_EXIT_CODE"`s at end. Bundled into this PR (out of P6's original file-scope but directly breaks P6 fail-fast). Cron callers that previously saw exit 0 on a failed recalc now see the container's actual exit code.

## Risks + mitigations

| Risk                                                                                   | Mitigation                                                                                                         |
|----------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| SIGKILL / power-loss during a real run leaves cron paused                              | Documented in runbook §7.6 with the manual recovery command (`crontab <backup>`). Backup file lives under `logs/regenerate_hooks/crontab_backup_<utc-ts>.txt` (NOT the umh `_tmp` workspace); the umh cleanup doesn't touch it. |
| Restore failure (`crontab <backup>` returns non-zero)                                  | Backup PERSISTS at `logs/regenerate_hooks/crontab_backup_<utc-ts>.txt`. Wrapper logs the manual-recovery command pointing at the surviving file. Operator runs it then deletes the backup. Pinned by `test_restore_failure_persists_backup_in_log_dir`. |
| Crontab parser too forgiving: `*/5 * * * *` is treated as "every 5 minutes" but day-of-month / month / weekday wildcards mean some lines that wouldn't fire today are counted | Conservative on purpose; false-positives (refusing to start when we could have) are cheap; false-negatives (starting near a real tick) are expensive. |
| The four hooks fail in a correlated way (e.g. iEH HF outage)                           | Default fail-fast surfaces the problem on the first hook. Operator fixes the upstream condition and reruns.        |
| Operator runs the meta-wrapper while the previous run is still alive                   | Not currently guarded. Architecture §Q9 placed a lock-file mechanism on the wishlist; deferred.                    |
| Long-term-skill underlying script renamed in a future branch                            | Graceful skip + WARNING, not a crash. Pinned by `test_long_term_skill_hook_handles_missing_script_gracefully`.    |

## Open questions for review

1. Is `current_year - 5` the right default hydrograph start-year? The architecture document doesn't pin a number; this was inferred from the writers' look-back windows.
2. Should the wrapper print the resolved backup-file path on startup so the operator can `crontab <backup>` it manually after a SIGKILL? Currently only the temp-dir parent is logged. Easy to add.
3. Should `--continue-on-error` propagate INTO the underlying scripts (e.g. by setting a known env var)? Currently the policy is applied only at the orchestrator level — each underlying script makes its own per-iteration decisions.

## References

- Architecture plan: `doc/plans/architecture_review_claude.md` + `doc/plans/architecture_review_copilot.md`
- Runbook: `doc/prod/update_data_migration_runbook.md` (§7 filled by this PR)
- Sibling wrappers (structural reference): `bin/initialize_long_forecast_history.sh` (P5)
- Four hook scripts (read-only for P6):
  - `bin/backfill_snow_stats_history.sh`
  - `bin/yearly_runoff_hydrograph_aggregation.sh`
  - `bin/yearly_skill_metrics_recalculation.sh`
  - `bin/bimonthly_long_term_skill_metrics_recalculation.sh`
