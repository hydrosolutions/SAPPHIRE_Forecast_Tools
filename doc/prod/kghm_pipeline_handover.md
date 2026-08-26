# Handoff - kghm pipeline: ML unblocked, long-horizon rows need a one-time backfill run

**Audience:** the operator of the kghm deployment.
**Date:** 2026-08-26.
**TL;DR:** Deploying the code is necessary but **not sufficient**. One extra run
(`bin/yearly_runoff_hydrograph_aggregation.sh`, already part of checklist §2.6) is needed to recover
long-horizon rows that were silently dropped, and the crontab should be audited before you trust it.
Also: that run, and any manual `apps/run_locally.sh daily`, will now **exit non-zero by design** —
expected, but only under the exact signature in §4. **Read §4 before you run anything.**

> Sentinel station code `19999` only in this doc. The affected stations are identified below by
> **property**, not by code — read the real codes from the `WARNING` lines in your own log, since the
> affected set can change. Never commit real codes, discharge values, or env contents.

---

## 1. What was wrong

Two bugs closed both routes to `machine_learning` **in the local orchestration script**
(`apps/run_locally.sh`).

`apps/run_locally.sh daily` aborted in Phase 2, before Phase 3 (`machine_learning`,
`linear_regression`, `postprocessing_forecasts`) ever ran. The abort came from a maintenance
sub-step — `sync_long_horizon_hydrograph.py`, invoked by `run_maintenance_preprocessing_runoff` —
exiting **4** whenever at least one station's iEasyHydro-HF monthly-norm SDK lookup **raised**. On
this deployment that condition is real and reproducible: a live probe against kghm's iEH-HF on
2026-08-21 found 62 stations attempted, 53 written, 5 with an absent norm, and 4 whose lookup raised,
with no API failures. That failure aborted the whole `preprocessing_runoff (maintenance)` module, and
the fail-fast guard stopped the run there, so `machine_learning`, `linear_regression` and
`postprocessing_forecasts` never executed — even though none of them reads month-horizon hydrograph
rows.

The manual fallback was blocked too: running the bare `machine_learning` target on its own crashed
with `ValueError: Prediction mode %s is not supported`, because that target never resolved
`SAPPHIRE_PREDICTION_MODE` before using it.

**Scope — read this before assuming your nightly runs were affected.** The nightly production cron
(entry (5), `bin/run_daily_maintenance.sh` → Luigi) does **not** invoke
`sync_long_horizon_hydrograph.py`. So the Phase 2 abort above affected **manual `run_locally.sh`
runs**, not the nightly cron. If your *nightly* pipeline is also producing no ML forecasts, that has
a different cause — start from
[`ml_no_forecasts_debug_runbook.md`](ml_no_forecasts_debug_runbook.md), not from this document.

The routes that *do* reach the long-horizon writer behave differently from each other, which matters
for §4:

| Route | Emits `LONG-HORIZON RUN SUMMARY` / exit 4? |
|---|---|
| `bin/yearly_runoff_hydrograph_aggregation.sh` — yearly cron entry **(9)**, and checklist §2.6 | Yes. §4's signature applies. |
| `apps/run_locally.sh` (maintenance phase / `daily`) | Yes, normalised to overall exit 1 plus a FAIL row. §4's signature applies. |
| `bin/backfill_discharge_aggregation.sh` — checklist **§3.5** | **No.** It calls the writer through a capturing client and deliberately treats the captured records, *not* the writer's status-bearing return value, as its output. It reports through its own dry-run JSON diff report and does not surface exit 4. **§4's signature does not cover this route** — review its diff report on its own terms. |
| `bin/dev_local_backfill.sh` | Developer tool; not part of this procedure. |

Underneath both of those orchestration bugs, the same four stations were losing **all** of their
long-horizon data on every run that hit this condition — not just the norm. Before the fix, a raised
SDK lookup made the writer drop the station entirely: zero monthly, seasonal and quarterly rows, no
exception made for the station's observed runoff (`previous`/`current` actuals), even though those
actuals come from a separate SDK call that already fails soft to a local fallback rather than raising.
A raised norm lookup was withholding data it had no bearing on. The visible symptom was empty
last-year runoff and no percent-of-norm for those four stations in the long-term monthly bulletin —
indistinguishable from "never configured." This part **did** affect the yearly cron, which is why the
backfill in §3 is needed.

## 2. What merged

| PR | Issue(s) | Effect on this deployment |
|---|---|---|
| **#468** | INFRA-037 + ML-016 | `run_locally.sh daily` no longer aborts on this condition. Exit 4 from the long-horizon sync is now recorded as its own `preprocessing_runoff (long-horizon sync): FAIL` row instead of failing the maintenance module, so the run continues into Phase 3 — but still exits non-zero overall. The bare `machine_learning` target now resolves its own mode instead of crashing on an unset `SAPPHIRE_PREDICTION_MODE`. |
| **#472** | (docs only, prerequisite for PREPQ-015) | The original PREPQ-015 draft was found **not implementable** — both readings of it *would have* shipped a new bug (one silently erasing the failure signal and exiting 0, the other creating a new partial-write mode). This PR corrected the draft and confirmed the failure's cause with a live probe against kghm's iEH-HF, unblocking #475. No deployment behaviour changed. |
| **#475** | PREPQ-015 | **The real cure.** A raised SDK lookup keeps the station's `SDK_FAILED` status **and** now also writes its 12 monthly, 1 seasonal and 4 quarterly rows, preserving any previously stored monthly norm. This recovers the discarded observed runoff; it does not fabricate a norm the station never had. |
| **#477** | INFRA-039 | Closes one silent-no-op path: an out-of-domain `SAPPHIRE_PREDICTION_MODE` or `ML_MODE` passed to the targets that dispatch `linear_regression`/`machine_learning` is now rejected at entry (exit 1) instead of running to completion having written nothing. |

Issue IDs above are named, not linked — look them up in
[`doc/plans/module_issues.md`](../plans/module_issues.md). Issue files move through
`gi_draft_*.md` → `review_gi_draft_*.md` → `issues/archive/` as they progress, so a direct link into
`doc/plans/issues/` goes stale the next time one of these advances.

## 3. What to do

**Read §4 first.** Two of these steps are expected to exit non-zero, and §4 is what tells you whether
a given non-zero exit is the expected one or a real incident.

0. **Prerequisites, before you start the checklist.**
   - `cd /data/SAPPHIRE_Forecast_Tools` (or wherever this repo lives on this server) and confirm with
     `pwd`. Several scripts the checklist invokes — `bin/backup_sapphire_db.sh` in §1.5 among them —
     resolve `sapphire/docker-compose.yml` as a *relative* path and abort if it is not found.
   - Confirm the iEasyHydro-HF SSH tunnel is up (checklist §1.2:
     `sudo systemctl status <tunnel-service>.service`). The aggregation script will also try to
     establish its own tunnel if `ieasyhydroforecast_ssh_to_iEH=true`, but do not rely on that alone.

1. **Audit the crontab — do not assume it matches the docs.** Three silent cron defects were found on
   a sibling deployment's *live* crontab in 2026-08 and fixed in the checklists; the checklist being
   right today does not mean this server's installed crontab is. §2.5 of the checklist has grep checks
   for the shapes that bit that deployment (a literal `${LOG_DIR}` cron never expands, an ambiguous
   `2>&1#` redirect that silently no-ops the command, and a stale `monthly_norms` task name). Also
   check by hand:
   - **`bin/backup_sapphire_db.sh` must be run with the repository root as its working directory.**
     Cron runs from `$HOME`, not the repo. The current canonical cron block in the checklist already
     includes `cd /data/SAPPHIRE_Forecast_Tools &&` before the backup call; confirm the **installed**
     line on this server has it too. The script does log an explicit `ERROR: Must run from the
     repository root (parent of sapphire/)` and exits 1 — but cron surfaces that to nobody, so a
     crontab predating that fix has been backing up nothing, nightly, in a log no one reads.
   - **Do not restore the retired `run_periodic_maintenance.sh monthly_norms` task.** If the installed
     crontab still calls it instead of `bin/yearly_runoff_hydrograph_aggregation.sh`, that wrapper
     accepts the retired task name, hands it to Luigi, which raises — but the wrapper has no `set -e`
     and never captures that exit code, so it prints "task submitted" and exits 0 regardless
     (INFRA-023, still open — Draft). Replace the entry with
     `bin/yearly_runoff_hydrograph_aggregation.sh`, matching entry **(9)** in the checklist's
     canonical block.
     **Note — the two docs disagree, and the one they name as authoritative is the stale one.**
     Checklist §2.5 points at [`doc/deployment.md`](../deployment.md) as "the authoritative source"
     and says its own block is kept in sync with it. For this entry that is currently untrue:
     `doc/deployment.md` still documents the retired `monthly_norms` line. Until that is corrected,
     use the **checklist §2.5 canonical block** for this entry and do not reinstall a cron line from
     `doc/deployment.md`. Flagged as a known documentation defect in §5.
   - **Confirm the long-term cron day still matches this deployment's configured
     `operational_issue_day` values**, per the operator setup block at the top of the checklist. The
     dangerous window is a **6–10 day** mismatch: the tolerance check admits the mode, then the model
     itself refuses it — nothing is written and the run exits 0. (A 1–5 day drift still passes; a
     drift beyond the tolerance is rejected earlier and more visibly.) Keeping the two identical
     avoids the question entirely.

2. **Update the deployment.** Follow
   [`doc/prod/update_deployment_checklist.md`](update_deployment_checklist.md) end to end.

   **Checklist §2.6 runs the long-horizon backfill for you** — it invokes
   `bash bin/yearly_runoff_hydrograph_aggregation.sh ${ENV_FILE_PATH}`, which is exactly the recovery
   run this handover is about. **That run is expected to exit 4** (§4). Do not treat it as a failed
   deployment step, and do not run it a second time afterwards.

   If you skipped §2.6, or need to recover a prior year, run it by hand instead:
   ```bash
   cd /data/SAPPHIRE_Forecast_Tools
   bash bin/yearly_runoff_hydrograph_aggregation.sh ${ENV_FILE_PATH}
   # optional: [--target-year YYYY] for a prior year
   ```

3. **Verify the backfill landed.** Read the `LONG-HORIZON RUN SUMMARY` block at the end of the run's
   log (path printed by the script; also under `${LOG_DIR}`). Check that:
   - `written` is the large majority of `total_attempted`, and `api_failed=0`;
   - `sdk_failed` matches the signature in §4 — not merely "non-zero";
   - the per-station `WARNING` lines are present and contain the word **"continuing"**.

   Then, for the station codes those `WARNING` lines name, confirm through the preprocessing API (or
   the dashboard's long-term monthly bulletin) that 12 monthly + 1 seasonal + 4 quarterly rows now
   exist for the current year, and that the bulletin shows observed **last-year runoff** for them
   instead of a blank. Row existence alone is not the goal — the blank last-year runoff was the
   original symptom, so that is the field to look at.

4. **Run the pipeline normally.** The nightly cron was never affected by the Phase 2 abort (§1). If
   you invoke `apps/run_locally.sh daily` by hand, it should now reach Phase 3 and produce ML
   forecasts, while still exiting 1 with the `long-horizon sync` FAIL row described in §4.

## 4. What is EXPECTED — and the exact signature that makes it expected

This is the most important section. The items below are the intended outcome of the fixes above —
**but only under the signature described here.** The same status code also covers real outages, so
"exit 4 is normal" is *not* a safe rule on its own.

- **Exit 4 from the long-horizon writer, under a specific signature.** This applies to the two
  routes marked "§4's signature applies" in the §1 table — the yearly wrapper (checklist §2.6) and
  `apps/run_locally.sh`. The writer exits 4 when at least one station's monthly-norm SDK lookup
  raised. Accept it as expected only when **all** of
  these hold:
  - `api_failed=0` (a non-zero `api_failed` exits **5** and is always fatal);
  - `sdk_failed` equals the small known count — **4** as of the 2026-08-21 probe, against
    `total_attempted=62 written=53 norm_absent=5`;
  - the failing station codes are the same set as the previous run;
  - each `WARNING` line names `ValueError: No path provided or the provided path is None`.

  **If any of those differ — especially if `sdk_failed` climbs toward `total_attempted` — treat it as
  an incident, not as this known condition.** The norm lookup catches `Exception` broadly
  (`apps/preprocessing_runoff/sync_long_horizon_hydrograph.py:295-300`), so an expired credential, a
  502, or a service-wide iEH-HF outage produces the *same* `sdk_failed` status and the *same* exit 4
  as the four known stations. The count and the exception text are what distinguish them. Re-baseline
  the numbers above if the station set legitimately changes.

  Where you see it: the standalone wrapper exits **4** directly; `apps/run_locally.sh` normalises it
  to overall exit **1** plus a `preprocessing_runoff (long-horizon sync): FAIL` row, while
  `preprocessing_runoff (maintenance)` itself shows `PASS`.

- **That FAIL row recurring indefinitely on this deployment.** Reclassifying it into a passing status
  was proposed and deliberately rejected — three separate grading approaches were each shown to be
  able to mask a genuine outage as success (see PREPQ-014 in `doc/plans/module_issues.md`). The
  recurring alarm is the accepted cost of not building that blind spot.

- **New `WARNING` log lines**, one per failing station, naming the station code and the SDK exception.
  These are new only because the message was moved from `DEBUG` to `WARNING` — the root logger caps at
  `WARNING` by default, so this failure was previously invisible even when it was already happening.
  The line reads `write_station_monthly_hydrograph: SDK call failed for site <code>, continuing with
  a read-merge of any previously stored norm.` **The word to check for is "continuing."** If a log
  instead says the station was "skipping," that checkout predates the fix.

- **Percent-of-norm staying blank for those four stations.** This backfill restores observed runoff,
  not norms. All four are members of iEasyHydro-HF's own authoritative virtual-site list, and their
  norm lookup fails at per-site UUID resolution — so on a first write there is no norm to read-merge
  and percent-of-norm stays blank. Two caveats worth knowing: the probe established virtual-list
  *membership*, not that these stations are absent from the hydrological registry, so "normless" is a
  current observation rather than a proven permanent property; and where a norm was stored for the
  target year previously, the read-merge preserves it and percent-of-norm can legitimately still
  appear. Deriving norms locally instead of from the SDK is PREPQ-010, and it is not done.

## 5. Open questions — unresolved, not blocking

- **Whether the yearly cron entry still calls the retired `run_periodic_maintenance.sh monthly_norms`**
  task. If it does, INFRA-023 means every 1 January rebuild has been silently skipped — the wrapper
  reports success ("task submitted") while doing nothing. Confirm the installed crontab uses
  `bin/yearly_runoff_hydrograph_aggregation.sh` instead (see §3, step 1).
- **Whether the long-term cron day still equals this deployment's configured `operational_issue_day`
  values.** A 6–10 day drift is admitted by the tolerance check and then refused by the model —
  nothing is written and the run exits 0, silently.
- **One of the four affected stations is recorded as non-virtual in the local config**
  (`config_virtual_stations.json` / `config_all_stations_library.json`), even though iEasyHydro-HF's
  own authoritative list reports it as virtual. Nothing in the changes described here reads those
  local config files, but something else in the pipeline might. Not naming the station here — read it
  from your own `WARNING` lines if you need to investigate.
- **`doc/deployment.md` is stale** on the retired `monthly_norms` cron entry, while checklist §2.5
  still names it "the authoritative source" and claims to be kept in sync with it (§3, step 1).
  Until one of the two is corrected, prefer the checklist's canonical cron block for that entry.

## 6. Verified before shipping — and what remains unverified on your server

- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` was green across all four PRs at merge time
  and again when this document was written: 16/16 modules and services, zero failures, no new skips.
- The 62/53/5/4/0 station counts described in §1 came from a live, read-only probe of kghm's iEH-HF
  on 2026-08-21, not a synthetic fixture; all four `SDK_FAILED` stations raised the identical
  `ValueError: No path provided or the provided path is None` and are members of the SDK's own
  authoritative virtual-site list.
- Every PR above went through at least one round of out-of-loop adversarial review
  (`codex exec`); #475 (PREPQ-015) went through several, the last confirming the fix does not
  silently mask an API outage as a norm failure.
- **The fix was confirmed end-to-end, post-merge, with a before/after read-back through the
  preprocessing API — not a test fixture.** PR #475 merged 2026-08-21; on 2026-08-24 the writer was
  exercised against a kghm configuration and the real iEasyHydro-HF SSH tunnel
  (`get_virtual_sites()` returned 6 sites), with a running SAPPHIRE stack. Read through the
  preprocessing API before the run, all four affected stations showed 0 month / 0 season / 0 quarter
  rows; after the run, the same query showed 12 month / 1 season / 4 quarter rows for all four,
  with counts unchanged at `total_attempted=62 written=53 norm_absent=5 sdk_failed=4 api_failed=0`
  and the four `WARNING` lines present, each naming its station, the `ValueError`, and the word
  "continuing." INFRA-039 was checked separately in the same environment: an out-of-domain
  `SAPPHIRE_PREDICTION_MODE` and an invalid `ML_MODE` were each rejected at entry with exit 1, naming
  the variable and the offending value; valid values passed.
- **Two limits on that verification, both of which §3 is what actually closes:**
  - It ran **locally** — a kghm configuration and the live tunnel, writing to a local SAPPHIRE
    database, not the deployed one. Nothing has been checked on the kghm production server.
  - It used `apps/run_locally.sh maintenance:preprocessing_runoff`, **not** the
    `bin/yearly_runoff_hydrograph_aggregation.sh` wrapper that §3 tells you to run. The two invoke the
    same writer, but the wrapper adds a Docker image, its own env-file argument, host networking,
    volume mounts and exit-code propagation — none of which that run exercised.

  So §3 step 3 is the **first** confirmation of this fix both on that server and through that
  entrypoint. The local run establishes that the fix works against kghm's real data shape; it does
  not establish that your deployment is already good.

## 7. Reference table

| Doc | Purpose |
|---|---|
| [`update_deployment_checklist.md`](update_deployment_checklist.md) | Routine update procedure; §1.2 tunnel check, §1.5 backup, §2.5 crontab audit + canonical cron block, §2.6 the long-horizon aggregation run. |
| [`ml_no_forecasts_debug_runbook.md`](ml_no_forecasts_debug_runbook.md) | Full triage flow if ML still produces nothing after this deployment — including the nightly-cron case, which §1 explains is *not* what these PRs fixed. |
| [`../plans/module_issues.md`](../plans/module_issues.md) | Look up any issue ID named in this doc (INFRA-037, INFRA-039, INFRA-023, ML-016, PREPQ-010, PREPQ-014, PREPQ-015) by name. |
| [`../operations/backup_restore.md`](../operations/backup_restore.md) | Backup/restore mechanics behind `bin/backup_sapphire_db.sh`, referenced in §3 step 1. |
| [`../../bin/yearly_runoff_hydrograph_aggregation.sh`](../../bin/yearly_runoff_hydrograph_aggregation.sh) | The backfill script — read its header comment for `--target-year` usage if you need prior years too. |
