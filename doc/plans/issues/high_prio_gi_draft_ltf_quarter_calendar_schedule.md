# LTF-014: Issue the quarter forecast once per calendar quarter (Q1–Q4), not monthly Mar–Sep

**Status**: Draft (2026-09-26, rev 4 after the second review round)
**Module**: `apps/long_term_forecasting` (tests only) + per-deployment data repos (config)
**Priority**: **High.** Target dates (soft):
- **tjhm Q4, issue 2026-10-01.** The current config skips it. It can be recovered with `lt_recovery`
  until 2026-11-30 once P0 is done (`apps/long_term_forecasting/lt_recovery.py:282-306`,
  `check_recovery_window`), so this is not a reason to skip the P0 gate. Recovery **refuses** if any
  member row for that date already exists (`lt_recovery.py:682-689`); see P0 gate step 2. Clearing the
  pre-existing tjhm rows dated 2026-10-01 before any recovery is owned by the decision-F step in PP-064
  Chunk C (round-2 decision 4), not by this plan.
- **kghm Q1, issue 2026-12-25.**

**Labels**: `long-term`, `quarter`, `config`, `deployment`
**Overview**: [`../quarter_calendar_product_plan.md`](../quarter_calendar_product_plan.md). The dependency
graph lives there only.
**Related**: PP-064, PP-065 (its temporary LR fallback, decision G, is removed once P0 and P2 are deployed
on both orgs), FD-029, DOC-009, LTF-015, LTF-007 (scheduler tolerance 10 vs execution gate 5), INFRA-022
(validate_pipeline FAIL on gated days)

## Problem

Owner decision (2026-09-25): both hydromets issue the quarterly forecast for calendar quarters, once per
quarter, and Q1 is required. The deployed configs instead issue a rolling 3-month window every month
from March to September:

- Per-model configs in each data repo:
  `config/models_and_scalers/long_term_forecasting/quarter/{Base/LR_Base,SnowMapper/LR_SM}/general_config.json`
  → `"forecast_months": [3, 4, 5, 6, 7, 8, 9]` (kghm and tjhm alike).
- Two gates read `forecast_months`, and both must agree:
  - the scheduler admits the quarter mode if **any** of its models is scheduled
    (`apps/long_term_forecasting/lt_schedule_query.py:100-121`, via `lt_utils.nearest_scheduled_issue_date`,
    `lt_utils.py:134-174`);
  - the per-model execution gate `check_valid_forecast_issue_date` (`lt_utils.py:177-228`, called at
    `run_forecast.py:341`) returns None for a model not scheduled within ±5 days, and the caller counts
    that skip as a failure (`run_forecast.py:345-347`). One wrong model file therefore activates the mode
    **and** fails the run for that model.
- The window **label** is issue month + lead, 3 months long (`post_process_lt_forecast.py:132-201`).

| Org | Issue day / lead (mode config `long_term_configs/quarter.json`) | Issued today | Calendar quarters among them | Missing |
|---|---|---|---|---|
| kghm | 25 / 1 | 25 Mar … 25 Sep | 25 Mar → Q2, 25 Jun → Q3, 25 Sep → Q4 | **Q1** (25 Dec) |
| tjhm | 1 / 0 | 1 Mar … 1 Sep | 1 Apr → Q2, 1 Jul → Q3 | **Q1** (1 Jan), **Q4** (1 Oct) |

## What the schedule change does and does not fix

**Enough for the schedule (verified 2026-09-25):**
- `forecast_months` is read only through `config_forecast.get_forecast_months` (`config_forecast.py:278-289`,
  default 1..12). Its consumers are the scheduler, the execution gate above, `lt_recovery.py:310-357`, and
  the pinned `lt_forecasting` hindcast (`LINEAR_REGRESSION.py:538-543`; pin at `pyproject.toml:68`).
  `predict_operational` ignores it.
- No cron line, Luigi task, `bin/` wrapper or `run_locally.sh` path hard-codes quarter months. Cron is
  `0 6 ${LT_ISSUE_DAY} * *` every month (kghm `10,25`, tjhm `1`).
- The Dec→Jan **label** works: `adjust_forecast_dates_dynamic` puts `valid_from` in the next year
  (`post_process_lt_forecast.py:171-190`); a simulation gives 2026-12-25 lead 1 → 2027-01-01..2027-03-31.
- No retraining artefacts are involved. LR_Base/LR_SM are refitted on every operational run per station
  and issue month-day (`LINEAR_REGRESSION.py:385-521`), and `save_model`/`load_model` are no-ops (`:779-799`).
- Loading a mode config **rewrites every model's `general_config.json`** with `indent=4` and refreshed
  `prediction_horizon`/`offset`/`forecast_days`/`allowable_missing_value_operational`
  (`config_forecast.py:157-180`). `forecast_months` survives the rewrite. So edit the live server copy,
  and compare parsed JSON, not bytes.

**Not fixed by the schedule change: the model target is a fixed 90-day window, relabelled as the quarter.**
- The mode configs set `prediction_horizon: 90` and `offset` 95 (kghm) / 90 (tjhm). The library builds
  the target as a 90-day rolling mean shifted by `offset` (`lt_forecasting` `FeatureExtractor.py:332-372`,
  lines ~362-366). `post_process_lt_forecast` then only rewrites the dates (`:716-741`).
- Examples: kghm 25 Dec learns **31 Dec – 30 Mar**, labelled 1 Jan – 31 Mar; tjhm 1 Oct learns
  **2 Oct – 30 Dec**, labelled Oct–Dec (92 days); a leap-year Q1 has 91 days.
- **Measured size (review round, 2026-09-26):** the median bias of the 90-day target against the
  calendar-quarter mean is ≤ 2% for every org and quarter; the largest is kghm Q2 at −1.7% (p05 −4.8%).
  This approximation already applies to today's Q2–Q4 issues.
- **Cheaper D1 option for the modeller:** the monthly modes already rescale the model window to the
  calendar month with a climatology ratio (`calendar_month_adjustment`, default True,
  `config_forecast.py:241-249`; path `post_process_lt_forecast.py:745-786`). `quarter.json` sets it to
  False. Applying the same ratio to the quarter is a possible D1 answer; it is not part of this plan.
- Whether to make the target calendar-exact is a **modelling decision** (overview D1). Nothing in this
  plan claims the forecast value is an exact calendar-quarter mean.

## Plan

### P0 — Change the schedule in the data repos (ops, no code)

**Executor:** the server steps are run by the owner or by hydromet IT; the PR that records the gate names
who. Code agents do not run P0.

**Gate: do not edit any config before all three are done.**
1. **Modeller confirmation (Sandro).** Record his answers in this file.
   - Why `forecast_months` was set to `[3..9]`. No reason is recorded anywhere (history checked
     2026-09-25), and his own evaluation script lists Mar–Jun issues only.
   - Whether LR_Base and LR_SM are valid for the winter issues (kghm 25 Dec → Q1; tjhm 1 Jan → Q1 and
     1 Oct → Q4). These issue months have never run operationally. Verification list:
     - LR_SM hindcasts for issue months 12, 1 and 10 run without an excess of flag 2/3 rows. Feature
       selection is correlation-based (`corr_threshold` 0.3, `LINEAR_REGRESSION.py:169`), and winter SWE is
       often constant zero: measured locally, SWE ≤ 1 mm at 44% of tjhm Oct-1 station-dates and 91% of kghm
       Sep-25 station-dates.
     - LOYO skill of LR_SM vs LR_Base per quarter.
     - SnowMapper availability on Dec 25, Jan 1 and Oct 1, and that LR_Base still issues when the snow
       fetch fails.
     - Ice-affected winter observations in the target and the features.
2. **Server state read, not assumed** (aggregate descriptions only in this file):
   - Read the two `general_config.json` files and `long_term_configs/quarter.json` on each server and
     compare them with the Dropbox copies as **parsed** JSON:
     `diff <(python3 -m json.tool --sort-keys A) <(python3 -m json.tool --sort-keys B)`. A byte diff is
     meaningless because of the rewrite above; file mtimes are not evidence for the same reason.
   - Read the live crontab LT line (issue days per org).
   - Check that `quarter` is in `ieasyhydroforecast_ml_long_term_supported_modes` in the env files that
     the LT container **and** the postprocessing container actually use. Both read it: LT at
     `config_forecast.py:44-48`, postprocessing through
     `apps/iEasyHydroForecast/long_term_horizon_resolver.py:11, 52-55` (e.g.
     `recalculate_skill_metrics.py:108`, `postprocessing_maintenance_long_term.py:66`). The local copy
     `kyg_data_forecast_tools/config/.env_kghm_server` has no such line, so the local copies do not
     answer this.
   - **Look for rows that already carry the new operational key** (QUARTER, LR_BASE/LR_SM, `date` = the
     first new issue date, calendar window, hv = config lead). They exist locally: the local DB holds
     hv0 LR_BASE and LR_SM rows dated 2026-10-01 with window 2026-10-01..2026-12-31, flag 0, plus
     ensembles for the same key (checked 2026-09-26). `long_forecasts` has no timestamp column, so such
     rows cannot be told apart from a new run by age. If they exist on the server:
     - a cron run upserts over them (the natural key has no `flag`), so the read-back needs the snapshot
       in step 5;
     - `lt_recovery` refuses the date (`lt_recovery.py:682-689`). For the tjhm 2026-10-01 rows, removal
       is owned by the decision-F step in PP-064 Chunk C; run it before any recovery of that date. Any
       other such rows need the same kind of reviewed step with the service owner.
   - The local copies (Dropbox, read 2026-09-26) have `forecast_months [3..9]` for both orgs, issue day 1 /
     lead 0 (tjhm) and 25 / 1 (kghm).
3. **Owner approval** of the exact change per org, noted in the PR or in this file.

**Files** (outside this repo: the deployment data repos, which are not git repositories, are shared via
Dropbox, and are deployed at `/data/<org>_data_forecast_tools`):

| Org | Files (both must change) | New `forecast_months` |
|---|---|---|
| tjhm | `quarter/Base/LR_Base/general_config.json`, `quarter/SnowMapper/LR_SM/general_config.json` | `[1, 4, 7, 10]` |
| kghm | the same two files | `[3, 6, 9, 12]` |

Do not change the mode JSONs, the cron lines or the issue days.

**Steps**
1. **Edit outside the LT cron window** (never on an LT cron day before that run has finished). Back up
   both files on the server **and** in the Dropbox master (`cp -p … .bak.<date>`).
2. Edit the server live copy and the Dropbox master the same way.
3. **Check both files directly.** Activation is an OR over models, so one wrong file hides behind the
   other at the scheduler and then fails at the execution gate:
   ```bash
   grep -H forecast_months /data/<org>_data_forecast_tools/config/models_and_scalers/long_term_forecasting/quarter/*/*/general_config.json
   ```
   Validate each file with `python3 -m json.tool <file> > /dev/null`.
4. **Offline schedule check** (read-only, stdlib only, host `python3`). Do **not** hand-build a
   `docker run` of `lt_schedule_query.py` (`doc/prod/long_term_recovery_runbook.md:96-102`) and do not go
   through Luigi (`LTScheduleQuery` deletes `lt_schedule_result.json`, `apps/pipeline/pipeline_docker.py:2140-2141`).
   Loading configs through pipeline code also rewrites them. The script below mirrors
   `lt_schedule_query.py:100-121` (`day_distance`, `nearest_scheduled_issue_date`, tolerance 10). Run it
   **before** the edit with the proposed months as the third argument, and **after** the edit without it.
   <details><summary>lt_quarter_schedule_check.py (checked 2026-09-26 against both local configs)</summary>

   ```python
   # Usage: python3 lt_quarter_schedule_check.py <config_dir> <cron_days e.g. 10,25> [months e.g. 3,6,9,12]
   # <config_dir> = the host directory named by ieasyhydroforecast_configuration_path.
   import calendar, datetime as dt, json, os, sys
   TOL = 10  # lt_schedule_query.ISSUE_DAY_TOLERANCE
   def day_distance(dom, issue_day):
       diff = abs(dom - issue_day)
       return min(diff, 30 - diff)
   def nearest(today, issue_day, months):  # lt_utils.nearest_scheduled_issue_date
       cands = []
       for d in range(-6, 7):
           y, m = today.year, today.month + d
           while m < 1: m += 12; y -= 1
           while m > 12: m -= 12; y += 1
           if m in months:
               cands.append(dt.date(y, m, min(issue_day, calendar.monthrange(y, m)[1])))
       return min(cands, key=lambda c: abs((today - c).days))
   cfg_dir, cron_days = sys.argv[1], [int(x) for x in sys.argv[2].split(",")]
   mode = json.load(open(os.path.join(cfg_dir, "long_term_configs", "quarter.json")))
   issue_day = int(mode["operational_issue_day"])
   months = {}
   for family, names in mode["models_to_use"].items():
       for name in names:
           p = os.path.join(cfg_dir, mode["model_folder"], family, name, "general_config.json")
           months[name] = json.load(open(p)).get("forecast_months", list(range(1, 13)))
   if len(sys.argv) > 3:
       months = {k: [int(x) for x in sys.argv[3].split(",")] for k in months}
   print("issue_day", issue_day, "forecast_months", months)
   for i in range(12):  # 2026-10 .. 2027-09
       y, m = 2026 + (9 + i) // 12, (9 + i) % 12 + 1
       for day in cron_days:
           t = dt.date(y, m, day)
           if day_distance(day, issue_day) <= TOL and any(
               not ms or ms == list(range(1, 13)) or abs((t - nearest(t, issue_day, ms)).days) <= TOL
               for ms in months.values()):
               print("ACTIVE", t)
   ```
   </details>

   Expected result (only cron days are evaluated):

   | Org | Cron days probed | `quarter` active on | Inactive on |
   |---|---|---|---|
   | tjhm | the 1st of each month, 2026-10 … 2027-09 | 2026-10-01, 2027-01-01, 2027-04-01, 2027-07-01 | the other eight 1sts |
   | kghm | the 10th and 25th of each month, 2026-10 … 2027-09 | 2026-12-25, 2027-03-25, 2027-06-25, 2027-09-25 | every 10th, and the other eight 25ths |

5. **Read-back around the first run** (aggregate counts only):
   - **The day before**, snapshot the target key per model: row count, flag counts, and a private value
     hash, e.g. `md5(string_agg(concat_ws('|', code, q, q05, q95, flag), ',' ORDER BY code))`. Keep the hash
     private.
   - **An unchanged hash does not prove the run failed to write.** The service skips upserts whose values
     are unchanged (`sapphire/services/postprocessing/app/crud.py:132-139`), so a run that reproduces
     the stored values leaves rows and hash as they were. Prove the write instead:
     - compare the DB rows at the target key with that run's own CSV output
       (`<model>_forecast.csv`, overwritten per run, `lt_utils.py:492-500`), privately;
     - check the run's write acknowledgements in the LT log: `Successfully wrote <n> long-term forecast
       records to DB` (`lt_utils.py:436-439`) and `DB save successful for <model>` (`:597`), with no
       `DB save failed for <model>` (`:599`).
   - tjhm after 2026-10-01: for **both** LR_Base and LR_SM, QUARTER rows with `date` 2026-10-01,
     `valid_from` 2026-10-01, `valid_to` 2026-12-31, `horizon_value` 0, a non-null forecast and **flag 0,
     or flag 1 if the date was recovered** (`RECOVERY_FLAG`, `lt_recovery.py:99, 728`). Report the station
     count per model and flag. Flag-2 rows (no prediction) need an explanation.
   - kghm after 2026-12-25: the same, with `date` 2026-12-25, window 2027-01-01..2027-03-31, `horizon_value` 1.
   - **Postprocessing follow-up:** after the first run (or a recovered tjhm Oct-1 run), confirm that the
     quarter Naive Mean at hv = config lead was produced for that issue, and the Skilled Mean where it
     forms (it can legitimately fail to form). There is no quarterly EM after PP-065 (round-2 decision 1).
     The quarterly gap detector keys on EM by default today
     (`apps/postprocessing_forecasts/src/gap_detector.py:370-389`) and treats any existing ensemble row
     for a (year, quarter, code[, hv]) as complete, so a pre-existing row hides a missing regeneration.
     PP-065 moves the quarter callers to Naive Mean.
6. **Re-grep both copies** (server and Dropbox) after the Dropbox sync and again after the first run. A
   local run on a synced machine rewrites these files (`config_forecast.py:157-180`) and can write old
   content back; Dropbox conflicted copies already exist in the data repos.

**Rollback**: restore the `.bak` files on both copies. If the tjhm Oct 1 run is missed, recover it with
`lt_recovery` before 2026-11-30 (`lt_recovery.py:282-306`), after the pre-existing rows are cleared
(gate step 2; for tjhm 2026-10-01 that is the decision-F step in PP-064 Chunk C).

### P1 — Lock the calendar schedule with additive tests (code agent)

These are **lock tests**. They pass on trunk by design and prove the code handles the new month lists.
They do **not** prove the deployed config (only P0 steps 3–6 do).

**Files (only these may be modified)**:
- `apps/long_term_forecasting/tests/test_lt_utils.py`
- `apps/long_term_forecasting/tests/test_lt_schedule_query.py`
- `apps/long_term_forecasting/tests/test_post_process_lt_forecast.py`
- optional: `apps/long_term_forecasting/tests/test_lt_recovery.py` (test 5 only)

**Agent instruction**: *"Do NOT change any existing function signatures, data flow logic, or control
flow. Your changes must be purely additive or modify only the specific behavior described."* Tests only;
use station code `19999`.

**Tests to add**
1. `adjust_forecast_dates_dynamic` (parametrised; next to `TestAdjustForecastDatesDynamic`,
   `tests/test_post_process_lt_forecast.py:1260`):
   - (2026-12-25, lead 1) → 2027-01-01..2027-03-31
   - (2026-10-01, lead 0) → 2026-10-01..2026-12-31
   - (2027-01-01, lead 0) → 2027-01-01..2027-03-31
   - (2027-12-25, lead 1) → 2028-01-01..2028-03-31 (leap year; `valid_to` = Mar 31)
2. `nearest_scheduled_issue_date` (parametrised, exact expected dates):
   - `[3,6,9,12]`, day 25: 2026-12-20 → 2026-12-25; 2027-01-10 → 2026-12-25; 2026-10-25 → 2026-09-25
   - `[1,4,7,10]`, day 1: 2026-12-30 → 2027-01-01; 2026-10-01 → 2026-10-01
3. `check_valid_forecast_issue_date` (next to `TestCheckValidForecastIssueDate`, `tests/test_lt_utils.py:81`,
   reusing its `_make_mock_config`). **Freeze both clocks**: patch `lt_utils.get_today` and
   `pd.Timestamp.now` (`patch.object(pd.Timestamp, "now", return_value=...)`), because `lt_utils.py:184`
   asserts `today <= now` and these dates are in the future:
   - kghm-like, day 25, `[3,6,9,12]`: 2026-12-25 → 2026-12-25; 2026-11-25 → None.
   - tjhm-like, day 1, `[1,4,7,10]`: 2026-10-01 → 2026-10-01; 2026-09-01 → None.
4. `query_schedule` with the existing `make_mock_config` helper (`test_lt_schedule_query.py:51`), one
   quarter mode:
   - kghm-like (day 25, `[3,6,9,12]` for both models): active on 2026-12-25 and 2026-06-25; skipped on
     2026-05-25, with a reason containing "no models scheduled".
   - tjhm-like (day 1, `[1,4,7,10]`): active on 2026-10-01 and 2027-01-01; skipped on 2026-09-01.
   - One model on `[3,6,9,12]` and the other on `[3..9]`: active on 2026-05-25. This documents the OR
     semantics that P0 step 3 guards against.
5. Optional: `resolve_scheduled_models` with the `FakeConfig` of `tests/test_lt_recovery.py`
   (`forecast_months` is a dict per model), issue day 1, both models on `[1,4,7,10]`: 2026-10-01 → both
   models.

**Acceptance**: the new tests pass; `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh long_term_forecasting`
gives zero failures and zero unexpected skips; `git diff --stat` shows only the files above.

### P2 — Hindcasts for the new issue months (modeller + ops; blocked on overview decision D2)

**Goal**: history for Q1 (both orgs) and tjhm Q4, i.e. hindcast rows for kghm issue month 12 and tjhm
issue months 1 and 10. Today's hindcast CSVs (`<data>/intermediate_data/long_term_predictions/quarter/`)
contain only issue months 3–9.

**Why the plain command is unsafe**:
`lt_forecast_mode=quarter python calibrate_and_hindcast.py --models LR_Base LR_SM`
1. **It covers every configured month and every year with features, not only the missing history.**
   - It hindcasts all of `forecast_months` (`LINEAR_REGRESSION.py:538-543`) for every year in the data
     (LOOCV over all years, `:571-578, 618`) and saves the whole result (`calibrate_and_hindcast.py:268`).
   - The `long_forecasts` natural key has no `flag` (`sapphire/services/postprocessing/app/models.py:193-202`,
     colleague-owned). Every existing row with the same `(code, date, model, window, hv)` is overwritten
     and flipped to flag 1. This includes:
     - current-year operational rows of the retained months (kghm Mar/Jun/Sep, tjhm Apr/Jul);
     - **current-year rows of the new issue months** (e.g. tjhm 2026-10-01 once P0 has run), including
       recovered flag-1 rows, which then cannot be told apart from hindcasts;
     - the local DB's hv0 LR rows dated Jan 1 and Oct 1 for ~27 years (provenance unknown, likely persisted
       postprocessing rows, see PP-064 population (c)).
2. **The CSV is fully overwritten** (`lt_utils.save_forecast_to_csv`, `lt_utils.py:500`). Operational runs
   append into the same `<model>_hindcast.csv` (`append_forecast_to_hindcast`, `lt_utils.py:509-543`), and
   `bin/initialize_long_forecast_history.sh` rebuilds DB history from it.
3. **CSV success is reported even if the DB write failed** (`lt_utils.py:623`).
4. **`bin/initialize_long_forecast_history.sh` cannot import the new rows into a populated DB.** It
   chooses `MODE=full-import` only when the target has no rows for the group (`:130`, `:495-618`);
   otherwise it runs pre-cutoff, which drops rows dated on or after the earliest existing date per
   (horizon, hv, code) (`bin/utils/migration_py/long_forecast.py:513-515`).

**Decision D2 (modeller + owner): choose the write set.** Options:
- (a) Run against a **scratch config copy** restricted to the new months (`ieasyhydroforecast_configuration_path`
  pointing at the copy) and a **scratch output path** (`ieasyhydroforecast_ml_long_term_output_path`,
  `config_forecast.py:50-52, 205-206`). Never restrict the live `forecast_months`: that truncates the live
  CSV and changes the operational schedule while it is in place. The DB write still happens, so steps
  1–4 apply.
- (b) CSV only, then import a filtered set. CSV-only needs `SAPPHIRE_API_ENABLED=false` (`lt_utils.py:405`).
  The importer has no issue-month filter, so filter the CSV to the new issue months under a scratch
  `data_root` and import it in full-import mode. The migrator runs full-import when it gets neither
  `--cutoff` nor `--cutoff-map` (`long_forecast.py:736`); the wrapper does that only on an empty target,
  so the exact invocation is a D2 deliverable and must be run with `--dry-run` first.
- (c) Set a historical cutoff before the first operational issue.

**Steps (after D2)**
1. Per org, **before** any postprocessing run on that day, record the counts of QUARTER LR rows by issue
   month, year, flag and hv for every issue month the run will write, **including the current year of the
   new issue months**. Save a private export of the natural keys and values of rows that must be
   preserved: all operational (flag 0) and recovered (flag 1 from `lt_recovery`) rows.
2. Keep a copy of the live `<model>_hindcast.csv`.
3. Run with the chosen write set.
4. Read back from the DB (not the CSV).

**Acceptance**:
- **Expected grid**, defined before the run: (station, year) pairs for kghm issue 25 Dec (→ Jan–Mar of the
  next year) and tjhm issues 1 Jan (→ Jan–Mar) and 1 Oct (→ Oct–Dec), for both models, with
  `horizon_value` = the config lead (kghm 1, tjhm 0).
- Each present key has `flag` 1 and a non-null prediction. A station-year without features produces
  **no row** (NaN predictions are not recorded, `LINEAR_REGRESSION.py:671`); flag 3 appears only when the
  whole `Q_<model>` column is absent (`calibrate_and_hindcast.py:237`; `:241` covers NaN rows, which the
  library does not emit). List the absent keys per model with the reason (aggregate).
- Preserved rows (step 1) are unchanged by natural key and value (compared privately).
- The post-P2 live `<model>_hindcast.csv` equals the old CSV plus the new-month rows, with no old row
  changed; under (a) or (b) the live CSV is untouched.
- **Only if D2 chooses a full rerun** that rewrites the retained months (not (a) or (b)): retained-month
  hindcasts match a before-and-after run on identical frozen inputs, not the old CSV (which mixes in
  operational rows). Under (a) or (b) the retained months are not rewritten, so this check does not apply.

## Out of scope

- Postprocessing selection and skill (PP-064, PP-065); dashboard (FD-029/FD-030); contract docs and the LT
  readme (DOC-009).
- A calendar-exact training target (overview D1).
- Deleting existing rolling-window rows (overview, stale rows), except the reviewed removal in P0 gate
  step 2 if pre-existing rows block recovery.
- The early-run window defect (LTF-015).
