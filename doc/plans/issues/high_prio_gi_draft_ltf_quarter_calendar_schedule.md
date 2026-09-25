# LTF-014: Issue the quarter forecast once per calendar quarter (Q1–Q4), not monthly Mar–Sep

**Status**: Draft (2026-09-25, rev 2 after out-of-loop review)
**Module**: `apps/long_term_forecasting` (tests only) + per-deployment data repos (config)
**Priority**: **High.** Target dates (soft):
- **tjhm Q4, issue 2026-10-01.** The current config skips it. It can be recovered with `lt_recovery`
  until 2026-11-30 once P0 is done (`apps/long_term_forecasting/lt_recovery.py:282-306`, `check_recovery_window`), so this is not a reason to skip the P0 gate.
- **kghm Q1, issue 2026-12-25.**

**Labels**: `long-term`, `quarter`, `config`, `deployment`
**Overview**: [`../quarter_calendar_product_plan.md`](../quarter_calendar_product_plan.md). The dependency
graph lives there only.
**Related**: PP-064, FD-029, DOC-009, LTF-015, LTF-007 (scheduler tolerance 10 vs execution gate 5),
INFRA-022 (validate_pipeline FAIL on gated days)

## Problem

Owner decision (2026-09-25): both hydromets issue the quarterly forecast for calendar quarters, once per
quarter, and Q1 is required. The deployed configs instead issue a rolling 3-month window every month
from March to September:

- Per-model configs in each data repo:
  `config/models_and_scalers/long_term_forecasting/quarter/{Base/LR_Base,SnowMapper/LR_SM}/general_config.json`
  → `"forecast_months": [3, 4, 5, 6, 7, 8, 9]` (kghm and tjhm alike).
- The scheduler admits the quarter mode in those months (`apps/long_term_forecasting/lt_schedule_query.py:107-121`,
  via `lt_utils.nearest_scheduled_issue_date`, `lt_utils.py:134-172`). A mode is active if **any** of its
  models is scheduled.
- The window **label** is issue month + lead, 3 months long (`post_process_lt_forecast.py:132-199`).

| Org | Issue day / lead (mode config `long_term_configs/quarter.json`) | Issued today | Calendar quarters among them | Missing |
|---|---|---|---|---|
| kghm | 25 / 1 | 25 Mar … 25 Sep | 25 Mar → Q2, 25 Jun → Q3, 25 Sep → Q4 | **Q1** (25 Dec) |
| tjhm | 1 / 0 | 1 Mar … 1 Sep | 1 Apr → Q2, 1 Jul → Q3 | **Q1** (1 Jan), **Q4** (1 Oct) |

## What the schedule change does and does not fix

**Enough for the schedule (verified 2026-09-25):**
- `forecast_months` is read only through `config_forecast.get_forecast_months` (`config_forecast.py:278-289`,
  default 1..12). Its consumers are the scheduler, `lt_recovery.py:310-357`, and the pinned `lt_forecasting`
  hindcast (`LINEAR_REGRESSION.py:538-565`; pin at `pyproject.toml:68`). `predict_operational` ignores it.
- No cron line, Luigi task, `bin/` wrapper or `run_locally.sh` path hard-codes quarter months. Cron is
  `0 6 ${LT_ISSUE_DAY} * *` every month (kghm `10,25`, tjhm `1`).
- The Dec→Jan **label** works: `adjust_forecast_dates_dynamic` puts `valid_from` in the next year
  (`post_process_lt_forecast.py:171-190`); a simulation gives 2026-12-25 lead 1 → 2027-01-01..2027-03-31.
- No retraining artefacts are involved. LR_Base/LR_SM are refitted on every operational run per station
  and issue month-day (`LINEAR_REGRESSION.py:385-521`), and `save_model`/`load_model` are no-ops (`:779-799`).
- `forecast_days` in `general_config.json` is overwritten from the mode JSON on every config load, and the
  file is **rewritten to disk** (`config_forecast.py:159-179`). `forecast_months` survives the rewrite.
  So edit the live server copy.

**Not fixed by the schedule change: the model target is a fixed 90-day window, relabelled as the quarter.**
- The mode configs set `prediction_horizon: 90` and `offset` 95 (kghm) / 90 (tjhm). The library builds
  the target as a 90-day rolling mean shifted by `offset` (`lt_forecasting` `FeatureExtractor.py:332-372`,
  lines ~362-366). `post_process_lt_forecast` then only rewrites the dates (`:716-741`).
- Examples:
  - kghm 25 Dec learns **31 Dec – 30 Mar** and is labelled 1 Jan – 31 Mar.
  - tjhm 1 Oct learns **2 Oct – 30 Dec**, labelled Oct–Dec (92 days).
  - A leap-year Q1 has 91 days.
- **This approximation already applies to today's Q2–Q4 issues** and to the monthly modes (see the kyg
  data repo `long_term_configs/README.md`).
- Whether to make the training target calendar-exact is a **modelling decision** (overview decision D1).
  It is library work (`lt_forecasting`), not part of this plan.
- Nothing in this plan claims the forecast value is an exact calendar-quarter mean.

## Plan

### P0 — Change the schedule in the data repos (ops, no code; owner/ops executes)

**Gate: do not edit any config before all three are done.**
1. **Modeller confirmation (Sandro).**
   - Why `forecast_months` was set to `[3..9]`. No reason is recorded anywhere (history checked
     2026-09-25), and his own evaluation script lists Mar–Jun issues only.
   - Whether LR_Base and LR_SM are valid for the winter issues this change adds:
     - kghm 25 Dec → Q1;
     - tjhm 1 Jan → Q1 and 1 Oct → Q4.

     LR_SM depends on snow data. These issue months have never run operationally.
   - Record his answer in this file.
2. **Server state read, not assumed.**
   - On each server, read the two `general_config.json` files and `long_term_configs/quarter.json`, and
     diff them against the Dropbox copies.
   - Record the result (aggregate description only).
   - The local copies (Dropbox, read 2026-09-25) have `forecast_months [3..9]` for both orgs, issue day 1 /
     lead 0 (tjhm) and 25 / 1 (kghm).
   - File mtimes are not evidence: the pipeline rewrites these files on every config load.
3. **Owner approval** of the exact change per org, noted in the PR or in this file.

**Files** (outside this repo: the deployment data repos, which are not git repositories, are shared via
Dropbox, and are deployed at `/data/<org>_data_forecast_tools`):

| Org | Files (both must change) | New `forecast_months` |
|---|---|---|
| tjhm | `quarter/Base/LR_Base/general_config.json`, `quarter/SnowMapper/LR_SM/general_config.json` | `[1, 4, 7, 10]` |
| kghm | the same two files | `[3, 6, 9, 12]` |

Do not change the mode JSONs, the cron lines or the issue days.

**Steps**
1. Back up both files on the server (`cp -p … .bak.2026-09-25`).
2. Edit the server live copy and the Dropbox master the same way.
3. **Check both files directly.** Activation is an OR over models, so one wrong file can hide behind the other:
   ```bash
   grep -H forecast_months /data/<org>_data_forecast_tools/config/models_and_scalers/long_term_forecasting/quarter/*/*/general_config.json
   ```
4. **Schedule smoke check** at every cron day of the next 12 months. Note that loading a mode config
   rewrites the `general_config.json` files with the same content (step 3 guards the result). Run it the
   way `pipeline_docker.py` `LTScheduleQuery` does, so the deployment env is loaded:
   `python lt_schedule_query.py --today <date>` prints JSON with `active_modes`.

   Expected result (the tolerance is ±10 days, `lt_schedule_query.py:52`; only cron days matter):

   | Org | Cron days probed | `quarter` active on | Inactive on |
   |---|---|---|---|
   | tjhm | the 1st of each month, 2026-10 … 2027-09 | 2026-10-01, 2027-01-01, 2027-04-01, 2027-07-01 | the other eight 1sts |
   | kghm | the 10th and 25th of each month, 2026-10 … 2027-09 | 2026-12-25, 2027-03-25, 2027-06-25, 2027-09-25 | every 10th, and the other eight 25ths |

   Probing a non-cron date (e.g. 2027-01-01 for kghm) can report quarter as active, because of the tolerance.
   That is expected and harmless; do not probe non-cron dates.
5. **Read-back after the first run** (aggregate counts only):
   - tjhm after 2026-10-01: for **both** LR_Base and LR_SM, QUARTER rows with `date` 2026-10-01,
     `valid_from` 2026-10-01, `valid_to` 2026-12-31, `horizon_value` 0, **flag 0 with a non-null forecast**.
     Report the station count per model and flag. Flag-2 rows (no prediction) need an explanation.
   - kghm after 2026-12-25: the same, with `date` 2026-12-25, window 2027-01-01..2027-03-31, `horizon_value` 1.

**Rollback**: restore the `.bak` files. If the tjhm Oct 1 run is missed, recover it with `lt_recovery`
before 2026-11-30 (`lt_recovery.py:282-306`).

### P1 — Lock the calendar schedule with additive tests (code agent)

These are **lock tests**. They pass on trunk by design and prove the code handles the new month lists.
They do **not** prove the deployed config (only P0 steps 3–5 do).

**Files (only these may be modified)**:
- `apps/long_term_forecasting/tests/test_lt_utils.py`
- `apps/long_term_forecasting/tests/test_lt_schedule_query.py`
- `apps/long_term_forecasting/tests/test_post_process_lt_forecast.py`

**Agent instruction**: *"Do NOT change any existing function signatures, data flow logic, or control
flow. Your changes must be purely additive or modify only the specific behavior described."* Tests only;
use station code `19999`.

**Tests to add**
1. `adjust_forecast_dates_dynamic` (parametrised):
   - (2026-12-25, lead 1) → 2027-01-01..2027-03-31
   - (2026-10-01, lead 0) → 2026-10-01..2026-12-31
   - (2027-01-01, lead 0) → 2027-01-01..2027-03-31
   - (2027-12-25, lead 1) → 2028-01-01..2028-03-31 (leap year; `valid_to` = Mar 31)
2. `nearest_scheduled_issue_date` (parametrised, exact expected dates):
   - `[3,6,9,12]`, day 25: 2026-12-20 → 2026-12-25; 2027-01-10 → 2026-12-25; 2026-10-25 → 2026-09-25
   - `[1,4,7,10]`, day 1: 2026-12-30 → 2027-01-01; 2026-10-01 → 2026-10-01
3. `query_schedule` with the existing `make_mock_config` helper (`test_lt_schedule_query.py:51`), one
   quarter mode:
   - kghm-like (day 25, `[3,6,9,12]` for both models): active on 2026-12-25 and 2026-06-25; skipped on
     2026-05-25, with a reason containing "no models scheduled".
   - tjhm-like (day 1, `[1,4,7,10]`): active on 2026-10-01 and 2027-01-01; skipped on 2026-09-01.
   - One model on `[3,6,9,12]` and the other on `[3..9]`: active on 2026-05-25. This documents the OR
     semantics that P0 step 3 guards against.

**Acceptance**: the new tests pass; `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh long_term_forecasting`
gives zero failures and zero unexpected skips; `git diff --stat` shows only the three test files.

### P2 — Hindcasts for the new issue months (modeller + ops; blocked on overview decision D2)

**Goal**: history for Q1 (both orgs) and tjhm Q4, i.e. hindcast rows for kghm issue month 12 and tjhm
issue months 1 and 10. Today's hindcast CSVs (`<data>/intermediate_data/long_term_predictions/quarter/`)
contain only issue months 3–9.

**Why the plain command is unsafe**:
`lt_forecast_mode=quarter python calibrate_and_hindcast.py --models LR_Base LR_SM`
1. **It covers every configured month, not only the new ones.**
   - It hindcasts all of `forecast_months` (`LINEAR_REGRESSION.py:538-565`) and saves the whole result
     (`calibrate_and_hindcast.py:268`).
   - The `long_forecasts` natural key has no `flag` (`sapphire/services/postprocessing/app/models.py:193-202`,
     colleague-owned). So every existing operational row with the same `(code, date, model, window, hv)`
     is overwritten and flipped to flag 1. This includes current-year Mar/Jun/Sep (kghm) and Apr/Jul (tjhm).
   - The local DB also holds hv0 LR rows dated Jan 1 and Oct 1 for ~27 years (provenance unknown, likely
     persisted postprocessing rows, see PP-064 population (c)). A tjhm hindcast overwrites all of them.
2. **The CSV is fully overwritten** (`lt_utils.save_forecast_to_csv`, ~`:500`). Operational rows had been
   appended into it (`lt_utils.py:539`).
3. **CSV success is reported even if the DB write failed** (`lt_utils.py:623`). Missing predictions are
   written with flag 3 (`calibrate_and_hindcast.py:229`).
4. **`bin/initialize_long_forecast_history.sh` cannot import the new rows into a populated DB.** Its
   pre-cutoff mode keeps only rows older than the earliest date present (`bin/utils/migration_py/long_forecast.py:513-515`).

**Decision D2 (modeller + owner): choose the write set.** Options:
- (a) run with `forecast_months` temporarily restricted to the new months only;
- (b) run to CSV only, then import a filtered set;
- (c) set a historical cutoff before the first operational issue.

Whatever is chosen, the steps below apply.

**Steps (after D2)**
1. Per org, **before** any postprocessing run on that day, record the counts of QUARTER LR rows by issue
   month, year, flag and hv for every issue month the run will write. Save a private export of the
   natural keys and values of rows that must be preserved.
2. Keep a copy of the old CSV.
3. Run with the chosen write set.
4. Read back from the DB (not the CSV).

**Acceptance**:
- For each station-year with sufficient input data, QUARTER rows exist for kghm issue 25 Dec (→ Jan–Mar of
  the next year) and tjhm issues 1 Jan (→ Jan–Mar) and 1 Oct (→ Oct–Dec), for both models. Each row has:
  - `horizon_value` = the config lead (kghm 1, tjhm 0);
  - `flag` 1 (hindcast) and a non-null prediction, or `flag` 3 with the station-year listed as missing. Station-years
  without data are listed with the reason (aggregate).
- Preserved rows are unchanged by natural key and value (compared privately).
- Retained-month hindcasts match a **before-and-after run on identical frozen inputs**, not the old CSV
  (which mixes in operational rows).

## Out of scope

- Postprocessing selection and skill (PP-064); dashboard (FD-029/FD-030); contract docs and the LT readme
  (DOC-009).
- A calendar-exact training target (overview D1).
- Deleting existing rolling-window rows (overview, stale rows).
- The early-run window defect (LTF-015).
