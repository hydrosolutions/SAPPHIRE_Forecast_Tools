# LTF-016: Monthly rows written before February 2026 carry offset windows or the wrong January year

**Status**: Draft (2026-09-26, rev 6 after the fourth review round). This is the follow-up to decision A of
the calendar-quarter plan set.
**Module**: `apps/long_term_forecasting` (verification only; **no producer change**).
The data fixes are in `long_forecasts` and need the postprocessing service owner.
**Priority**: High. The stale rows mis-score **monthly** skill for both orgs today, independent of the
quarter work.
**Labels**: `long-term`, `bug`, `data-quality`, `monthly`
**Overview**: [`../quarter_calendar_product_plan.md`](../quarter_calendar_product_plan.md)
**Found**: 2026-09-26 by the out-of-loop review of PP-065 (two reviewers independently). The producer
status and the counts were corrected in the second review round. Sources are the local dev DB and the
Dropbox CSVs; aggregate counts only.
**Related**:
- PP-065 matches monthly rows by (issue date, `horizon_value`) precisely so that it tolerates both defects.
  P2's delete-and-regenerate changes derived inputs; the quarterly recalc follows.
- LTF-014 P2: its hindcast write-set hazards apply to the regeneration in P2 below.
- LTF-017: a separate, **live** producer bug in the GBT-family bounds (raw window month).
- PP-041: other stale `long_forecasts` rows.
- Memory/prior finding "taj raw valid_from" ("Finding B"): the prod taj MONTH rows were 100 % snapped on
  2026-07-10.

## The producer is already fixed; the stored rows are stale

- **Defect 1 is fixed since `fdfb6ae1` (2026-02-04).** Hindcasts are filtered to the configured issue day
  and post-processed to calendar months (`apps/long_term_forecasting/calibrate_and_hindcast.py:250-254`).
- **Defect 2 is fixed since `99c5a552` (2026-02-06).** The mapping to calendar months is model-agnostic:
  - the target year is the issue year + 1 when the target month wraps
    (`post_process_lt_forecast.py:464-469`);
  - `valid_from`/`valid_to` are built from that year (`:482-490`);
  - leave-one-out climatology uses the same target year (`:752-757`).

  `tests/test_post_process_lt_forecast.py:377-425` (`test_month_overflow`: Nov 25, lead 2 → 2021-01-01)
  locks it.
- What remains is data: rows written before these commits were never regenerated or re-imported.

## Defect 1: raw offset windows (hindcasts before `fdfb6ae1`)

- **Shape:**
  - `valid_from`/`valid_to` hold the model's raw 30-day window, e.g. the 2015-01-01 issue has windows
    01-02..02-01, 02-01..03-03 and 03-03..04-02;
  - a raw window can **start on day 1 and still end wrong**: tjhm month_2 from a Jan-1 issue is raw
    02-01..03-03 (`valid_from` = issue + 1 + (offset − horizon), `valid_to` = `valid_from` + horizon;
    pinned `lt_forecasting/forecast_models/SciRegressor.py:1145-1155`), while current post-processing
    gives 02-01..02-28 (`post_process_lt_forecast.py:483-490`). A "`valid_from` on day 1" test misses it;
  - issue dates fall on days 1, 5, 10, 15, 20 and 25, not only on the configured issue day.
- **Not tjhm-only.** Unsnapped MONTH rows (`valid_from` not on day 1) in the local DB, 2026-09-26 review
  count:
  - kghm: 18,758 flag-1 and 1,180 flag-0 rows;
  - tjhm: 497,612 of ~522k flag-1 rows and 18,909 of 25,510 flag-0 rows. These are mostly EM, Skilled Mean
    and Naive Mean rows built from raw members.
- **Source files.** The tjhm Dropbox hindcast CSVs are still about 90 % unsnapped (month_2 90 %, month_3
  89 %, checked 2026-09-26). The kghm CSVs are 0 % unsnapped. The from-file importer copies windows
  verbatim (`bin/utils/migration_py/long_forecast.py:310`, `_build_record`).
- **Prod hazard.** Prod tjhm was 100 % snapped on 2026-07-10. Any import of the raw tjhm CSVs would
  bring defect 1 into prod:
  - LTF-014 P2 option (b), if it imports an unfiltered CSV;
  - `bin/initialize_long_forecast_history.sh` on an empty target;
  - a DB reset, which re-runs the long-forecast import (`bin/reset_sapphire_db.sh:530`).
- **Impact:**
  - **Flag OFF.** Consumers that key on the `valid_from` month can mis-attribute a month when the raw
    window starts in the previous month. These consumers are monthly skill pairing via `data_reader` and
    `forecast_skill_eval`'s alignment check. They can also pair non-issue-day rows as if they were
    operational.
  - **Flag ON.** `select_operational_issuances` keeps only rows whose derived lead and `date.day` match a
    configured schedule (`apps/postprocessing_forecasts/src/data_reader.py:346-353`), so off-day rows
    drop.
    - An issue-day row whose raw window starts one month early derives a lead one lower. It can match
      another mode's schedule: a tjhm month_3 Jul-1 row with raw start 08-31 derives lead 1, which is
      month_2's (1, 1).
    - It then competes with the genuine row for the same selection key (`:383-391`, where the last row
      wins).
    - This is expected from the code, not measured. P0 counts it.
  - **PP-065** tolerates the defect, but P2's delete-and-regenerate changes its inputs (see Related).

## Defect 2: kghm GBT, SM_GBT and SM_GBT_NORM label January targets with the issue year (before `99c5a552`)

- **Shape and scope:**
  - For issues on Oct 25 (hv 3), Nov 25 (hv 2) and Dec 25 (hv 1), the January row has `valid_from` in the
    **issue** year.
  - About 940–1030 flag-1 rows per model per issue month; GBT has 1030 for Oct issues (local DB).
  - There are also flag-0 rows issued 2025-12-25, so operational runs before 2026-02-06 are affected too.
  - LR_Base/LR_SM and the other models are correct.
- **The current kghm Dropbox CSVs are correct.** The month_1–3 GBT, SM_GBT and SM_GBT_Norm hindcast CSVs
  have 0 wrong-year labels among their 1,004 Oct–Dec-issued January rows each (checked 2026-09-26). The DB
  rows are stale: the GBT family was not re-imported after the fix.
- **Ensembles already hold both year versions** (local DB): Naive Mean has 161–171 keys per issue month,
  EM and Skilled Mean 1–14.
- **Impact depends on the flag:**
  - **Flag ON.** `select_operational_issuances` derives lead −9 (Oct), −10 (Nov) or −11 (Dec) for these
    rows and drops them (`data_reader.py:346-353`). January pairs of the three models are **lost**, not
    mis-scored.
  - **Flag OFF.** Monthly skill for January scores these models against the wrong year's observations.
    - Naive Mean inherits the error, and so do the Skilled Mean and EM where these models qualify.
    - The dashboard may show a January forecast under the wrong year.

## Plan

**P0 — measure, read-only, per server.** Aggregate counts only.
- **Full calendar-window contract audit** (both defects). A MONTH row passes only if all hold, with
  `hv` = the stored `horizon_value`:
  - **window clauses:**
    - target month and year = `date` month + `hv` (year-aware, as `post_process_lt_forecast.py:450-469`);
    - `valid_from` = day 1 of that target month and year;
    - `valid_to` = the last day of that target month;
  - **schedule clause:** `date.day` = the mode's configured `operational_issue_day`.

  Count failures by org × flag × model × failed clause. "Zero rows with `valid_from` not on day 1" is not
  proof: it misses the day-1-start / wrong-end rows above.
- **Split the failing rows into two disjoint populations.** The manifest (P2) uses the same split.
  - **Remediation population (proven stale-label defects):**
    - every row that fails a window clause;
    - every hindcast-flag row (flag 1 or 3, `calibrate_and_hindcast.py:237-242`) that fails only the
      schedule clause. The current hindcast keeps issue-day rows only (`calibrate_and_hindcast.py:250-254`),
      and recovery (flags 1/2) writes only the exact scheduled date (`lt_recovery.py:329-347`), so such a
      row predates `fdfb6ae1`;
    - the ensemble rows at those keys.
  - **Off-schedule rows (preserved by default):** operational-flag rows (flag 0 or 2,
    `run_forecast.py:368-378`) that pass every window clause and fail only the schedule clause, plus the
    ensemble rows at their keys. The current producer writes them: a run up to 5 days before the scheduled issue
    date is accepted and dated **today** (`lt_utils.py:194-228`; window `:202-209`, warning `:219-227`),
    and LTF-015 keeps accepting same-month early runs. Report them per org × model, split into:
    - same calendar month as the scheduled issue date: permitted current-producer early runs; kept;
    - previous calendar month (issue days 1–5): the LTF-015 defect, with corrupted values as well as
      labels. Also kept by default; deleting them is a separate owner decision;
    - any other: listed for the owner, kept by default.
- **Defect 1:** MONTH rows by org × flag × model × (`valid_from` on day 1?) × (`date.day` = the mode's
  issue day?). Count rows with `date.day` ≠ the issue day separately: flag ON never selects them
  (`data_reader.py:346-353`), but the flag-0/2 ones with a correct window are off-schedule rows (above),
  not defects.
- **Defect 1, flag-ON collision:** issue-day MONTH rows whose lead derived from `date` and `valid_from`
  differs from the stored `horizon_value`.
- **Defect 2:** MONTH rows by org × model × issue month where the target month (issue month + hv) is
  January but the `valid_from` year equals the issue year.

**P1 — verify the producer (ops).** No producer change and no code change.
- **Deployed LT image contains both fixes.** Image tags carry no git revision (`IMAGE_TAG: latest`,
  `.github/workflows/deploy_production.yml:4`), so check the code in the image, not the tag:
  - `/app/apps/long_term_forecasting/post_process_lt_forecast.py` builds `valid_from` from
    `mapped_data['target_year']`;
  - `calibrate_and_hindcast.py` filters on `forecast_issue_day`.

  For example, `docker run --rm --entrypoint grep <LT image> -n target_year <file>`.
- **Server CSV label counts.** Run the P0 defect-1 and defect-2 counts on each server's
  `<data>/intermediate_data/long_term_predictions/month_*/<model>/<model>_hindcast.csv`. Expect 0
  wrong-year rows for kghm. The tjhm CSVs are expected to be unsnapped; P2 regenerates them.
- **No new lock test.** The rollover is already locked by `test_month_overflow`
  (`apps/long_term_forecasting/tests/test_post_process_lt_forecast.py:377-425`). Adding one would also
  collide with LTF-014 P1, which edits the same test file.

**P2 — data fix: re-import and delete, never relabel or snap in place.** The owner and the service owner
run it; it is a one-way step.
- **Pause every writer for the whole import → delete → recalc window** (ops instruction, not code):
  - the MONTH producers: LT cron runs, manual `run_forecast.py` runs and `lt_recovery`;
  - postprocessing operational runs;
  - long-term maintenance (`LongTermPostProcessingMaintenance`,
    `apps/pipeline/pipeline_docker.py:1946-1972`; `run_maintenance_postprocessing_long_term` in
    `apps/run_locally.sh`, e.g. `:1745-1748`);
  - recalc runs other than the ones in the steps below, and any manual writer;
  - if PP-065 is live, the quarterly consumers too: they derive quarters from these MONTH rows.

  Wait for running jobs to finish before the backup. Resume only after the post-checks pass.
- **Before anything else:** a per-org backup of the MONTH `long_forecasts` and `skill_metrics` rows
  (`pg_dump`/`COPY`), kept out of the repo.
- **Manifest before any delete** (private, not in the repo): the full natural key of every row to delete
  (`horizon_type`, `horizon_value`, `code`, `date`, `model_type`, `valid_from`, `valid_to`;
  `sapphire/services/postprocessing/app/models.py:193-202`), each paired with its corrected counterpart
  (the key the regenerated or re-imported row carries, or "none" with a reason). Deletes run only on keys
  in the manifest; a key without a counterpart needs owner sign-off. Only remediation-population rows (P0)
  enter the manifest. Off-schedule rows go into the preserve set unless the owner signs off a subset.
- **Import mechanism for both defects:**
  - run the migrator in full-import mode (no `--cutoff`/`--cutoff-map`, `long_forecast.py:736`) with
    `--mode`/`--model`;
  - point it at a CSV filtered to the affected keys under a scratch `data_root`;
  - run it with `--dry-run` first, then read back.

  `bin/initialize_long_forecast_history.sh` imports nothing on a populated DB
  (`long_forecast.py:513-515`).
- **Defect 2 (kghm):**
  1. Re-import the correct-year January rows of GBT, SM_GBT and SM_GBT_Norm (month_1–3) from the current
     CSVs, filtered to issue months 10–12 with a January target. Record the flag the imported
     2025-12-25 rows carry.
  2. Delete the wrong-year rows: MONTH rows of **any** model with issue month 10–12 and `valid_from` in
     January of the issue year. That covers the three members **and** the wrong-year Naive Mean,
     Skilled Mean and EM rows. Run a dry-run count first.
  3. Run the monthly recalc. It rewrites the ensemble rows at the keys it emits and the skill
     (`apps/postprocessing_forecasts/recalculate_skill_metrics.py:320-332`). Also run the quarterly recalc
     if PP-065 is live.
- **Defect 1 (both orgs, mostly tjhm):**
  1. **Regenerate** the tjhm hindcasts with current code. kghm needs no regeneration: its CSVs are already
     snapped, so only the filtered re-import applies. Filter it to the (code, `date`, model, hv) of the raw
     rows that step 2 deletes.
     - The plain `calibrate_and_hindcast.py` run overwrites the live CSV and upserts over operational rows.
       LTF-014 P2 "Why the plain command is unsafe" applies unchanged.
     - Choose the write set the same way: a scratch output path, `SAPPHIRE_API_ENABLED=false`
       (`lt_utils.py:405`), then a filtered import.
     - This is a modeller + owner decision.
  2. **Delete the raw rows:** the P0 remediation population. That is MONTH rows that fail **any** window
     clause (not only `valid_from` off day 1: the day-1-start / wrong-end rows too), flag-1/3 rows whose
     `date.day` is not the mode's issue day, and the ensemble rows at those keys. Only manifest keys. The
     off-schedule rows are **not** deleted by default.
     - Snapping in place is **not** allowed: it would give non-issue-day rows a calendar window and make
       them look operational.
     - The deletion also removes pre-fix flag-0 rows; their regenerated replacements carry flag 1. The
       owner accepts this or keeps a preserve list.
  3. Run the monthly recalc (as above) to regenerate the ensembles and the skill.
- **Post-checks:**
  - the P0 audit reports an empty remediation population (0 failures on every window clause, 0 flag-1/3
    schedule failures), and the defect-2 count is 0;
  - the preserved off-schedule rows are reported separately: the same count per org × model as before
    P2, and their raw rows unchanged by natural key and value (private compare against the backup);
  - a named day-1-start / wrong-end case is checked explicitly: tjhm month_2, Jan-1 issue, has
    `valid_to` on the last day of February, not 03-03;
  - every manifest key is gone and its counterpart present;
  - monthly skill row counts per org before and after (aggregate); flag ON: GBT-family January `n_pairs`
    rise.
- **Durable source publication** (after the post-checks pass). Without it, the next import from the
  unchanged source CSVs (the prod hazards above) brings the rows back. The importer reads
  `<data_root>/long_term_predictions/<mode>/<model>/<model>_hindcast.csv`
  (`bin/utils/migration_py/long_forecast.py:277-302`, `_discover_hindcast_csvs`).
  1. Preserve the operational and recovered appends: operational runs append to the live CSV
     (`append_forecast_to_hindcast`, `lt_utils.py:509-543`, dedup on (`date`, `code`) keeping the last),
     so take the live CSV as of publication time, not the copy saved before P2.
  2. **Before any DB change, fix two disjoint sets:**
     - **replace/delete**: the manifest keys, including any defective operational (flag-0) rows approved
       for replacement;
     - **preserve**: operational and recovered appends that are *not* in the manifest.
  3. Merge the approved regenerated or corrected rows into `<model>_hindcast.csv`. **Approved manifest
     replacements take precedence** on a (`date`, `code`) collision. Preserved rows are kept unchanged.
     The importer copies windows verbatim (`bin/utils/migration_py/long_forecast.py:334`), so a kept
     defective row would come back on the next import.
  4. Verify the merged file:
     - no manifest key remains, and every counterpart is present;
     - every preserved append is unchanged;
     - the P0 audit on the file reports an empty remediation population; preserved off-schedule rows
       are reported separately;
     - a test fixture covers a defective operational-row collision alongside an unaffected append;
     - a **dry-run import** of the published CSV passes.
  5. Update the authoritative server copy **and** the Dropbox copy.

  Alternative: keep a reviewed replacement artifact and keep ordinary imports
  (`bin/initialize_long_forecast_history.sh`, a DB reset) blocked until it is published.

## Out of scope

- Any producer change: both are fixed on trunk.
- PP-065's derivation, which already tolerates both defects.
- Snapping quarter windows (PP-064).
- Other stale rows (PP-041 and the stale-rows decision).
- The GBT-family bounds month (LTF-017).
