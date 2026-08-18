# Recovering Stranded Short-Term Period Forecasts (PP-045)

Operator procedure for `apps/postprocessing_forecasts/backfill_period_forecasts.py`,
the recovery tool for missing short-term per-model PENTAD/DECADE rows in the
`forecasts` table.

> **Status of this procedure.** Sections 1-4 and 6 (diagnosis) are ready to use.
> **The real write in section 5 is PROHIBITED**: contract C8 requires a tested
> rollback and none exists (section 7). Use this document to decide whether the tool
> applies and to establish coverage; do not use it to perform a recovery yet.

---

## 1. Does this tool apply? — read this before anything else

**Most reported cases of "period rows are missing" are NOT fixable by this tool.**

The CLI does not generate forecasts. It re-runs the ordinary aggregation over inputs
that already exist and writes the result. If the inputs for your dates were never
produced, it cannot recreate them.

Be precise about what you will see, because neither outcome means "repaired":

- **Some dates lack coverage, the rest of the year has it** — the likely case. The run
  **exits 0**, having re-upserted every *other* period of that year while writing
  nothing new for the dates you cared about. Exit 0 here is not evidence of recovery.
- **The whole horizon-year has no writable rows** — the API writer returns failure,
  `require_api=True` raises, and the run **exits 1**.

*(In the field cases examined so far the inputs were absent because the pipeline had
not run. How representative that is across deployments is **inferred, not measured** —
see §H of the issue.)*

Work through this in order:

| Question | If yes | If no |
|---|---|---|
| Do the affected issue dates have usable aggregation inputs? (section 3) | Continue | **Stop.** The gap is upstream — see section 8. This tool cannot help. |
| Is the gap inside the current calendar year? | The next boundary-day operational run may heal it on its own; consider waiting | Continue — cross-year is where this tool is the most controlled option |
| Are per-model rows present but only `EM`/`NE` missing? | **Not this tool.** EM has an independent skill gate; absent EM is not by itself evidence of a stranded period | Continue |

Two situations where this tool is the right answer:

1. **Inputs exist but the boundary-day postprocessing was missed** — ML produced its
   DAY forecasts, postprocessing failed or was skipped.
2. **Cross-year recovery where inputs exist** — its per-year iteration is what avoids
   the yearless-key collapse (PP-046) that defeats an unbounded skill recalculation.
   It is the most controlled cross-year option, not the only one: a manual
   `SAPPHIRE_FORECAST_DATE` operational run reaches a chosen historical year (the
   chosen date must itself be a boundary date), and the un-wired raw-SQL
   `apps/machine_learning/reaggregate_day_to_periods.py` reaches any year while
   bypassing the application's guards.

Background and the per-entrypoint matrix: section H of
`doc/plans/issues/review_gi_draft_pp_missed_boundary_period_gap.md`.

## 2. Prerequisites

- The postprocessing API is reachable and `SAPPHIRE_API_ENABLED` is not `false`. This
  is a database-write tool; an API-disabled run is meaningless.
- ML reading is configured: `ieasyhydroforecast_run_ML_models=true` and a non-empty
  `ieasyhydroforecast_available_ML_models`. With ML reading off, the aggregation has
  no ML inputs regardless of what the archive holds.
- The deployment env file is known.

```bash
export ENV_FILE=<deployment-env-file>
export ieasyhydroforecast_env_file_path=$ENV_FILE
```

Confirm which deployment you are pointed at **before** any command in section 5.

## 3. Establish coverage — the applicability test

**Test merged-archive coverage, not DAY-row presence.** Checking only for DAY rows
gives the wrong answer: the reader falls back to the migrated period archive, and for
dates before each (code, model)'s first DAY issue date it serves retained
period-archive rows instead. A date with no DAY row can still be recoverable; a date
with DAY rows can still be unrecoverable if their `target` falls outside the following
period or their discharge is null.

Run the production reader over the affected year and print what it actually yields —
this is read-only and saves nothing:

```python
# Run from apps/postprocessing_forecasts/ with the module's venv.
import sys; sys.path.insert(0, "../iEasyHydroForecast")
import pandas as pd
import setup_library as sl
from src import data_reader

sl.load_environment()          # REQUIRED. Exporting the env path alone does not load
                               # the .env, and ML reading then defaults OFF, which
                               # would make every date look uncoverable.

DATES  = ["2026-07-25", "2026-07-31"]     # the affected issue dates
CODES  = ["19999"]                        # the affected station codes

_, modelled = data_reader.read_observed_and_modelled_data(
    "pentad",                             # or "decad"
    codes=CODES,
    start_year=2026, end_year=2026,
)

if modelled.empty or "date" not in modelled.columns:
    print("NO COVERAGE AT ALL for this horizon/year -> section 8")
else:
    m = modelled.copy()
    m["date"] = pd.to_datetime(m["date"]).dt.strftime("%Y-%m-%d")
    m = m[m["model_short"] != "LR"]       # LR is dropped before the API write
    # Report PER MODEL: this is a per-model recovery tool, so one healthy model
    # must not be allowed to mask another that is missing or null.
    expected = sorted(m["model_short"].unique())
    print("models seen in this horizon/year:", expected)
    for d in DATES:                       # check EVERY date, not the frame as a whole
        for c in CODES:
            hit = m[(m["date"] == d) & (m["code"].astype(str) == c)]
            covered = sorted(
                hit.loc[hit["forecasted_discharge"].notna(), "model_short"].unique()
            )
            missing = [x for x in expected if x not in covered]
            print(f"{d} {c}: covered={covered} missing={missing}")
```

Read the result **per date, per code and per model** — a covered date elsewhere in the
frame, or one healthy model on the right date, tells you nothing about the per-model row
you are actually missing.

- **`missing` empty for every affected date/code** → the tool can recover them. Continue.
- **A model listed in `missing`** → that specific per-model row cannot be recovered by
  this tool. Go to section 8.
- Compare `expected` against the models you believe should be present: a model absent
  from the whole horizon-year will not appear in `expected` at all, so check it against
  `ieasyhydroforecast_available_ML_models` rather than against this list alone.

Two caveats on this probe. The empty-frame guard matters: with no forecasts at all the
reader returns a frame without a `date` column, so an unguarded filter raises rather
than reporting "no rows". And the frame is a *necessary*, not sufficient, condition — it
precedes ensemble creation, the yearless dedup and the writer's null-drop and dedup, so
a covered date can still fail to land.

Useful corroboration, needing no database: the rotated logs under `apps/logs/` record
which days the pipeline actually ran. Rotation happens only on run days, so a missing
date in the `log_operational.YYYY-MM-DD` sequence is a strong hint that nothing ran
and the inputs were never produced.

## 4. What the CLI actually does

Read this before choosing arguments; several behaviours are not what the flag names
suggest.

- **Only the YEARS of `--start-date`/`--end-date` are used.** Day and month are used
  for validation only. Every selected year is reprocessed **in full, for every
  configured station**. `--start-date 2026-07-25 --end-date 2026-08-10` does exactly
  the same work as `--start-date 2026-01-01 --end-date 2026-12-31`. Size the blast
  radius from the years and from `--horizon`, never from the dates.
- **One year per internal aggregation/save call.** The CLI accepts a multi-year range
  and isolates each year itself, so separate invocations are *not* required. The
  isolation is deliberate: `file_writer.get_latest_forecasts` de-duplicates on the
  yearless key `(code, period_in_year, model_short)` (`src/file_writer.py:120-122`)
  *before* the `year >= latest_year - 1` filter (`:129`), so a multi-year read would
  collapse the same period across years. This is PP-046.
- **Scope `--horizon` to the horizon you actually need.** `both` rewrites the entire
  selected year for pentad *and* decad.
- **Issue dates, not targets.** A period whose target starts 1 January of year Y is
  produced by the 31 December (year Y-1) issue date. To heal it, the range must extend
  back into the prior calendar year.
- **API-only by default.** `--write-csv` is off so the backfill never clobbers the
  operational combined CSVs. Pass it only if you explicitly want the CSVs rewritten.
- **EM is not a replay.** Ensemble Mean is recomputed against *current* skill metrics,
  so backfilling a historical period does not reproduce the ensemble values that
  period originally had. Anything consuming historical EM — skill evaluation,
  dashboards, bulletins — sees the new values.
- **The CLI's own output does not currently reach you at all (INFRA-029, 2026-08-18).**
  Every operator-facing line the backfill emits goes through `logger.info` — `:237`
  (failure-mode notice), `:247` (plan), `:273` (per-year "ok"), `:290` ("Backfill finished
  successfully.") — and so does the dry-run summary quoted below
  (`postprocessing_operational.py:209-214`). Importing `setup_library` configures the root
  logger at WARNING, which makes the module's own `basicConfig(level=INFO)` a no-op, so the
  effective level is WARNING and **none of those lines is printed**. Verified directly on this
  module: `isEnabledFor(INFO)` is `False`. Until INFRA-029 is fixed, a run of this CLI shows you
  its exit code and nothing else — you cannot distinguish "wrote nothing" from "did not run",
  and the dry-run step below produces no output whatsoever. Nothing about what the CLI *writes*
  changes; this is an observability limit, and it is one more reason the write path stays
  prohibited (section 7).
- **`--dry-run` is not coverage proof.** Its summary line reports only totals — row
  count, distinct period count, distinct model count
  (`"%s DRY-RUN: would write %d row(s) (%s); save skipped."`) — and never names the
  dates or codes involved, so it cannot tell you whether *your* dates survived. Two
  further traps: that row total is counted **before** `get_latest_forecasts`, the
  LR-drop, the null-drop and the API dedup, so it overstates the payload; and other log
  lines emitted during the same dry run (ensemble diagnostics, archive-cutover
  warnings) *do* contain codes and dates, which is easy to mistake for coverage
  evidence. Section 3 is the coverage evidence.

## 5. Write-safety procedure (contract C8)

**Do not skip any step — and note that step 4 cannot currently be satisfied, so the
real run is prohibited (section 7).** The command rewrites every period of the
selected year for every configured station.

1. **Name the target.** State which deployment you are writing to and confirm it is
   the intended one.
2. **Open a maintenance window — a point-in-time check is not enough.** A "nothing is
   running right now" check does not hold exclusion across snapshot -> write ->
   read-back. Disable the cron entries for `postprocessing_forecasts` operational,
   maintenance and skill recalculation for the whole duration, confirm no other
   session holds the tunnels, and record when the window opened and closed. On
   orchestrated deployments disable the **orchestrator** entries too, not only cron —
   `apps/pipeline/` schedules the same tasks. Note the
   yearly skill recalculation runs at 01:00 UTC on 31 December — do not run a backfill
   across that boundary.
3. **Snapshot the complete write set** (section 6) — the whole horizon-year for all
   configured stations, values included, not just the dates of interest.
4. **Build the rollback manifest** (section 7) before writing anything.
5. **Run**, then **verify the full submitted payload against a read-back**, key and
   value. Reading back only the dates of interest proves nothing about the rest of the
   year. This is mandatory because `_write_combined_forecast_to_api` can return `True`
   over a zero or partial server write (**PP-047**), so the CLI's `require_api=True`
   catches an unreachable API, an exception, or an explicit failure — but does **not**
   prove rows persisted.
6. **Re-run once and compare values, not counts.** "Row counts unchanged" is a count
   check; idempotence is a value claim.

The command itself, once 1-4 are satisfied:

The dry run is safe and useful on its own:

```bash
ieasyhydroforecast_env_file_path=$ENV_FILE \
  python backfill_period_forecasts.py \
  --start-date 2026-01-01 --end-date 2026-12-31 --horizon pentad --dry-run
```

The same command without `--dry-run` is the real write. **Do not run it until section
7's rollback requirement is met.**

Exit codes: `0` success; `1` one or more (year, horizon) combinations failed —
remaining years are still attempted, so read the whole log rather than only the tail,
and note an environment-load failure also surfaces as `1`, so `1` does not by itself
mean a year/horizon failed; `2` invalid arguments (argparse errors also exit `2`). The run sets `SAPPHIRE_API_FAILURE_MODE=fail` so API write
errors surface instead of being swallowed.

## 6. Snapshot

Snapshot the whole horizon-year for the configured stations before any write.
Substitute the real station codes; `19999` is a placeholder.

**Note the enum case.** `horizon_type` and `model_type` are PostgreSQL enums whose
labels are **uppercase** (`PENTAD`, `DECADE`, `TFT`, `ENSEMBLE_MEAN`, …). A lowercase
literal does not silently match nothing — PostgreSQL raises
`invalid input value for enum horizontype` and the statement fails outright, which is
the safer of the two behaviours. The same trap is documented in
`doc/prod/remediate_quarter_horizon_type_422.md`. **And `\copy` is a psql meta-command: it must be on one
line.** Both mistakes were made in the first draft of this runbook.

```bash
docker exec sapphire-postprocessing-db psql -U postgres -d postprocessing_db -c "\copy (SELECT horizon_type, code, model_type, date, target, horizon_value, horizon_in_year, composition, forecasted_discharge, q05, q25, q75, q95, flag FROM forecasts WHERE horizon_type = 'PENTAD' AND EXTRACT(YEAR FROM date) = 2026 AND code IN ('19999')) TO '/tmp/pp045_snapshot_pentad_2026.csv' WITH (FORMAT csv, HEADER true)"

docker cp sapphire-postprocessing-db:/tmp/pp045_snapshot_pentad_2026.csv ./
```

Keep the snapshot outside the repository — it contains operational data.

## 7. Rollback is UNDEFINED — the write path is prohibited

> **There is no rollback procedure for this tool today, so section 5's real run must
> not be performed.** Contract C8 requires a tested rollback, and none exists. This
> runbook is therefore complete as a **diagnosis** document (sections 1-4, 6) and
> explicitly incomplete as a recovery document.

This is a deliberate decision, not an omission. A first draft of this section carried
hand-written `DELETE` SQL; independent review found two defects in it that would have
caused data loss or silent failure on a production database (see the requirements
below). Shipping plausible-looking but unexecuted destructive SQL into a runbook is
worse than shipping none: an operator under pressure will copy it.

### What a correct rollback must satisfy

Whoever implements this needs database access and a scratch target. The requirements,
which are the useful output of the analysis so far:

1. **A snapshot alone is not a rollback.** Rows the backfill *inserts* have no prior
   value to restore. Rollback must be partitioned by whether each key existed before
   the run: pre-existing keys are **restored** to their snapshot values, keys that did
   not exist are **deleted**. That partition — the manifest — must be captured *before*
   the run, as its own artefact.
2. **The identity is the unique constraint**
   `(horizon_type, code, model_type, date, target)`, named
   `uq_forecasts_horizon_code_model_date_target`.
3. **`target` is nullable, so the manifest join must be NULL-safe.** In PostgreSQL,
   `NULL = NULL` is not true, so a plain equality anti-join treats a legacy row with
   `target IS NULL` as "not in the manifest" and **deletes a row that existed before
   the run**. Use `IS NOT DISTINCT FROM`, or exclude NULL-target rows from the
   operation and handle them separately. This was defect one.
4. **Enum literals are uppercase** (section 6). A lowercase predicate raises
   `invalid input value for enum horizontype` and aborts the statement — loud, not
   silent, but it means a rollback drafted with lowercase literals does not run at all
   at the moment it is needed. This was defect two.
5. **Both directions must be exercised on a scratch or development target** — insert
   rollback and update rollback — and the result recorded here before the banner above
   is lifted.

### On the 2026-07-23 write

The only real backfill write on record (2026-07-23, Tajik dev DB) is documented in the
issue with before/after row counts and an idempotence re-run. **No maintenance window
and no rollback manifest is documented in the tracked record of that run.** That is a
statement about the record, not proof that no precaution was taken — but it means the
C8 procedure in section 5 has no evidence of ever having been exercised, and must not
be presented as validated.

## 8. When this tool cannot help

If section 3 shows no usable coverage, the gap is upstream and the fix is to
regenerate the ML DAY forecasts first, then return to section 3:

- `apps/machine_learning/fill_ml_gaps.py` — wired into `maintenance:machine_learning`
  and the default ML run mode. **Important limitation:** it detects only gaps
  *between* consecutive existing dates, so it cannot see an empty archive, a leading
  gap, or a trailing gap. A stale period with no forecasts on either side may be
  invisible to it.
- `apps/machine_learning/hindcast_ML_models.py` — the underlying hindcast, which
  `fill_ml_gaps.py` calls.
- `apps/machine_learning/recalculate_nan_forecasts.py` — for rows present but
  null-valued. Note it selects only flag `1`/`2` rows, so it is not a general remedy
  for every null discharge.

## References

- Issue: `doc/plans/issues/review_gi_draft_pp_missed_boundary_period_gap.md` (PP-045),
  especially section H for the per-entrypoint matrix.
- Related: **PP-046** (yearless dedup key), **PP-047** (writer reports success on a
  zero or partial write — the reason section 5 step 5 is mandatory), **PP-048**.
- Module README: `apps/postprocessing_forecasts/README.md`.
