# Coordination note — Option B: one-time in-DB remap of `lr_forecasts.horizon_in_year` (issue→target)

**To:** owner of `sapphire/services/` + the shared postprocessing DB
**From:** forecast-tools side (read-only investigation; nothing changed)
**Status:** DRAFT for sign-off (rev. 2). No writes executed. Requires colleague approval — touches `sapphire/services/postprocessing` semantics and the shared DB.

## 0. Revisions from independent review (2026-07-03) — fold these in before running
1. **Stale skill rows are NOT replaced by a plain recompute (blocking).** `skill_metrics` upserts on `(horizon_type, code, model_type, date, horizon_in_year, horizon_value)`, so recomputed rows get INSERTED alongside the old issue-labeled rows; the dashboard then dedups by latest `date` (`db.py:548`), not by provenance. → **Back up `skill_metrics` and add an explicit DELETE/REPLACE of the stale pentad/decade rows** in the affected date/year scope before/around the recompute (see §3).
2. **Remap must not assume every LR `date` is a period boundary (blocking).** `period_of(date) + 1` only equals the target when `date` is the last day of the prior period. → **Compute the target as `period_of(date + interval '1 day')`** (matching the recompute reader `data_reader.py:1934`) and validate against `date + 1`, not `issue + 1`. Add a **preflight count of non-boundary LR dates** (see §4).
3. **Set `SAPPHIRE_SKILL_METRICS_YEAR` explicitly (medium).** The wrapper passes only `..._START_YEAR`, so the write-year defaults to current year (`recalculate_skill_metrics.py:230`). Set it deliberately for the migration run (see §3).

Reviewer confirmed as correct: LR constraint-safety (only `(horizon_type,code,date)` is unique) and the single dashboard lockstep change `utils.py:352`.

## 1. Bug, root cause, volume
- `lr_forecasts.horizon_in_year` should be the **TARGET** pentad/decade period. For LR short-term rows generated/migrated **before 2026-04-13** it holds the **ISSUE** period (`target = issue + 1`).
- Root cause: the generation-side fix runs only for NEW forecasts (`apps/linear_regression/linear_regression.py:930-938`); the CSV→DB migrator copies the label verbatim with no +1 (`sapphire/services/postprocessing/app/data_migrator.py:288` pentad, `:315` decade).
- Two-vintage overlay: upsert key is `(horizon_type, code, date)` only — `horizon_in_year` excluded (`models.py:190-193`, `crud.py:204-211`) → last-writer-wins per issue date.
- Volume: ≈117,700 short-term LR rows mislabeled (≈78.4k pentad + 39.3k decade), ~all pre-2024; plus a pre-2024 **already-target-indexed minority** (≈21.1k pentad / 10.6k decade) to leave untouched → detection must be **per-row against `date`**, not a year threshold.

## 2. Consumer inventory / blast radius
- **Service read** `GET /lr-forecast/` → `crud.get_lr_forecast` (`crud.py:248-274`): filters on `horizon_type/code/date` only, returns `horizon_in_year` verbatim. Transparent to the fix. **No risk.**
- **MUST flip in lockstep (forecast-tools-side):** `apps/forecast_dashboard/dashboard/utils.py:352` — `df[df[horizon_in_year] == (horizon_value - 1)]`. The `-1` compensates for issue-indexing; after remap it must be `== horizon_value`. Only period-level ±1 compensation in the dashboard.
- **Pre-existing inconsistency:** `src/vizualization.py:3400-3403` already filters `== horizon_value` (no offset) → disagrees with `utils.py:352` today; the fix resolves it.
- **Agnostic / become-correct after remap:** `src/db.py:439,487-496` (pass-through), `data_manager.py:355-360` (bulletin metadata), `processing.py:1358-1360` (best models), `widgets.py:333-346,605-622` (labels).
- **Join to watch (benefits):** `src/db.py:821` merges LR↔skill on `[code, pentad/decad_in_year, model_short]` with no offset; aligns once LR is remapped **and** skill is recomputed/relabeled.
- **Not consumers of this label:** `apps/postprocessing_forecasts` recalc drops `horizon_in_year` and derives period from `date`; ML/preprocessing use other tables.

**Net: exactly one line (`utils.py:352`) changes in lockstep; everything else agnostic or already-correct.**

## 3. Skill-metric recompute — REQUIRED (for labeling)
- Recalc read path derives target from `date + 1 day` and **drops** `horizon_in_year` (`apps/postprocessing_forecasts/src/data_reader.py:1934-1956`); pairing on `[code,date]` (`skill_metrics.py:1847-1849`). A fresh recompute yields correct, date-derived, target-labeled skill regardless of the remap.
- BUT currently-stored LR skill rows may carry an issue-period label (skill CSV→DB migrator copies `horizon_in_year` verbatim and synthesizes `date` from it — `data_migrator.py:483-490`; cf. `.bak_corrupted` skill CSVs). So the dashboard skill tile for target P may actually be P+1, and `db.py:821` would mismatch.
- **Action:** one-time recompute after remap to relabel LR pentad/decad skill to date-derived target.
- **Trigger:** `bash bin/yearly_skill_metrics_recalculation.sh <config/.env>` (`SAPPHIRE_PREDICTION_MODE=BOTH`; scope via `PENTAD`/`DECAD`, `SAPPHIRE_RECALC_STATION_CODE`, `..._START_YEAR`). Entry `apps/postprocessing_forecasts/recalculate_skill_metrics.py:208/480`; writes `skill_metrics` (upsert key `(horizon_type,code,model_type,date,horizon_in_year,horizon_value)`).
- **Caveat (top risk):** pentad/decad recalc has historically produced degenerate `n_pairs∈{1,2}`. **Validate n_pairs on a full local-DB dry run before any server.**

## 4. Remap design (DRAFT — do not run without sign-off)
Detection = SQL translation of the eval shim: a row is issue-indexed iff `horizon_in_year == period_of(date)`. Cleanly bimodal (target rows are `issue+1 ≠ issue`), catches the 2024 tail, skips the target-indexed minority. `lr_forecasts` has no `target` column, so no year-wrap field to touch — only `horizon_in_year`/`horizon_value`.

> ⚠️ **Rev. 2 (see §0.2):** before running, change the target derivation to `period_of(date + interval '1 day')` (not `period_of(date) + 1`) and validate against `date + 1`. First run a **preflight count of non-boundary LR dates** — `SELECT count(*) FROM lr_forecasts WHERE horizon_type IN ('pentad','decade') AND EXTRACT(DAY FROM date)::int NOT IN (5,10,15,20,25,31 /* + decade/pentad boundaries */)` — to confirm how many rows the boundary assumption would misclassify. The SQL below is the pre-review draft kept for reference.

```sql
-- PENTAD. Count first; run UPDATE in one txn AFTER backup.
UPDATE lr_forecasts AS t
SET horizon_in_year = CASE WHEN p.issue = 72 THEN 1 ELSE p.issue + 1 END,
    horizon_value   = ((CASE WHEN p.issue = 72 THEN 1 ELSE p.issue + 1 END) - 1) % 6 + 1
FROM (
  SELECT id,
    ((EXTRACT(MONTH FROM date)::int - 1) * 6
      + LEAST((EXTRACT(DAY FROM date)::int - 1) / 5 + 1, 6)) AS issue
  FROM lr_forecasts WHERE horizon_type = 'pentad'
) AS p
WHERE t.id = p.id AND t.horizon_in_year = p.issue;   -- idempotency + selectivity guard
```
Decade analog: `horizon_type='decade'`, `issue = (month-1)*3 + CASE WHEN day<=10 THEN 1 WHEN day<=20 THEN 2 ELSE 3 END`, wrap `36→1`, `horizon_value=((target-1)%3)+1`.

- **Constraint safety:** `horizon_in_year`/`horizon_value` in no unique constraint/index (`models.py:189-193`) → cannot violate uniqueness.
- **Backup (before UPDATE):** `CREATE TABLE lr_forecasts_backup_20260703 AS SELECT * FROM lr_forecasts WHERE horizon_type IN ('pentad','decade');` — keep until skill validated.
- **Idempotency:** `AND t.horizon_in_year = p.issue` self-limits; second run matches nothing.

## 5. Validation
```sql
-- Expect 0 (every short-term LR row target-indexed):
SELECT count(*) FROM (
  SELECT horizon_in_year,
    ((EXTRACT(MONTH FROM date)::int-1)*6 + LEAST((EXTRACT(DAY FROM date)::int-1)/5+1,6)) AS issue
  FROM lr_forecasts WHERE horizon_type='pentad') s
WHERE horizon_in_year <> (CASE WHEN issue=72 THEN 1 ELSE issue+1 END);
```
Row counts before/after identical; matched-update count ≈ 78.4k pentad / 39.3k decade; post-recompute spot-check `n_pairs` sane (not 1–2) and `db.py:821` LR↔skill periods align.

## 6. Option A (eval shim) interaction
`api_readers._repair_lr_issue_indexing` (default off) only remaps `H == issue_period_of(date)`. After B, all rows are `issue+1 ≠ issue` → shim skips every row = **verified no-op**. Safe to leave enabled as a guard.

## 7. Residual risks (ranked — need sign-off)
1. **Skill-recompute `n_pairs` degeneracy (highest)** — mandatory relabel step; do a full local dry run + inspect n_pairs first.
2. **`utils.py:352` lockstep** — ship the dashboard change with the DB remap or period shifts by one.
3. **Pre-2024 target-indexed minority provenance** — confirm it's just post-override re-runs; no third `H==issue` population.
4. **Stored skill provenance** — recompute fixes both cases; low risk given (1).
5. **Shared-DB coordination** — sequence: backup → count → UPDATE (txn) → validate → skill recompute (BOTH) → validate n_pairs → deploy `utils.py:352`. Per-org-env.

**Coordinated (colleague / shared DB):** the UPDATE, backup table, skill-recompute writes. **Forecast-tools-side:** only `apps/forecast_dashboard/dashboard/utils.py:352` (lands together with the remap).
