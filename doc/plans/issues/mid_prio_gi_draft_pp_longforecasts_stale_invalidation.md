# PP: stale-aggregate invalidation for long_forecasts (forecast-side) — deferred P1b

**Priority:** mid (lower impact than the skill-side fix already shipped).
**Module:** `apps/postprocessing_forecasts` (+ forecast/dashboard read consumers).
**Status:** deferred follow-up to the min-n + stale-aggregate effort (branch
`feature_lt_skill_min_n_stale`). The skill-side staleness (P1) is done; this is the
forecast-side analogue (plan phase **P1b**), consciously deferred 2026-07-08 with owner
sign-off to land the high-impact P2+P1 fixes first.

> **Correction 2026-07-14 (out-of-loop review) — the natural key below was WRONG.**
> This draft originally gave the `long_forecasts` unique key as
> `(horizon_type, code, model_type, date, target)`. **`long_forecasts` has no `target` column.**
> The real unique key is
> **`(horizon_type, horizon_value, code, date, model_type, valid_from, valid_to)`**
> (`sapphire/services/postprocessing/app/models.py:159`; CRUD `crud.py:107`) — it is corrected
> inline below. **Any stale-diff keyed on the old tuple would not match a single row.** Note the key
> carries `horizon_value` (the lead) and the validity interval, so the diff must be computed per
> lead and per `valid_from`/`valid_to`, not per "target".

> **Correction 2026-09-17 (plan rev4, P6b) — the implementation recipe below was also stale, and
> one citation was wrong.** The 2026-07-14 fix corrected the natural key stated in "Problem" but
> never propagated into "Proposed approach" step 1, which still read *"diff emitted `(code, date,
> target, model)` keys"* — the same pre-correction, nonexistent key, restated. That step is
> rewritten below rather than left standing next to the correct key. Separately, the discard-site
> citation `ensemble_calculator.py:183-195 monthly` was wrong: that line range (`:186-189`,
> verified at `89a6ffc7`) is inside `create_ensemble_forecasts`, the **short-term** (pentad/decad)
> EM-only calculator — not monthly. The actual per-tier discard sites are listed below. This edit
> also adds the scope boundary against the new sibling issue **PP-063** (trigger vs. disposal) and
> does not restate PP-063's own content — see that issue for the trigger-side evidence and
> acceptance contract.

## Problem
`long_forecasts` ensemble rows (EM / Skilled Mean / Naive Mean) are written upsert-only
(`api_writer.py` `write_long_forecasts` → service CRUD `create_long_forecast`,
`crud.py:107-158`, unique key
`(horizon_type, horizon_value, code, date, model_type, valid_from, valid_to)`,
`models.py:151` `class LongForecast`, `UniqueConstraint
uq_long_forecasts_horizon_type_value_code_date_model_from_to` at `models.py:193-202`).
When the ensemble builder discards a single-model/empty ensemble at a given
`(period, code[, horizon_value])` group, the prior run's ensemble **forecast** row at that key
survives — a stale forecast value that the dashboard/bulletins still display, even though the
skill-side is now correctly tombstoned.

**Discard sites, by tier (verified at `89a6ffc7`; corrected from the previous single mislabeled
citation):**
- **Short-term (pentad/decad), EM only** — `create_ensemble_forecasts`,
  `ensemble_calculator.py:186-189` (`ensemble_avg = ensemble_avg[ensemble_avg["composition"]
  .apply(is_multi_model_composition)].copy()`).
- **Monthly, all three aggregates** — `create_monthly_ensemble_forecasts`: `em_avg` discard at
  `ensemble_calculator.py:352`, `sm_avg` discard at `:475`, `naive_avg` discard at `:524`.
- **Quarterly and seasonal, all three aggregates** — both `create_quarterly_ensemble_forecasts`
  and `create_seasonal_ensemble_forecasts` delegate to the shared
  `_create_aggregated_ensemble_forecasts` (`ensemble_calculator.py:611`); its `em_avg`/`sm_avg`/
  `naive_avg` discards are at `:767`, `:873`, `:915`.

Each discard drops rows from the newly computed frame before it is appended
(`_append_to_joint` / `pd.concat`) into that run's `joint` / `q_merged` / `s_merged` output.
Because the write path is upsert-only (above) and no code path removes a `long_forecasts` row
whose key is no longer emitted, a group discarded this run leaves its previous row at the same
key untouched in the database.

## Scope boundary with PP-063: trigger versus disposal

**PP-063** (`doc/plans/issues/mid_prio_gi_draft_pp_longterm_aggregate_not_refreshed.md`, Medium,
Draft) covers the companion defect: a long-term aggregate whose *membership or value* has
changed is never even selected for regeneration, because the gap detectors in
`gap_detector.py` test key *presence*, not content. Read together, the two issues partition the
problem with no gap and no overlap:

- **PP-063 owns the trigger** — selecting and regenerating the groups whose inputs changed,
  *including groups whose regeneration produces no replacement row* (the case this issue's
  discard sites produce). Ownership attaches to the decision to recompute, before the output of
  that recomputation is known. Without PP-063's fix, this issue's tombstone logic is never
  invoked for a stale-but-still-keyed aggregate, because nothing identifies that group as needing
  regeneration in the first place.
- **PP-041 (this issue) owns the disposal** — what happens to a row that survives when a
  regeneration PP-063 triggers emits no replacement: tombstone production and read-side/display
  suppression. This issue does not decide *when* to regenerate; it decides what to do with the
  result once regeneration has run and produced nothing for a key that used to have a row.
- **Null-tombstone handling** splits the same way: **PP-063 owns null-row eligibility and
  reconsideration** — whether a null-discharge row should have been produced at all for a given
  key, and whether that decision is revisited when the underlying member set changes. **This
  issue owns tombstone production and suppression** — once a row is null (whether produced by
  this issue's discard-and-tombstone path or by PP-063's null-row path), how it is marked and
  hidden downstream. A null row must not be treated as a permanent gap-filler by either issue:
  PP-063's detector must be able to reconsider it if inputs change, and this issue's suppression
  must not be read as also deciding when that reconsideration happens.

Neither issue restates the other's fix or evidence. See PP-063 for its own grouping-key evidence,
the flag-OFF quarterly incidental-refresh path, and its acceptance contract.

## Why deferred
- Impact is lower: P1 already removed the catastrophic *skill* values from the dashboard;
  this is stale *forecast values* for historical dates whose ensemble membership later changed.
- Effort is ~2× the skill side: `long_forecasts` is **date-indexed**, so invalidation needs
  its own write-side tombstone-diff over the full forecast date range *and* its own read-side
  suppression in the forecast/dashboard display (a forecast tombstone = NULL discharge/quantiles,
  which the forecast readers must suppress — distinct from the skill readers already handled).

## Proposed approach (mirror P1)

1. **Write-side.** After `create_<tier>_ensemble_forecasts` returns the newly computed ensemble
   frame for a run (`em_avg` / `sm_avg` / `naive_avg`, or the shared
   `_create_aggregated_ensemble_forecasts` equivalents), and **before** that frame is appended
   into `joint` / `q_merged` / `s_merged` and deduplicated with `keep="last"` for writing, diff
   its emitted keys against the existing `long_forecasts` keys for the same domain, and upsert a
   forecast tombstone (NULL `q`/quantile columns) for any existing key with no matching emitted
   row. The diff must be precise on four points — each one broke the pre-2026-09-17 recipe, or
   would silently reproduce PP-063's failure mode on this side of the boundary:
   - **The complete seven-column natural key**:
     `(horizon_type, horizon_value, code, date, model_type, valid_from, valid_to)` — not
     `(code, date, target, model)` (`long_forecasts` has no `target` column; see the 2026-07-14
     correction above). This is the same key PP-063's acceptance contract writes against.
   - **The canonical model-name mapping between `model_short` and the stored enum name.**
     `long_forecasts.model_type` is backed by a Postgres native enum
     (`sapphire/services/postprocessing/app/models.py` `class ModelType`, created in
     `sapphire/services/postprocessing/alembic/versions/34b227f37299_baseline.py`) whose column
     stores the enum **NAME**, not the enum value — confirmed two ways: the baseline migration's
     literal enum labels are `'ENSEMBLE_MEAN'`, `'SKILLED_MEAN'`, `'NAIVE_MEAN'` (not `'EM'` /
     `'Skilled Mean'` / `'Naive Mean'`), and `ModelType.coerce()`'s own docstring states "The DB
     column stores the enum NAME (e.g. `"TIDE"`) while the API schema validates against the enum
     VALUE (e.g. `"TiDE"`)". For the three ensemble aggregates this issue concerns, the mapping
     from the apps-side `model_short` value to the name stored in the database is:

     | `model_short` (apps-side) | `ModelType` member | stored enum NAME |
     |---|---|---|
     | `EM` | `ModelType.ENSEMBLE_MEAN` | `ENSEMBLE_MEAN` |
     | `Skilled Mean` | `ModelType.SKILLED_MEAN` | `SKILLED_MEAN` |
     | `Naive Mean` | `ModelType.NAIVE_MEAN` | `NAIVE_MEAN` |

     Do not confuse this with `api_writer.MODEL_TYPE_MAP` (`api_writer.py:24`), which maps
     `model_short` to the enum **value** form used in `write_long_forecasts` request payloads
     (e.g. `"EM"`, `"Skilled Mean"`) — correct for building a write, but the wrong form for
     comparing against `model_type` as stored in the database. Reuse the existing apps-side
     canonicalizer, `src/model_names.canonical_model_short_series` (already used by the shipped
     skill-side `stale_tombstones.py` for exactly this normalization) — do not write a second
     one.
   - **An explicitly bounded regeneration domain.** The diff's "existing" side must be scoped to
     exactly the group universe (`period`, `code`, and `horizon_value` where present) that was
     part of *this run's* input to `create_<tier>_ensemble_forecasts` — the codes/dates the
     caller queried (`postprocessing_maintenance_long_term.py`'s gap-fill window, or
     `postprocessing_operational_long_term.py`'s per-run codes/dates) — never the full
     `long_forecasts` history. A diff against the whole table would tombstone every historical
     period this run never touched, since none of those keys appear in this run's emitted output
     either — they were never candidates for regeneration, not victims of a discard.
   - **Compare genuinely regenerated ensemble output before merge-back, not after.** The diff's
     "emitted" side must be the tier's own freshly computed frame (`em_avg`/`sm_avg`/`naive_avg`,
     or the `_create_aggregated_ensemble_forecasts` equivalents) taken **before** concatenation
     into `joint` / `q_merged` / `s_merged`. After merge-back, a row carried through unchanged
     from the prior combined data (see PP-063's documented flag-OFF quarterly incidental-refresh
     path, where `q_new` is concatenated with `q_combined` and deduplicated `keep="last"`) is
     indistinguishable in the merged frame from a row this run genuinely regenerated — a
     carried-through row would read as regenerated, and the diff would never detect anything as
     stale.
2. **Read-side.** Suppress forecast tombstones in the forecast readers and dashboard forecast
   display (NULL discharge → not plotted / not selected).
3. **Tests** (acceptance criteria — extended 2026-09-17 to cover the corrections above; a
   correction to the recipe without a matching test is how a partial fix closes the issue):
   - Stale forecast key (group discarded this run, inside the bounded domain) → tombstoned +
     suppressed.
   - Idempotency: running the diff twice does not re-tombstone or double-write.
   - Short-term (`create_ensemble_forecasts`, EM-only) untouched by any monthly/quarterly/
     seasonal-specific change.
   - **A discarded group that falls *outside* this run's bounded regeneration domain must NOT be
     tombstoned** — guards against the "diff against the whole table" mistake above; without this
     case, a fix that widens the diff to the entire `long_forecasts` history would still pass the
     stale-key case and silently tombstone untouched history.
   - **A case exercising each of the three ensemble aggregates** (`EM`, `Skilled Mean`,
     `Naive Mean`), not only `EM`, so the model-name mapping table above is actually exercised and
     a mapping bug in the `Skilled Mean` / `Naive Mean` rows is not masked by an EM-only fixture.
   - A carried-through (not regenerated) row present in the post-merge-back frame must not be
     mistaken for regenerated output — construct a fixture where a key's row would look "emitted"
     post-merge but was not part of this run's `em_avg`/`sm_avg`/`naive_avg`, and assert the diff
     is computed against the pre-merge-back frame.

## Reference
Plan: `doc/plans/working/skill_min_n_and_stale_aggregate_plan.md` (§P1b). Skill-side
implementation to mirror: `src/stale_tombstones.py`, the `recalculate_skill_metrics.py`
wiring, `src/model_names.py` (canonical model-name normalization, reused above), and
`data_reader._drop_tombstone_rows`. Placeholder code `19999` in tests.

**PP-063** — `doc/plans/issues/mid_prio_gi_draft_pp_longterm_aggregate_not_refreshed.md` —
trigger-side (detection/regeneration) counterpart. See "Scope boundary with PP-063" above; not
restated here.
