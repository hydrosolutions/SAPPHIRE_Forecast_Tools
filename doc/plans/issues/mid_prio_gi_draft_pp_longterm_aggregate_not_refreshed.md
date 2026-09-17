# PP-063: a long-term aggregate is not refreshed when its membership changes

**Priority**: Medium
**Module**: postprocessing_forecasts
**Status**: Draft
**Created**: 2026-09-17

## Problem

The long-term gap detectors in `apps/postprocessing_forecasts/src/gap_detector.py` identify
gaps by key absence only:

```python
gaps = merged[merged["_merge"] == "left_only"][...].copy()
```

(verified at `89a6ffc7`: `detect_missing_ensembles` line 126, `detect_missing_monthly_ensembles`
line 348, `detect_missing_quarterly_ensembles` line 479, `detect_missing_seasonal_ensembles`
line 563 — each is the tail of a `left`-merge against an `indicator=True` column, filtering to
`_merge == "left_only"`).

**Changed membership does not itself trigger gap detection or regeneration.** If a row exists
for a key, the tier is considered complete, however incomplete the member set behind it was when
that row was written. A model that becomes newly eligible for a period (e.g. a freshness/lookback
gate that later admits it, or a previously-excluded model added to `ensemble_models`) does not
make the existing ensemble row a "gap" — the key is already present — so nothing re-selects that
group for regeneration. An existence-only verification passes over an aggregate that is no longer
correct.

## Evidence: grouping keys of each detector (verified at `89a6ffc7`)

All four detectors share the same `merge(..., indicator=True)` / `_merge == "left_only"` shape,
but key on different columns:

- **`detect_missing_ensembles`** (short-term, pentad/decad) — `gap_detector.py:16`. Grouping key:
  `["date", "code"]` for the pair universe (`gap_detector.py:120-126`), with `model_short` checked
  per model in the `for model in sorted(ensemble_models):` loop (`gap_detector.py:112`).
- **`detect_missing_monthly_ensembles`** — `gap_detector.py:230`. Grouping key: `["year", "month",
  "code"]`, extended to `["year", "month", "code", "horizon_value"]` when
  `skill_lead_aware_enabled()` is true **and** `horizon_value` is present in the input
  (`gap_detector.py:257-263`). `model_short` is checked per model at `gap_detector.py:339-348`.
- **`detect_missing_quarterly_ensembles`** — `gap_detector.py:370`. Grouping key: `["year",
  "quarter_in_year", "code"]`, extended with `horizon_value` under the same lead-aware condition
  (`gap_detector.py:395-401`). `model_short` checked per model at `gap_detector.py:470-479`.
- **`detect_missing_seasonal_ensembles`** — `gap_detector.py:498`. Grouping key: **unconditionally**
  `["season_year", "season_in_year", "code"]` (`gap_detector.py:518`, `:550`). This function
  contains no call to `skill_lead_aware_enabled()` at all — confirmed by grep, the only two
  call-sites of that function in this file are in the monthly (`:257`) and quarterly (`:395`)
  detectors — and it never adds `horizon_value` as a separate key column. `model_short` checked
  per model at `gap_detector.py:554-563`.

Why seasonal is unconditional: `season_in_year` is not a grain the flag toggles a lead onto — it
already **is** the issue lead. `data_reader._normalize_combined_forecasts` (`data_reader.py:3734`,
`horizon_type == "season"` branch, `data_reader.py:3760-3769`) derives it directly from
`horizon_value`:

```python
if "horizon_value" in df.columns:
    lead = pd.to_numeric(df["horizon_value"], errors="coerce")
    df["season_in_year"] = lead.astype("Int64") if lead.isna().any() else lead.astype(int)
else:
    df["season_in_year"] = 1
```

So a seasonal key already distinguishes leads without a conditional extra column. One
consequence worth stating explicitly: because monthly and quarterly only carry `horizon_value` as
a *separate* key column when the flag is on, an earlier issue date can satisfy the same long-term
period key when the flag is off — collapsing what would otherwise be distinct lead-scoped
completeness into one grain.

## The null-value blind spot is specific to the long-term detectors

The short-term detector removes null-discharge rows from the presence side before testing for
gaps:

```python
if not recent_combined.empty and "forecasted_discharge" in recent_combined.columns:
    recent_combined = recent_combined[recent_combined["forecasted_discharge"].notna()]
```

(`gap_detector.py:107-108`, inside `detect_missing_ensembles`, applied before the per-model
presence check at `:112-126`). None of `detect_missing_monthly_ensembles`,
`detect_missing_quarterly_ensembles`, or `detect_missing_seasonal_ensembles` filters on
`forecasted_discharge` (or any value column) before building `all_pairs` / `model_pairs`
(`gap_detector.py:230-367`, `:370-495`, `:498-579` — no `forecasted_discharge` reference in any
of the three function bodies). A long-term ensemble row with a null discharge value therefore
counts as "present" for gap-detection purposes in all three long-term tiers, even though the
short-term detector treats the same condition as a phantom record that should count as missing.
This blind spot is orthogonal to the membership-change defect above but shares its root cause:
existence of a key row, not the row's content, is what the detector checks.

## Not "never recomputed" — verify the quarterly flag-OFF incidental-refresh path

Do not read this issue as claiming an aggregate can never be regenerated by any code path once
written. `apps/postprocessing_forecasts/postprocessing_maintenance_long_term.py`'s quarterly
gap-fill block regenerates ensemble output for every code in the affected gap-years' range
(`q_fc = data_reader.read_quarterly_forecasts(codes, int(q_years.min()), int(q_years.max()))`,
line 306-310; `q_new = q_joint[q_joint["model_short"].isin(q_ens_models)].copy()`, line 321) and
only restricts `q_new` to the actual gap keys **when the lead-aware flag is on**:

```python
if skill_lead_aware_enabled() and not q_new.empty:
    ...
    q_new = q_new[q_new.apply(...)]
```

(`postprocessing_maintenance_long_term.py:331-356`; the in-code comment at lines 322-330 states
this precisely: "Flag OFF: unchanged (no q_new filtering)."). With the flag off, this filter is
skipped entirely, so `q_new` still holds regenerated ensemble rows for **every** `(year,
quarter_in_year, code, model_short)` combination in the queried year range — not only the ones
that were gaps. That unfiltered `q_new` is then concatenated with the existing `q_combined` and
deduplicated with `keep="last"`:

```python
q_merged = pd.concat([q_combined, q_new], ignore_index=True)
...
q_merged = q_merged.drop_duplicates(subset=q_dedup_subset, keep="last")
```

(`postprocessing_maintenance_long_term.py:357-375`). Because `q_new`'s rows sort after
`q_combined`'s in the concatenation, `keep="last"` means every regenerated row for that year
range — gap or not — overwrites the previously-written row at the same key. So when an unrelated
key in the same year range is genuinely a gap, flag-OFF quarterly processing incidentally
refreshes every other ensemble row for that year range too, including ones whose membership had
silently changed. Other operational and recalculation writers elsewhere in the pipeline also
regenerate aggregates outright. **The defect this issue files is that nothing *detects* the
staleness case described above — not that regeneration of a stale aggregate is structurally
impossible.** An implementer must not rely on this incidental path as a substitute for detection:
it only fires when an unrelated gap happens to exist in the same scan window, is scoped to
quarterly with the flag off, and does nothing at all for monthly, seasonal, or flag-on quarterly.

## Boundary with PP-041: trigger versus disposal

**PP-041** (`doc/plans/issues/mid_prio_gi_draft_pp_longforecasts_stale_invalidation.md`, Medium,
Draft) covers *invalidating* a `long_forecasts` row that regeneration no longer emits — a
forecast-side tombstone, the analogue of the shipped skill-side tombstoning. Read together with
this issue, the two do not overlap and nothing falls between them:

- **PP-063 (this issue) owns the trigger**: selecting and regenerating the groups whose inputs
  changed — *including groups whose regeneration produces no replacement row*. Ownership attaches
  to the decision to recompute, before the output of that recomputation is known. Without this
  issue's fix, PP-041's tombstone logic is never invoked for a stale-but-still-keyed aggregate,
  because nothing identifies that group as needing regeneration in the first place.
- **PP-041 owns the disposal**: what happens to a row that survives when a regeneration PP-063
  triggers emits no replacement — tombstone production and read-side/display suppression. PP-041
  does not decide *when* to regenerate; it decides what to do with the result.
- **Null-tombstone handling** splits the same way: **PP-063 owns null-row eligibility and
  reconsideration** — whether a null-discharge row should have been produced at all for a given
  key, and whether that decision is revisited when the underlying member set changes (see the
  null-value blind spot above). **PP-041 owns tombstone production and suppression** — once a row
  is null, how it is marked and hidden downstream. A null row must not be treated as a permanent
  gap-filler by either side: PP-063's detector must be able to reconsider it if inputs change, and
  PP-041's suppression must not be read as also deciding when reconsideration happens.

Neither issue restates the other's fix. An implementer picking up either file should read this
section as authoritative for where the line falls, rather than re-deriving it from the "stale
aggregate" language that appears in both files' history.

**Sibling, not owner overlap — PP-061.** `_write_aggregated_forecasts_to_api`
(`apps/postprocessing_forecasts/src/api_writer.py`) hardcoding `flag=0` on aggregated-writer
output, tracked separately as **PP-061**
(`doc/plans/issues/high_prio_gi_draft_pp_aggregated_writer_clobbers_recovery_flag.md`), is a
sibling defect on the same write path: PP-061 is about what flag value a write stamps, this issue
is about whether a write is triggered at all. Fixing one does not fix the other. See PP-061 for
its own details; not restated here.

## Acceptance contract

A fixture must demonstrate all of the following, and only fail if any is violated:

1. **No unrelated gap present anywhere in the fixture's scan window.** This is required precisely
   because of the incidental-refresh path documented above: with an unrelated gap in scope, a
   flag-OFF quarterly refresh could accidentally repair the target aggregate and mask a
   still-broken detector. The fixture must isolate the membership-change case from that path.
2. **An existing aggregate with two members** at a chosen long-term key (any one of monthly,
   quarterly, or seasonal grain — station code `19999`), each member itself present and eligible.
3. **A third member newly eligible** for that same key after the aggregate was last written (e.g.
   a model that only became eligible after the row was originally written).
4. Running the detector-plus-regeneration path must **persist the changed composition and
   values** at the complete seven-column natural key `(horizon_type, horizon_value, code, date,
   model_type, valid_from, valid_to)` — the same key PP-041 uses for `long_forecasts` — not merely
   at the detector's own grouping key. A fix that regenerates the right group but writes at the
   wrong grain would pass detection and still fail this check.
5. **Unrelated keys in the fixture must be verified unchanged** — same values, same flags — after
   the run, so a fix cannot pass by widening its regeneration scope indiscriminately.
6. The aggregate must be **detected as needing regeneration**, not refreshed by accident. The
   test must be able to tell these apart — e.g. by asserting the detector's output set contains
   the target key before regeneration runs, not only by asserting the post-run value is correct.
7. **An accidental null row with otherwise-eligible members** must be included as a distinct case:
   a key whose current row has a null discharge value despite its member set being complete and
   eligible. The fixture must prove this key is **reconsidered** when its inputs change (i.e. it
   is not treated as a permanent gap-filler that the detector skips because a row already exists
   there) — exercising the null-value blind spot and the null-row eligibility ownership described
   above.

## Hard rules

- No real station codes, discharge values, or credentials in this issue or any fixture built from
  it — station code `19999` only.
- All line numbers above are verified at `89a6ffc7`; if the referenced files change, re-verify by
  symbol name (`detect_missing_ensembles`, `detect_missing_monthly_ensembles`,
  `detect_missing_quarterly_ensembles`, `detect_missing_seasonal_ensembles`,
  `_normalize_combined_forecasts`) rather than trusting stale line numbers.
- This file carries its own ID (**PP-063**) in the body. Its `doc/plans/module_issues.md` index
  row is deferred to a separate follow-up (tracked as INFRA-057) and is not added here.

## References

- **PP-041** — `doc/plans/issues/mid_prio_gi_draft_pp_longforecasts_stale_invalidation.md` —
  disposal-side (tombstone/invalidation) counterpart. See "Boundary with PP-041" above.
- **PP-061** — `doc/plans/issues/high_prio_gi_draft_pp_aggregated_writer_clobbers_recovery_flag.md`
  — sibling defect on the same aggregated-writer path (flag value, not trigger). Not restated
  here.
- `apps/postprocessing_forecasts/src/gap_detector.py` — all four detectors.
- `apps/postprocessing_forecasts/postprocessing_maintenance_long_term.py` — quarterly gap-fill
  block, incidental-refresh behaviour under lead-aware flag OFF.
- `apps/postprocessing_forecasts/src/data_reader.py` — `_normalize_combined_forecasts`,
  `season_in_year` derivation.
