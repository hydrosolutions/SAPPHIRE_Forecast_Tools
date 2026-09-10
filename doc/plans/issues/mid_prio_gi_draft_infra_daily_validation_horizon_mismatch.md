# INFRA-055: `daily` validates only one horizon after running both

**Status**: Draft (2026-09-10)
**Module**: `apps/run_locally.sh` + `apps/validate_pipeline/validate_pipeline.py`
**Priority**: Medium — this is the false-PASS-on-half-the-run shape the repo has been
closing (PP-051, ML-021, INFRA-047, INFRA-053), which argues for High. Weighing against
that: `validate_pipeline` has no production invoker (per INFRA-050's finding, restated
here rather than re-argued) — the gate exists to catch problems *before* a deployment,
not while one runs, so the blast radius is a misled developer, not a masked production
failure. Scoped narrower than the family's High members too: this is one target's
aggregate call silently narrowing to one horizon, not an entire module invisible to
every check (INFRA-020) or a freshness/presence check passing on absent data across many
call sites (INFRA-026).
**Labels**: `infra`, `run_locally`, `validate_pipeline`, `silent-success`, `mode-resolution`
**Found**: 2026-09-10, surfaced while filing INFRA-054 during review of PR #507 (`ML_MODE`
removal) — both issues were explicitly deferred there as out of scope.

---

## Defect

`run_daily_pipeline` (`apps/run_locally.sh`) runs its forecast and maintenance phases for
**both** horizons regardless of the ambient `SAPPHIRE_PREDICTION_MODE`: the forecast
phase hard-codes `for mode in PENTAD DECAD` (`:1780`) and the maintenance phase does the
same (`:1805`). The invocation-time value is captured once (`:1759`) and restored
(`:1830`) after those loops — before Phase 5 (long-term) and before the run's single
aggregate validation call, `run_api_validation "daily"` (`:1854`).

`run_api_validation` (`:1463-1484`) invokes `validate_pipeline.py` with only
`--target "$target"` (`:1471-1472`) — no `--horizon` argument. `validate_pipeline.py`'s
`resolve_horizons` (`:1335-1349`) then derives the horizon list itself: for any target
other than `"long-term"` (so `"daily"` included), it reads the *ambient*
`SAPPHIRE_PREDICTION_MODE` env var and maps it through `MODE_TO_HORIZONS`
(`:78-82`: `PENTAD`→`["pentad"]`, `DECAD`→`["decade"]`, `BOTH`→`["pentad","decade"]`),
falling back to `["pentad"]` for any other value, including empty (`:1349`).

Net effect, confirmed by tracing the code: with `SAPPHIRE_PREDICTION_MODE` unset at
invocation, `daily` runs ML, LR and postprocessing for PENTAD **and** DECAD, restores the
(empty) invocation-time value before validating, and `resolve_horizons` then falls back
to `["pentad"]` — so the run's single validation call checks pentad data only. A missing
or stale DECAD output from that run passes the run's final validation summary silently.

This is specific to `daily` among the aggregate targets. `run_short_term_pipeline` and
`run_maintenance_pipeline` derive *which horizons to dispatch* from the same
`original_mode` variable they later restore before validating (`:1545-1557`,
`:1601-1603`), so whatever mode value survives to validation time accurately describes
what was dispatched. `run_daily_pipeline` is the one target whose dispatch is fully
decoupled from the ambient mode (always both, per the hard-coded loops above) while its
validation call still depends on that ambient mode — which is exactly what creates the
mismatch.

## Pre-existing, and NOT grown by PR #507 the way it first appeared

Both halves of this mechanism — the hard-coded `for mode in PENTAD DECAD` loops and
`run_api_validation`/`resolve_horizons`'s ambient-mode fallback to `["pentad"]` — predate
PR #507 (`dac4cbbb`, merged 2026-09-09) unchanged; the defect is pre-existing.

**Correction to the premise checked here**: it is tempting to assume PR #507 *widened*
this gap because it made `machine_learning` in `daily` run both horizons where it
previously ran only one. Checking the pre-#507 code (`git show dac4cbbb^:apps/run_locally.sh`)
shows this is not the right read. Before #507, `machine_learning` in `daily` was filtered
by the now-removed `ML_MODE` (default `DECAD`) via `should_skip_ml_for_mode`: under the
`PENTAD` iteration of both hard-coded loops it was skipped entirely (`elif
should_skip_ml_for_mode "$mode"` branches, both loops), and under the `DECAD` iteration it
ran. So `machine_learning`'s DECAD output in `daily` **already existed and was already
unvalidated** before #507 — the aggregate check always defaulted to pentad-only,
regardless of `ML_MODE`. What #507 changed is that `machine_learning` now *also* produces
PENTAD output in `daily` (the iteration that used to be skipped) — and PENTAD is exactly
the horizon the aggregate check does cover, so that new output is validated, not missed.
Net effect on this specific defect: the pre-existing DECAD blind spot for
`machine_learning` is unchanged in volume by #507; if anything, #507 closed a gap on the
PENTAD side rather than opening one on the DECAD side. `linear_regression` and
`postprocessing_forecasts` were unaffected by #507 either way — both already ran full
PENTAD+DECAD in `daily`'s hard-coded loops before and after, so their DECAD output was
and remains equally unvalidated by this same mechanism.

## Contrast with the per-module validation path

`run_module_validation` (`:1497-1536`) takes an optional label-suffix argument
(INFRA-037), and the bare `machine_learning` target uses it: it loops over every mode it
actually ran and calls `run_module_validation "machine_learning" "$mode"` per mode
(`:2679-2682`), giving PENTAD and DECAD their own PASS/FAIL summary rows. That mechanism
was built and wired for exactly this class of problem. The aggregate `--target` path that
`run_daily_pipeline` uses did not get the same treatment — `run_api_validation` still
calls `validate_pipeline.py` with a single `--target`, and horizon selection falls
through to the ambient-env-var default described above. That asymmetry — one call path
made mode-aware, the sibling path left dependent on an ambient variable that no longer
reflects what ran — is the sharpest way to state this defect.

## Relation to INFRA-020 and INFRA-026

Distinct from both, by mechanism:

- **INFRA-020** (`validate_pipeline --module machine_learning` matches zero checks) is a
  Tier-1 check-tagging gap: no check is tagged `module="machine_learning"`, so the module
  filter matches nothing regardless of which horizons were requested. This issue is
  upstream of that — it is about which horizons `resolve_horizons` decides to check *at
  all* for the `--target daily` path, before any per-check module tagging is consulted.
- **INFRA-026** (validation reports PASS/fresh on absent data) covers norm-only rows,
  freshness-exclusion of absent datasets, and per-station absence hidden by aggregate
  counts — all cases where data-content interpretation masks an absence that a check *did*
  look at. This issue's data isn't misinterpreted; it is never looked at, because the
  horizon was excluded from the check list before any row was queried.

Same broader false-PASS-on-incomplete-evidence family as both, but neither issue's fix
would address this one, and this one's fix would not address either of theirs.

## Out of scope

- Whether `run_short_term_pipeline`/`run_maintenance_pipeline` should default to both
  horizons when unset — that is INFRA-054, a distinct defect (production-parity of what
  *runs*, not what gets *validated* afterward).
- INFRA-020's Tier-1 tagging gap and INFRA-026's freshness/presence masking — referenced
  for contrast, not fixed here.
- Any change to `run_module_validation`'s existing per-module, per-mode behavior — already
  correct; used here only as the contrasting reference implementation.

## Acceptance criteria

- [ ] After `bash apps/run_locally.sh daily` with `SAPPHIRE_PREDICTION_MODE` unset, the
      aggregate validation step checks both pentad and decade data, matching what the run
      actually dispatched.
- [ ] A test pins that `run_api_validation "daily"` (or `resolve_horizons` given
      `target="daily"` and an empty/unset mode) resolves to both horizons, not just
      `["pentad"]`.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero
      unexpected skips.

---

## Related

| ID | Relation |
|---|---|
| INFRA-054 | Found in the same review pass; a distinct defect — local runs cover fewer horizons than production, rather than this issue's "ran both, validated one" |
| INFRA-037 | Source of the mode-aware label-suffix mechanism (`run_module_validation`) that this issue's aggregate path lacks |
| INFRA-020 / INFRA-026 | Same false-PASS family, different mechanism — see "Relation" section above |
| PP-051 / ML-021 / INFRA-047 / INFRA-053 | Same failure-visibility family this repo has been closing |
