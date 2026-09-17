# LTF-013: the recovery does not exclude non-operational (calibration-only) modes

**Status**: Draft (2026-09-17)
**Module**: `apps/long_term_forecasting/lt_recovery.py`, `apps/long_term_forecasting/config_forecast.py`
**Priority**: **Low** — reaching it needs an operator error on top of a deployment-specific
precondition (see "Preconditions" below), and `doc/prod/long_term_recovery_runbook.md` warns
against it once that documentation lands. Not low because it is harmless: see "Why it matters".
**Labels**: `ltf`, `recovery`, `mode-validation`
**Found**: 2026-09-1x, review of the long-term recovery path (same sweep as **LTF-011**,
**LTF-012**). Filed separately from LTF-012 on the reviewer's point that shared authorship and
small size are not reasons to couple two unrelated code paths into one issue's acceptance
criteria — LTF-012 is a reporting defect in `run_forecast.py`'s `run_single_model`; this is a
missing-guard defect in `lt_recovery.py`'s `run_recovery`. Different files, different failure
shapes, no shared fix.
**Related**: **LTF-011** (splits `run_recovery`'s exit-2 meaning; unaffected by this issue — this
adds a new refusal reason inside the existing taxonomy, it does not change how refusals are
reported), **LTF-012** (sibling from the same review; see above — not restated here).

---

## What happens now

`apps/long_term_forecasting/lt_schedule_query.py` defines, at module scope:

```python
# Modes used only for calibration / retraining, not operational scheduling.
# Keep them in ieasyhydroforecast_ml_long_term_supported_modes so the
# maintenance pipeline can reference them, but skip in query_schedule().
NON_OPERATIONAL_MODES = {"monthly"}
```

(`lt_schedule_query.py:57`, verified at `89a6ffc7`). `query_schedule` — the function the
operational scheduler calls to decide which long-term modes are due — checks it before doing
anything else:

```python
    for mode in supported_modes:
        if mode in NON_OPERATIONAL_MODES:
            skipped_modes[mode] = "non-operational (calibration/retraining only)"
            continue
```

(`lt_schedule_query.py:88-91`). So the operational scheduler never attempts `"monthly"`. The
comment states why the mode still has to live in
`ieasyhydroforecast_ml_long_term_supported_modes`: the calibration entry point,
`calibrate_and_hindcast.py`, reads that same env var and is documented to accept it —
`calibrate_and_hindcast.py:8` and `:439` both show `lt_forecast_mode=monthly python
calibrate_and_hindcast.py --all` as the intended invocation. `"monthly"` is therefore a
real, supported, documented value of `lt_forecast_mode` — just not an operational one.

`apps/long_term_forecasting/lt_recovery.py`'s `run_recovery` (`lt_recovery.py:584-775`,
verified at `89a6ffc7`) has no equivalent check anywhere in its body. `grep -n
"NON_OPERATIONAL\|monthly" apps/long_term_forecasting/lt_recovery.py` (excluding tests) returns
nothing. The mode string is validated only for non-emptiness (`lt_recovery.py:638-640`) and then
handed straight to `config_factory(mode)` (`lt_recovery.py:645`).

The recovery is invoked from `run_forecast.py --recover --today YYYY-MM-DD`, which reads the same
env var and passes it through unmodified:

```python
        sys.exit(
            run_recovery(
                issue_date=args.today,
                forecast_mode=os.getenv("lt_forecast_mode"),
                run_forecast_fn=run_forecast,
                station_codes_fn=_read_station_codes,
            )
        )
```

(`run_forecast.py:609-616`, verified at `89a6ffc7`). Nothing at this call site or inside
`run_recovery` distinguishes `lt_forecast_mode=monthly` (calibration-only, per
`lt_schedule_query.py:57`) from `lt_forecast_mode=month_0` (operational). An operator who has
seen `monthly` accepted as a legitimate `lt_forecast_mode` value for calibration — it is the
documented value for `calibrate_and_hindcast.py` — and who reads mode names out of the
long-term configuration directory (`ieasyhydroforecast_ml_long_term_configuration`, which
`monthly.json` sits in alongside the operational `month_N.json` / `quarter_N.json` /
`season_N.json` files) has no signal from `lt_recovery.py` that `monthly` is excluded from
recovery.

## Why it matters

This is not merely "the wrong mode gets rejected late instead of early" — the two things it can
actually do to the running system are at different depths, and the issue is understating the
defect if it does not separate them.

**Depth 1 — configuration writes, reached unconditionally.** `config_factory(mode)`
(`lt_recovery.py:645`, default implementation `_default_config_factory` at
`lt_recovery.py:578-581`) calls `ForecastConfig.load_forecast_config(forecast_mode)`
(`config_forecast.py:57`), which — after loading `monthly.json` and validating each member
model's path exists (`config_forecast.py:64-87`) — unconditionally calls
`self.synchronize_forecast_settings()` as its last step (`config_forecast.py:111`).
`synchronize_forecast_settings` (`config_forecast.py:157-180`) loops over every member model of
the mode and, for each, reads `model_config.json`/`general_config.json`/`feature_config.json`
(`config_forecast.py:113-140`, via `get_model_specific_config`) and then **overwrites
`general_config.json` on disk** with `prediction_horizon`, `offset`, `forecast_days` and
`allowable_missing_value_operational` taken from `monthly.json`
(`config_forecast.py:168-180`, the write itself at `:178-180`). This happens as soon as
`config_factory(mode)` returns successfully — before `member_model_types`, before
`resolve_scheduled_models`, before `check_station_codes`, and before the existing-member-row
guard (`lt_recovery.py:647-689`) has run at all. So **calling `run_recovery` with
`forecast_mode="monthly"` rewrites every calibration member model's `general_config.json` with
recovery-derived settings unconditionally** — reachable from nothing more than a valid
`monthly.json` existing on the deployment, regardless of what happens afterward (including a
guard refusal for an unrelated reason, such as pre-existing member rows).

**Depth 2 — the forecast attempt itself, reached only if several further conditions hold.**
Getting from the config write to `run_forecast_fn` actually running (`lt_recovery.py:722-732`,
Stage 2) additionally requires: `member_model_types(config)` to be non-empty
(`lt_recovery.py:647-652`); `resolve_scheduled_models` to find the requested `effective_date`
matching a scheduled issue date for at least one member model of `monthly.json`, not merely
close to one (`lt_recovery.py:654`, `:310-356` — a near-miss raises `RecoveryRefused`, not a
snap); `check_station_codes` to return a non-empty list (`lt_recovery.py:655`, `:359-`); and the
existing-member-row guard (`pre_count`, `lt_recovery.py:674-689`) to find zero member rows already
present for `(horizon_type, horizon_value, effective_date)` under `monthly`'s member models. All
of these depend on the specific shape of `monthly.json` on a given deployment and are not
guaranteed to align — so the depth an operator actually hits from a single mistaken invocation
is deployment-dependent: every invocation reaches Depth 1, only some reach Depth 2. State both,
and do not imply the forecast attempt (Depth 2) is guaranteed just because the config write
(Depth 1) is.

## Preconditions — stated completely

Reaching Depth 1 requires all of:

1. The deployment has a `monthly` config file present under
   `ieasyhydroforecast_ml_long_term_configuration` (kept there deliberately, per
   `lt_schedule_query.py:54-57`, for the calibration pipeline).
2. `"monthly"` is listed in `ieasyhydroforecast_ml_long_term_supported_modes` — required for
   `ForecastConfig.load_forecast_config`'s own assertion (`config_forecast.py:58-60`) to pass;
   this is the same env var `lt_schedule_query.py`'s comment says the mode is deliberately kept
   in.
3. An operator manually sets `lt_forecast_mode=monthly` when invoking
   `run_forecast.py --recover --today ...` — this is not a value the scheduler or any automated
   caller would ever supply; it requires a human reading mode names from the config directory or
   from `calibrate_and_hindcast.py`'s documented usage and reusing one in the wrong context.

Reaching Depth 2 additionally requires the deployment-specific alignment described above
(scheduled issue date match, no pre-existing member rows, non-empty member/station lists).

This is why the priority is **low, not why the defect is harmless**: it needs an operator error
on top of a deployment shape that not every environment has (`monthly.json` present and
`"monthly"` listed as supported). Once `doc/prod/long_term_recovery_runbook.md` lands — committed
on the unpushed branch `docs_lt_recovery_runbook` (`2e552e28`), not yet in this branch's base —
it is expected to document `monthly` as calibration-only and warn operators against passing it to
`--recover`, further narrowing the path to a documented-but-ignored warning. Reference the runbook
as pending at that commit; do not write a citation that does not resolve in `89a6ffc7`.

## The fix constraint

**Reject the mode before `config_factory(mode)` runs** (`lt_recovery.py:645`), not after. Because
Stage 1 loads and synchronizes each member model's configuration and writes `general_config.json`
as a side effect of loading (see "Depth 1" above), a check placed anywhere after the
`config_factory` call — inside the existing-row guard, inside `resolve_scheduled_models`, or as a
post-hoc validation of `model_types` — still leaves configuration writes on disk for a request
that was never valid. The check must gate entry to `config_factory`, mirroring how
`lt_schedule_query.py:88-91` gates entry to `config.load_forecast_config` in the scheduler: there,
the `NON_OPERATIONAL_MODES` check runs first in the loop body, before any config load is
attempted for that mode.

A reasonable shape: import or duplicate `NON_OPERATIONAL_MODES` (or a recovery-scoped constant
naming the same set, if importing from `lt_schedule_query.py` is judged to create an unwanted
coupling — that judgment call belongs to whoever implements this, not to this issue) and raise
`RecoveryRefused` for a non-operational mode inside Stage 1's `try` block, before the
`config = config_factory(mode)` line, with a message that says the mode is calibration-only and
names which stage would otherwise have written configuration for it.

## Files that may be modified

- `apps/long_term_forecasting/lt_recovery.py`
- `apps/long_term_forecasting/tests/test_lt_recovery.py`

**Do not** modify `apps/long_term_forecasting/lt_schedule_query.py`,
`apps/long_term_forecasting/config_forecast.py`, or `apps/long_term_forecasting/run_forecast.py`
— the fix is a guard added in the recovery path, not a change to how the scheduler defines
non-operational modes or how configuration loading writes `general_config.json`.

## Tests

Use `19999` as a placeholder station code if a fixture needs one.

1. **`forecast_mode="monthly"` is refused before any config work happens.** Call `run_recovery`
   with a `config_factory` double that raises (or records a call and raises) if invoked — assert
   it is **never called** for `forecast_mode="monthly"`, and that the return is `EXIT_REFUSED`.
   This is the acceptance-defining test: a bare "returns non-zero" assertion does not prove
   `config_factory` was skipped, only that the run ended unsuccessfully — which could also be
   true if the mode were rejected inside `config_factory` after it already wrote configuration.
2. **The API client and forecast entry point are untouched.** In the same test (or a companion
   using the same doubles), assert `client_factory` and `run_forecast_fn` are never called for a
   non-operational mode — proving the rejection happens strictly before Stage 1's guard and
   Stage 2, not merely before Stage 2.
3. **Operational modes are unaffected.** An existing or new test with `forecast_mode="month_0"`
   (or another operational mode already covered by the suite) must still reach
   `config_factory` and proceed exactly as before the fix — a regression guard that the new check
   is scoped to non-operational modes only.
4. **The set of excluded modes matches the scheduler's**, so the two do not silently drift apart
   in a future edit — e.g. by importing `NON_OPERATIONAL_MODES` from `lt_schedule_query` in the
   test and asserting the recovery's guard rejects every mode in it, or by asserting the two
   constants are equal if the fix duplicates rather than imports the set.

Check by hand that test 1 and test 2 fail against today's code (where `config_factory`,
`client_factory` and, given a suitable `monthly.json`, potentially `run_forecast_fn` all can be
reached for `forecast_mode="monthly"`), and say so in the report.

## Acceptance criteria

- [ ] `run_recovery` refuses `forecast_mode="monthly"` (and any other member of
      `NON_OPERATIONAL_MODES`) before `config_factory` is invoked — proved by test 1/2 above, not
      merely by a non-zero exit code.
- [ ] The refusal is `EXIT_REFUSED` with a message naming the mode as calibration-only.
- [ ] Operational modes reach `config_factory` exactly as before (test 3).
- [ ] The excluded-mode set cannot silently diverge from the scheduler's without a test failing
      (test 4).
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh long_term_forecasting` — zero failures,
      zero unexpected skips.
- [ ] `ruff check` / `ruff format --check` clean on the modified files.

## Phases

- **P1 — add the pre-`config_factory` guard.** Files: `lt_recovery.py`, `test_lt_recovery.py`.
  Depends on: none. Agents: 1. Accept: tests 1-4 pass; the two "fails against today's code" checks
  above are confirmed and reported.

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 }
  }
}
```

## Out of scope

- Any change to `NON_OPERATIONAL_MODES` itself, to `lt_schedule_query.py`'s scheduling logic, or
  to which modes are considered operational — this issue only closes the gap between the
  scheduler's exclusion and the recovery path's lack of one.
- LTF-011's exit-code taxonomy split — this issue adds a new refusal reason that fits inside the
  existing `RecoveryRefused` → `EXIT_REFUSED` mapping; it does not need LTF-011 to land first and
  does not change how any existing refusal is classified.
- LTF-012's reporting defect (`run_forecast.py`'s `run_single_model` reporting `SUCCESS` on an
  empty forecast) — unrelated code path, filed separately; see "Related" above.
- Fixing `calibrate_and_hindcast.py`'s or the calibration pipeline's use of `monthly` — that usage
  is correct and intentional; this issue is only about the recovery path accepting the same value
  without the scheduler's exclusion.
