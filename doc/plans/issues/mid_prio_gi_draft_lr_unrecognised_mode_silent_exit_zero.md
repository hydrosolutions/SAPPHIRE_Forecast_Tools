# LR-012: `linear_regression.py` exits 0 having written nothing when `SAPPHIRE_PREDICTION_MODE` is unrecognised

**Status**: Draft
**Module**: `apps/linear_regression/linear_regression.py`
**Priority**: Medium — silent no-op, exit 0, no error. Guarded at the `run_locally.sh` entry
point (INFRA-039) but **not at the module boundary**, so a smaller-than-originally-claimed set of
other invocation paths is still exposed (see "Exposure" below — the original filing overstated
this).
**Labels**: `linear_regression`, `silent-noop`, `input-validation`
**Found**: 2026-08-21, out-of-loop review of INFRA-039, recorded there as out-of-scope because it
is a module-level fix reachable from entry points that patch did not touch. Filed 2026-08-24 on
branch `docs_infra040_lr012_followups`, which was later abandoned before the file reached
`module_issues.md` on trunk. Salvaged and corrected against trunk `e09d48d5` on 2026-09-09 (see
"Corrections from the original draft" below).
**Related**: **INFRA-039** (PR [#477](https://github.com/hydrosolutions/SAPPHIRE_Forecast_Tools/pull/477),
merged 2026-08-24, added the `run_locally.sh`-side domain check; this is the module-side half).
Its plan/issue file is now `doc/plans/issues/review_gi_draft_infra_run_locally_unvalidated_modes.md`,
**Status: Review**. **LR-010** / **LR-011** are *not* duplicates — both concern API
**write-failure** reporting, not mode handling. **INFRA-038** (`mid_prio_gi_draft_infra_connect_to_ieh_boolean_parsing.md`,
Draft) is the same *case-sensitivity* defect class in a different variable — see "Case
sensitivity" below.

---

## Defect

`linear_regression.py`'s `main()` resolves the mode with an empty-string-to-`BOTH` fallback
(`apps/linear_regression/linear_regression.py:634`):

```python
prediction_mode = os.getenv("SAPPHIRE_PREDICTION_MODE", "") or "BOTH"
logger.info(f"Running in {prediction_mode} prediction mode")
```

and then derives both horizon flags by exact-string membership, with no `else` and no
validation (`:646-647`):

```python
run_pentad = prediction_mode in ["PENTAD", "BOTH"]
run_decad  = prediction_mode in ["DECAD", "BOTH"]
```

Any value outside `{PENTAD, DECAD, BOTH}` — empty is handled by the `or "BOTH"` fallback, but
`ALL`, `MONTHLY`, `SEASONAL`, or a typo like `PENTAAD` is not — sets **both flags false**. The
log line at `:635` reports `Running in ALL prediction mode`, which reads as confirmation that the
value was accepted; nothing downstream contradicts it.

With both flags false, the per-horizon block inside the date loop starting at `:832`
(`if run_pentad and forecast_flags.pentad:` — see the comment at `:828-831` for the documented
reason both a `run_pentad` and a `forecast_flags.pentad` check are needed) is skipped for every
day, no forecast is produced, and the process falls through to `sys.exit(0)` at `:1096`.

## This is not a cheap no-op

Worth stating because "it does nothing" understates the cost and overstates the safety. With both
flags false, the module still:

- loads the environment (`sl.load_environment()`, `:631`) and performs SDK/SSH-tunnel setup
  (`:649` onward);
- calls `fl.get_pentadal_and_decadal_data(...)` **unconditionally** before the per-day loop even
  starts (`:765`), and that function itself forces `forecast_flags.pentad = True` and
  `forecast_flags.decad = True` as a documented side effect
  (`apps/iEasyHydroForecast/forecast_library.py:1301-1302`, comment: "This is required as a
  remnant from the previous implementation") and then unconditionally loads discharge data
  (`forecast_library.py:1315`, `read_daily_discharge_data(...)`), regardless of `run_pentad`/
  `run_decad`;
- enters the date loop and computes predictor dates (`linear_regression.py:816`, `:826`).

So it burns real I/O and wall-clock — one full discharge-data fetch per invocation — before the
per-day `run_pentad`/`run_decad` guards (added specifically to prevent exactly this class of
double-counting, per the comment at `:828-831`) ever get a chance to matter. It can fail partway
for unrelated reasons, and only then exits 0 without having written a forecast. A caller cannot
distinguish "no forecast was due today" from "the mode was not understood."

## OWNER DECISION (2026-09-09) — `ALL` is aliased to `BOTH`; fail loudly on everything else

There is a live contract conflict between three places that all touch
`SAPPHIRE_PREDICTION_MODE`:

- `doc/configuration.md:870` tells operators the variable may be `PENTAD`, `DECAD`, `BOTH`, **or
  `ALL`**, and lists `linear_regression` as one of the consumers.
- `apps/pipeline/pipeline_docker.py:639` declares the Luigi `LinearRegression` task's own
  parameter default as `prediction_mode = luigi.Parameter(default="ALL")`.
- `apps/linear_regression/linear_regression.py:54` and `:216` document only `PENTAD, DECAD, or
  BOTH (default: BOTH)` — `ALL` is not in the module's own domain, and (per the defect above) it
  silently makes both horizon flags `False`.

**Decision (owner, 2026-09-09): `ALL` is aliased to `BOTH` inside `linear_regression.py`.** This
is a decided design choice, not open for re-litigation in this issue. The fix must:

1. Accept `ALL` as an alias for `BOTH` (both horizons run) — resolving the doc/Luigi-default vs.
   module-domain conflict in favor of the documented, operator-facing contract, rather than by
   changing the documentation or the Luigi default.
2. Update the module docstring at `linear_regression.py:54` and `:216` to list `ALL` alongside
   `PENTAD, DECAD, BOTH`.
3. Treat any other value (case-correct or not) as invalid: log an error naming the variable, the
   offending value, and the full accepted set, then `sys.exit(1)` — **before** the discharge-data
   load at `:765`, so it fails fast rather than after minutes of I/O.

**Explicitly out of scope under this decision:** changing the Luigi `LinearRegression` task's
`default="ALL")` at `pipeline_docker.py:639`, and changing `doc/configuration.md:870`. Under this
decision both are already correct as written; only `linear_regression.py`'s own domain check and
docstrings are wrong.

## Exposure — corrected scope

The original filing of this issue (on the abandoned branch) claimed "every other invocation path"
is exposed and cited the production cron wrappers at the wrong line. Both are corrected here:

- **Line citation fix**: the hardcoded production values are at
  `bin/run_pentadal_forecasts.sh:92` (`-e SAPPHIRE_PREDICTION_MODE=PENTAD`) and
  `bin/run_decadal_forecasts.sh:92` (`-e SAPPHIRE_PREDICTION_MODE=DECAD`), not `:68` as originally
  cited. The conclusion these lines back — that the production cron path is **not** exposed —
  still holds; only the line number was wrong.
- **Scope fix**: reaching the `ALL` default at all requires invoking the Luigi `LinearRegression`
  task directly with no `--prediction-mode`, and both real Luigi entry-point workflows avoid this:
  `RunPentadalWorkflow.requires()` instantiates `LinearRegression(prediction_mode="PENTAD")`
  (`pipeline_docker.py:1480`) and `RunDecadalWorkflow.requires()` instantiates
  `LinearRegression(prediction_mode="DECAD")` (`:1542`). No `bin/` shell script invokes the bare
  `LinearRegression` or `PostProcessingForecasts` Luigi tasks directly (checked: no match for
  `PostProcessingForecasts` or a bare `LinearRegression` target in `bin/`).
- There is a **second**, previously undocumented, equally latent path: `PostProcessingForecasts`
  (`pipeline_docker.py:802`, `prediction_mode` defaulting to `"PENTAD"` at `:806`) passes its own
  `self.prediction_mode` straight through to its own `LinearRegression` dependency with no domain
  check (`:813`, `dependencies = [LinearRegression(prediction_mode=self.prediction_mode)]`). A
  direct `luigi ... PostProcessingForecasts --prediction-mode ALL` (or any other unrecognised
  value) invocation would reach the same defect via this second class. Like the primary path,
  this requires a direct Luigi CLI invocation of a specific task class — no `bin/` script does
  this either, so it does not change the overall "latent, not live" conclusion.
- **Confirmed still exposed**: `bin/locally_run_forecast_tools.sh` forwards an operator-supplied
  `SAPPHIRE_PREDICTION_MODE` unvalidated (export guard `:33-34`, LR invocation at `:113`), but the
  script marks itself deprecated at `:4` (`# DEPRECATED: Use apps/run_locally.sh instead.`).
  Any direct `python linear_regression.py` invocation, including inside the container, is also
  unvalidated.
- **INFRA-039** (PR #477, merged 2026-08-24) closed the `apps/run_locally.sh` `validate_env` gap
  for the targets that dispatch LR — its guard is the `case "$target" in ... PENTAD|DECAD|BOTH`
  block at `apps/run_locally.sh:1977`. It explicitly did not touch this module-level path (see its
  file's own "Scope" section). Its current status on trunk is **Review**, file
  `doc/plans/issues/review_gi_draft_infra_run_locally_unvalidated_modes.md`.

Net correction: this is a **latent** defect (no shipped shell script or the two real Luigi
workflows can trigger it), not a live one — reachable only via a direct, argument-omitting or
argument-supplying Luigi CLI invocation of `LinearRegression` or `PostProcessingForecasts`, or via
the deprecated `bin/locally_run_forecast_tools.sh`, or a direct module invocation.

## Case sensitivity — same defect class as INFRA-038

The membership tests at `:646-647` (`prediction_mode in ["PENTAD", "BOTH"]` /
`in ["DECAD", "BOTH"]`) are exact-string comparisons with no case normalisation. Lowercase or
mixed-case values (`pentad`, `decad`, `both`, `all`) fail exactly the same way as an unrecognised
value — both flags silently `False`, `sys.exit(0)`. This should be fixed by the same change (case
normalisation, e.g. `.strip().upper()`, before the domain check), rather than treated as a
separate issue.

This is the same *class* of defect as **INFRA-038**
(`mid_prio_gi_draft_infra_connect_to_ieh_boolean_parsing.md`, Draft) — a boolean/enum-like
environment variable compared with case-sensitive exact-match logic across the codebase, where an
unexpected case produces a silent wrong branch instead of a loud rejection. INFRA-038 covers
`connect_to_iEH`/`ssh_to_iEH`; this is the same shape in `SAPPHIRE_PREDICTION_MODE`. Not proposing
to merge the two issues — different variables, different modules — but a fix here should not
reintroduce the pattern INFRA-038 is about to remove elsewhere.

## Desired outcome

Validate at the module boundary and fail loudly, matching the closest existing sibling pattern in
the same tier of consumers, `postprocessing_maintenance.py:124-130`:

```python
prediction_mode = os.getenv("SAPPHIRE_PREDICTION_MODE", "") or "BOTH"
if prediction_mode not in ["PENTAD", "DECAD", "BOTH"]:
    logger.error(
        f"Invalid SAPPHIRE_PREDICTION_MODE: {prediction_mode}. "
        f"Expected one of {valid_modes}."
    )
    sys.exit(1)
```

adjusted per the owner decision above to also alias `ALL` to `BOTH` before the check (unlike
`postprocessing_maintenance.py`, whose own domain does not include `ALL`). `postprocessing_operational.py:244-251`
is a second, slightly-further precedent: it already accepts `ALL` (and `MONTHLY`) alongside
`PENTAD`/`DECAD`/`BOTH` and rejects everything else with the same log-then-`sys.exit(1)` shape —
useful as a second reference for the error-message wording, though its domain is wider than LR's
and should not be copied verbatim.

- Accept `PENTAD`, `DECAD`, `BOTH`, and unset/empty (which must keep resolving to `BOTH` — that is
  the documented default and callers rely on it, `:54`/`:216`).
- Accept `ALL` as an alias for `BOTH` (owner decision above).
- Normalise case before the domain check (see "Case sensitivity" above).
- Anything else: log an error naming the variable, the offending value, and the accepted set
  (`PENTAD`, `DECAD`, `BOTH`, `ALL`), then `sys.exit(1)`.
- Do **not** silently map unknown values onto a horizon.

## What a fix must not break

- Unset/empty must still resolve to running both horizons — the `or "BOTH"` default is a
  documented contract, not a bug.
- `PENTAD`, `DECAD`, `BOTH` (and now `ALL`) must behave exactly as `BOTH`/their own horizon does
  today.
- `--hindcast` and the other CLI paths must be unaffected for valid modes.
- The CSV write path and its existing behavior are out of scope — this is a mode-validation fix,
  not a write-path change.
- Do not change `pipeline_docker.py:639`'s Luigi default or `doc/configuration.md:870` — the owner
  decision above treats both as already correct.

## Out of scope

- The `run_locally.sh`-side check (shipped, INFRA-039, PR #477).
- `bin/locally_run_forecast_tools.sh`'s own passthrough — fixing the module makes it fail loudly,
  which is sufficient; hardening the deprecated wrapper is optional follow-up, if it is not simply
  removed as part of the module's ongoing deprecation.
- LR-010 / LR-011's API write-failure reporting.
- Any change to `pipeline_docker.py`'s Luigi parameter defaults or to `doc/configuration.md` (see
  owner decision above).

## Acceptance criteria

1. `SAPPHIRE_PREDICTION_MODE=ALL python linear_regression.py` runs **both** horizons (the aliasing
   decision), not an error.
2. `SAPPHIRE_PREDICTION_MODE=MONTHLY python linear_regression.py` (and `SEASONAL`, and a typo like
   `PENTAAD`) exits **non-zero**, naming the variable, the value, and the accepted set — and does
   so **before** the discharge-data load at `:765`, so it fails fast rather than after minutes of
   I/O.
3. `PENTAD`, `DECAD`, `BOTH` behave exactly as today.
4. Lowercase/mixed-case variants of all accepted values (`pentad`, `decad`, `both`, `all`) are
   accepted via case normalisation, not rejected and not silently dropped.
5. **Unset and empty still resolve to `BOTH`** and run both horizons — this is the regression
   guard that matters most; the `or "BOTH"` default must not be broken by the new check.
6. `--hindcast` and the other CLI paths are unaffected for valid modes.
7. Unit tests in `apps/linear_regression/` cover 1-5, asserting exit status and message content
   rather than internal flags. No real station codes in new tests — use a placeholder such as
   `19999`.
8. `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero new skips.

## Corrections from the original draft

This issue was filed 2026-08-24 on branch `docs_infra040_lr012_followups`, which was abandoned
before it reached `module_issues.md` on trunk. It is salvaged here with the following corrections
made against trunk `e09d48d5` (2026-09-09):

- The hardcoded-mode citations for the production cron wrappers were `bin/run_pentadal_forecasts.sh:68`
  / `bin/run_decadal_forecasts.sh:68` in the original; the correct line in both files is `:92`.
- The `sl.load_environment()` citation was `:631` in the original. Re-verified directly against
  trunk (`grep -n "sl.load_environment()"`): **`:631` is in fact correct** — this issue does not
  carry forward a suggested correction to `:630` (that line is a comment, `# Configuration`) that
  was proposed during salvage triage but could not be confirmed.
- The claim "every other invocation path is still exposed" was too broad. Corrected to the
  narrower, directly-verified set in "Exposure — corrected scope" above, which also adds a
  previously undocumented second latent path through `PostProcessingForecasts`.
- Added: the case-sensitivity gap (no normalisation of `PENTAD`/`DECAD`/`BOTH`/`ALL`), not present
  in the original draft.
- Added: the owner decision that `ALL` is aliased to `BOTH`, resolving the `doc/configuration.md`
  / `pipeline_docker.py` Luigi-default / module-docstring conflict that the original draft did not
  address (it treated only `PENTAD`/`DECAD`/`BOTH` as the target domain and would have made the
  Luigi default `ALL` an accepted-but-undocumented value at best, or a newly-introduced hard
  failure on every direct `LinearRegression` invocation at worst).
- The original draft attributed INFRA-039 to "PR #477" without an inline verification note;
  verified here via `gh pr view 477` (title "INFRA-039: validate SAPPHIRE_PREDICTION_MODE and
  ML_MODE at entry", merged 2026-08-24) — the attribution is correct and is kept.
- INFRA-039's issue file has since moved from a "Draft" reference to its current name
  `review_gi_draft_infra_run_locally_unvalidated_modes.md`, **Status: Review** — updated from the
  original draft, which referenced it before that rename.

## References

- `apps/linear_regression/linear_regression.py:54, 216` (docstring domain), `:631`
  (`sl.load_environment()`), `:634` (mode resolution), `:646-647` (horizon flags), `:765`
  (unconditional `get_pentadal_and_decadal_data` call), `:816, :826` (date loop / predictor
  dates), `:828-831` (comment documenting the `run_pentad`/`forecast_flags.pentad` double-guard),
  `:1096` (`sys.exit(0)`)
- `apps/iEasyHydroForecast/forecast_library.py:1301-1302` (forced `forecast_flags.pentad/decad =
  True`), `:1315` (unconditional discharge load)
- `bin/run_pentadal_forecasts.sh:92`, `bin/run_decadal_forecasts.sh:92` (hardcoded production
  values)
- `bin/locally_run_forecast_tools.sh:4` (deprecation notice), `:33-34` (mode passthrough guard),
  `:113` (LR invocation)
- `apps/pipeline/pipeline_docker.py:639` (`LinearRegression` Luigi default `ALL`), `:806`, `:813`
  (`PostProcessingForecasts` default and pass-through), `:1480`, `:1542` (real workflow call
  sites)
- `doc/configuration.md:870` (documented domain including `ALL`)
- `apps/run_locally.sh:1977` (INFRA-039's `validate_env` guard)
- `apps/postprocessing_forecasts/postprocessing_maintenance.py:124-130` (closest existing
  fail-loudly precedent), `apps/postprocessing_forecasts/postprocessing_operational.py:244-251`
  (precedent that already accepts `ALL`)
- `apps/postprocessing_forecasts/recalculate_skill_metrics.py:94-103` (`VALID_MODES`, for context
  on how wide the `ALL`/`MONTHLY` domain is for other consumers)
- Precedent / related: `doc/plans/issues/review_gi_draft_infra_run_locally_unvalidated_modes.md`
  (INFRA-039), `doc/plans/issues/mid_prio_gi_draft_infra_connect_to_ieh_boolean_parsing.md`
  (INFRA-038, same case-sensitivity defect class)
