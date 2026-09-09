# FD-026: dashboard's manual decade re-run buttons send `DECADE` instead of `DECAD` — operational decadal forecasting (cron/Luigi) is unaffected

**Status**: Draft
**Module**: `apps/forecast_dashboard` (fair game)
**Priority**: High — confined to the dashboard's two manual re-run buttons ("Save Changes",
"Trigger forecasts") when decade is selected; **operational decadal forecasting via cron/Luigi is
unaffected** (see "What is NOT affected"). Within that confined scope, it is reachable on every
decade-horizon use of either button and breaks **every** downstream consumer in that manual-run
chain, not just one. See "Severity reasoning" below.
**Labels**: `forecast-dashboard`, `silent-noop`, `naming-inconsistency`, `linear_regression`,
`machine_learning`, `postprocessing_forecasts`
**Found**: 2026-09-09, out-of-loop review of LR-012 (`mid_prio_gi_draft_lr_unrecognised_mode_silent_exit_zero.md`),
while verifying that issue's "latent, not live" exposure claim. A second, owner-directed pass the
same day mapped the **full** chain of modules the dashboard feeds this variable to (see "Full
chain" below) — the original filing of this issue understated its own scope by covering only LR
and citing ML as a side note. Verified directly against trunk `3791fa31`.
**Related**: **LR-012** (`mid_prio_gi_draft_lr_unrecognised_mode_silent_exit_zero.md`) — carries
the detailed defect analysis and the governing normalization decision for
`linear_regression.py` specifically; this issue (FD-026) is the dashboard's own root cause and
the umbrella tracker for the analogous fix needed in the other three modules the dashboard also
drives. **FD-008** (`high_prio_gi_draft_fd_inner_run_docker_error_handling.md`, repriced High
2026-09-09) is a separate, third defect: the dashboard swallows container failures instead of
surfacing them, which is why none of the failures documented below have been visibly breaking
anything — see "Related finding: FD-008" below. **P-007**
(`archive/high_prio_gi_draft_pipeline_container_exit_status_discarded.md`, Complete) is the same
exit-code-discarding shape, already fixed once in the Luigi pipeline; FD-008 is that same class
recurring in the dashboard's own, separate `run_docker_container` implementations.

---

## Summary

**Operational decadal forecasting is NOT affected by this defect.** The production cron path
(`bin/run_decadal_forecasts.sh:92`) and Luigi's `RunDecadalWorkflow`
(`apps/pipeline/pipeline_docker.py:1524-1557`) all pass the correct, hardcoded `DECAD` value —
decadal forecasts continue to be produced, stored, and displayed normally, exactly as the owner
observed. See "What is NOT affected" below for the full verification.

The defect is confined to the forecast dashboard's **two manual re-run buttons** — "Save
Changes" and "Trigger forecasts" — and only when an operator has the **decade** horizon selected.
Those two handlers are the one path in the whole codebase that derives
`SAPPHIRE_PREDICTION_MODE` from the horizon selector widget's own internal value
(`horizon.upper()`), and that widget's internal decade value is spelled `"decade"`, not
`"decad"` (`apps/forecast_dashboard/dashboard/widgets.py:99`) — producing `"DECADE"`, which
matches **no** downstream consumer's actual domain. Every module and script elsewhere in the
codebase that produces or documents this variable uses `DECAD`, never `DECADE` — the dashboard's
two manual-re-run handlers are the one place with the mismatched spelling, and it poisons every
container those two buttons launch, for the decade horizon only.

## What is NOT affected (verified 2026-09-09)

- **Production cron**: `bin/run_decadal_forecasts.sh:92` — `-e SAPPHIRE_PREDICTION_MODE=DECAD`
  (hardcoded, correct).
- **Luigi `RunDecadalWorkflow`** (`apps/pipeline/pipeline_docker.py:1524-1561`):
  `LinearRegression(prediction_mode="DECAD")` (`:1542`),
  `RunMLModel(model_type=model, prediction_mode="DECAD", run_mode="forecast")` for every enabled
  ML model (`:1549`), and `PostProcessingForecasts(prediction_mode="DECAD")` (`:1557`) — all
  three hardcoded, all correct.
- Consequently: decadal forecasts, decadal ML runs, decadal postprocessing, and decadal skill
  metrics from the **operational pipeline** are produced and displayed normally, on schedule,
  today. A reader must not come away from this issue believing decadal forecasting itself is
  broken — it is not. Only a **manual, dashboard-triggered re-run of a decade forecast** is
  affected, via "Save Changes" or "Trigger forecasts" specifically.

## Context

`create_horizon_selector` (`apps/forecast_dashboard/dashboard/widgets.py:90-110`) defines the
horizon dropdown's option values:

```python
horizon_types = {
    _("pentad"): "pentad",
    _("decade"): "decade",
}
if display_ML_forecasts:
    horizon_types[_("month")] = "month"
    horizon_types[_("season")] = "season"
```

`select_and_plot_data(...)` (`apps/forecast_dashboard/src/vizualization.py:3436`) reads the
selected value into a local `horizon` variable at `:3449`
(`horizon = wm.horizon_selector.value`), which two nested closures defined later in the same
function then use to build container environments — see "Full chain" below for exactly which
containers.

When `horizon == "decade"`, every handler below produces `SAPPHIRE_PREDICTION_MODE=DECADE`. That
value does not appear anywhere in `linear_regression.py`'s, `postprocessing_operational.py`'s,
`recalculate_skill_metrics.py`'s, or `make_forecast.py`'s own domain — all of them expect
`DECAD`. The mismatch is not a one-off typo: the *surrounding* code in `vizualization.py` itself
expects `"decad"`, e.g. the `else:  # decad` comments at `:4092` and `:4232` — meaning even the
author of the container-launch code assumed the value would be `"decad"`, not `"decade"`. The
widget is the one place using `"decade"`.

## Full chain: every module the dashboard feeds `SAPPHIRE_PREDICTION_MODE` to

Mapped by starting from each button handler's `environment` list and following every
`run_docker_container` call that receives it, per button:

**"Save Changes" — `save_to_database(event)` (`vizualization.py:3984`)**

`environment` built at `:4078-4082`, `SAPPHIRE_PREDICTION_MODE={horizon.upper()}` at `:4080`.
Three containers, run in sequence with the same `environment` (the third with one field
appended):

1. `:4131` — `mabesa/sapphire-linreg:latest`, default command → `linear_regression.py`
   (`apps/linear_regression/Dockerfile:34`). Domain (before LR-012):
   `{PENTAD, DECAD, BOTH}`. **Fails silently** (exits 0, writes nothing) — see LR-012.
2. `:4137-4138` — `mabesa/sapphire-postprocessing:latest`, default command →
   `postprocessing_operational.py` (`apps/postprocessing_forecasts/Dockerfile:38`). Domain
   (`postprocessing_operational.py:245`): `{PENTAD, DECAD, BOTH, MONTHLY, ALL}`. **Fails loudly**
   — `sys.exit(1)` at `:251` today, for every decade Save Changes click, independent of whether
   LR-012 ships.
3. `:4147-4157` — same image, **explicit** `command=["uv", "run", "recalculate_skill_metrics.py"]`
   override (`:4156`), plus `SAPPHIRE_RECALC_STATION_CODE={station_code}` appended to
   `environment` (`:4152`). Domain (`recalculate_skill_metrics.py`'s `VALID_MODES`, `:94-103`):
   `{PENTAD, DECAD, BOTH, MONTHLY, DAILY, QUARTERLY, SEASONAL, ALL}`. **Fails loudly** the same
   way, but this call is wrapped in its own `try`/`except Exception as e: logger.warning(...)`
   (`:4145` / `:4159-4160`) — deliberately non-fatal by design (its docstring comment says so),
   so this one failing does not block the reload that follows. Still means skill metrics are
   never actually recalculated for a decade Save Changes today.

**"Trigger forecasts" — `run_pipeline(event)` (`:4317`)**

`environment` built at `:4352-4357`, `SAPPHIRE_PREDICTION_MODE={horizon.upper()}` at `:4355`.
Three containers/loops, in sequence:

4. `:4400` — `mabesa/sapphire-linreg:latest` again. Same as (1).
5. `:4415-4421` — conditional on `ieasyhydroforecast_run_ML_models` (default `"True"`, i.e. **on
   by default**): for each configured ML model, `mabesa/sapphire-ml:latest` with `RUN_MODE=forecast`
   → `make_forecast.py` (`apps/machine_learning/Dockerfile:40`). `mode = horizon.upper()` is
   re-derived independently at `:4415` (same value, same bug). Domain
   (`make_forecast.py:605-607`): `{PENTAD, DECAD}` **only** — deliberately excludes `BOTH`/`ALL`,
   since ML runs one horizon per invocation. **Fails loudly** — `raise ValueError` at `:606-608`.
6. `:4432` — `mabesa/sapphire-postprocessing:latest`, same as (2).

So **every module in both flows** either silently no-ops or crashes on a decade action today.
None of this is specific to one consumer; it is the dashboard's own value that is wrong, feeding
a wrong value to four different modules with four different (correct, legitimately different)
domains.

## Governing decision (owner, 2026-09-09) — applies to every module in this chain except the dashboard itself

Recorded in full in **LR-012** (`## GOVERNING DECISION`); restated here because it governs the
fixes needed in the other three modules, not only `linear_regression.py`:

**Normalize spelling and case; keep each module's own domain.**

- Case-insensitive comparison everywhere (`pentad` ≡ `PENTAD`, etc.).
- `decade` ≡ `decad` — one horizon, two spellings, both accepted, in every module whose domain
  includes the decad horizon.
- `ALL` ≡ `BOTH` **within `linear_regression.py` only** (LR-012's scope) — not a claim about any
  other module's `ALL`.
- **Not a single global whitelist.** Each module keeps the domain it legitimately has:
  - `make_forecast.py` must gain `decade`≡`decad` and case-insensitivity **only** — it must
    **not** start accepting `BOTH` or `ALL`.
  - `postprocessing_operational.py` must gain `decade`≡`decad` and case-insensitivity **only** —
    its existing `ALL` (which already means "run pentad, decad, *and* monthly", per its
    `:268/:278/:288` branches — a wider meaning than LR's `ALL`) and `MONTHLY` must not change.
  - `recalculate_skill_metrics.py` must gain the same two additions **only** — its wider
    `VALID_MODES` (`DAILY`, `QUARTERLY`, `SEASONAL` included) must not change.
- Exit 1 (or raise, per each module's existing failure convention) only on a value that is
  genuinely unrecognised **after** normalization. Nothing that succeeds today may start failing.

This issue (FD-026) tracks applying that rule to `postprocessing_operational.py`,
`recalculate_skill_metrics.py`, and `make_forecast.py`, **and/or** fixing the dashboard's own
`"decade"` value at the source (see "Desired outcome" — both remain valid, non-exclusive
approaches, exactly as before this override; the override changes what "normalize" means, not
which side of the wire fixes it).

## Related finding: FD-008 (why none of this has been visibly failing)

Every "fails loudly" consequence above (postprocessing, skill-recalc, ML) should, in principle,
have been producing a visible dashboard error for as long as an operator has clicked either
button for the decade horizon. It has not, because the dashboard's own container-runner code
swallows the failure before it reaches the calling function:

- `save_to_database`'s nested `run_docker_container` (`vizualization.py:3858`) raises
  `docker.errors.ContainerError` on a non-zero exit code (`:3949-3961`), but that `raise` is
  caught by its own enclosing `except Exception as e: print(...)` (`:3970-3971`) — printed to the
  server console only, never surfaced to `save_to_database`, which proceeds to the next container
  regardless and eventually sets the progress bar to 100%.
- `run_pipeline` uses a **separate**, module-level `run_docker_container` (`:4491`), which is
  more direct still: on a non-zero exit code it only prints a message (`:4560-4562`, with the
  comment `# Optionally log the error or add to a list of failed containers`) and falls through —
  it never raises anything, so `run_pipeline` also always continues to the next step.

This is the exact defect shape as **P-007** (`pipeline_docker.py`'s `run_docker_container`
discarding `container.wait()`'s exit code across 20 Luigi call sites, fixed PR #478) — a failed
container is indistinguishable from a successful one at the calling layer. It is tracked as its
own **pre-existing** issue, **FD-008**, filed during FD-007's review (before this investigation)
but scoped only to `save_to_database` and incorrectly claiming the `run_pipeline` path "does not
have this bug" — corrected and repriced Low → High as part of this round; see that file, not
duplicated here. FD-008 is **not** a blocking dependency of this issue or of LR-012: once every
module in the chain is normalized (this issue + LR-012), none of them fail on `DECADE` any more,
which makes FD-008's swallow moot for this specific value even before FD-008 ships. FD-008 stays
independently necessary for every *other* kind of container failure in these two flows.

## Severity reasoning

- **How would an operator notice?** For LR: not at the point of the click (silent no-op — see
  LR-012). For postprocessing/skill-recalc/ML: in principle a crash, but per "Related finding:
  FD-008" that crash is swallowed today, so in practice: not there either. The only way to notice
  is downstream — decade data (forecasts, skill metrics) that was supposedly just
  corrected/re-run stays stale or absent, with no correlated error anywhere in the dashboard.
- **How common is the trigger?** Not a misconfiguration or typo — it fires on every decade-horizon
  use of two real, currently-visible dashboard buttons ("Save Changes", "Trigger forecasts"),
  which exist specifically for an operator to correct or re-run a forecast, and it breaks **all
  four** modules in the chain, not one. The pentad horizon is unaffected in all four
  (`"pentad".upper() == "PENTAD"`, in every consumer's domain).
- **Why High**: this is the root cause of a decade-horizon manual-workflow chain that has been
  fully broken (one silent failure plus three swallowed loud failures) for as long as the widget
  has used `"decade"`. Priced at the same tier as LR-012, which shares this root cause; not
  higher, because — once the governing decision's normalization lands in every module — the
  practical remaining defect is the naming inconsistency itself plus whatever FD-008 leaves
  unresolved, both narrower in ongoing consequence than LR-012's original silent-data-loss shape.

## Desired outcome

Two non-exclusive approaches remain valid under the governing decision (owner to choose which, or
both — not decided here):

**Option A — Rename the widget's internal value from `"decade"` to `"decad"`.** Smallest change
at the source; every place downstream that already special-cases `"decad"` (e.g. the `:4092`,
`:4232` comments, and the branches they annotate) starts matching its own comments. Requires
auditing every other place `horizon_selector.value == "decade"` (or a bare `"decade"` string) is
compared elsewhere in the dashboard, not just the container-launch sites above — there are at
least eight other `horizon = wm.horizon_selector.value` reads in `vizualization.py`
(`:511, :688, :1716, :1934, :2103, :3243, :4586`) plus `widgets.py:336`
(`elif horizon == "decade":`) that would all need to move to `"decad"` together, in one change,
to avoid splitting the codebase between two conventions. Renders the per-module normalization in
`postprocessing_operational.py`/`recalculate_skill_metrics.py`/`make_forecast.py` unnecessary for
*this specific* trigger, but each of those modules still independently benefits from
case-insensitivity per the governing decision (a different producer sending `pentad` lowercase,
say, would otherwise still break them).

**Option B — Keep `"decade"` as the dashboard-internal/display value, and translate it to
`DECAD` only at the point each container's environment is built** (`vizualization.py:4080,
:4355, :4415`), analogous to `LR-012`'s normalization but fixing it at the one producer instead
of (or in addition to) every consumer. Smaller diff, does not touch the widget's user-facing
option (`_("decade")`, the translatable label, is unrelated to the internal value and does not
need to change either way). Under this option, the per-module normalization in
`postprocessing_operational.py`, `recalculate_skill_metrics.py`, and `make_forecast.py` is
**still required** by the governing decision — the dashboard is not the only possible producer of
a `decade`-spelled value (a future script or a different UI could reintroduce it), and each
module should not depend on every producer getting the spelling right.

Either option must not change the **user-facing** label text (`_("decade")`) — only the internal
value used to build `SAPPHIRE_PREDICTION_MODE`.

## What a fix must not break

- The user-facing label (translated via `_(...)`) must be unaffected — this is about the
  internal value only.
- Any other dashboard logic that currently branches on `horizon == "decade"` (at minimum the
  sites listed under Option A) must be updated consistently in the same change, not left half
  migrated.
- `postprocessing_operational.py`'s existing `ALL`/`MONTHLY` semantics, and
  `recalculate_skill_metrics.py`'s wider `VALID_MODES`, must not change — only case and the
  `decade`/`decad` spelling are in scope for those two.
- `make_forecast.py` must not gain `BOTH`/`ALL` — it must keep rejecting them; ML still runs one
  horizon per invocation.
- `linear_regression.py`'s own normalization (LR-012) should remain in place even after this
  issue ships — it costs nothing to keep, and it protects against any future producer (not just
  the dashboard) that sends a `decade`-spelled or lowercase value again.

## Out of scope

- `linear_regression.py`'s own fix (governing-decision normalization, plus failing loudly on
  everything still unrecognised) — tracked as **LR-012**, decided and independent of this issue.
- **FD-008** (the dashboard swallowing container failures instead of surfacing them) — tracked
  separately; not a dependency of this issue, see "Related finding: FD-008".
- Whether `make_forecast.py`'s `ValueError` on an invalid `PREDICTION_MODE` is itself
  well-handled by the dashboard's `run_docker_container` — that is exactly FD-008's subject, not
  this issue's.

## Deployment note: behaviour change (state this in the release/deployment note, don't let it be discovered afterwards)

**Scope correction (2026-09-09): the affected population is much smaller than an earlier draft of
this note implied.** This is not "all decade forecasts" — the operational cron/Luigi pipeline
already produces decade forecasts, skill metrics, and postprocessing output correctly and
continuously (see "What is NOT affected"). The only affected population is: **the specific
station/period combinations where an operator manually clicked "Save Changes" or "Trigger
forecasts" with the decade horizon selected**, intending to persist an edit or force a re-run,
between whenever the dashboard's `"decade"` value was introduced and whenever this issue and
LR-012 both ship. For those specific manual actions only, the forecast/skill-metric update the
operator believed they had just triggered did not actually happen (LR silently wrote nothing;
postprocessing and skill-metrics recalculation failed and were swallowed — see FD-008).

Once this issue and LR-012 both ship, those same two buttons will, for the first time, actually
carry out a decade edit/re-run when clicked — new or corrected rows will appear for whichever
specific station/period an operator acts on *going forward*. That is the intended fix, not a
regression, and should be called out in the release note so operators know the buttons now work
where they may have appeared to work (silently) before.

**Cannot bound the affected population from the repository.** There is no log, in this
codebase, of which stations or periods an operator manually re-ran via these two buttons before
this fix ships — that history exists only in each deployment's own operational memory (dashboard
usage logs, if any, or operator recollection), not in anything checked into this repo. Rather
than estimate a count, this issue records only the *mechanism* (which action, which precondition)
so that whoever owns each deployment can decide whether a one-time manual review of decade
stations that were "corrected" via the dashboard is warranted for their own operational history.

## Acceptance criteria

- [ ] Decide between Option A and Option B above (owner decision, not made in this draft) for the
  dashboard's own value.
- [ ] `postprocessing_operational.py`, `recalculate_skill_metrics.py`, and `make_forecast.py`
  each accept `decade`/`DECADE` (any case) as `decad`, and accept every existing valid value
  case-insensitively, without gaining or losing any other value in their domain.
- [ ] A decade-horizon "Save Changes" click results in a real LR forecast being written, a
  successful `postprocessing_operational.py` run, and a successful skill-metrics recalculation —
  verified by an integration-level test or captured container output, not just code reading.
- [ ] A decade-horizon "Trigger forecasts" click results in the same for LR, ML (for every
  enabled model), and postprocessing.
- [ ] All other `horizon == "decade"` comparisons in the dashboard (see Option A's list) are
  updated consistently if Option A is chosen; unaffected if Option B is chosen.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero new skips, run
  across `forecast_dashboard`, `postprocessing_forecasts`, and `machine_learning` (all three
  touched by the per-module normalization).

## References

- `apps/forecast_dashboard/dashboard/widgets.py:90-110` (`create_horizon_selector`, `"decade"`
  value at `:99`), `:336` (`elif horizon == "decade":` in `get_period_warning`)
- `apps/forecast_dashboard/src/vizualization.py:3436` (`select_and_plot_data`), `:3449`
  (`horizon = wm.horizon_selector.value`), `:3858` (nested `run_docker_container`),
  `:3949-3971` (its exit-status check and swallow — FD-008), `:3984, :4080, :4131, :4137-4138,
  :4147-4157, :4195` (`save_to_database` / "Save Changes", all three containers), `:4317, :4355,
  :4400, :4415-4421, :4432, :4471` (`run_pipeline` / "Trigger forecasts", all three
  containers/loops), `:4092, :4232` (`# decad` comments), `:4491-4573` (module-level
  `run_docker_container`, used by `run_pipeline` — FD-008), other `horizon =
  wm.horizon_selector.value` reads at `:511, :688, :1716, :1934, :2103, :3243, :4586`
- `apps/linear_regression/linear_regression.py:646-647` (silent no-op on an unmatched mode —
  LR-012's subject), `apps/linear_regression/Dockerfile:34` (default CMD)
- `apps/postprocessing_forecasts/postprocessing_operational.py:245-251` (domain check, loud
  `sys.exit(1)` on an unmatched mode, `:268/:278/:288` for how `ALL` is actually used),
  `apps/postprocessing_forecasts/Dockerfile:38` (default CMD)
- `apps/postprocessing_forecasts/recalculate_skill_metrics.py:94-103` (`VALID_MODES`), `:266-270`
  (domain check)
- `apps/machine_learning/make_forecast.py:605-608` (loud `ValueError` on an unmatched mode;
  domain deliberately excludes `BOTH`/`ALL`), `apps/machine_learning/Dockerfile:40` (default CMD,
  branches on `RUN_MODE`)
- `doc/plans/issues/mid_prio_gi_draft_lr_unrecognised_mode_silent_exit_zero.md` (LR-012 — the
  full defect analysis, the governing decision in full, and the LR-specific fix)
- `doc/plans/issues/high_prio_gi_draft_fd_inner_run_docker_error_handling.md` (FD-008 — the
  swallowed-container-failure defect, repriced High, corrected to cover both button flows)
- `doc/plans/issues/archive/high_prio_gi_draft_pipeline_container_exit_status_discarded.md`
  (P-007 — same defect shape as FD-008, already fixed once in the Luigi pipeline)
