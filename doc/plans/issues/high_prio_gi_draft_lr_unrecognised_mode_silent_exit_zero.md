# LR-013: `linear_regression.py` exits 0 having written nothing when `SAPPHIRE_PREDICTION_MODE` is unrecognised

**Status**: Draft
**Module**: `apps/linear_regression/linear_regression.py`
**Priority**: High (raised from Medium 2026-09-09 — see "Priority reconsidered" below).
**Renumbered from LR-012 to LR-013, 2026-09-09**: `LR-012` collided with an unrelated issue
("mixed-freshness frames write prior-year values stamped with today's forecast date") already
claimed on a local-only branch (`docs_fd024_fd025_doc008`, 2026-08-28, unpushed — invisible to an
origin-only check). Verified free by surveying `git for-each-ref refs/heads` across all ~285
local branches, not just `origin/*`. File renamed from `mid_prio_gi_draft_lr_unrecognised_mode_silent_exit_zero.md`
to `high_prio_gi_draft_lr_unrecognised_mode_silent_exit_zero.md` in the same change, since the
`mid_prio_` prefix no longer matched this issue's High priority.
**Labels**: `linear_regression`, `silent-noop`, `input-validation`, `forecast-dashboard`
**Found**: 2026-08-21, out-of-loop review of INFRA-039, recorded there as out-of-scope because it
is a module-level fix reachable from entry points that patch did not touch. Filed 2026-08-24 on
branch `docs_infra040_lr012_followups`, which was later abandoned before the file reached
`module_issues.md` on trunk. Salvaged and corrected against trunk `e09d48d5` on 2026-09-09 (see
"Corrections from the original draft" below); a second out-of-loop pass the same day found the
defect is live via the forecast dashboard (see "Second-pass corrections"); a third pass the same
day — an **owner override** — replaced two separate per-value aliasing decisions with one
governing normalization rule (see "GOVERNING DECISION" and "Third-pass corrections"); a fourth
pass, from an out-of-loop review of the resulting PR, found the "live" trigger is narrower than
stated (only **one** of the two dashboard buttons — "Save Changes" — actually sends `DECADE`;
"Trigger forecasts" has its own, different, worse defect, split out as **FD-028**) and the blast
radius on the working button is *wider* than stated (every configured station, not the one
selected) — see "Fourth-pass corrections" below. Rebased onto trunk `3791fa31` (PR #504,
docs-only) same day.
**Related**: **INFRA-039** (PR [#477](https://github.com/hydrosolutions/SAPPHIRE_Forecast_Tools/pull/477),
merged 2026-08-24, added the `run_locally.sh`-side domain check; this is the module-side half).
Its plan/issue file is now `doc/plans/issues/review_gi_draft_infra_run_locally_unvalidated_modes.md`,
**Status: Review**. **LR-010** / **LR-011** are *not* duplicates — both concern API
**write-failure** reporting, not mode handling. **INFRA-038** (`mid_prio_gi_draft_infra_connect_to_ieh_boolean_parsing.md`,
Draft) is the same *case-sensitivity* defect class in a different variable — see "Case
sensitivity" below. **FD-027** (`high_prio_gi_draft_fd_decade_horizon_prediction_mode_spelling.md`)
is the dashboard-side root cause and the umbrella issue for the **full chain** of modules the
dashboard feeds this variable to — this issue's normalization fixes `linear_regression.py` only;
FD-027 covers the rest (`postprocessing_operational.py`, `recalculate_skill_metrics.py`,
`make_forecast.py`) and must ship for the dashboard's decade actions to actually work, not just
this issue. **FD-008** (`high_prio_gi_draft_fd_inner_run_docker_error_handling.md`, repriced High
2026-09-09) is a **separate, third defect**: the dashboard swallows container failures instead of
surfacing them, which is the actual reason the decade defects below have gone unnoticed — see
"Related finding: FD-008" below. **FD-028** (new, filed alongside this correction) is a
**separate, fourth defect**: "Trigger forecasts" captures the horizon selector's value once, at
widget-construction time, and never re-reads it — so it always sends `PENTAD`, never `DECADE`,
regardless of what horizon is selected when clicked. It does not expose this issue at all; it is
its own, arguably worse, silent-wrong-horizon defect.

---

> **Scope note, read before the rest of this issue**: this is a module-boundary validation
> defect in `linear_regression.py` — it triggers on *any* unrecognised `SAPPHIRE_PREDICTION_MODE`
> value, from any caller. Its one confirmed **live** trigger is the forecast dashboard's
> **"Save Changes"** button when an operator has the decade horizon selected — see
> "Exposure → LIVE" below. (Correction, fourth pass: the dashboard's other manual button,
> "Trigger forecasts", does **not** trigger this — a stale-closure defect of its own,
> **always** sends `PENTAD` regardless of selection, and is tracked separately as **FD-028**.)
> **Operational decadal forecasting via the production cron path and Luigi's `RunDecadalWorkflow`
> is unaffected**; both hardcode `DECAD` and never reach the unrecognised-value code path this
> issue is about. See FD-027's "What is NOT affected" section for the full verification.

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
`ALL`, `DECADE`, `MONTHLY`, `SEASONAL`, or a typo like `PENTAAD` is not — sets **both flags
false**. The log line at `:635` reports `Running in DECADE prediction mode`, which reads as
confirmation that the value was accepted; nothing downstream contradicts it.

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

## Exposure

This defect has both a **latent** surface (requires a direct, non-standard invocation) and a
**live** surface (a normal operator action in the shipped forecast dashboard). Both are detailed
below; do not read either in isolation.

### Latent: direct Luigi task invocation and the deprecated wrapper script

The original filing of this issue (on the abandoned branch) claimed "every other invocation path"
is exposed and cited the production cron wrappers at the wrong line. Both are corrected here, and
the Luigi-side list is extended with three routes the original draft missed:

- **Line citation fix**: the hardcoded production values are at
  `bin/run_pentadal_forecasts.sh:92` (`-e SAPPHIRE_PREDICTION_MODE=PENTAD`) and
  `bin/run_decadal_forecasts.sh:92` (`-e SAPPHIRE_PREDICTION_MODE=DECAD`), not `:68` as originally
  cited. The conclusion these lines back — that the **production cron path** is not exposed —
  still holds; only the line number was wrong.
- **`LinearRegression`** (`pipeline_docker.py:635`): its own Luigi parameter defaults to
  `prediction_mode = luigi.Parameter(default="ALL")` (`:639`). Reaching that default requires
  invoking this bare Luigi task directly with no `--prediction-mode`. Both real Luigi
  entry-point workflows avoid it: `RunPentadalWorkflow.requires()` instantiates
  `LinearRegression(prediction_mode="PENTAD")` (`:1480`) and `RunDecadalWorkflow.requires()`
  instantiates `LinearRegression(prediction_mode="DECAD")` (`:1542`).
- **`PostProcessingForecasts`** (`:802`, `prediction_mode` defaulting to `"PENTAD"` at `:806`)
  passes its own `self.prediction_mode` straight through to its own `LinearRegression`
  dependency with no domain check (`:813`,
  `dependencies = [LinearRegression(prediction_mode=self.prediction_mode)]`). Because its
  *default* is the safe `"PENTAD"`, this route is exposed only if an unrecognised value is
  passed to it **explicitly** (`--prediction-mode ALL` or similar) — not by omission, unlike
  bare `LinearRegression`.
- **`LinRegMaintenance`** (`:1764`, `prediction_mode = luigi.Parameter()` at `:1767`, forwarded
  at `:1789`) and **`LinRegInitial`** (`:2596`, parameter at `:2604`, forwarded at `:2630`), plus
  **`SkillMetricsInitial`** (`:2655`, parameter at `:2658`) which forwards into `LinRegInitial`
  at `:2669` before setting its own environment at `:2678`. All three declare
  `prediction_mode = luigi.Parameter()` with **no default** — a bare invocation without
  `--prediction-mode` fails at the Luigi parameter-resolution layer, before this module ever
  runs, so these three are exposed only if an unrecognised value is passed **explicitly**, the
  same class as `PostProcessingForecasts`. Their own wrapper workflows hardcode safe values:
  `LinRegMaintenance(prediction_mode="PENTAD"/"DECAD")` at `:1867-1868`, and
  `SkillMetricsInitial(prediction_mode="PENTAD"/"DECAD")` at `:2716-2717`.
- No `bin/` shell script invokes any of `LinearRegression`, `PostProcessingForecasts`,
  `LinRegMaintenance`, `LinRegInitial`, or `SkillMetricsInitial` by name (checked directly:
  no match for any of the five class names in `bin/`) — reaching any of the routes above
  requires a hand-typed `luigi ...` CLI invocation.
- **Confirmed still exposed**: `bin/locally_run_forecast_tools.sh` forwards an operator-supplied
  `SAPPHIRE_PREDICTION_MODE` unvalidated (export guard `:33-34`, LR invocation at `:113`), and
  this one **is** a shipped shell script — it is just deprecated (`:4`,
  `# DEPRECATED: Use apps/run_locally.sh instead.`), not absent. Any direct
  `python linear_regression.py` invocation, including inside the container, is also unvalidated.
- **INFRA-039** (PR #477, merged 2026-08-24) closed the `apps/run_locally.sh` `validate_env` gap
  for the targets that dispatch LR — its guard is the `case "$target" in ... PENTAD|DECAD|BOTH`
  block at `apps/run_locally.sh:1977`. It explicitly did not touch this module-level path (see its
  file's own "Scope" section). Its current status on trunk is **Review**, file
  `doc/plans/issues/review_gi_draft_infra_run_locally_unvalidated_modes.md`.

Summary of this subsection: none of these routes fire from a production cron wrapper or from
either real Luigi workflow. Reaching them requires a direct, hand-typed Luigi CLI invocation
(bare or with an explicit bad value, depending on the task), or the deprecated
`bin/locally_run_forecast_tools.sh`, or a direct module invocation. **This subsection alone would
still support calling the defect latent** — but it is not the whole exposure picture; see below.

### LIVE: the forecast dashboard's decade actions (found 2026-09-09)

**Scope, stated plainly up front: operational decadal forecasting is NOT affected.** The
production cron path (`bin/run_decadal_forecasts.sh:92`) and Luigi's `RunDecadalWorkflow`
(`pipeline_docker.py:1526-1557`) all hardcode `DECAD` and are untouched by anything below — see
FD-027's "What is NOT affected" section for the full verification. Everything in this subsection
is confined to two **manual** dashboard buttons, described next.

The forecast dashboard's horizon selector offers the value `"decade"`, not `"decad"`
(`apps/forecast_dashboard/dashboard/widgets.py:97-99`):

```python
horizon_types = {
    _("pentad"): "pentad",
    _("decade"): "decade",
}
```

`select_and_plot_data(...)` reads this into a local `horizon` variable
(`apps/forecast_dashboard/src/vizualization.py:3449`, `horizon = wm.horizon_selector.value`),
and a nested callback closure defined later in the same function builds a
`SAPPHIRE_PREDICTION_MODE` value directly from it via `.upper()`:

- `save_to_database(event)` (`:3984`, wired to the **"Save Changes"** button at `:4195`) —
  `:4080`: `f'SAPPHIRE_PREDICTION_MODE={horizon.upper()}'`, then `:4131` runs
  `mabesa/sapphire-linreg:latest` with that environment. Because `save_to_database` is nested
  inside `select_and_plot_data` and closes over its *live* local `horizon`, this genuinely reads
  the current selector value on every click.

**Correction (fourth pass, out-of-loop PR review, 2026-09-09): "Trigger forecasts" does NOT send
`DECADE`, ever.** The original filing of this issue (and its own second pass) claimed both
buttons were affected — that was wrong. `run_pipeline` (the "Trigger forecasts" handler) is a
*different* closure, built once by `create_reload_button(self.horizon_selector.value)` at
dashboard-construction time (`widget_manager.py:107`) while the selector still holds its default
`"pentad"` (`widgets.py:107`), and never rebuilt afterward — the code that shows/hides its card
only ever toggles `.visible` (`vizualization.py:125-143`). So `run_pipeline`'s closed-over
`horizon` is **permanently `"pentad"`**, and its own `SAPPHIRE_PREDICTION_MODE={horizon.upper()}`
(`:4355`) is always `PENTAD`, never `DECADE` — regardless of what the operator has selected. This
means "Trigger forecasts" cannot expose *this* issue at all; it has a distinct, arguably worse
defect of its own (silently running the wrong horizon's pipeline while the operator believes they
re-ran decade), filed separately as **FD-028** — see "Related" above. Every claim below is
therefore scoped to **"Save Changes" only**.

When `horizon == "decade"` (the widget's own decade value), `horizon.upper()` is `"DECADE"`, not
`"DECAD"` — both LR horizon flags go false inside the container, and the operator's decade
save/re-run silently writes nothing while the dashboard's progress bar reports completion. The
mismatch is not a typo introduced once: the surrounding code in the same file expects `"decad"`
in most places — e.g. the `else:  # decad` comments at `:4092` and `:4232` — but `"decade"` is
not itself wrong; it is the API's own vocabulary for a *different* contract. See FD-027's "Option
A is dangerous" finding: `"decade"` is the value the postprocessing API's `HorizonType` enum
requires (`sapphire/services/postprocessing/app/models.py:26`), and `save_to_database` also POSTs
the raw `horizon` value as `"horizon_type"` to that API (`:4017`) — so the dashboard is *correct*
to use `"decade"` for that contract. The defect is that the same string is reused, unmodified,
for a second contract (`SAPPHIRE_PREDICTION_MODE`) with a different vocabulary.

This is a real, currently-shipped button in the dashboard's manual forecast-editing panel, not a
hidden or dev-only code path — an operator correcting a decade forecast and clicking **Save
Changes** hits this every time, deterministically, for the decade horizon specifically. The
pentad horizon is unaffected (`"pentad".upper() == "PENTAD"`, which is in LR's domain already).

**Correction (fourth pass): the blast radius on "Save Changes" is station-wide, not
station-scoped.** An earlier pass of this issue implied the effect was limited to whichever
station the operator had selected. Verified directly: neither `linear_regression.py` nor
`postprocessing_operational.py` reads any station-scoping environment variable (no
`SAPPHIRE_RECALC_STATION_CODE` or similar appears in either file) — `linear_regression.py` loads
its pentad/decad site lists from configuration (`sl.get_pentadal_forecast_sites()` /
`sl.get_decadal_forecast_sites_from_pentadal_sites()`, `:673-691`), independent of which station
the operator was viewing. **Only the third, skill-metrics container is station-scoped** — it
receives `SAPPHIRE_RECALC_STATION_CODE={station_code}` (`:4152`), which
`recalculate_skill_metrics.py` reads at `:169`/`:212`. So one "Save Changes" click, at the decade
horizon, today: silently fails to write a decade forecast for **every configured station** (not
just the one on screen) at the selected boundary date, while only the skill-metrics
recalculation — itself failing loudly, see below — was ever scoped to the single station.

**Critical correction (owner override, 2026-09-09): fixing this module alone does not fix the
dashboard.** `save_to_database` runs a `mabesa/sapphire-postprocessing:latest` container with the
**same** `environment` list, immediately after the LR container (`:4137-4138`). That image's
default command is `postprocessing_operational.py` (`apps/postprocessing_forecasts/Dockerfile:38`),
whose own domain check (`postprocessing_operational.py:245-251`) **already rejects `DECADE`
today** and exits 1 — again for every configured station, not just one. So the dashboard's decade
chain, as it stands today, is: LR silently writes nothing (for every station) and exits 0,
**then** postprocessing crashes on the same bad value (also for every station). Normalizing
`linear_regression.py` alone converts this into: LR now writes real decade forecasts for every
configured station, **then postprocessing still crashes** on `DECADE` — a materially different,
still-broken intermediate state, not a fix. See **FD-027** for the full module map
(`postprocessing_operational.py`, `recalculate_skill_metrics.py`, `make_forecast.py` all need the
same normalization) and "Deployment note: behaviour change" below.

The dashboard's own root cause (`"decade"` reused across two contracts) and the full list of
modules that need normalizing are filed and tracked in **FD-027** — see "Related" above and the
References below.

## Related finding: FD-008 (why none of this has been visibly failing)

A further owner-directed check found the reason the "Save Changes" decade path has not been
visibly crashing despite postprocessing rejecting `DECADE` loudly today: both of the dashboard's
`run_docker_container` implementations **swallow container failures** rather than surfacing them
— a general defect, independent of which value triggers the underlying failure. (`run_pipeline`'s
own copy is not implicated in *this* issue's `DECADE` failure — per the correction above, it
never receives `DECADE` — but its swallow is exactly why FD-028's own defect, and any other
container failure in that flow, also goes unnoticed.)

- `save_to_database`'s nested `run_docker_container` (`vizualization.py:3858`) raises
  `docker.errors.ContainerError` on a non-zero exit code (`:3949-3961`), but that `raise` is
  inside a `try` whose own `except Exception as e: print(...)` (`:3970-3971`) catches it —
  `save_to_database` never sees the failure, so it proceeds to the next container and eventually
  sets the progress bar to 100% regardless.
- `run_pipeline` uses a **separate**, module-level `run_docker_container` (`:4491`), which is
  even more direct about it: on a non-zero exit code it only `print`s a message
  (`:4560-4562`, with the comment `# Optionally log the error or add to a list of failed
  containers`) and falls through — it never raises anything at all, so `run_pipeline` also
  continues to the next container unconditionally.

This is the same defect *shape* as **P-007** (`pipeline_docker.py`'s `run_docker_container`
discarding `container.wait()`'s exit code across 20 Luigi call sites, fixed in PR #478) — a
container that fails is indistinguishable from one that succeeded, at the calling layer. It is
tracked as its own pre-existing issue, **FD-008**
(`high_prio_gi_draft_fd_inner_run_docker_error_handling.md`), which already documented the first
bullet above (filed during FD-007 review, before this investigation); it is being **repriced
Low → High and corrected** (its own "Out of scope" section incorrectly claimed the
`run_pipeline` / "Trigger forecasts" path does not have this bug — it does, in a more direct
form) as part of this round rather than duplicated here. This issue (LR-013) and FD-027 do not
depend on FD-008 shipping first — normalizing every module's domain (this issue + FD-027) means
none of them will fail on `DECADE` in the first place, which makes FD-008's swallow moot **for
this specific value** even before FD-008 itself ships. FD-008 remains independently worth fixing
for every *other* kind of container failure (crashes, OOM, network errors) in these two flows,
which will keep being invisible until it does.

## Priority reconsidered: Medium → High (2026-09-09)

The original salvage (first pass, same day) kept this issue's original Medium rating, reasoned
from the (incomplete) latent-only exposure picture above. That reasoning does not survive the
dashboard finding:

- **Frequency/certainty**: the dashboard route is not config-dependent or typo-dependent — it
  fires on 100% of decade-horizon uses of the real, currently-visible **"Save Changes"** button
  (corrected fourth pass: **not** "Trigger forecasts" — see the correction above; that button has
  its own, separately-tracked defect, FD-028), which exists specifically so an operator can
  correct a forecast. And per the station-scope correction above, each such click affects every
  configured station, not one.
- **Consequence class**: an operator who notices a wrong decade forecast, edits it, and clicks
  Save believes the correction was persisted (the button re-enables, the progress bar completes,
  no error is shown — see "Related finding: FD-008" for why) when nothing was recomputed for any
  configured station — the exact "wrong data, not a failure" hazard CLAUDE.md's Data I/O
  Transition section calls out for CSV fallbacks, occurring here for a different reason but with
  the same operator-facing shape. The visibility edit itself (which years/columns show in the
  table) **does** persist — it is POSTed to the API before any container runs
  (`vizualization.py:4016`, confirmed by the "Changes Saved Successfully" alert shown at `:4044`,
  defined at `:3849`) — only the *regenerated forecast/skill-metric data* goes stale, not the
  operator's visibility choice. This narrows what needs recovery, but does not change the
  priority reasoning.
- **Precedent**: **LR-011** (High) and **ML-021** (High) are both "exits 0 / reports success
  having written nothing" defects reachable in real operational use, not only via
  misconfiguration or typo — the same shape this issue now has, once the dashboard route is
  counted. **LR-010** (Low–Medium) is a different shape (over-reporting a skip as a failure, not
  under-reporting a failure as success) and is not the right comparison.
- **Why not the LR-011/ML-021 blast radius exactly**: the fully-automated pentad/decad Luigi
  cron path (the highest-frequency, highest-consequence path in the system) remains completely
  unaffected — it always passes an explicit, valid mode. This keeps it from being priced above
  LR-011/ML-021, not below them.

**Repriced High**, effective 2026-09-09. Do not read this as silently keeping Medium — the
original rating is being explicitly superseded, not carried forward.

## GOVERNING DECISION (2026-09-09, owner override — supersedes both earlier decisions below)

An earlier pass in this same draft recorded two separate decisions ("`ALL` is aliased to `BOTH`"
and, later the same day, "`DECADE` is aliased to `DECAD`"). The owner has since overridden both
with one general rule, which governs this issue and FD-027 alike. **Do not read either superseded
decision below as still independently in force — they are folded into this rule.**

### The rule: normalize spelling and case; keep each module's own domain

The owner's instruction, in substance: do not break anything that works today; the fix must be
backwards compatible with what the dashboard actually sends. Rather than hard-failing on a value
the dashboard sends, loosen the accepted domain along two axes only:

1. **Case-insensitive comparison** — `pentad` ≡ `PENTAD`, `decad` ≡ `DECAD`, `both` ≡ `BOTH`,
   `all` ≡ `ALL`, etc., in every module that touches `SAPPHIRE_PREDICTION_MODE`.
2. **`decade` ≡ `decad`** — one horizon, two accepted spellings, in every module whose domain
   includes the decad horizon at all.
3. **`ALL` ≡ `BOTH`** *within `linear_regression.py`'s own domain specifically* — carried over
   from the earlier (superseded) decision, because LR has no `MONTHLY`/quarterly/seasonal concept
   for `ALL` to mean anything beyond "both horizons."
4. **Exit 1 only on a value that is genuinely unrecognised after normalization.** Nothing that
   succeeds today may start failing.

**This is explicitly NOT a single global whitelist.** Each module keeps the domain it legitimately
has today; normalizing spelling/case must not widen or narrow any module's real domain:

- `make_forecast.py:605-607` deliberately rejects `BOTH` — ML runs one horizon per invocation.
  Normalizing spelling/case there must add `decade`≡`decad` and case-insensitivity **only**; it
  must not gain `BOTH` or `ALL`.
- `postprocessing_operational.py:245` accepts `MONTHLY` as a value, but its `MONTHLY`/`ALL`
  branch (`:288-291`) does no monthly work — it only logs that monthly is handled by
  `postprocessing_operational_long_term.py` and skips it. **Correction (fourth pass): `ALL` for
  this module computes exactly the same two horizons as `BOTH`** (`:268` and `:278` both check
  `["PENTAD","BOTH","ALL"]` / `["DECAD","BOTH","ALL"]`) — an earlier pass of this issue wrongly
  said `ALL` here means "pentad, decad, and monthly"; it does not. Normalizing there must add
  `decade`≡`decad` and case-insensitivity **only**; its existing (narrower-than-previously-stated)
  `ALL`/`MONTHLY` semantics must not change.
- `recalculate_skill_metrics.py`'s `VALID_MODES` (`:94-103`) is wider still, and genuinely — its
  `ALL` really does mean "run pentad **and** decad **and** monthly **and** quarterly **and**
  seasonal **and** daily" (verified: `["PENTAD","BOTH","ALL"]` at `:277`, `["MONTHLY","ALL"]` at
  `:301`, `["QUARTERLY","ALL"]` at `:372`, `["SEASONAL","ALL"]` at `:443`, `["DAILY","ALL"]` at
  `:528`). Collapsing this module's `ALL` to `BOTH` anywhere would silently drop four of those six
  behaviors — a real narrowing, not a hypothetical one. Same treatment as the others: add
  `decade`≡`decad` and case-insensitivity only.
- **This normalization must happen inside each consuming module, on that module's own copy of
  the value — never at an orchestrator boundary, and never by rewriting the ambient
  `SAPPHIRE_PREDICTION_MODE` env var itself.** Concretely verified hazards of doing it at the
  orchestrator level: (a) `apps/run_locally.sh`'s own `validate_env` (`:1977`) and its ML
  resolver (`resolve_ml_bare_target_modes`, `:541`) both accept only `PENTAD`/`DECAD`/`BOTH` for
  the targets they gate — an **unset** value is accepted only for the outer-loop targets, which
  default it later; the bare `machine_learning` target **rejects unset** and exits 1
  (`run_locally.sh:544-546`, since `refactor_run_locally_drop_ml_mode`) — and `apps/pipeline/tests/test_run_locally_orchestration.py`'s
  `test_out_of_domain_mode_rejected_before_any_module_runs` (`:1677-1690`) explicitly requires
  `"ALL"` (alongside the typo `"PENTAAD"`) to be **rejected** there — a global `ALL`→`BOTH`
  rewrite ahead of that gate would break this passing regression test; (b)
  `RunAllMLModels.requires()` (`pipeline_docker.py:792`) checks `self.prediction_mode == "ALL"`
  by exact string to expand into two separate, valid `RunMLModel(prediction_mode="PENTAD"/"DECAD")`
  tasks — if `ALL` were rewritten to `BOTH` before this check, it would never match, and the
  `else` branch would instead create a single `RunMLModel(prediction_mode="BOTH")` task, which
  `make_forecast.py` rejects outright (`:605-607`); (c) `apps/reset_forecast_run_date/rerun_forecast.py`
  reads the same env var (`:191`) and uses it **verbatim, uppercased-or-not**, as a filename
  suffix (`_{prediction_mode}.txt`, `:83`) — a blanket case-normalization of the ambient value
  would risk turning a working `_decad.txt` lookup into `_DECAD.txt`.

**Decision, as applied to `linear_regression.py` (this issue's own scope):**

1. Accept `PENTAD`, `DECAD`, `BOTH` case-insensitively, plus `decade`/`DECADE` (and any-case
   variant) as an accepted spelling of `decad`, plus `ALL` (any case) as an accepted spelling of
   `both`.
2. Update **both** the module docstring text at `linear_regression.py:54` **and** the separate
   `ArgumentParser` epilog text at `:216` (two independent copies of the same sentence in two
   different string literals — fixing one does not fix the other) to document the widened,
   normalized domain: `PENTAD`, `DECAD` (also `DECADE`), `BOTH`, `ALL` (default: `BOTH`),
   case-insensitive.
3. Treat any value that is still unrecognised **after** normalization as invalid: log an error
   naming the variable, the offending (pre-normalization) value, and the accepted set, then
   `sys.exit(1)` — **before** the discharge-data load at `:765`, so it fails fast rather than
   after minutes of I/O.

**Explicitly out of scope under this decision:** changing the Luigi `LinearRegression` task's
`default="ALL"` at `pipeline_docker.py:639`, and changing `doc/configuration.md:870`. Under this
decision both are already correct as written; only `linear_regression.py`'s own domain check and
its two docstring/epilog copies are wrong. The **other three modules** in the dashboard's chain
(`postprocessing_operational.py`, `recalculate_skill_metrics.py`, `make_forecast.py`) need the
same case/spelling normalization applied to *their own* domains — tracked in **FD-027**, not
duplicated here.

<details>
<summary>Superseded text (kept for the record — do not implement as written)</summary>

~~### Decision 1 — `ALL` is aliased to `BOTH`~~ ~~### Decision 2 — `DECADE` is aliased to
`DECAD`~~ — both are superseded by the single rule above. The practical difference: "aliased to"
implied a one-to-one rewrite of a small enumerated set of values (`ALL`→`BOTH`, `DECADE`→`DECAD`)
with everything else still exiting 1 on a **case-sensitive** basis; the governing rule additionally
requires case-insensitive comparison for every accepted value (`pentad`, `Pentad`, `both`, etc.),
which the two superseded decisions did not state as a general requirement (only "Case sensitivity"
below already flagged it as a related, fixable-by-the-same-change gap — this override makes it
part of the decision itself, not a separately-optional improvement).
</details>

## Case sensitivity — same defect class as INFRA-038

The membership tests at `:646-647` (`prediction_mode in ["PENTAD", "BOTH"]` /
`in ["DECAD", "BOTH"]`) are exact-string comparisons with no case normalisation. Lowercase or
mixed-case values (`pentad`, `decad`, `both`, `all`, `decade`) fail exactly the same way as an
unrecognised value — both flags silently `False`, `sys.exit(0)`. The governing decision above
folds this in as a first-class requirement, not an optional add-on: case-insensitive comparison,
applied via the same normalization pass as the `decade`/`ALL` handling.

This is the same *class* of defect as **INFRA-038**
(`mid_prio_gi_draft_infra_connect_to_ieh_boolean_parsing.md`, Draft) — a boolean/enum-like
environment variable compared with case-sensitive exact-match logic across the codebase, where an
unexpected case produces a silent wrong branch instead of a loud rejection. INFRA-038 covers
`connect_to_iEH`/`ssh_to_iEH`; this is the same shape in `SAPPHIRE_PREDICTION_MODE`. Not proposing
to merge the two issues — different variables, different modules — but a fix here should not
reintroduce the pattern INFRA-038 is about to remove elsewhere.

## Desired outcome

Validate at the module boundary and fail loudly, but only after normalizing case and the
`decade`/`ALL` spellings (governing decision above). Two existing sibling patterns are useful
references for the fail-loud shape, quoted exactly as written (do not copy either verbatim — LR's
domain differs from both, and neither currently normalizes case or spelling either — that gap is
this issue's own fix, not something to inherit from them):

`postprocessing_maintenance.py:124-130` (closest domain — `PENTAD`/`DECAD`/`BOTH` only, no
`ALL`):

```python
prediction_mode = os.getenv("SAPPHIRE_PREDICTION_MODE", "") or "BOTH"
if prediction_mode not in ["PENTAD", "DECAD", "BOTH"]:
    logger.error(
        f"Invalid SAPPHIRE_PREDICTION_MODE: {prediction_mode}. "
        f"Expected 'PENTAD', 'DECAD', or 'BOTH'."
    )
    sys.exit(1)
```

`postprocessing_operational.py:244-251` (wider domain — already includes `ALL` and `MONTHLY`,
useful for the error-message shape once LR's domain also includes `ALL`):

```python
prediction_mode = os.getenv("SAPPHIRE_PREDICTION_MODE", "") or "BOTH"
valid_modes = ["PENTAD", "DECAD", "BOTH", "MONTHLY", "ALL"]
if prediction_mode not in valid_modes:
    logger.error(
        f"Invalid SAPPHIRE_PREDICTION_MODE: {prediction_mode}. "
        f"Expected one of {valid_modes}."
    )
    sys.exit(1)
```

LR's own fix needs the normalization pass (case + `decade`→`decad` + `all`→`both`) applied
**before** either precedent's domain check, e.g. (illustrative, not a literal implementation
mandate):

```python
mode = (os.getenv("SAPPHIRE_PREDICTION_MODE", "") or "BOTH").strip().upper()
if mode == "ALL":
    mode = "BOTH"
if mode == "DECADE":
    mode = "DECAD"
if mode not in ["PENTAD", "DECAD", "BOTH"]:
    logger.error(
        f"Invalid SAPPHIRE_PREDICTION_MODE: {mode}. "
        f"Expected 'PENTAD', 'DECAD' (or 'DECADE'), 'BOTH', or 'ALL' (case-insensitive)."
    )
    sys.exit(1)
```

- Accept `PENTAD`, `DECAD`, `BOTH`, and unset/empty (which must keep resolving to `BOTH` — that is
  the documented default and callers rely on it, `:54` / `:216`) — all case-insensitively.
- Accept `ALL` (any case) as equivalent to `BOTH`, within LR's own domain only.
- Accept `DECADE` (any case) as equivalent to `DECAD`.
- Anything else, after normalization: log an error naming the variable, the offending
  (pre-normalization) value, and the accepted set (`PENTAD`, `DECAD`, `BOTH`, `ALL`), then
  `sys.exit(1)`.
- Do **not** silently map unknown values onto a horizon.

## What a fix must not break

- Unset/empty must still resolve to running both horizons — the `or "BOTH"` default is a
  documented contract, not a bug.
- `PENTAD`, `DECAD`, `BOTH`, `ALL`, and any case/spelling variant covered by the governing
  decision must all behave exactly as their normalized target does today.
- `--hindcast` and the other CLI paths must be unaffected for valid modes.
- The CSV write path and its existing behavior are out of scope — this is a mode-validation fix,
  not a write-path change.
- Do not change `pipeline_docker.py:639`'s Luigi default or `doc/configuration.md:870` — the
  governing decision treats both as already correct.
- Do not change `apps/forecast_dashboard/...` as part of this fix — normalizing LR resolves the
  dashboard's `DECADE` value at the LR layer only; the dashboard's own root-cause spelling
  inconsistency, and the other three modules the dashboard also feeds this variable to, are
  FD-027's scope, not this issue's. Fixing LR alone does **not** make the dashboard's decade
  chain succeed end to end — see "Exposure → LIVE" above.

## Out of scope

- The `run_locally.sh`-side check (shipped, INFRA-039, PR #477).
- `bin/locally_run_forecast_tools.sh`'s own passthrough — fixing the module makes it fail loudly,
  which is sufficient; hardening the deprecated wrapper is optional follow-up, if it is not simply
  removed as part of the module's ongoing deprecation.
- LR-010 / LR-011's API write-failure reporting.
- Any change to `pipeline_docker.py`'s Luigi parameter defaults or to `doc/configuration.md` (see
  the governing decision).
- FD-027 itself (the dashboard's `"decade"` vs. `"DECAD"` root cause, and the normalization needed
  in `postprocessing_operational.py`, `recalculate_skill_metrics.py`, `make_forecast.py`) —
  tracked separately; this issue's normalization is a necessary but not sufficient part of making
  the dashboard's decade path work.
- FD-008 (the dashboard swallowing container failures) — tracked separately; not a dependency of
  this issue (see "Related finding: FD-008").
- The ML container's loud `ValueError` on `DECADE` (`make_forecast.py:605-608`) — cited only as
  corroborating evidence above; its own normalization is FD-027's scope.

## Acceptance criteria

1. `SAPPHIRE_PREDICTION_MODE=ALL python linear_regression.py` (and `all`, `All`) runs **both**
   horizons, not an error.
2. `SAPPHIRE_PREDICTION_MODE=DECADE python linear_regression.py` (and `decade`, `Decade`) runs the
   **decad** horizon only, not an error and not a no-op — this is the regression test for the
   live dashboard path.
3. `PENTAD`, `DECAD`, `BOTH` (and their lowercase/mixed-case forms) behave exactly as today /
   as their normalized target.
4. `SAPPHIRE_PREDICTION_MODE=MONTHLY python linear_regression.py` (and `SEASONAL`, and a typo
   like `PENTAAD`, in any case) exits **non-zero** after normalization still fails to match, naming
   the variable, the value, and the accepted set — and does so **before** the discharge-data load
   at `:765`, so it fails fast rather than after minutes of I/O.
5. **Unset and empty still resolve to `BOTH`** and run both horizons — this is the regression
   guard that matters most; the `or "BOTH"` default must not be broken by the new check.
6. `--hindcast` and the other CLI paths are unaffected for valid modes.
7. Unit tests in `apps/linear_regression/` cover 1-5, asserting exit status and message content
   rather than internal flags. No real station codes in new tests — use a placeholder such as
   `19999`.
8. `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero new skips.

## Deployment note: behaviour change (state this in the release/deployment note, don't let it be discovered afterwards)

**Scope correction (fourth pass): this is about "Save Changes" only** (Trigger forecasts never
sent `DECADE` — see FD-028) **and affects every configured station**, not one (see the
station-scope correction above). With that scope: once this issue's normalization lands **in
isolation** (before FD-027), clicking "Save Changes" at the decade horizon will start producing
real, written decade LR forecasts, for every configured station, where it previously wrote none —
a genuine, intended change in observable data, not a regression. But because
`postprocessing_operational.py` still rejects `DECADE` until FD-027 also ships (see "Exposure →
LIVE" above), the *practical* result of shipping this issue alone is a **new**, still-incomplete
state: LR forecast rows for decade start appearing for every station, while the postprocessing
step that runs immediately after — **not** skill metrics (see FD-027's correction: operational
`postprocessing_operational.py` explicitly does not recalculate skill metrics, per its own
header comment) — keeps failing on the same unnormalized value for every station. That failure,
per "Related finding: FD-008," will very likely continue to be invisible in the dashboard's own
UI. Operators who believe "Save Changes" decade actions "have been running all along" should be
told, before this ships, that: (a) new decade LR forecast rows will start appearing, for every
configured station, that were not being written before, and (b) shipping this issue without
FD-027 leaves the postprocessing step still silently broken for decade, for every station — the
full, correct fix requires both to land together (or FD-027 first). The visibility *edit* itself
(which years/columns to show) was never lost — see the "Consequence class" note under "Priority
reconsidered" above; only the regenerated forecast/skill-metric data is affected.

## Corrections from the original draft

This issue was filed 2026-08-24 on branch `docs_infra040_lr012_followups`, which was abandoned
before it reached `module_issues.md` on trunk. It is salvaged here with the following corrections
made against trunk `e09d48d5` (2026-09-09, first pass):

- The hardcoded-mode citations for the production cron wrappers were `bin/run_pentadal_forecasts.sh:68`
  / `bin/run_decadal_forecasts.sh:68` in the original; the correct line in both files is `:92`.
- The `sl.load_environment()` citation was `:631` in the original. Re-verified directly against
  trunk (`grep -n "sl.load_environment()"`): **`:631` is in fact correct** — this issue does not
  carry forward a suggested correction to `:630` (that line is a comment, `# Configuration`) that
  was proposed during salvage triage but could not be confirmed.
- The claim "every other invocation path is still exposed" was too broad. Corrected to the
  narrower, directly-verified latent set (see "Exposure" above) — which, per the second pass
  below, turned out to still be incomplete in the opposite direction (it missed a live path).
- Added: the case-sensitivity gap (no normalisation of `PENTAD`/`DECAD`/`BOTH`/`ALL`), not present
  in the original draft.
- Added: an owner decision that `ALL` maps onto `BOTH`, resolving the `doc/configuration.md` /
  `pipeline_docker.py` Luigi-default / module-docstring conflict that the original draft did not
  address (it treated only `PENTAD`/`DECAD`/`BOTH` as the target domain and would have made the
  Luigi default `ALL` an accepted-but-undocumented value at best, or a newly-introduced hard
  failure on every direct `LinearRegression` invocation at worst). This decision was later folded
  into the governing decision (third pass, below).
- The original draft attributed INFRA-039 to "PR #477" without an inline verification note;
  verified here via `gh pr view 477` (title "INFRA-039: validate SAPPHIRE_PREDICTION_MODE and
  ML_MODE at entry", merged 2026-08-24) — the attribution is correct and is kept.
- INFRA-039's issue file has since moved from a "Draft" reference to its current name
  `review_gi_draft_infra_run_locally_unvalidated_modes.md`, **Status: Review** — updated from the
  original draft, which referenced it before that rename.

## Second-pass corrections (2026-09-09, out-of-loop review)

An out-of-loop confirm pass re-derived every citation in the first-pass salvage independently and
found **no stale or off-by-one citation** in it — including confirming `:631` above and that the
original draft's `:68` citations resolve to a bare comment. It did find the following, all
addressed in this revision:

- **The headline correction**: the "latent, not live" conclusion was wrong. The forecast
  dashboard's `"Save Changes"` and `"Trigger forecasts"` buttons build `SAPPHIRE_PREDICTION_MODE`
  from the horizon selector's own value via `.upper()`, and the selector's decade value is
  `"decade"`, not `"decad"` — a live, deterministic trigger on a normal operator action. See
  "Exposure" above (new "LIVE" subsection) for the full citation chain.
- **Priority reconsidered from Medium to High** as a direct consequence — see "Priority
  reconsidered" above; not silently left at Medium.
- A now-superseded owner decision (`DECADE` aliased to `DECAD`) was recorded at this stage — see
  "GOVERNING DECISION" above for its replacement.
- Added three previously-missed direct-Luigi routes to the latent-exposure list:
  `LinRegMaintenance`, `LinRegInitial`, and `SkillMetricsInitial` (the last forwarding into
  `LinRegInitial`) — all reachable only via direct Luigi CLI invocation with an explicit bad
  value, same class as `PostProcessingForecasts`.
- Fixed a self-contradiction: the first-pass draft said `bin/locally_run_forecast_tools.sh` was
  exposed, then separately concluded "no shipped shell script" could trigger the defect. The
  "Exposure" section above now states the narrower, accurate claim — no **production cron**
  wrapper is exposed — and keeps the deprecated wrapper's exposure as a distinct, correct fact.
- Fixed a misquoted precedent: the first-pass "Desired outcome" snippet attributed
  `postprocessing_operational.py`'s `Expected one of {valid_modes}.` message (with an undefined
  `valid_modes` in the snippet) to `postprocessing_maintenance.py`, which actually reads
  `Expected 'PENTAD', 'DECAD', or 'BOTH'.` (`:128`). Both precedents are now quoted separately and
  accurately in "Desired outcome" above.
- Distinguished `:54` (the module-level docstring, lines 1-98) from `:216` (a separate string —
  the `ArgumentParser` epilog, lines 190-232) wherever both were previously cited together as if
  they were the same kind of location; both need their own edit, since fixing one does not fix
  the other.
- Rebased onto trunk `3791fa31` (PR #504, docs-only — adds INFRA-051 and ML-024/025/026; no code
  citation in this issue was affected).

## Third-pass corrections (2026-09-09, owner override)

The owner overrode both per-value aliasing decisions recorded above with one governing
normalization rule (see "GOVERNING DECISION"), and directed a re-check of the dashboard's full
chain. This pass:

- Replaced "`ALL`→`BOTH`" and "`DECADE`→`DECAD`" framing with the general rule: normalize case
  and the `decade`/`decad` spelling everywhere; keep each module's own domain; exit 1 only on a
  value still unrecognised after normalization.
- **Corrected the scope claim that fixing LR alone resolves the dashboard's decade path.**
  Verified directly: `save_to_database` and `run_pipeline` both run
  `mabesa/sapphire-postprocessing:latest` (default command `postprocessing_operational.py`,
  `Dockerfile:38`) with the same `environment` list right after the LR container
  (`vizualization.py:4137-4138`, `:4432`), and `postprocessing_operational.py:245-251` already
  rejects `DECADE` today. Normalizing LR alone therefore does not make the dashboard's decade
  actions succeed — it changes what fails and where. Mapped the full module set (LR,
  `postprocessing_operational.py`, `recalculate_skill_metrics.py` via an explicit `command=`
  override at `:4147-4157`, and `make_forecast.py` via `run_pipeline`'s ML branch) into FD-027.
- Added "Related finding: FD-008" — the dashboard's two `run_docker_container` implementations
  both swallow container failures (verified: `:3949-3971` for `save_to_database`'s nested one,
  `:4491-4573` for `run_pipeline`'s module-level one, the latter never raising at all), which is
  the actual reason none of these failures — LR's silent no-op, postprocessing's and ML's loud
  crashes — have been visible. Corrected FD-008's own stale line numbers and its incorrect
  "Trigger forecasts does not have this bug" claim; repriced FD-008 Low → High.
- Added the "Deployment note: behaviour change" section — shipping this issue's normalization
  without FD-027 changes dashboard-observable behaviour (new decade LR rows appear) without fully
  fixing the dashboard flow, and that needs to be called out proactively, not discovered later.

## Fourth-pass corrections (2026-09-09, out-of-loop review of PR #506)

A set of out-of-loop reviews of the PR built from this issue and FD-027 returned 15 findings, one
CRITICAL that invalidated half of FD-027's claims. All were re-verified directly against trunk
before being applied here (a small number of the reviewers' own citations were themselves
slightly off — noted individually below and in the coordinating session's report, not silently
adopted).

- **CRITICAL, invalidates the "both buttons send DECADE" claim.** Verified directly:
  `widgets.py:107` defaults the horizon selector to `"pentad"`; `widget_manager.py:107` calls
  `create_reload_button(self.horizon_selector.value)` exactly once, at dashboard-construction
  time; `vizualization.py:125-143` only ever toggles the resulting `reload_card`'s `.visible`
  flag, never rebuilds it. So `run_pipeline`'s closed-over `horizon` is permanently `"pentad"` —
  "Trigger forecasts" **never** sends `DECADE`, regardless of the operator's selection. Corrected
  throughout "Exposure → LIVE," "Priority reconsidered," the Deployment note, and the References.
  The distinct, worse defect this reveals (Trigger Forecasts silently running the wrong horizon)
  is filed separately as **FD-028**, not folded into this issue.
- **Renumbered LR-012 → LR-013** (see header) after a local-only branch was found to have already
  claimed `LR-012` for an unrelated defect.
- **Station-scope correction**: this issue's own silent failure is not limited to the selected
  station — verified neither `linear_regression.py` nor `postprocessing_operational.py` reads any
  station-scoping env var; only the (separately non-fatal) skill-metrics container is
  station-scoped. Corrected in "Exposure → LIVE," "Priority reconsidered," and the Deployment
  note.
- **`postprocessing_operational.py`'s `ALL` semantics corrected**: its `MONTHLY`/`ALL` branch
  (`:288-291`) only logs and skips — `ALL` computes the same two horizons as `BOTH` for this
  module, not "pentad, decad, and monthly" as an earlier pass claimed. Corrected in GOVERNING
  DECISION.
- **`recalculate_skill_metrics.py`'s `ALL` is genuinely wider (six horizons), confirmed exact
  line-by-line** (`:277, :301, :372, :443, :528`) — this one **is** correctly described as wider
  than LR's `ALL`; only `postprocessing_operational.py`'s was misstated. The two modules must not
  be conflated.
- **Orchestrator-boundary hazards added**: `run_locally.sh`'s own domain gates (`:1977`, `:541`)
  and their regression test (`test_run_locally_orchestration.py:1677-1690`, which requires `"ALL"`
  rejected) must not be touched by this issue's normalization; `RunAllMLModels`'s exact-string
  `"ALL"` expansion (`pipeline_docker.py:792`) and `rerun_forecast.py`'s raw-value filename suffix
  (`:83`) are both concrete reasons normalization must happen inside each consumer, never on the
  ambient env var.
- **Recovery-scope correction**: "Save Changes" does persist the visibility edit before any
  container runs (POST at `:4016`, "Changes Saved Successfully" alert at `:4044`) — only the
  regenerated forecast/skill-metric data goes stale, not the configuration edit. Added to
  "Priority reconsidered" and the Deployment note.
- **Citation fixes**: `RunDecadalWorkflow` starts at `pipeline_docker.py:1526`, not `1524`;
  the module-level `run_docker_container`'s silent status-ignore is at `:4559-4561` (not
  `:4572-4573`, which is only the unrelated generic-exception handler's header/body at
  `:4573-4574`).
- The dashboard's `"decade"` value is now described as **correct** for the API's `HorizonType`
  contract (`sapphire/services/postprocessing/app/models.py:26`), not a typo — see FD-027 for the
  full "Option A is dangerous" finding this issue's earlier passes did not have.

## References

- `apps/linear_regression/linear_regression.py:54` (module docstring), `:216` (separate
  `ArgumentParser` epilog string), `:631` (`sl.load_environment()`), `:634` (mode resolution),
  `:646-647` (horizon flags), `:765` (unconditional `get_pentadal_and_decadal_data` call),
  `:816, :826` (date loop / predictor dates), `:828-831` (comment documenting the
  `run_pentad`/`forecast_flags.pentad` double-guard), `:1096` (`sys.exit(0)`)
- `apps/iEasyHydroForecast/forecast_library.py:1301-1302` (forced `forecast_flags.pentad/decad =
  True`), `:1315` (unconditional discharge load)
- `bin/run_pentadal_forecasts.sh:92`, `bin/run_decadal_forecasts.sh:92` (hardcoded production
  values)
- `bin/locally_run_forecast_tools.sh:4` (deprecation notice), `:33-34` (mode passthrough guard),
  `:113` (LR invocation)
- `apps/pipeline/pipeline_docker.py:635, :639` (`LinearRegression` class and Luigi default
  `ALL`), `:792` (`RunAllMLModels`'s exact-string `"ALL"` expansion — orchestrator hazard),
  `:802, :806, :813` (`PostProcessingForecasts` default and pass-through), `:1480, :1542`
  (`RunPentadalWorkflow`/`RunDecadalWorkflow`'s LR call sites), `:1526` (`RunDecadalWorkflow`
  class start), `:1549, :1557` (`RunDecadalWorkflow`'s ML and postprocessing call sites, both
  hardcoded `DECAD`), `:1764, :1767, :1789` (`LinRegMaintenance`), `:1867-1868`
  (`LinRegMaintenance`'s own wrapper, hardcoded), `:2596, :2604, :2630` (`LinRegInitial`),
  `:2655, :2658, :2669, :2678` (`SkillMetricsInitial`), `:2716-2717` (`SkillMetricsInitial`'s own
  wrapper, hardcoded)
- `apps/run_locally.sh:541` (`resolve_ml_bare_target_modes`, PENTAD/DECAD/BOTH/unset only),
  `apps/pipeline/tests/test_run_locally_orchestration.py:1677-1690`
  (`test_out_of_domain_mode_rejected_before_any_module_runs`, requires `"ALL"` rejected — do not
  break)
- `apps/reset_forecast_run_date/rerun_forecast.py:83` (raw value used as a filename suffix,
  `_{prediction_mode}.txt`), `:191` (read, with a legacy `sapphire_forecast_horizon` fallback) —
  cited only as an orchestrator-boundary-normalization hazard, see GOVERNING DECISION
- `doc/configuration.md:870` (documented domain including `ALL`)
- `apps/run_locally.sh:1977` (INFRA-039's `validate_env` guard)
- `apps/postprocessing_forecasts/postprocessing_maintenance.py:124-130` (closest existing
  fail-loudly precedent, literal message at `:128`), `apps/postprocessing_forecasts/postprocessing_operational.py:244-251`
  (precedent that already accepts `ALL`, literal message at `:249`; also rejects `DECADE` today —
  see "Exposure → LIVE"), `apps/postprocessing_forecasts/Dockerfile:38` (default CMD
  `postprocessing_operational.py`)
- `apps/postprocessing_forecasts/recalculate_skill_metrics.py:94-103` (`VALID_MODES`), `:266-271`
  (domain check including `sys.exit(1)`), `:277, :301, :372, :443, :528` (the six `ALL`-inclusive
  branches proving `ALL` genuinely spans six horizons for this module — see GOVERNING DECISION)
- `apps/forecast_dashboard/dashboard/widgets.py:90-110` (`create_horizon_selector`, `"decade"`
  value at `:99`, default `"pentad"` at `:107`), `:336` (`elif horizon == "decade":`)
- `apps/forecast_dashboard/dashboard/widget_manager.py:107` (`create_reload_button` called once,
  at construction — the root cause of FD-028, not this issue, cited here only to support the
  "Trigger forecasts never sends DECADE" correction above)
- `apps/forecast_dashboard/src/vizualization.py:3449` (`horizon = wm.horizon_selector.value`,
  the live read `save_to_database` closes over), `:125-143` (`reload_card` toggled `.visible`
  only, never rebuilt — FD-028 evidence), `:3858` (nested `run_docker_container` def),
  `:3949-3961` (exit-status check / raise), `:3970-3971` (the swallow), `:3984, :4016, :4017,
  :4044, :4080, :4131, :4137-4138, :4147-4157, :4152, :4195` (`save_to_database` / "Save
  Changes": the visibility-record POST via `_db._save_data`, its `horizon_type` field, the
  "Changes Saved Successfully" alert, the LR/postprocessing/skill-recalc containers, and the
  skill-recalc-only station-code parameter), `:4092, :4232` (`# decad` comments showing most of
  the module's own expected spelling), `:4491-4574` (module-level `run_docker_container` def,
  used by `run_pipeline` — its silent status-ignore is at `:4559-4561` (no raise on a bad exit
  code), a separate generic-exception swallow at `:4573-4574`; not implicated in this issue's own
  `DECADE` failure, since `run_pipeline` never receives `DECADE` — see FD-028)
- `apps/machine_learning/make_forecast.py:605-608` (`PREDICTION_MODE` validation, `{PENTAD,DECAD}`
  only — cited only as a domain-precedent for the governing decision, not as evidence this issue
  reaches ML: `run_pipeline`'s ML branch always sends `PENTAD`, per FD-028)
- `apps/machine_learning/Dockerfile:40`, `apps/linear_regression/Dockerfile:34` (default CMDs,
  confirming which script each dashboard-launched container actually runs)
- `sapphire/services/postprocessing/app/models.py:26` (`HorizonType.DECADE = "decade"` — the API
  contract that makes the dashboard's `"decade"` value correct for `horizon_type`, not a typo)
- Precedent / related: `doc/plans/issues/review_gi_draft_infra_run_locally_unvalidated_modes.md`
  (INFRA-039), `doc/plans/issues/mid_prio_gi_draft_infra_connect_to_ieh_boolean_parsing.md`
  (INFRA-038, same case-sensitivity defect class),
  `doc/plans/issues/high_prio_gi_draft_fd_decade_horizon_prediction_mode_spelling.md` (FD-027,
  the dashboard's own root cause and full module-chain map),
  `doc/plans/issues/high_prio_gi_draft_fd_inner_run_docker_error_handling.md` (FD-008, the
  swallowed-failure defect, repriced High),
  `doc/plans/issues/high_prio_gi_draft_fd_trigger_forecasts_stale_horizon_closure.md` (FD-028,
  the "Trigger forecasts" stale-closure defect),
  `doc/plans/issues/archive/high_prio_gi_draft_pipeline_container_exit_status_discarded.md`
  (P-007, same defect shape, fixed in the Luigi pipeline)
