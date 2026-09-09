# FD-027: "Save Changes" sends `DECADE` instead of `DECAD` — operational decadal forecasting (cron/Luigi) is unaffected, and "Trigger forecasts" has a separate, worse defect (FD-028)

**Status**: Draft
**Module**: `apps/forecast_dashboard` (fair game)
**Priority**: High — confined to the dashboard's **"Save Changes"** button when decade is
selected; **operational decadal forecasting via cron/Luigi is unaffected** (see "What is NOT
affected"). Within that confined scope, it is reachable on every decade-horizon "Save Changes"
click and breaks three of the four downstream consumers it feeds. See "Severity reasoning" below.
**Labels**: `forecast-dashboard`, `silent-noop`, `naming-inconsistency`, `linear_regression`,
`machine_learning`, `postprocessing_forecasts`
**Found**: 2026-09-09, out-of-loop review of LR-013 (`high_prio_gi_draft_lr_unrecognised_mode_silent_exit_zero.md`),
while verifying that issue's "latent, not live" exposure claim. A second, owner-directed pass the
same day mapped the **full** chain of modules the dashboard feeds this variable to. A third pass
— an out-of-loop review of the PR built from this issue and LR-013 — found a **CRITICAL** error:
"Trigger forecasts" does not send `DECADE` at all (it has a stale-closure defect that always
sends `PENTAD`, split out as **FD-028**), and found the true blast radius of "Save Changes" is
*wider* than stated (every configured station), while the "every module uses `DECAD`, never
`DECADE`" and "Option A is the smallest fix" claims were both wrong. See "Fourth-pass
corrections" below for the full list. Verified directly against trunk `3791fa31`.
**Related**: **LR-013** (`high_prio_gi_draft_lr_unrecognised_mode_silent_exit_zero.md`) — carries
the detailed defect analysis and the governing normalization decision for
`linear_regression.py` specifically; this issue (FD-027) is the dashboard's own root cause and
the umbrella tracker for the analogous fix needed in the other three modules the dashboard also
drives (only via "Save Changes" — see the correction below). **FD-008**
(`high_prio_gi_draft_fd_inner_run_docker_error_handling.md`, repriced High 2026-09-09) is a
separate, third defect: the dashboard swallows container failures instead of surfacing them,
which is why none of the failures documented below have been visibly breaking anything — see
"Related finding: FD-008" below. **FD-028** (new) is a separate, fourth defect: "Trigger
forecasts" captures the horizon selector's value once, at widget-construction time, and never
re-reads it, so it always sends `PENTAD` — it does not expose this issue, and is not fixed by
anything here. **P-007** (`archive/high_prio_gi_draft_pipeline_container_exit_status_discarded.md`,
Complete) is the same exit-code-discarding shape FD-008 recurs.

---

## Summary

**Operational decadal forecasting is NOT affected by this defect.** The production cron path
(`bin/run_decadal_forecasts.sh:92`) and Luigi's `RunDecadalWorkflow`
(`apps/pipeline/pipeline_docker.py:1526-1557`) all pass the correct, hardcoded `DECAD` value —
decadal forecasts continue to be produced, stored, and displayed normally, exactly as the owner
observed. See "What is NOT affected" below for the full verification.

**The defect is confined to the forecast dashboard's "Save Changes" button**, and only when an
operator has the **decade** horizon selected. (An earlier pass of this issue also implicated
"Trigger forecasts" — that was wrong; see the correction below and **FD-028**.) `save_to_database`
(the "Save Changes" handler) is the one path in the whole codebase that derives
`SAPPHIRE_PREDICTION_MODE` from the *live* value of the horizon selector widget
(`horizon.upper()`), and that widget's internal decade value is spelled `"decade"`, not `"decad"`
(`apps/forecast_dashboard/dashboard/widgets.py:99`) — producing `"DECADE"`, which matches **no**
module-side consumer's `SAPPHIRE_PREDICTION_MODE` domain. That mismatch poisons every container
"Save Changes" launches, for the decade horizon only — and affects every configured station, not
just the one on screen (see "Full chain" below).

**Correction: `"decade"` is not a mistake — it is a second, legitimate contract's vocabulary,
reused where it does not belong.** `save_to_database` also POSTs the raw `horizon` value as
`"horizon_type"` to the postprocessing API's visibility endpoint (`vizualization.py:4017`, also
`:3533` elsewhere in the same file), and that API's `HorizonType` enum requires exactly `"decade"`
(`sapphire/services/postprocessing/app/models.py:26`, `DECADE = "decade"`). The dashboard is
**correct** to use `"decade"` for that contract, and is not alone: `data_manager.py:167`,
`bulletin_publish.py:51`, `utils.py:183` (whose own docstring says *"pentad" or "decade" (NOT the
legacy "decad")*), `db.py:66`, and `vizualization.py:3123` all use `"decade"` deliberately, for
the same API-facing reason. The actual defect is that `save_to_database` reuses the **same**
`horizon` value, unmodified, to build `SAPPHIRE_PREDICTION_MODE` — a second contract with a
different, `DECAD`-based vocabulary — without translating between the two. See "Option A is
dangerous" below for why the fix is not to change the dashboard's own vocabulary.

## What is NOT affected (verified 2026-09-09)

- **Production cron**: `bin/run_decadal_forecasts.sh:92` — `-e SAPPHIRE_PREDICTION_MODE=DECAD`
  (hardcoded, correct).
- **Luigi `RunDecadalWorkflow`** (`apps/pipeline/pipeline_docker.py:1526-1561`):
  `LinearRegression(prediction_mode="DECAD")` (`:1542`),
  `RunMLModel(model_type=model, prediction_mode="DECAD", run_mode="forecast")` for every enabled
  ML model (`:1549`), and `PostProcessingForecasts(prediction_mode="DECAD")` (`:1557`) — all
  three hardcoded, all correct.
- Consequently: decadal forecasts, decadal ML runs, and decadal postprocessing from the
  **operational pipeline** are produced and displayed normally, on schedule, today. A reader must
  not come away from this issue believing decadal forecasting itself is broken — it is not. Only
  a **manual "Save Changes" re-run of a decade forecast** is affected.
- **Correction: do not describe the operational pipeline as producing decadal *skill metrics*.**
  `postprocessing_operational.py`'s own header states it explicitly (`:2-4`): *"Daily operational
  entry point: reads pre-calculated skill metrics, creates ensemble forecasts, and saves results.
  Does NOT recalculate skill metrics (fast path)."* `RunDecadalWorkflow` has no skill-metrics
  recalculation task at all. Decadal skill metrics are recalculated through separate
  maintenance/yearly/initial routes, not the operational path described above.

## Context

`create_horizon_selector` (`apps/forecast_dashboard/dashboard/widgets.py:90-110`) defines the
horizon dropdown's option values, defaulting to `"pentad"` (`:107`):

```python
horizon_types = {
    _("pentad"): "pentad",
    _("decade"): "decade",
}
if display_ML_forecasts:
    horizon_types[_("month")] = "month"
    horizon_types[_("season")] = "season"
```

`select_and_plot_data` (`apps/forecast_dashboard/src/vizualization.py:3436`) reads the selected
value into a local `horizon` variable at `:3449` (`horizon = wm.horizon_selector.value`), which
`save_to_database` — nested inside the same function — closes over *live*, since it is
re-evaluated on every render of `select_and_plot_data`. (`run_pipeline`, by contrast, is a
separate closure built once at dashboard-construction time over a *stale* copy of that value —
see "Correction: 'Trigger forecasts' never sends `DECADE`" below and **FD-028**.)

## Correction: "Trigger forecasts" never sends `DECADE` (fourth pass, CRITICAL)

An earlier pass of this issue claimed both dashboard buttons send `SAPPHIRE_PREDICTION_MODE=DECADE`.
That is false for "Trigger forecasts." Verified directly:

- `widgets.py:107` sets the horizon selector's default value to `"pentad"`.
- `apps/forecast_dashboard/dashboard/widget_manager.py:107` calls
  `cfg.viz.create_reload_button(self.horizon_selector.value)` **exactly once**, during dashboard
  construction — at that point the selector still holds its default `"pentad"`.
- `create_reload_button(horizon)` (`vizualization.py:4248`) takes `horizon` as a plain parameter
  and defines `run_pipeline` as a nested closure over it. Nothing ever calls
  `create_reload_button` again.
- The code that shows or hides the resulting `reload_card` based on the active tab
  (`vizualization.py:125-143`) only ever toggles `.visible` — it never rebuilds the card or
  re-invokes `create_reload_button`.

So `run_pipeline`'s closed-over `horizon` is **permanently `"pentad"`**, for the lifetime of the
dashboard session, regardless of what the operator has selected. `run_pipeline`'s own
`SAPPHIRE_PREDICTION_MODE={horizon.upper()}` (`:4355`) and its ML loop's `mode = horizon.upper()`
(`:4415`) are therefore always `PENTAD`, never `DECADE` — "Trigger forecasts" cannot expose this
issue. It has its own, separate, arguably worse defect: clicking "Trigger forecasts" while
viewing decade silently re-runs the **pentad** pipeline (and may overwrite pentad outputs) while
the operator believes they re-ran decade. That is filed separately as **FD-028** and is not fixed
by anything in this issue or LR-013.

**Every "Full chain" entry below involving "Trigger forecasts" is therefore latent for *this*
defect** (it would matter only once FD-028 is separately fixed to read the live selector value) —
kept in the inventory because the launch-site mapping and each module's domain remain accurate
and relevant background, but the "Fails ... on `DECADE`" verdicts for "Trigger forecasts" sites
do not apply today.

## Full chain: every static site the dashboard's two buttons launch a container from

Mapped by starting from each button handler's `environment` list and following every
`run_docker_container` call that receives it. **Seven static launch sites** (not six, as an
earlier pass counted — it missed one), plus the ML loop expands to one container per configured
model, so the actual container count on a "Trigger forecasts" run is **N+6** where N is the
number of enabled ML models:

**"Save Changes" — `save_to_database(event)` (`vizualization.py:3984`) — three sites, all live for this defect today**

`environment` built at `:4078-4082`, `SAPPHIRE_PREDICTION_MODE={horizon.upper()}` at `:4080`
(live read — see above).

1. `:4131` — `mabesa/sapphire-linreg:latest`, default command → `linear_regression.py`
   (`apps/linear_regression/Dockerfile:34`). Domain (before LR-013): `{PENTAD, DECAD, BOTH}`.
   **Fails silently today** (exits 0, writes nothing, for every configured station — see the
   station-scope correction below) — see LR-013.
2. `:4137-4138` — `mabesa/sapphire-postprocessing:latest`, default command →
   `postprocessing_operational.py` (`apps/postprocessing_forecasts/Dockerfile:38`). Domain
   (`postprocessing_operational.py:245`): `{PENTAD, DECAD, BOTH, MONTHLY, ALL}`. **Fails loudly
   today** — `sys.exit(1)` at `:251`, for every decade Save Changes click, for every configured
   station, independent of whether LR-013 ships.
3. `:4147-4157` — same image, **explicit** `command=["uv", "run", "recalculate_skill_metrics.py"]`
   override (`:4156`), plus `SAPPHIRE_RECALC_STATION_CODE={station_code}` appended to
   `environment` (`:4152`) — the **only** one of the seven sites that is station-scoped. Domain
   (`recalculate_skill_metrics.py`'s `VALID_MODES`, `:94-103`):
   `{PENTAD, DECAD, BOTH, MONTHLY, DAILY, QUARTERLY, SEASONAL, ALL}`. **Fails loudly today** the
   same way, but this call is wrapped in its own `try`/`except Exception as e: logger.warning(...)`
   (`:4145` / `:4160-4161`) — deliberately non-fatal by design (its surrounding comment says so),
   so this one failing does not block the reload that follows. Still means skill metrics are
   never actually recalculated for the affected station's decade Save Changes today.

**"Trigger forecasts" — `run_pipeline(event)` (`:4317`) — four sites, all currently latent for this defect (see correction above)**

`environment` built at `:4352-4357`, `SAPPHIRE_PREDICTION_MODE={horizon.upper()}` at `:4355`
(**stale** — always `PENTAD`, per the correction above).

4. `:4390` — `mabesa/sapphire-preprunoff:latest` (a site an earlier pass of this issue missed
   entirely). Runs `preprocessing_runoff`, which receives `environment` (including
   `SAPPHIRE_PREDICTION_MODE`) but **does not read it anywhere** (verified: no
   `SAPPHIRE_PREDICTION_MODE` reference in `apps/preprocessing_runoff/`) — a launch site with no
   domain to violate.
5. `:4400` — `mabesa/sapphire-linreg:latest` again. Same module/domain as (1); today always
   receives `PENTAD`, which LR already accepts.
6. `:4410-4422` — conditional on `ieasyhydroforecast_run_ML_models` (default `"True"`, i.e. **on
   by default**, `:4410-4411`): for each configured ML model, `mabesa/sapphire-ml:latest` with
   `RUN_MODE=forecast` → `make_forecast.py` (`apps/machine_learning/Dockerfile:40`).
   `mode = horizon.upper()` re-derived independently at `:4415` (same stale `horizon`, same
   value: always `PENTAD`). Domain (`make_forecast.py:605-607`): `{PENTAD, DECAD}` **only** —
   deliberately excludes `BOTH`/`ALL`, since ML runs one horizon per invocation. Today always
   receives `PENTAD`, which ML already accepts — no crash, contrary to what an earlier pass of
   this issue claimed as corroborating evidence (that crash was real in principle, but never
   actually reachable via `run_pipeline`, since it never sends `DECADE`).
7. `:4432` — `mabesa/sapphire-postprocessing:latest`, same module/domain as (2); today always
   receives `PENTAD`.

So today, only the three "Save Changes" sites are exposed to the `DECADE` mismatch: LR fails
silently, postprocessing fails loudly, and skill-recalc fails loudly-but-non-fatally — for every
configured station. The four "Trigger forecasts" sites are unaffected by *this* defect (they
always run pentad, correctly) but have their own defect (FD-028): the operator believes they
selected decade and get pentad instead, silently.

## Full reader inventory of `SAPPHIRE_PREDICTION_MODE` (context for the governing decision)

Beyond the four modules the dashboard's two buttons drive, the codebase has several other
readers of this variable, each with its own domain — relevant because any fix must normalize
*inside* each consumer, never by rewriting the shared/ambient value (see "Orchestrator-boundary
hazards" below). None of these are reached by the dashboard's buttons; listed for completeness so
a future normalization pass has the full picture:

- `apps/machine_learning/fill_ml_gaps.py:164` (read) / `:166` (check) and
  `apps/machine_learning/recalculate_nan_forecasts.py:160` (read) / `:162` (check) — both
  `{PENTAD, DECAD}` only, invoked via `machine_learning/Dockerfile:40`'s maintenance/default
  branches.
- `apps/postprocessing_forecasts/postprocessing_maintenance.py:124` — `{PENTAD, DECAD, BOTH}`;
  not reached by either dashboard button (they only ever launch the default
  `postprocessing_operational.py` command).
- `apps/validate_pipeline/validate_pipeline.py:1348` — case-insensitive already
  (`.upper()`), but **silently defaults any unrecognised value to `["pentad"]`**
  (`MODE_TO_HORIZONS.get(mode, ["pentad"])`) rather than failing loudly — a different failure
  shape from every other reader in this inventory.
- `apps/reset_forecast_run_date/rerun_forecast.py:191` (read, with a legacy
  `sapphire_forecast_horizon` env var fallback) — uses the raw value as a **filename suffix**
  (`:83`, `_{prediction_mode}.txt`), a different semantic domain entirely (identity, not a
  forecast-horizon selector).
- `apps/postprocessing_forecasts/postprocessing_forecasts.py.deprecated:112` (read) — checks
  `{PENTAD, DECAD, BOTH}` in its code, but its own comment at `:5` says *"Accepts SAPPHIRE
  PREDICTION MODE to be PENTAD, DECADE, or BOTH"* — a **self-contradicting, deprecated** file.
  This is the counter-example to any blanket claim that "every module and script uses `DECAD`,
  never `DECADE`" — such a claim is false as stated; it holds only for the *live* consumers this
  issue and LR-013 touch.

**Documentation itself does not reflect the per-consumer reality.** `doc/configuration.md:870`
presents `SAPPHIRE_PREDICTION_MODE` as one shared row across `linear_regression`,
`postprocessing_forecasts`, and `machine_learning`, with one domain — `PENTAD, DECAD, BOTH, or
ALL` — as if all three consumers share it. They do not: `machine_learning` rejects `BOTH`/`ALL`
outright. This is a real documentation defect worth recording, but **`doc/configuration.md` is
not edited by this issue or LR-013** — it needs its own per-consumer rework, out of scope here.

## Governing decision (owner, 2026-09-09) — applies to every module in this chain except the dashboard itself

Recorded in full in **LR-013** (`## GOVERNING DECISION`); restated here because it governs the
fixes needed in the other three modules, not only `linear_regression.py`:

**Normalize spelling and case; keep each module's own domain.**

- Case-insensitive comparison everywhere (`pentad` ≡ `PENTAD`, etc.).
- `decade` ≡ `decad` — one horizon, two spellings, both accepted, in every module whose domain
  includes the decad horizon.
- `ALL` ≡ `BOTH` **within `linear_regression.py` only** (LR-013's scope) — not a claim about any
  other module's `ALL`.
- **Not a single global whitelist.** Each module keeps the domain it legitimately has:
  - `make_forecast.py` must gain `decade`≡`decad` and case-insensitivity **only** — it must
    **not** start accepting `BOTH` or `ALL`.
  - `postprocessing_operational.py` must gain `decade`≡`decad` and case-insensitivity **only**.
    **Correction (fourth pass): its `ALL` does NOT mean "run pentad, decad, and monthly"** — an
    earlier pass of this issue said so; it is wrong. Verified: the `MONTHLY`/`ALL` branch
    (`:288-291`) only logs *"Monthly postprocessing is handled by
    postprocessing_operational_long_term.py. Skipping monthly in operational mode."* and does no
    monthly work. Under `ALL`, this module computes exactly the same two horizons as `BOTH`
    (`:268`/`:278` both gate on `["PENTAD"/"DECAD", "BOTH", "ALL"]`). Its existing `ALL`/`MONTHLY`
    semantics (narrower than previously stated) must not change.
  - `recalculate_skill_metrics.py` must gain the same two additions **only**. Unlike
    `postprocessing_operational.py`, this module's `ALL` genuinely is wider — confirmed
    line-by-line: `["PENTAD","BOTH","ALL"]` (`:277`), `["MONTHLY","ALL"]` (`:301`),
    `["QUARTERLY","ALL"]` (`:372`), `["SEASONAL","ALL"]` (`:443`), `["DAILY","ALL"]` (`:528`) —
    six horizons genuinely fold into `ALL` here. Collapsing it to `BOTH` anywhere would silently
    drop four of those six. Its wider `VALID_MODES` must not change.
- Exit 1 (or raise, per each module's existing failure convention) only on a value that is
  genuinely unrecognised **after** normalization. Nothing that succeeds today may start failing.

### Orchestrator-boundary hazards — do not normalize the ambient value

The normalization above must happen **inside each consuming module**, on that module's own copy
of the value — never by rewriting `SAPPHIRE_PREDICTION_MODE` itself at an orchestrator boundary,
and never as a single global whitelist. Concretely verified reasons:

- `apps/run_locally.sh`'s own `validate_env` (`:1977`) and its ML resolver
  (`resolve_ml_bare_target_modes`, `:541`) both accept only unset/`PENTAD`/`DECAD`/`BOTH` for the
  targets they gate, and `apps/pipeline/tests/test_run_locally_orchestration.py`'s
  `test_out_of_domain_mode_rejected_before_any_module_runs` (`:1677-1690`) explicitly requires
  `"ALL"` (and the typo `"PENTAAD"`) to be **rejected** there. A global `ALL`→`BOTH` rewrite ahead
  of that gate would break this passing regression test.
- `RunAllMLModels.requires()` (`pipeline_docker.py:792`) checks `self.prediction_mode == "ALL"`
  by exact string to expand into two separate, valid `RunMLModel(prediction_mode="PENTAD"/"DECAD")`
  tasks. If `ALL` were rewritten to `BOTH` ahead of this check, it would never match, and the
  `else` branch would produce a single `RunMLModel(prediction_mode="BOTH")` task instead — which
  `make_forecast.py` rejects outright.
- `rerun_forecast.py` uses the raw env var value, uppercased-or-not, as a filename suffix
  (`_{prediction_mode}.txt`, `:83`) — a blanket case-normalization of the ambient value could turn
  a working `_decad.txt` lookup into `_DECAD.txt`.
- **Normalization must replace the operative value used in later branches, not just relax a
  validation check.** `make_forecast.py` branches on the exact raw string at `:611`, `:638`,
  `:792`, `:933` (all `PREDICTION_MODE == "PENTAD"`/`"DECAD"` comparisons). An implementation that
  validates `raw.upper()` for acceptance but keeps comparing the un-normalized `raw` downstream
  would accept lowercase `pentad`, then silently execute the `else` branch at `:792`
  (`"decad"` — since raw `"pentad" != "PENTAD"`), running the wrong horizon's logic while
  believing it validated correctly. Any implementation (here or in `postprocessing_operational.py`
  / `recalculate_skill_metrics.py`) must reassign the normalized value into the same variable the
  downstream branches read, and acceptance criteria must assert on the *executed branch/output*
  for every accepted case/spelling variant, not merely that validation passed.

This issue (FD-027) tracks applying the module-local rule to `postprocessing_operational.py`,
`recalculate_skill_metrics.py`, and `make_forecast.py`, **and/or** fixing the dashboard's own
value at the "Save Changes" source (see "Desired outcome" below — both remain valid, non-exclusive
approaches).

## Related finding: FD-008 (why none of this has been visibly failing)

Every "fails loudly" consequence above on "Save Changes" (postprocessing, skill-recalc) should,
in principle, have been producing a visible dashboard error for as long as an operator has
clicked that button for the decade horizon. It has not, because the dashboard's own
container-runner code swallows the failure before it reaches the calling function:

- `save_to_database`'s nested `run_docker_container` (`vizualization.py:3858`) raises
  `docker.errors.ContainerError` on a non-zero exit code (`:3949-3961`), but that `raise` is
  caught by its own enclosing `except Exception as e: print(...)` (`:3970-3971`) — printed to the
  server console only, never surfaced to `save_to_database`. Even if it were re-raised, and even
  if `save_to_database`'s own outer `except docker.errors.DockerException` (`:4175`) let it
  through, that function's `finally` block unconditionally sets `progress_bar.value = 100`
  (`:4181`) regardless of outcome.
- `run_pipeline` uses a **separate**, module-level `run_docker_container` (`:4491`), which is
  more direct still: on a non-zero exit code it only prints a message (`:4559-4561`, with the
  comment `# Optionally log the error or add to a list of failed containers`) and falls through —
  it never raises anything, so `run_pipeline` also always continues to the next step. (Not
  implicated in *this* issue's own `DECADE` failure, since `run_pipeline` never receives
  `DECADE` — but the same swallow hides FD-028's own defect and any other real container failure
  in that flow.)

This is the exact defect shape as **P-007** — a failed container is indistinguishable from a
successful one at the calling layer. It is tracked as its own **pre-existing** issue, **FD-008**,
which this round corrected (its own scope claim about "Trigger forecasts" not having the bug was
wrong) and repriced Low → High; see that file, not duplicated here. FD-008 is **not** a blocking
dependency of this issue or of LR-013: once every module in the chain is normalized (this issue +
LR-013), none of them fail on `DECADE` any more, which makes FD-008's swallow moot for this
specific value even before FD-008 ships. FD-008 stays independently necessary for every *other*
kind of container failure in these two flows.

## Severity reasoning

- **How would an operator notice?** For LR: not at the point of the click (silent no-op — see
  LR-013). For postprocessing/skill-recalc: in principle a crash, but per "Related finding:
  FD-008" that crash is swallowed today, so in practice: not there either. The only way to notice
  is downstream — decade data (forecasts, skill metrics) that was supposedly just
  corrected/re-run stays stale or absent, for every configured station, with no correlated error
  anywhere in the dashboard.
- **How common is the trigger?** Not a misconfiguration or typo — it fires on every decade-horizon
  use of the real, currently-visible **"Save Changes"** button, which exists specifically for an
  operator to correct a forecast, and it breaks three of the four modules that button's chain
  touches (LR, postprocessing, skill-recalc). The pentad horizon is unaffected in all three
  (`"pentad".upper() == "PENTAD"`, in every consumer's domain).
- **Why High**: this is the root cause of a decade-horizon manual-workflow chain that has been
  fully broken (one silent failure plus two swallowed loud failures) for as long as the widget
  has used `"decade"`, affecting every configured station on each click. Priced at the same tier
  as LR-013, which shares this root cause; not higher, because — once the governing decision's
  normalization lands in every module — the practical remaining defect is the naming
  inconsistency itself plus whatever FD-008 leaves unresolved.

## Desired outcome

Two non-exclusive approaches; the recommendation below has changed from an earlier pass of this
issue.

**Option A — Rename the widget's internal value from `"decade"` to `"decad"` — REMOVED as a
recommended fix; DANGEROUS as written.** An earlier pass called this the smallest change. It is
not, and must not be implemented as originally described: `save_to_database` sends the **same**
raw `horizon` value as the API's `horizon_type` field (`:4017`, `:3533`), and the postprocessing
API's `HorizonType` enum **requires** `"decade"` (`sapphire/services/postprocessing/app/models.py:26`).
Renaming the widget's internal value to `"decad"` would break that currently-working visibility
save — a regression the owner's own "do not break anything that works today" constraint forbids.
`sapphire/services/` is also colleague-managed and out of bounds per CLAUDE.md, so even if this
were desirable, the fix could not reach across that boundary from this repo's `apps/` side alone.
**Do not rename the widget's internal value.**

**Option B — Translate `"decade"` to `DECAD` only at the point each `SAPPHIRE_PREDICTION_MODE`
value is built** (`vizualization.py:4080` for "Save Changes"; `:4355`/`:4415` for "Trigger
forecasts", moot until FD-028 is fixed), leaving the API-facing `horizon`/`horizon_type` value
untouched. This is the owner's chosen direction: translate at the boundary between the two
contracts, rather than changing the vocabulary either contract already correctly uses. The
per-module normalization in `postprocessing_operational.py`, `recalculate_skill_metrics.py`, and
`make_forecast.py` (governing decision above) is **independently required regardless of Option
B** — the dashboard is not the only conceivable producer of a `decade`-spelled or lowercase
value, and each module should not depend on every producer getting the translation right.

Either the boundary translation, the per-module normalization, or (preferably) both must ship;
neither should be treated as sufficient alone given how many production sites already rely on
`"decade"` remaining `"decade"` in its own (API) contract.

## What a fix must not break

- The user-facing label (translated via `_(...)`) and the **API-facing** `horizon`/`horizon_type`
  value (`:4017`, `:3533`, and the other production sites listed in "Summary") must be
  unaffected — none of those are this issue's fix target; only the `SAPPHIRE_PREDICTION_MODE`
  translation point (Option B) or the per-module domain checks are in scope.
- `postprocessing_operational.py`'s existing `ALL`/`MONTHLY` semantics (which, per the correction
  above, are narrower than an earlier pass of this issue stated — `ALL` ≡ `BOTH` for this module,
  monthly is always skipped), and `recalculate_skill_metrics.py`'s wider, genuinely six-horizon
  `VALID_MODES`, must not change — only case and the `decade`/`decad` spelling are in scope for
  those two.
- `make_forecast.py` must not gain `BOTH`/`ALL` — it must keep rejecting them; ML still runs one
  horizon per invocation. Any normalization must reassign the value actually used by its
  downstream exact-string branches (`:611`, `:638`, `:792`, `:933`), not just pass a relaxed
  validation check (see "Normalization must replace the operative value" above).
- `linear_regression.py`'s own normalization (LR-013) should remain in place even after this
  issue ships.
- Do not normalize `SAPPHIRE_PREDICTION_MODE` at any orchestrator boundary (`run_locally.sh`,
  Luigi task parameters) — see "Orchestrator-boundary hazards" above.
- Do not edit `doc/configuration.md` as part of this issue — its shared-domain-across-consumers
  presentation is recorded as a finding above, not fixed here.

## Out of scope

- `linear_regression.py`'s own fix (governing-decision normalization, plus failing loudly on
  everything still unrecognised) — tracked as **LR-013**, decided and independent of this issue.
- **FD-008** (the dashboard swallowing container failures instead of surfacing them) — tracked
  separately; not a dependency of this issue, see "Related finding: FD-008".
- **FD-028** ("Trigger forecasts" always sending `PENTAD` regardless of selection) — a separate
  defect with a separate fix (rebuild or re-read the closure); not fixed by anything here, and
  fixing it does not require or depend on this issue.
- Rewriting `doc/configuration.md`'s per-variable table into per-consumer domains — recorded as a
  finding, not implemented here.
- Whether `make_forecast.py`'s `ValueError` on an invalid `PREDICTION_MODE` is itself
  well-handled by the dashboard's `run_docker_container` — that is exactly FD-008's subject.

## Deployment note: behaviour change (state this in the release/deployment note, don't let it be discovered afterwards)

**Scope correction (fourth pass): the affected population is both narrower and wider than
earlier passes of this note said.** Narrower: this is "Save Changes" only — "Trigger forecasts"
never sent `DECADE` (see the correction above; FD-028 is its own, separate issue). Wider: each
affected "Save Changes" click is not scoped to the station on screen — it silently fails to write
a decade LR forecast, and fails loudly-but-invisibly in postprocessing, for **every configured
station**, at the selected boundary date. Only the skill-metrics recalculation was ever
station-scoped, and it already fails non-fatally-by-design.

The operational cron/Luigi pipeline already produces decade forecasts correctly and continuously
(see "What is NOT affected") — decadal skill metrics come from separate maintenance/yearly/initial
routes, not the operational path, and are not claimed to be affected by, or fixed by, this issue.

Once this issue and LR-013 both ship, "Save Changes" will, for the first time, actually carry out
a decade edit/re-run for every configured station when clicked. That is the intended fix, not a
regression, and should be called out in the release note. The visibility *edit* itself was never
lost — it POSTs to the API and shows "Changes Saved Successfully" (`vizualization.py:4016`,
`:4044`) *before* any container runs — only the regenerated forecast/skill-metric data for every
configured station has been going stale.

**The affected population may be reconstructable from logs, softened from an earlier pass of
this note that said it "cannot be bounded."** Both `run_docker_container` implementations write a
timestamped log per container under `docker_logs/` (`_write_container_log`, referenced near
`vizualization.py:3374`), and `save_to_database` itself logs horizon, station code, and target
period at `:4111` (`"D10 save_to_database: horizon=%s, target_horizon_value=%d, ..."`). A
deployment that has retained these logs may be able to reconstruct which decade "Save Changes"
clicks occurred and when, rather than relying purely on operator recollection — each deployment
should check its own log retention before assuming the population is unrecoverable.

## Acceptance criteria

- [ ] Decide whether to add the Option B boundary translation, the per-module normalization, or
  both (owner decision on emphasis; per-module normalization is required either way).
- [ ] `postprocessing_operational.py`, `recalculate_skill_metrics.py`, and `make_forecast.py`
  each accept `decade`/`DECADE` (any case) as `decad`, and accept every existing valid value
  case-insensitively, without gaining or losing any other value in their domain — verified by
  asserting the **executed branch/output** for each accepted variant, not merely that validation
  passed (see "Normalization must replace the operative value").
- [ ] A decade-horizon "Save Changes" click results in a real LR forecast being written for every
  configured station, a successful `postprocessing_operational.py` run for every configured
  station, and a successful skill-metrics recalculation for the selected station — verified by an
  integration-level test or captured container output, not just code reading.
- [ ] The API-facing `horizon`/`horizon_type` value sent by `save_to_database` (`:4017`, `:3533`)
  is unchanged by this fix.
- [ ] `postprocessing_operational.py`'s `ALL`/`MONTHLY` behavior (pentad+decad only under `ALL`,
  monthly always skipped) is unchanged; `recalculate_skill_metrics.py`'s six-horizon `ALL` is
  unchanged; `make_forecast.py` still rejects `BOTH`/`ALL`.
- [ ] `run_locally.sh`'s own domain gates and
  `test_run_locally_orchestration.py:1677-1690`'s requirement that `"ALL"` be rejected there are
  unaffected by this issue's changes.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero new skips, run
  across `forecast_dashboard`, `postprocessing_forecasts`, `machine_learning`, and `pipeline`
  (the last for the orchestration regression guard above).

## Fourth-pass corrections (2026-09-09, out-of-loop review of PR #506)

An out-of-loop review set returned 15 findings, one CRITICAL, against the PR built from this
issue and LR-013. All were re-verified directly against trunk before being applied (a reviewer
named a nonexistent file, `dashboard/viz.py` — its line numbers were correct, its filename was
not; treated with the same scepticism applied throughout this salvage).

- **CRITICAL: "Trigger forecasts" never sends `DECADE`.** See the dedicated correction section
  above. This invalidated roughly half of this issue's prior claims — every "Full chain" entry,
  the severity reasoning, the deployment note, and the acceptance criteria all previously implied
  both buttons were exposed. Corrected throughout; the revealed defect is split out as **FD-028**.
- **Renumbered FD-026 → FD-027** (see title/header) after a local-only branch was found to have
  already claimed `FD-026` for an unrelated defect (`get_all_stations_from_iehhf()`'s fallback
  message).
- **Launch-site count corrected**: seven static sites, not six — `:4390` (`preprunoff`, which
  receives but does not read the variable) was missing from the inventory. Actual container count
  is N+6 (N = configured ML models).
- **Option A reversed from "smallest fix" to "dangerous, do not implement."** `"decade"` is the
  correct value for the postprocessing API's `HorizonType` enum and is used deliberately at five
  other production sites (`data_manager.py:167`, `bulletin_publish.py:51`, `utils.py:183`,
  `db.py:66`, `vizualization.py:3123`) plus two API POST sites (`:3533`, `:4017`) — renaming it
  would break a working API contract.
- **`postprocessing_operational.py`'s `ALL` semantics corrected** — computes the same two
  horizons as `BOTH`; monthly is explicitly skipped, not run. `recalculate_skill_metrics.py`'s
  wider, six-horizon `ALL` was independently confirmed and is **not** affected by this
  correction — the two modules must not be conflated.
- **Station-scope corrected — widened, not narrowed.** LR and postprocessing run for every
  configured station on a "Save Changes" click; only skill-metrics recalculation is
  station-scoped. An earlier pass of this issue implied the opposite.
- **"Every module uses `DECAD`, never `DECADE`" corrected** — the deprecated
  `postprocessing_forecasts.py.deprecated:5` comment documents `DECADE`. Softened to scope the
  claim to live consumers only.
- **`doc/configuration.md`'s shared-domain presentation recorded as a finding**, not edited here
  — it presents one domain across three consumers with materially different real domains.
- **Backfill-population claim softened** from "cannot be bounded" to "may be reconstructable" —
  both container runners write timestamped logs, and `save_to_database` itself logs horizon,
  station code, and period.
- **Full reader inventory added** (`fill_ml_gaps.py`, `recalculate_nan_forecasts.py`,
  `postprocessing_maintenance.py`, `validate_pipeline.py`, `rerun_forecast.py`, the deprecated
  file) and **orchestrator-boundary hazards added** (`run_locally.sh`'s gates and their
  regression test, `RunAllMLModels`'s exact-string `ALL` expansion, `rerun_forecast.py`'s
  filename-suffix usage) — none of these existed in the prior pass.
- **Citation fixes**: `RunDecadalWorkflow` starts at `pipeline_docker.py:1526`, not `1524`;
  `recalculate_skill_metrics.py`'s domain check spans `:266-271` (the `sys.exit(1)` is at `:271`,
  not `:270`); the ML loop block is `:4410-4422` (an earlier citation, `:4415-4421`, omitted the
  `run_ML_models` condition at `:4410-4411` and stopped one line short of the call's own closing
  paren); the "eight other selector reads" wording was off by one against its own (correct) list
  of seven — restated as seven other reads, eight total including `:3449`.
- **Skill-metrics claim on the operational pipeline corrected** — `postprocessing_operational.py`
  explicitly does not recalculate skill metrics (`:2-4`); "What is NOT affected" no longer implies
  otherwise.

## References

- `apps/forecast_dashboard/dashboard/widgets.py:90-110` (`create_horizon_selector`, `"decade"`
  value at `:99`, default `"pentad"` at `:107`), `:336` (`elif horizon == "decade":`)
- `apps/forecast_dashboard/dashboard/widget_manager.py:107` (`create_reload_button` called once,
  at construction — root cause of FD-028)
- `apps/forecast_dashboard/src/vizualization.py:3436` (`select_and_plot_data`), `:3449`
  (`horizon = wm.horizon_selector.value`, the live read), `:125-143` (`reload_card` toggled
  `.visible` only, never rebuilt — FD-028 evidence), `:3123` (a `horizon == "decade"` production
  use), `:3533, :4017` (the API `horizon_type` POST sites), `:3858` (nested
  `run_docker_container`), `:3949-3971` (its exit-status check and swallow — FD-008), `:4016,
  :4044` (visibility-save POST and "Changes Saved Successfully" alert, before any container
  runs), `:4092, :4111, :4232` (`# decad` comments and the horizon/station/period log line),
  `:3984, :4080, :4131, :4137-4138, :4145, :4147-4157, :4152, :4160-4161, :4195`
  (`save_to_database` / "Save Changes", all three containers), `:4317, :4355, :4390, :4400,
  :4410-4422, :4432, :4471` (`run_pipeline` / "Trigger forecasts", all four sites), `:4491-4574`
  (module-level `run_docker_container`, its silent status-ignore at `:4559-4561`, its separate
  generic-exception swallow at `:4573-4574` — FD-008), other `horizon =
  wm.horizon_selector.value` reads at `:511, :688, :1716, :1934, :2103, :3243, :4586` (seven
  other reads; eight total including `:3449`)
- `apps/linear_regression/linear_regression.py:646-647` (silent no-op on an unmatched mode —
  LR-013's subject), `apps/linear_regression/Dockerfile:34` (default CMD)
- `apps/postprocessing_forecasts/postprocessing_operational.py:2-4` (module header: does NOT
  recalculate skill metrics), `:245-251` (domain check, loud `sys.exit(1)` on an unmatched mode),
  `:268, :278` (`ALL`≡`BOTH` for this module), `:288-291` (`MONTHLY`/`ALL` branch: logs and skips,
  does no monthly work), `apps/postprocessing_forecasts/Dockerfile:38` (default CMD)
- `apps/postprocessing_forecasts/recalculate_skill_metrics.py:94-103` (`VALID_MODES`), `:169,
  :212` (station-code scoping), `:266-271` (domain check incl. `sys.exit(1)`), `:277, :301, :372,
  :443, :528` (the six genuinely-`ALL`-inclusive branches)
- `apps/machine_learning/make_forecast.py:605-608` (loud `ValueError` on an unmatched mode;
  domain deliberately excludes `BOTH`/`ALL`), `:611, :638, :792, :933` (exact-string branches —
  normalization must replace the operative value here, not just relax validation),
  `apps/machine_learning/Dockerfile:40` (default CMD, branches on `RUN_MODE`)
- `apps/machine_learning/fill_ml_gaps.py:164, :166`, `apps/machine_learning/recalculate_nan_forecasts.py:160, :162`
  (both `{PENTAD, DECAD}` only), `apps/postprocessing_forecasts/postprocessing_maintenance.py:124`
  (`{PENTAD, DECAD, BOTH}`, not reached by the dashboard),
  `apps/validate_pipeline/validate_pipeline.py:1348` (case-insensitive, silently defaults to
  pentad), `apps/reset_forecast_run_date/rerun_forecast.py:83, :191` (filename-suffix usage — a
  different semantic domain), `apps/postprocessing_forecasts/postprocessing_forecasts.py.deprecated:5, :112`
  (self-contradicting: comment says `DECADE`, code checks `DECAD`)
- `apps/run_locally.sh:541, :1977` (orchestrator-level domain gates),
  `apps/pipeline/tests/test_run_locally_orchestration.py:1677-1690` (requires `"ALL"` rejected —
  do not break), `apps/pipeline/pipeline_docker.py:792` (`RunAllMLModels`'s exact-string `"ALL"`
  expansion — orchestrator hazard), `:1526-1561` (`RunDecadalWorkflow`, all hardcoded)
- `doc/configuration.md:870` (presents one shared domain across three consumers with materially
  different real domains — recorded as a finding, not edited here)
- `sapphire/services/postprocessing/app/models.py:26` (`HorizonType.DECADE = "decade"` — the API
  contract `"decade"` correctly serves)
- `apps/forecast_dashboard/dashboard/data_manager.py:167`, `apps/forecast_dashboard/dashboard/bulletin_publish.py:51`,
  `apps/forecast_dashboard/dashboard/utils.py:183`, `apps/forecast_dashboard/src/db.py:66`
  (production sites correctly using `"decade"` for the API-facing contract)
- `doc/plans/issues/high_prio_gi_draft_lr_unrecognised_mode_silent_exit_zero.md` (LR-013 — the
  full defect analysis, the governing decision in full, and the LR-specific fix)
- `doc/plans/issues/high_prio_gi_draft_fd_inner_run_docker_error_handling.md` (FD-008 — the
  swallowed-container-failure defect, repriced High)
- `doc/plans/issues/high_prio_gi_draft_fd_trigger_forecasts_stale_horizon_closure.md` (FD-028 —
  the "Trigger forecasts" stale-closure defect)
- `doc/plans/issues/archive/high_prio_gi_draft_pipeline_container_exit_status_discarded.md`
  (P-007 — same defect shape as FD-008, already fixed once in the Luigi pipeline)
