# INFRA-051: `ieasyhydroforecast_run_ML_models` is compared incompatibly across consumers, and the documented lowercase value silently disables Luigi's ML scheduling

**Status**: Draft (2026-09-09)
**Module**: `apps/pipeline` (`pipeline_docker.py`) + `apps/forecast_dashboard` (`dashboard/config.py`,
`src/vizualization.py`) + `apps/postprocessing_forecasts` (`src/data_reader.py`) + `bin/` (`setup_docker.sh`,
`daily_ml_maintenance.sh`) + `doc/configuration.md` + `apps/pipeline/README`
**Priority**: High — the documented, shipped-template value (`true`/`false`, lowercase) makes the
Luigi pipeline's automated ML scheduling silently schedule **zero** ML tasks, on every pentad,
decade, and maintenance run, with no error and no log distinguishing this from "ML genuinely
disabled".
**Labels**: `infra`, `pipeline`, `dashboard`, `env-contract`, `silent-failure`, `case-sensitivity`
**Found**: 2026-09-09. **Owner decision 2026-09-09: file this now, do not implement a fix** — the
fix requires an owner decision on which literal (`true`/`false` vs `True`/`False`) is canonical,
recorded as the open decision below.

---

## Defect

`ieasyhydroforecast_run_ML_models` gates whether Luigi schedules ML forecast/maintenance
containers. It is read and compared differently at every site that reads it. Verified by opening
each file at HEAD:

### Luigi scheduling (`apps/pipeline/pipeline_docker.py`) — case-sensitive, exact `"True"`

Read once with **no default** at `:40`:

```python
RUN_ML_MODELS = env.get("ieasyhydroforecast_run_ML_models")
```

(`Environment.get`, `apps/pipeline/src/environment.py:10`, is `os.getenv(key, default=None)` — an
**absent** variable resolves to `None`, which is not `"True"`, so absence is silently treated as
"off" here.)

Compared `== "True"` at four sites that actually gate ML task scheduling:

| Line | Task | Effect |
|---|---|---|
| `:816` | `PostProcessingForecasts.requires()` | adds `RunMLModel` deps for the given `prediction_mode` |
| `:1483` | `RunPentadalWorkflow.requires()` | adds `RunMLModel` deps for `PENTAD` |
| `:1545` | `RunDecadalWorkflow.requires()` | adds `RunMLModel` deps for `DECAD` |
| `:1870` | `PostProcessingMaintenance.requires()` | adds `MLMaintenance` deps (used by the canonical
`bin/run_daily_maintenance.sh` → `RunDailyMaintenanceWorkflow`) |

**Correction to an earlier note**: `:1500` and `:1562` are `RUN_ML_MODELS == "True" or
RUN_CM_MODELS == "True"` guards inside `RunPentadalWorkflow.requires()` /
`RunDecadalWorkflow.requires()`, but they gate whether `DeleteOldGatewayFiles()` is appended as a
*cleanup* dependency — they do not gate `RunMLModel` scheduling and are not part of the ML-gating
list above. Confirmed by reading `apps/pipeline/pipeline_docker.py:1494-1501` and `:1556-1563`.

With the documented lowercase value (`"true"`), all four gates above evaluate `False` and **no ML
task is scheduled** — silently, with no log line distinguishing this from an operator who
genuinely set `false`.

**`RunAllMLModels` (`:776-799`) does not consult `RUN_ML_MODELS` at all** — confirmed by reading
its `requires()`, which yields `RunMLModel` for every model × both prediction modes
unconditionally. This is a real gap in the code (if this task is ever invoked, the flag is
ignored entirely), but **it is not wired to any current production entrypoint**: neither
`apps/pipeline/Dockerfile:38`'s `CMD` nor any `command:` in `bin/docker-compose-luigi.yml` (which
invoke `PreprocessingRunoff`, `RunPreprocessingGatewayWorkflow`, `RunPreprocessingRunoffWorkflow`,
`RunPentadalWorkflow`, `RunDecadalWorkflow`, the maintenance workflows, or the long-term workflow)
names `RunAllMLModels`. Its only other repository references are two test files and one cosmetic
string inside a notification-message body (`pipeline_docker.py:1397`, `"- RunAllMLModels\n"`)
that lists a task name for an email and does not instantiate the class. Record this as a latent
inconsistency to close (a manual `luigi --module ... RunAllMLModels` invocation would silently
ignore the flag) rather than a demonstrated production gap.

### `apps/iEasyHydroForecast/setup_library.py:4352-4358`, `:4522-4533` — legacy, not on the live
read path; an earlier note about its effect was wrong

`read_observed_and_modelled_data_pentade()` (`:4314`) and `read_observed_and_modelled_data_decade()`
(`:4497`) each do:

```python
if read_ml_results is None:
    ...  # no read
elif read_ml_results == "False":
    ...  # no read
elif read_ml_results == "True":
    ...  # reads TIDE/TFT/TSMIXER/ARIMA
else:
    logger.warning(
        "... is set to an invalid value. Assuming no ML forecasts to be read."
    )  # :4405, :4573 — still no read
```

Two corrections here:

1. **The effect of a non-matching value is wrong in the earlier description.** The `else` branch
   (`:4404-4406`, `:4572-4574`) does **not** fall through to reading — it explicitly logs an
   "invalid value" warning and reads nothing, the same practical outcome as `"False"`, just with a
   misleading log message. A lowercase `"false"` or `"true"` both land here and both skip the
   read. This function does **not** make "the dashboard proceed to read ML results" under the
   documented lowercase schema.
2. **These two functions are legacy and not called from production code.** Confirmed by grep:
   their only references outside their own definitions are comments in
   `apps/postprocessing_forecasts/tests/test_workflow_integration.py` and
   `test_monthly_workflow_integration.py` describing "the old" equivalent, and a docstring in
   `apps/postprocessing_forecasts/src/data_reader.py:2755-2756` naming
   `read_observed_and_modelled_data()` as the "API-first reader that replaces" them. They are not
   part of the live dashboard-read path.

### Live consumers found beyond the ones above (all case-insensitive, and correct)

- `apps/forecast_dashboard/dashboard/config.py:202` (`display_weather_and_snow_data`):
  `os.getenv('ieasyhydroforecast_run_ML_models', 'False').lower() in ('true', 'yes', '1', 't', 'y')`
  — case-insensitive, handles the documented lowercase value correctly.
- `apps/postprocessing_forecasts/src/data_reader.py:2647` (`read_individual_model_forecasts`,
  the current live reader): `os.getenv("ieasyhydroforecast_run_ML_models", "false").lower()`
  compared `== "true"` — case-insensitive, correct.
- `bin/setup_docker.sh:89` sets `RUN_ML_MODELS="${ieasyhydroforecast_run_ML_models:-false}"`, then
  `:110`'s `if [ "${RUN_ML_MODELS,,}" = "true" ]` lower-cases before comparing — correct either
  way.

### One more live case-sensitive site, with the opposite absent-variable default

`apps/forecast_dashboard/src/vizualization.py:4410-4411`, inside `run_pipeline(event)` — the click
handler wired to the dashboard's manual "reload forecasts" button
(`create_reload_button`, `:4248` onward) that runs the ML Docker containers directly, independent
of Luigi:

```python
run_ML_models = os.getenv("ieasyhydroforecast_run_ML_models", "True")
if run_ML_models == "True":
```

Case-sensitive like `pipeline_docker.py`, so the documented lowercase `"true"` also fails this
check and the button silently skips launching ML containers. But its **default differs**: an
*absent* variable resolves to `"True"` here (ML runs), versus `None`/off in
`pipeline_docker.py:40`. The same variable is therefore silently on-by-default in one live
consumer and off-by-default in another.

## Documentation says lowercase; one doc contradicts it

- `doc/configuration.md:138` documents the value as `` `true`/`false` ``.
- `doc/configuration.md:317`: "Set `ieasyhydroforecast_run_ML_models=true`."
- `apps/config/.env_develop:12` ships `ieasyhydroforecast_run_ML_models=false` (the shipped
  minimal-deployment template).
- `apps/pipeline/README:225` documents it as `` `True`/`False` `` — contradicting
  `doc/configuration.md`.

Following `doc/configuration.md` exactly (as the shipped `.env_develop` template does) produces a
value that silently disables Luigi's four ML-scheduling gates.

## Reachability — cannot be confirmed from this repository

Real deployment env files live outside this repository. `.gitignore:185` excludes
`apps/config/.env_develop_kghm` by name, and the pattern covers the other per-org files the same
way — none of them are in the repo to inspect. This issue cannot state which live deployments are
affected. An operator must inspect each live `.env_develop_<org>` file's literal value (`True` vs
`true`) against the gates above. Do not treat any specific deployment as confirmed broken from
this issue alone.

**Do not start with uzhm.** An earlier draft of this issue named it as the deployment to check
first; that is wrong. `apps/run_locally.sh:225` lists `machine_learning` in `UZHM_SKIP_MODULES`,
so uzhm does not run ML by design and the value of this flag is moot there. Check the
**ML-enabled** deployments instead — kghm and tjhm — which do produce ML forecasts today and
therefore presumably already carry the capitalised literal. The population genuinely at risk is a
**new** ML-enabled deployment configured from `doc/configuration.md`, which instructs the
lowercase value.

## Why this is silent, and where the asymmetry actually is

`RUN_ML_MODELS == "True"` failing does not raise or log a distinguishable message — Luigi's
`requires()` methods simply return a shorter dependency list, identical in shape to a
deliberately-disabled deployment. Meanwhile `config.py:202`'s case-insensitive check would report
`display_ML_forecasts = True` for the same lowercase value, so the dashboard's own "is ML
configured" signal disagrees with what the Luigi-scheduled pipeline actually produced — an
operator sees ML represented as configured while the automated pipeline silently schedules none.
This is the same family of defect as PP-051/ML-021/PREPG-026: a component reporting (or implying)
success/configuration having done nothing.

The maintenance side does **not** add a second asymmetry on the canonical path:
`bin/run_daily_maintenance.sh` drives `RunDailyMaintenanceWorkflow` → `PostProcessingMaintenance`,
gated at the same case-sensitive `:1870` predicate as forecast scheduling, so on the documented
Luigi path, forecast scheduling and maintenance scheduling fail together, not asymmetrically. The
legacy `bin/daily_ml_maintenance.sh:41` (`!= "true" && != "True"`, case-handled for those two
literals) is documented as manual-debug-only and explicitly **not** to be cron-scheduled
(`doc/prod/update_deployment_checklist.md:708`, `:877`); it would reproduce the asymmetry the
maintenance-succeeds-while-forecasts-are-empty framing describes, but only on a deployment where
it is still cron-scheduled despite that guidance — unconfirmed from this repo, and worth an
operator crontab check given this repo's history of crontab drift from documented practice.

## Do not fix by normalising only one side

The live case-sensitive-positive consumers (`pipeline_docker.py`'s four gates,
`vizualization.py:4411`) and the live case-insensitive consumers (`config.py:202`,
`data_reader.py:2647`, `setup_docker.sh:110`) already agree on which literal means "on" — the risk
is not opposite-literal disagreement (an earlier note claiming that was wrong; see the
`setup_library.py` correction above). The real risk of a partial fix is narrower but still real:
normalising only `pipeline_docker.py` to accept lowercase, without touching
`vizualization.py:4410-4411`, leaves the manual "reload forecasts" button silently skipping ML
containers under the documented value while the automated Luigi path starts working — a new,
harder-to-notice split between the two ML-triggering paths in the same dashboard/pipeline system.
The absent-variable default split (`None`/off in `pipeline_docker.py:40` vs `"True"`/on in
`vizualization.py:4410`) is a second, independent inconsistency that a same-literal fix does not
resolve on its own.

## Open decision for the owner (not resolved here)

Either:
(a) normalise every consumer to compare case-insensitively and keep the documented lowercase
    schema (`true`/`false`), correcting `apps/pipeline/README:225` and any other `True`/`False`
    documentation to match, or
(b) standardise all docs and templates on the capitalised literal (`True`/`False`), correcting
    `doc/configuration.md:138`/`:317` and `apps/config/.env_develop:12` to match.

Either way, decide:
- whether an unrecognised value (a typo, or any value outside the chosen two-token set) should
  fail loudly at startup rather than being silently treated as "off" (today's behaviour in all
  four `pipeline_docker.py` gates and in `vizualization.py:4411`), and
- what an *absent* variable should mean, given `pipeline_docker.py:40` and
  `vizualization.py:4410` currently disagree (off vs on) with no test pinning either as
  intentional.

## Out of scope

- Implementing any fix — explicitly deferred per the 2026-09-09 owner decision above.
- `ieasyhydroforecast_run_CM_models`, which has the same case-sensitive-at-`pipeline_docker.py`
  shape (`:827`, `:1491`, `:1553`) and is already tracked as newly-reachable drift under
  INFRA-048's note and related to INFRA-038 — not duplicated here.
- `apps/iEasyHydroForecast/setup_library.py`'s legacy `read_observed_and_modelled_data_pentade`/
  `_decade` functions themselves — dead code, not read by any production path; noted only to
  correct the earlier claim about their effect, not proposed for cleanup here.
- `RunAllMLModels`'s missing gate — recorded above as a latent inconsistency to fix alongside the
  rest, not escalated as a standalone production incident since no entrypoint currently reaches it.

## Acceptance criteria

- [ ] Every consumer of `ieasyhydroforecast_run_ML_models` (at minimum:
      `pipeline_docker.py`'s four gates and `RunAllMLModels`, `vizualization.py:4410-4411`,
      `config.py:202`, `data_reader.py:2647`, `setup_docker.sh:89-110`,
      `daily_ml_maintenance.sh:41`) agrees on the same predicate for the same literal value.
- [ ] A value that is neither the recognised "true" token nor the recognised "false" token is
      reported loudly (raised or logged at error/warning level in a way a monitored run would
      surface) rather than silently treated as "off".
- [ ] `doc/configuration.md`, `apps/pipeline/README`, and `apps/config/.env_develop` all state the
      same literal for this variable, per whichever direction (a) or (b) above the owner picks.
- [ ] A test pins the predicate for each consumer's chosen implementation (or for one shared
      helper, if the fix consolidates them) against `True`/`true`/`TRUE`/`False`/`false`/`FALSE`
      and an absent variable, so a future casing regression fails a test instead of a production
      run.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero unexpected
      skips.

---

## Related

| ID | Relation |
|---|---|
| INFRA-038 | Same defect class (`connect_to_iEH`/`ssh_to_iEH` parsed four incompatible ways) in a different pair of variables |
| INFRA-048 | Notes the sibling variable `ieasyhydroforecast_run_CM_models` has the same case-sensitive-at-`pipeline_docker.py:827` shape, newly reachable via PREPG-023's case-insensitive gateway gate |
| PP-051 / ML-021 / PREPG-026 | Same family: a component reports or implies success/configuration while having silently done nothing |
