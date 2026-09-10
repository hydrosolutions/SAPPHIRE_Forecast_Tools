# INFRA-051: `ieasyhydroforecast_run_ML_models` is compared incompatibly across consumers, and the documented lowercase value silently disables Luigi's ML scheduling

**Status**: Draft (2026-09-09)
**Revised**: 2026-09-10 — decision framing restructured after an out-of-loop review found the
defect analysis sound but the decision unusable as written (6 Critical + 8 Important findings, all
addressed below). The defect analysis itself is unchanged by this revision except where a
correction is called out explicitly.
**Module**: `apps/pipeline` (`pipeline_docker.py`) + `apps/forecast_dashboard` (`dashboard/config.py`,
`src/vizualization.py`) + `apps/postprocessing_forecasts` (`src/data_reader.py`) + `bin/` (`setup_docker.sh`,
`daily_ml_maintenance.sh`) + `doc/configuration.md` + `apps/pipeline/README`
**Priority**: High — the documented, shipped-template value (`true`/`false`, lowercase) makes the
Luigi pipeline's automated ML scheduling silently schedule **zero** ML tasks, on every pentad,
decade, and maintenance run, with no error and no log distinguishing this from "ML genuinely
disabled".
**Labels**: `infra`, `pipeline`, `dashboard`, `env-contract`, `silent-failure`, `case-sensitivity`
**Found**: 2026-09-09. **Owner decision 2026-09-09: file this now, do not implement a fix** — the
fix requires several owner decisions (restructured below into D1–D5, a third design option, and a
migration plan), not the single-question framing this issue originally offered.

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
**They are, however, in the fix's scope** (see "In scope" below): they read the same
module-level `RUN_ML_MODELS` variable, so any change to how that variable is parsed changes their
behavior too, whether or not the guard lines themselves are edited.

With the documented lowercase value (`"true"`), all four gates above evaluate `False` and **no ML
task is scheduled** — silently, with no log line distinguishing this from an operator who
genuinely set `false`.

**`RunAllMLModels` (`:776-799`) does not consult `RUN_ML_MODELS` at all** — confirmed by reading
its `requires()`, which yields `RunMLModel` for every model × both prediction modes
unconditionally. This is a real gap in the code (if this task is ever invoked, the flag is
ignored entirely), but **no repository-wired production entrypoint was found for it**: neither
`apps/pipeline/Dockerfile:38`'s `CMD` nor any `command:` in `bin/docker-compose-luigi.yml` (which
invoke `PreprocessingRunoff`, `RunPreprocessingGatewayWorkflow`, `RunPreprocessingRunoffWorkflow`,
`RunPentadalWorkflow`, `RunDecadalWorkflow`, the maintenance workflows, or the long-term workflow)
names `RunAllMLModels`. Its only other repository references are two test files and one cosmetic
string inside a notification-message body (`pipeline_docker.py:1397`, `"- RunAllMLModels\n"`)
that lists a task name for an email and does not instantiate the class. External or manual Luigi
invocation (e.g. an operator running `luigi --module ... RunAllMLModels` by hand) cannot be
excluded from repository evidence alone. **`RunAllMLModels` is now in scope** (see D5 and "In
scope" below) — it is not merely cosmetic: `pipeline_docker.py:1393-1400` hard-codes
`"- RunAllMLModels\n"` into the kghm branch of the message sent at `:1415` by
`SendPipelineCompletionNotification.run()` (class starts `:1318`). That code path does not
instantiate `RunAllMLModels`, so this is a **non-invoking but misleading production notification**
— a kghm run's completion email can list "RunAllMLModels" as a completed task regardless of
whether any ML task actually ran. Whether to fix the notification itself (beyond not making it any
less accurate than today) is a separate, still-pending owner decision — not resolved here; flagging
it so it isn't lost, per the "Related" table below (same family as PP-051/ML-021/PREPG-026).

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
2. **These two functions are legacy, with no active production caller found — but they are not
   dead code with zero references.** They are actually called (not merely mentioned in comments)
   by test helpers: `apps/postprocessing_forecasts/tests/test_workflow_integration.py:158-172`
   and `test_monthly_workflow_integration.py:221-223` monkeypatch
   `data_reader.read_observed_and_modelled_data` to delegate to
   `real_sl.read_observed_and_modelled_data_pentade()` / `_decade()`, exercising the real legacy
   functions against test-CSV fixtures. They are also still called from the deprecated module
   `apps/postprocessing_forecasts/postprocessing_forecasts.py.deprecated:126,166`. A docstring in
   `apps/postprocessing_forecasts/src/data_reader.py:2755-2756` names
   `read_observed_and_modelled_data()` as the "API-first reader that replaces" them, confirming
   they are superseded — but "superseded, with test and deprecated-module references" is more
   precise than "not called from production code," and is what this issue means by "legacy": they
   are not part of the live dashboard-read path, not that they are unreferenced anywhere.

### Live consumers found beyond the ones above (all case-insensitive, and correct)

- `apps/forecast_dashboard/dashboard/config.py:202` (`display_weather_and_snow_data`):
  `os.getenv('ieasyhydroforecast_run_ML_models', 'False').lower() in ('true', 'yes', '1', 't', 'y')`
  — case-insensitive, handles the documented lowercase value correctly, and additionally accepts
  the aliases `yes`/`1`/`t`/`y` that no other consumer recognizes (see "Aliases" under Decisions).
- `apps/postprocessing_forecasts/src/data_reader.py:2647` (`read_individual_model_forecasts`,
  the current live reader): `os.getenv("ieasyhydroforecast_run_ML_models", "false").lower()`
  compared `== "true"` — case-insensitive, correct, but does **not** accept the aliases
  `config.py` accepts.
- `bin/setup_docker.sh:89` sets `RUN_ML_MODELS="${ieasyhydroforecast_run_ML_models:-false}"`, then
  `:110`'s `if [ "${RUN_ML_MODELS,,}" = "true" ]` lower-cases before comparing — correct either
  way, and also does not accept aliases.

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

## Documentation inventory

Verified by opening each file at the cited line (2026-09-10). Split by whether the text is a
current, prescriptive operator instruction (must be updated together per D1, below) or historical
material that does not need to change for this fix.

### Authoritative — current operator-facing docs; must agree on one literal per D1

| File:line | Current text |
|---|---|
| `doc/configuration.md:138` | `` `ieasyhydroforecast_run_ML_models` \| Required \| dashboard, pipeline \| `true`/`false` — gates ML forecast container `` — also the only place the variable is labeled **Required**, relevant to D3 (absent policy) below. |
| `doc/configuration.md:317` | "Set `ieasyhydroforecast_run_ML_models=true`." |
| `apps/config/.env_develop:12` | `ieasyhydroforecast_run_ML_models=false` — the shipped minimal-deployment template. |
| `apps/pipeline/README:225` | `` `ieasyhydroforecast_run_ML_models` \| Enable ML tasks (`True`/`False`) `` — contradicts `doc/configuration.md`. |
| `doc/prod/update_deployment_checklist.md:348` | `` `ieasyhydroforecast_run_ML_models` \| Enable ML forecasting \| `true` or `false` `` |
| `doc/prod/update_deployment_checklist.md:504` | "If `ieasyhydroforecast_run_ML_models=true` in your .env file:" |
| `doc/prod/backfill_period_forecasts_runbook.md:61` | "ML reading is configured: `ieasyhydroforecast_run_ML_models=true` and a non-empty ..." |
| `doc/prod/historical_backfill_runbook.md:338` | "P5 runs only when ieasyhydroforecast_run_ML_models is true and configured ML models are present." |
| `doc/plans/deployment_new_hydromet_aws.md:323` | `` `ieasyhydroforecast_run_ML_models` \| `true` / `false` — ML container is optional \| Required `` |

### Historical/diagnostic — not prescriptive; no change required for this fix

| File:line | Why it's excluded |
|---|---|
| `doc/prod/historical_backfill_runbook.md:318` | `echo "run_ml=${ieasyhydroforecast_run_ML_models:-unset}"` — echoes whatever the live value already is; not an instruction to set a particular literal. Worth an operator sanity-checking against whichever literal D1 picks, but not a doc bug. |
| `doc/plans/configuration_update_plan.md:392` | `ieasyhydroforecast_run_ML_models=False` inside an aspirational future `.env` template. The document's own header states `**Status**: Planning` — not implemented. Leave as-is; reconcile only if/when that plan is adopted. |
| `doc/plans/configuration_update_plan.md:828` | "`ieasyhydroforecast_run_ML_models` - True/False" in that same planning doc's "Appendix: Current Environment Variables Audit". Same status caveat. |

## Existing test surface (must reconcile with any chosen contract)

These tests already encode a predicate for this variable. A fix that doesn't reconcile with them
will pass its own new tests while silently breaking (or masking) these:

- `apps/pipeline/tests/conftest.py:40` — `os.environ.setdefault("ieasyhydroforecast_run_ML_models", "False")`: the pipeline test suite's shared fixture default is **capitalized**.
- `apps/pipeline/tests/test_task_implementations.py:185`, `:196` — monkeypatches `pipeline_docker.RUN_ML_MODELS` directly to the string `"True"` (enabled case), and relies on the `"False"` fixture default for the disabled case; pins the exact-capitalized-string contract for `PostProcessingForecasts.requires()`.
- `apps/pipeline/tests/test_maintenance_tasks.py:193` — same pattern (`"True"`) for `PostProcessingMaintenance.requires()`.
- `apps/postprocessing_forecasts/tests/test_data_reader.py:2867`, `:2901`, `:2920` — sets the env var to **lowercase** `"true"`/`"false"` via `patch.dict`, pinning the case-insensitive-lowercase contract for `read_individual_model_forecasts`.
- `apps/postprocessing_forecasts/tests/test_workflow_integration.py:105` and `test_monthly_workflow_integration.py:309` — set it to **capitalized** `"True"`/`"False"` in their fixture environments.

Verified 2026-09-10: the existing test suite is **not** internally consistent on casing either —
the pipeline tests and the two workflow-integration tests all assume/pin `True`/`False`, while
`test_data_reader.py` pins lowercase `true`/`false`. Whichever D1/D2 combination is chosen, these
five files are the concrete check for "did this silently change behavior a test already depends
on" — update them deliberately as part of the fix, rather than adding new tests alongside
contradictory old ones that happen to still pass.

## Reachability — cannot be confirmed from this repository

Real deployment env files live outside this repository. `.gitignore:185` is a single **literal
path**, `apps/config/.env_develop_kghm` — not a wildcard or pattern. `git check-ignore` confirms it
does not match the tjhm or uzhm equivalents (`apps/config/.env_develop_tjhm`,
`apps/config/.env_develop_uzhm`); those files are simply not tracked in this repository at all
(never committed), for the same reason real per-org env files generally aren't — not because a
`.gitignore` rule excludes them. Either way, none of the real per-org files are in the repo to
inspect, and this issue cannot state which live deployments are affected or infer how other org
env files are managed from this one literal exclusion. An operator must inspect each live
`.env_develop_<org>` file's literal value (`True` vs `true`) against the gates above. Do not treat
any specific deployment as confirmed broken — or confirmed fine — from this issue alone.

**uzhm correction (narrowed 2026-09-10):** an earlier draft of this issue said "do not start with
uzhm" and, separately, claimed the flag's value is "moot" there — both need correcting.
`apps/run_locally.sh:215` (not `:225` — PR #507 shifted it) lists `machine_learning` in
`UZHM_SKIP_MODULES`, so **`run_locally.sh`'s own local-dev orchestration** skips ML for uzhm by
design and the flag is moot *for that script specifically*. But the canonical production tasks —
`RunPentadalWorkflow`, `RunDecadalWorkflow`, `PostProcessingForecasts`, `PostProcessingMaintenance`
(the ones `bin/run_*.sh` invoke) — have **no organization check anywhere in their `requires()`
bodies**; confirmed by reading `pipeline_docker.py:802-825`, `:1464-1502`, `:1526-1570`,
`:1856-1875` — the only `ORGANIZATION ==` branches in the file are inside the notification-message
builder (`:1388-1410`), which only affects email text, not scheduling. They depend on
`RUN_ML_MODELS` alone. So an unexpected `True` in a uzhm production env **could** schedule ML
through Luigi regardless of `run_locally.sh`. The corrected advice: still check kghm/tjhm first
(they are the deployments intended to run ML), but **do not skip uzhm** — verify its live env
holds `false`/absent as intended, rather than assuming `run_locally.sh`'s skip list makes the
production flag irrelevant there.

**Soften the kghm/tjhm claim:** the repository cannot establish any live deployment's current
literal value or whether ML is actually running there today. kghm and tjhm are **ML-intended**
deployments — they are the ones documentation and code assume produce ML forecasts — not
deployments this issue can claim are currently succeeding. Treat "kghm and tjhm produce ML
forecasts today" as the deployments' *intent*, to be confirmed by inspection, not as a fact this
issue asserts.

**Precondition before choosing D1–D5: a live-env inventory.** Before the owner picks among the
options below, record — per deployment (kghm, tjhm, uzhm, and any other live org) — the exact
literal value of `ieasyhydroforecast_run_ML_models` in its live `.env_develop_<org>` (or "absent"),
with the observation date. This is a manual inspection step (the files are outside this repo, see
above) but it is what turns the migration matrix below from hypothetical into an actual rollout
plan, and what makes "is ML actually running there" answerable instead of assumed.

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

## Decisions for the owner (not resolved here)

The original framing offered one binary — (a) normalise every consumer to case-insensitive and
keep the documented lowercase schema, or (b) standardise docs/templates on `True`/`False` — as if
it settled everything. It settles only D1 below, and only for the *repository's* docs, not for any
deployed environment (Critical 1). The real decisions are separable:

### D1 — Canonical serialized spelling
Which literal does the repository's documentation standardize on: `true`/`false` or
`True`/`False`? Affects `doc/configuration.md`, `apps/pipeline/README`, `apps/config/.env_develop`,
and the "Authoritative" docs listed above. On its own this changes nothing at runtime — see D2 and
Critical 1.

### D2 — Accepted token set per consumer
Independent of D1: what literals does every consumer accept, and is it the same set everywhere?
Observed today, three different sets:
- exact-match only, one literal (`pipeline_docker.py`'s four gates and two cleanup guards,
  `vizualization.py:4411`, `setup_library.py`'s legacy readers)
- case-insensitive `true`/`false` only (`data_reader.py:2647`, `setup_docker.sh:110`)
- case-insensitive plus aliases `yes`/`1`/`t`/`y` (`config.py:202` only)
Recommend one set for every in-scope consumer, so no consumer is more (or less) permissive than
the rest.

**Aliases are their own decision, with live consequences.** Today only `config.py` accepts
`yes`/`1`/`t`/`y`. An operator (or template) using one of those sees `config.py` report
`display_ML_forecasts = True` while every scheduling and read consumer treats it as off — the same
shape of defect this issue is about, on a different token, already live in the code today.
Retaining aliases needs the same call-site tests as the primary literal; dropping them is a
behavior change for `config.py` specifically and needs a test proving it.

### D3 — Absent-variable policy
Three policies exist today, not two:
- **off** — `pipeline_docker.py:40` (`None`), `config.py:202` (default `'False'`),
  `data_reader.py:2647` (default `"false"`), `setup_docker.sh:89` (default `false`),
  `setup_library.py` (`is None` branch, explicit "assuming no ML forecasts" log)
- **on** — `vizualization.py:4410` (default `"True"`)
- **fail-fast** — `daily_ml_maintenance.sh:36-38` (missing variable → logged error, `exit 1`)

`doc/configuration.md:138` labels the variable **Required**, which makes fail-fast a legitimate
third candidate, not just today's outlier. Pick one; state the migration impact for deployments
that currently rely on absence meaning "off" (or "on", for whoever uses the reload button today).

### D4 — Invalid-value policy
An unrecognized token (typo, an out-of-set case, an alias if D2 rejects them, etc.) is today
silently "off" everywhere except `setup_library.py` (warns, still off) and
`daily_ml_maintenance.sh` (`exit 1`). Should every in-scope consumer fail loudly — raise, or log
at error/warning level in a way a monitored run would surface — matching the stricter existing
precedent, rather than silently defaulting to off?

### D5 — Should an explicitly-invoked `RunAllMLModels` respect the flag?
`RunAllMLModels` (`pipeline_docker.py:776-799`) currently ignores `RUN_ML_MODELS` unconditionally.
Should it consult the same predicate as the other four gates, so a manual/external Luigi
invocation can't run ML against an operator's explicit "off"? This requires an actual code change
(a new comparison, not just a reformatted literal), since today it performs no comparison at all.

### A third design: one validated contract with a migration window
Instead of picking a spelling and cutting over, define **one** validated boolean-parsing helper —
a single accepted-token set (per D2), a single absent policy (per D3), a single invalid-value
policy (per D4, a raise or equivalent loud failure) — used by every in-scope consumer, with an
explicit **compatibility/migration window**: both the old and new canonical spellings accepted for
one release, with the non-canonical one logging a deprecation warning rather than silently working
forever. This is what avoids the Critical 1 regression below without a hard, all-deployments-at-
once cutover; weigh it against a straight (a)/(b)-style cutover.

### Critical 1 — no option here is docs-only
Whichever combination of D1–D5 is picked, editing this repository's docs and templates does not
touch any deployed `.env_develop_<org>` file — those live outside the repo (see Reachability). Any
option that changes which literal a scheduling consumer accepts needs, as part of the *same*
rollout, not a follow-up:
- a mandatory per-deployment env-file migration (edit the literal in each live
  `.env_develop_<org>`)
- a preflight check that fails the deploy/restart if the file still carries a value the new
  contract doesn't recognize
- a defined restart order, so `config.py`'s dashboard signal and `pipeline_docker.py`'s Luigi
  scheduling don't disagree mid-rollout
- a post-restart verification step confirming ML tasks were actually scheduled (e.g., checking the
  next run's Luigi task list or logs), not just that a container started

Without these, a deployment can end up "fixed in the repository, still broken in production"
indefinitely — worse than today, because it now looks fixed.

## Truth table

Replaces the prose predicates above with one table per value, over every in-scope consumer.
Assumes `RUN_CM_MODELS` is `"False"` where a predicate ORs the two flags together (isolates the
`RUN_ML_MODELS` column; the CM flag has its own sibling issue, see "Related"). `setup_library.py`
is included for reference only — it is out of scope (see below) and its column does not need to
change for this fix to be complete.

| Value | Luigi 4 ML gates (`:816`/`:1483`/`:1545`/`:1870`) | Luigi cleanup guards (`:1500`/`:1562`) | `RunAllMLModels` (`:776-799`) | `vizualization.py:4411` | `config.py:202` | `data_reader.py:2647` | `setup_docker.sh:110` | `daily_ml_maintenance.sh:41` | `setup_library.py` (legacy, out of scope) |
|---|---|---|---|---|---|---|---|---|---|
| `True` | ON | ON | **always ON (bug — flag ignored)** | ON | ON | ON | ON | passes (ON) | ON |
| `true` | **OFF** | **OFF** | always ON | **OFF** | ON | ON | ON | passes (ON) | OFF (warns "invalid value") |
| `TRUE` | OFF | OFF | always ON | OFF | ON | ON | ON | fails, `exit 1` | OFF (warns) |
| `False` | OFF | OFF | always ON | OFF | OFF | OFF | OFF | fails, `exit 1` | OFF (matches literal, no warning) |
| `false` | OFF | OFF | always ON | OFF | OFF | OFF | OFF | fails, `exit 1` | OFF (warns) |
| `FALSE` | OFF | OFF | always ON | OFF | OFF | OFF | OFF | fails, `exit 1` | OFF (warns) |
| alias (`yes`/`1`/`t`/`y`) | OFF | OFF | always ON | OFF | **ON** | OFF | OFF | fails, `exit 1` | OFF (warns) |
| `""` (empty string) | OFF | OFF | always ON | OFF | OFF | OFF | OFF | treated as unset, `exit 1` | OFF (warns) |
| absent | OFF | OFF | always ON | **ON** | OFF | OFF | OFF | `exit 1` ("not set") | OFF ("not set" info log) |
| invalid (e.g. `on`) | OFF | OFF | always ON | OFF | OFF | OFF | OFF | fails, `exit 1` | OFF (warns) |

The two cells that most concretely show why this needs a decision, not a one-line patch: `true`
splits Luigi/`vizualization.py` (OFF) from `config.py`/`data_reader.py`/`setup_docker.sh` (ON), and
absent splits `vizualization.py` (ON) from every other consumer (OFF) while
`daily_ml_maintenance.sh` treats it as a hard error.

## Migration matrix

The single most decision-relevant addition to this issue: for a deployment whose live env
currently holds each literal, what Luigi does **today**, and what changes **after** each candidate
option, specifically whether ML silently starts or stops running at the next restart. "Luigi
schedules ML?" is the previously-silent headline question; "`config.py` reports configured?" is
the dashboard signal an operator would actually see.

| Live env literal | Today: Luigi schedules ML? | Today: `config.py` reports configured? | After (A) — normalize to case-insensitive, docs stay `true`/`false` | After (B) — normalize to strict `True`/`False`, docs/templates recapitalized | After (C) — one validated contract + migration window |
|---|---|---|---|---|---|
| `True` | ON | ON | ON — unchanged | ON — unchanged | ON — unchanged |
| `true` (matches current docs) | **OFF — the defect** | ON | **Flips to ON at next restart**: new ML containers, new compute, forecasts start appearing where none did before. Confirm this is intended before deploying, using the live-env inventory above. | Stays **OFF** until the deployment's env file is migrated to `True`. If `config.py` is also tightened to exact-match (as consistency requires), the dashboard's currently-ON "configured" signal **flips to OFF too** — a new, user-visible regression layered on the pre-existing Luigi bug, unless the env-file migration (Critical 1) lands in the same change. | Same OFF-until-migrated outcome as (B), but the compatibility window keeps `config.py` (and everything else) accepting `true` with a deprecation warning during the window, avoiding the (B) regression above. |
| alias (`yes`/`1`/`t`/`y`) | OFF everywhere except `config.py` (ON) | ON | Depends on the alias decision (D2): if aliases are dropped, resolves to OFF everywhere including `config.py` — a `config.py` behavior change; if kept, resolves to ON everywhere. Either way, `config.py`'s behavior changes from today. | OFF everywhere — a strict two-token contract has no room for aliases, so `config.py`'s current alias acceptance must be removed. | Same as (B) unless the owner explicitly keeps aliases in the declared token set. |
| absent | OFF in Luigi/`config.py`/`data_reader.py`/`setup_docker.sh`; **ON** in `vizualization.py`; `exit 1` in `daily_ml_maintenance.sh` | OFF | Collapses to whichever single absent-policy (D3) is chosen. If "off" is chosen, `vizualization.py`'s manual reload button silently **stops** launching ML containers where it used to — flag this to whoever relies on that button before shipping. | Same collapse, same `vizualization.py` regression risk if "off" is chosen. | Same collapse; `doc/configuration.md:138`'s "Required" label is a point in favor of fail-fast (D3) over silent off. |

## In scope (must land together for the acceptance criteria to hold)

- `pipeline_docker.py`'s four ML-scheduling gates (`:816`, `:1483`, `:1545`, `:1870`)
- `pipeline_docker.py`'s two cleanup guards (`:1500`, `:1562`) — necessarily affected because they
  read the same `RUN_ML_MODELS` variable (see the "Correction" note above); test them explicitly,
  don't assume the ML-gate tests cover them.
- `RunAllMLModels` (`:776-799`) — per D5; if the owner decides it should respect the flag, this is
  a real code change (today it performs no comparison at all), not a reformatting.
- `vizualization.py:4410-4411`
- `config.py:202`
- `data_reader.py:2647`
- `setup_docker.sh:89-110`
- `daily_ml_maintenance.sh:36-41` (the absence check at `:36`, not only the value check at `:41`,
  given D3)
- The `RunAllMLModels` line in the completion-notification message (`pipeline_docker.py:1397`) —
  not a fix target itself, but whatever is decided for D5 must not make the notification's claim
  *less* accurate than it is today.

## Out of scope

- Implementing any fix — explicitly deferred per the 2026-09-09 owner decision above.
- `ieasyhydroforecast_run_CM_models`, which has the same case-sensitive-at-`pipeline_docker.py`
  shape (`:827`, `:1491`, `:1553`) and is already tracked as newly-reachable drift under
  INFRA-048's note and related to INFRA-038 — not duplicated here; may be fixed together or
  separately, owner's call.
- `apps/iEasyHydroForecast/setup_library.py`'s legacy `read_observed_and_modelled_data_pentade`/
  `_decade` functions — explicitly exempted from "every consumer" in the acceptance criteria
  below, so the exemption is stated rather than silently implied. Legacy, superseded by
  `data_reader.py` per its own docstring, no active production caller found (still called by two
  test files and a deprecated module, see the correction above). Included in the truth table for
  reference only, not as a fix target.
- Whether to fix the `RunAllMLModels` notification-message inaccuracy
  (`pipeline_docker.py:1393-1400`/`:1415`) itself, beyond the "no less accurate than today"
  constraint above — a separate, still-pending owner decision; not resolved here.

## Acceptance criteria

- [ ] Every consumer listed under "In scope" above matches the truth table for every row, with
      `setup_library.py`'s legacy readers explicitly exempted per "Out of scope" (not silently
      excluded).
- [ ] The truth table above is the starting point for the fix; if the chosen design produces a
      different result for any cell (e.g. a dropped alias, a changed absent policy), the table is
      updated to match and committed alongside the code change.
- [ ] A value that is neither an accepted "true" token nor an accepted "false" token is reported
      loudly per D4 — raised, or logged at error/warning level in a way a monitored run would
      surface — for every in-scope consumer, not silently treated as "off".
- [ ] `doc/configuration.md`, `apps/pipeline/README`, `apps/config/.env_develop`, and the
      "Authoritative" docs listed in the inventory above all state the same literal, per D1.
- [ ] Tests pin the predicate **at each in-scope call site** — e.g. a test that calls
      `pipeline_docker.PostProcessingForecasts.requires()` (or the equivalent for each other
      in-scope task/script) with each truth-table value and asserts the resulting dependency list
      or script behavior — not only a test of a shared helper function in isolation, if the fix
      consolidates the comparison logic into one. Cover `True`/`true`/`TRUE`/`False`/`false`/
      `FALSE`, any retained aliases, empty string, and absent, for every in-scope consumer.
- [ ] If `RunAllMLModels` is changed to respect the flag (D5), a test exercises that change
      directly — no existing test does, since it currently performs no comparison.
- [ ] The five existing test files listed under "Existing test surface" are updated to match the
      chosen contract, not left contradicting it.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero unexpected
      skips.

## Rollback

If the chosen option turns ML on somewhere it should not (an env file migrated to the wrong
literal, or a case-insensitive normalization making a `true` that was previously silently inert
start scheduling real containers):

1. Set the deployment's `.env_develop_<org>` value to whatever the **new** contract's chosen "off"
   token is (per D1/D2) — don't rely on the old lowercase/uppercase split as an off-switch once
   consumers are normalized to treat both the same way.
2. Restart the Luigi pipeline containers so `pipeline_docker.py` re-reads
   `ieasyhydroforecast_run_ML_models` — it is a module-level read at `:40`, evaluated once at
   import time, not per task.
3. Verify via the post-restart check from Critical 1: confirm no `RunMLModel`/`MLMaintenance`
   tasks appear in the next run's Luigi log, rather than trusting `config.py`'s dashboard signal
   alone — that signal and Luigi's actual scheduling are exactly the two things known to disagree.
4. If ML containers already started and are consuming compute or producing unwanted forecasts,
   stop them via the normal Docker/Luigi task-kill path — an operational response, not a code
   rollback.

---

## Related

| ID | Relation |
|---|---|
| INFRA-038 | Same defect class (`connect_to_iEH`/`ssh_to_iEH` parsed four incompatible ways) in a different pair of variables |
| INFRA-048 | Notes the sibling variable `ieasyhydroforecast_run_CM_models` has the same case-sensitive-at-`pipeline_docker.py:827` shape, newly reachable via PREPG-023's case-insensitive gateway gate |
| PP-051 / ML-021 / PREPG-026 | Same family: a component reports or implies success/configuration while having silently done nothing — includes the `RunAllMLModels` completion-notification inaccuracy noted above, whose own fix is still a pending, undecided item |
