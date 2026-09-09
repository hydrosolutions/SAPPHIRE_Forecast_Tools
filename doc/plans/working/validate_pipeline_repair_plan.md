# Plan — minimal fail-closed repair of `validate_pipeline.py`

**Date**: 2026-08-16 (rev 3) · **reconciled 2026-09-09 (rev 4)** — issue ids corrected, two M2 steps
superseded, two added, and one open question re-premised; see § 0.
**Target**: `apps/validate_pipeline/validate_pipeline.py`, `apps/iEasyHydroForecast` packaging
**Scope**: **deliberately minimal.** Kill the three false-greens this review actually hit.
Nothing else.
**Closes**: **INFRA-020**, fully. *(Rev 4 first wrote "fully, once M2.4's open question is
settled", which contradicts M2.4 itself — that step says M2 **may ship under option (ii) without
waiting for a decision**. M2.4 governs how much the check may *claim*, not whether INFRA-020
closes.)*
**Closes in part**:
- **INFRA-025** — M1 hardens *this validator* against the package shadow, but does not repair the
  editable-install exposure the issue also requires (a working `iEasyHydroForecast` import from an
  unrelated cwd).
- **INFRA-026** — M3 kills the null-value false-green, but leaves `check_snow_operational_values`
  untouched, excludes snow from the freshness verdict (M3.4) and defers per-station coverage (§ 5).

**Explicitly does NOT close**: INFRA-021, INFRA-022, INFRA-028 (see § 5)

*(Rev 4: rev 3 claimed unqualified closure of three issues — two of which it does not touch at all,
and a third only in part — which sat oddly beside its own "deliberately minimal" scope. The
qualified claims above are what the milestones actually deliver.)*

---

## 0. Rev 4 reconciliation — read this before implementing

This plan was written 2026-08-16 and the issue drafts it references have moved underneath it since.
Its *shape* and its § 2 governing constraint are untouched and remain correct.

### (a) The issue ids were stale — and two of them now name **shipped, unrelated** work

They were allocated before a renumbering. **Anyone implementing from rev 3 would have claimed
closure of an issue that is already merged and has nothing to do with this validator.**

| rev 3 said | Actually is | What that id is on trunk today |
|---|---|---|
| **INFRA-023** | **INFRA-025** | `iEasyHydroForecast` package shadowing — validation dies at import. **Open (Draft).** Today's INFRA-023 is the `monthly_norms` cron-mapping issue: **Complete, shipped in PR #494** |
| **INFRA-024** | **INFRA-026** | Tier-1 PASS + "all datasets fresh" when values are absent. **Open (Draft).** Today's INFRA-024 is failure-cause attribution — `run_locally.sh` normalising a module's exit code to 1 — **Draft, and not in scope here** |

*Verified 2026-09-09 against `doc/plans/module_issues.md`: INFRA-025 and INFRA-026 are both Draft
and both describe what this plan fixes; INFRA-023 is Complete and INFRA-024 is an unrelated open
draft. PR #494 confirmed merged with `gh pr view`.*

This is the same id drift that had to be corrected inside the issue drafts. It lived here too, and
that pass did not reach it.

### (b) Two M2 steps are superseded, two are added, one is re-premised

See M2 for the detail. In short: do **not** retag the six period-forecast checks (M2.2); the
mode-provenance caveat is an **open question on a corrected premise**, no longer a required step
(M2.4); `machine_learning` must come out of `FORECAST_DAY_MODULES` (**M2.5**, new); and the ML checks
must be gated on deployment-level ML enablement (**M2.6**, new) or M2.5 makes things worse.

### (c) The cluster's reach was mis-stated

`validate_pipeline` has **no production invoker**. It runs only from `apps/run_locally.sh`
(`run_api_validation` at `:1471`, invoking the validator at `:1479`; `run_module_validation` at
`:1504`, invoking it at `:1530`) and from `apps/run_validation.sh`, which describes itself as the
pre-commit / pre-merge workflow (`:7-8`) and drives `run_locally.sh` (`:187`, `:213`). Luigi never
calls it, and the pipeline image cannot — it copies only `apps/iEasyHydroForecast` and `apps/pipeline`
(`apps/pipeline/Dockerfile:20`, `:23-24`). The full sweep and its evidence table are in **INFRA-031**.

**This does not change what this plan should do** — a review gate that cannot fail is still
worthless — but it does mean *"expect some currently-green runs to turn red"* costs reviewer time,
not operator trust. The operational gap is **INFRA-031**, and it is a separate, unbuilt thing.

**Priorities are deliberately left as the tracker has them.** Rev 4 states the reach; it does not
reprice INFRA-020/021/022/028.

### (d) Division of labour

The issue drafts state the *problem* and the constraints; this plan states the *fix*. Where they
disagree on implementation detail, this plan wins after rev 4 — **except where a draft cites a
locked test, which always wins.**

---

**History**: rev 1 proposed a separate `verify_pipeline_state.py` — rejected by out-of-loop
`codex exec` as a second validator that could go false-green for new reasons. rev 2 proposed a
target-aware, input-conditioned redesign (Rules 1 / 1b / 2) — also rejected, because its
foundations do not exist in the code (see § 5). This rev keeps only what is implementable
today and verifiable against ground truth we already hold.

---

## 1. The three false-greens, and the smallest fix for each

| # | Observed 2026-08-14/16 | Fix |
|---|---|---|
| **INFRA-025** | Validation died at import; module target still exited 0 | M1 — make the tool run |
| **INFRA-020** | ML: `0 passed, 0 failed, 0 skipped` → **PASS**, over 100% NULL forecasts | M2 — a check set that matches nothing is a failure |
| **INFRA-026** | Snow: 6/6 provider tasks errored, `80 records` OK + `all datasets fresh` OK | M3 — count values, not rows |

---

## 2. Governing constraint — **do not add statuses, do not touch the exit contract**

Out-of-loop review established what rev 2 got wrong here, and it decides this plan's shape:

*(Citations in this section re-derived 2026-09-09; every one of rev 3's had drifted.)*

- There is **no `--strict`**. `print_summary` counts only the four statuses (`validate_pipeline.py:1315`)
  and returns non-zero **only** for `FAIL` — `return 1 if counts["FAIL"] > 0 else 0`
  (`:1327`; the function definition is `:1313`).
- `run_locally.sh` consumes only the process exit code. `run_api_validation` captures it at `:1481`
  and turns it into a PASS/FAIL row (`:1486`/`:1489`); `run_module_validation` does the same at
  `:1531`/`:1536`/`:1539`; `print_summary` converts a recorded FAIL into overall exit 1 at `:2725`.
  So **a new `ERROR` status would render green**, and `run_locally.sh` could not display it anyway.
- JSON is keyed by **check name** (`results_to_json`, `:199`; `payload[r.name] = {...}` at `:218`),
  and baseline deltas compare only `record_count` (`:305`, `:309`).
- Tests recognise exactly the four statuses (`apps/validate_pipeline/test/test_validate_pipeline.py:1083`
  — note the directory is `test/`, not `tests/`).

**Therefore: every new failure condition in this plan is reported as `FAIL`.** No `GAP`, no
`UPSTREAM`, no `ERROR`. That sidesteps the entire status/exit/consumer-contract problem, which
is real but is not what is blocking us today.

> **Expect this to turn some currently-green runs red.** That is the point — they are green
> today because nothing was checked. Land M1 first and read one full run before M2/M3, so the
> new reds are understood rather than merely absorbed.

---

## 3. Milestones

### M1 — Make the tool run at all (INFRA-025)

**Files**: `apps/iEasyHydroForecast/pyproject.toml`, `apps/validate_pipeline/validate_pipeline.py`

1. Fix the import fallback so it can recover from the failure it exists to catch: put `apps/`
   on `sys.path` **before** the first import, or purge `sys.modules['iEasyHydroForecast']` and
   `importlib.invalidate_caches()` inside the `except`. As written, the parent package is
   already cached with the wrong `__path__`, so the retry cannot succeed.
2. Fix the documented pytest invocation in `pyproject.toml`. `--directory iEasyHydroForecast
   pytest iEasyHydroForecast/tests/` resolves to `apps/iEasyHydroForecast/iEasyHydroForecast/tests/`
   — the shadowing path itself — so following the documented command **recreates the bug**.
   Use `pytest tests/`, matching the existing `testpaths = ["tests"]`.
3. **Do NOT `.gitignore` the nested path.** rev 2 said to ignore it "so a recreated shadow is
   visible" — that is backwards; ignoring makes it *less* visible. Empty dirs are already
   untracked and unreported. If a tripwire is wanted, add an explicit check, not an ignore rule.

**Acceptance**: with the stray directory recreated, `--module <any>` still runs its checks.

**Note**: the stray directory currently sits in the session scratchpad, moved aside during the
review. M1 must hold with it restored.

### M2 — A check set that matches nothing is a failure (INFRA-020)

**Files**: `validate_pipeline.py`
**Depends on**: M1

*(All citations in M2 re-derived 2026-09-09.)*

1. **A module with no *registered* checks ⇒ `FAIL`**, never PASS — but scope it to *"no checks are
   **registered** for this module"*, **not** *"the results list is empty"*. **Refined rev 4.**
   API-absent and `SAPPHIRE_API_ENABLED=false` deliberately execute zero checks and return 0
   (`validate_pipeline.py:1739-1746`), locked by `test_api_unavailable_exits_zero` (`:131`) and
   `test_api_disabled_exits_zero` (`:137`) in
   `apps/validate_pipeline/test/test_validate_pipeline.py`. An unready postprocessing API and
   incompatible combinations such as `--module long_term_forecasting --target short-term` also
   legitimately match nothing. **A blanket empty-results rule flips two passing tests and conflates
   configuration defects with intentional skips.**

2. **Add** ML-tagged checks. ~~Retag the TFT / TiDE / TSMixer presence checks~~ — **superseded
   rev 4.** Those six period-forecast checks are *deliberately* tagged `postprocessing_forecasts`
   (the `model_modules` map is `validate_pipeline.py:469-476`, applied at `:483`), and that tagging
   is locked by `test_tier1_short_term_module_mapping` (`test_validate_pipeline.py:378-418`,
   assertions `:409-414`). Retagging would strip processed-output coverage from postprocessing
   validation in order to fix ML. **Add** new day-horizon checks tagged `machine_learning` alongside
   them, and update the exact Tier-1 count assertion deliberately
   (`test_validate_pipeline.py:333`, currently `assert len(results) == 13`).

3. Query the rows ML actually writes: `horizon_type="day"` regardless of the triggering mode. The
   contract is documented at `machine_learning/scr/utils_ml_forecast.py:717-718` ("All daily
   forecasts are stored with horizon_type='day' regardless of the caller's horizon_type") and at
   `:739` ("Informational only"); the hard-coded field assignment is `:818`. Do **not** query the
   requested pentad/decade horizon.

4. **OPEN QUESTION (owner) — is a mode-provenance caveat worth printing?**
   *Rev 4 first struck this step outright; a cross-check showed that went too far. It is recorded as
   an open question rather than decided either way — and it does **not** block M2.*

   > **Premise corrected 2026-09-09.** An earlier rev-4 draft of this step rested on *"ML API write
   > failures are swallowed"*. **That is no longer true. ML-021 shipped 2026-09-09 (PR #503,
   > merged):** `_write_ml_forecast_to_api` now raises for a genuine delivery failure (readiness
   > false, or zero rows stored) and returns `False` only for benign no-ops;
   > `write_pentad_forecast` / `write_decad_forecast` capture that outcome and return it
   > (`make_forecast.py:151`, `:234`), and `make_ml_forecast` exits **5** (`:972`). A failed write is
   > no longer silent at the module boundary, so **this step is no longer a fix for a live silent
   > failure — it is optional hardening of what the check may claim.**

   **What is settled**: day rows are not mode-specific evidence in general, and ML runs **daily** in
   production. So the original caveat's framing — *"this check cannot verify the mode"* — is too
   pessimistic for the normal case.

   **What survives, stated narrowly**: PENTAD and DECAD write overlapping day spans from the same
   issue date — PENTAD `forecast_horizon = 6`, DECAD `= 11` (`make_forecast.py:611-614`) — and both
   are stored as `horizon_type="day"` (`utils_ml_forecast.py:818`) under a unique key of
   `(horizon_type, code, model_type, date, target)` (`:790-791`) that carries **no source-mode
   component in practice**, since `horizon_type` is always `"day"`. **A mode-agnostic day check
   therefore cannot prove which mode produced overlapping rows.** That is a limit on what the check
   may *claim*, not a silent failure it lets through.

   **The overlap is asymmetric, which is what makes option (iii) possible at all.** PENTAD's 6-day
   span is a *subset* of DECAD's 11-day span:

   | Situation | Distinguishable by a bare day check? | Detectable how |
   |---|---|---|
   | DECAD rows absent, PENTAD rows present | no | **Yes** — assert rows at target dates in the **7-11 day** window, which only a DECAD run produces |
   | PENTAD rows absent, DECAD rows present | no | **No** — DECAD's span covers PENTAD's dates entirely; span cannot distinguish them |

   **Options** — now *optional hardening*, not a required correctness fix:
   - **(i) Print a caveat**, scoped to cross-mode ambiguity on the current run (not to provenance in
     general). Honest, cheap, changes no logic.
   - **(ii) No caveat** — accept that the check verifies "ML wrote day rows today", not "the
     requested mode wrote day rows today". Simplest, and defensible now that ML-021 makes a genuine
     delivery failure fail loudly on its own.
   - **(iii) Check the target-date span**, catching the DECAD case outright and leaving only the
     PENTAD case ambiguous. Strongest; needs a target-date range assertion rather than bare presence.

   **M2 may ship under (ii) without waiting for a decision.** (i) and (iii) are compatible with each
   other and can land later. What must **not** happen is a PASS whose printed wording claims
   mode-level verification it did not perform.

5. **Remove `machine_learning` from `FORECAST_DAY_MODULES`** (`validate_pipeline.py:116-120`), or
   make that gate horizon-aware. **Added rev 4.** It sits there under the comment *"Modules that
   only produce data on forecast days (not daily)"* (`:115`), which is false for ML. Left as-is, the
   new daily checks are downgraded FAIL → SKIP by `_apply_non_forecast_day_skip` (`:1352`; the
   downgrade loop is `:1380-1389`, testing `r.module in FORECAST_DAY_MODULES` at `:1385`) on every
   non-boundary day — reinstating the false-green this milestone exists to kill, on ~24 days a month.

   **This is a contract change, not a cleanup**: `test_all_forecast_modules_affected`
   (`test_validate_pipeline.py:937-963`, class `TestNonForecastDaySkip` at `:849`) explicitly
   requires `machine_learning` to become SKIP. Replace it deliberately, with a comment naming this
   milestone — **do not let it fail and then "fix" it.**

   > **Coordinate with INFRA-045 decision D1.** D1 makes `--target daily` derive
   > `["pentad", "decade"]`, and its stated operator consequence depends on
   > `_apply_non_forecast_day_skip()` downgrading absent decade data to SKIP away from decade
   > forecast days. M2.5 changes the module set that same function consults. They edit adjacent
   > halves of one guard and both run through the locked test above. **Whoever lands second must
   > re-derive the line numbers and re-check the other's stated operator consequence**, correcting
   > the other issue's file if it no longer holds.

6. **Gate the ML checks on deployment-level ML enablement.** **Added rev 4 — without it, M2.5 makes
   things worse.** Removing the calendar gate in (5) without adding this one leaves unconditional ML
   checks running on deployments that do not run ML at all. `machine_learning` is in both
   `DEMO_SKIP_MODULES` and `UZHM_SKIP_MODULES` (`run_locally.sh:224-225`), and the short-term
   pipeline skips the module at `:1580` but then calls `run_api_validation "short-term"` at `:1612`
   with **no module filter** — so ML-tagged checks would run anyway. `run_all` does the same at
   `:1697`, and `run_daily_pipeline` at `:1872`.

   > **Half of this step's motivating example is stale — corrected 2026-09-09.** The **bare
   > `machine_learning` target** is already guarded: `run_module_validation "machine_learning"`
   > (`:2693`) sits inside the `else` of `if should_skip_module machine_learning` (`:2664`), so on
   > demo/uzhm the module is recorded as a skip and validation never runs. *(`git log -L` attributes
   > that guard to the original org-aware filtering commit `d81adb68`, with its `record_skip` line
   > added by INFRA-030 (`bf311583`). The 2026-09-09 pass **could not confirm** the attribution to
   > INFRA-039 that an earlier draft asserted — the guard's existence is verified; which issue closed
   > it is not.)* **The pipeline-level paths above are what keeps this step necessary.**

   Read enablement from deployment config / `ORG` — a static per-deployment fact, so this step needs
   **no** run manifest and **no** INFRA-028 (or INFRA-052) dependency.

**Acceptance**:
- `--module machine_learning` on an ML-enabled deployment runs real day-horizon checks, **emitted
  once per run**. Note `run_tier1_short_term` is called **once per horizon**
  (`validate_pipeline.py:1449-1465`) and `results_to_json` keys on check name (`:199`;
  `payload[r.name]` at `:218`), so under `SAPPHIRE_PREDICTION_MODE=BOTH`, placing the new checks
  inside the horizon loop duplicates them and silently overwrites one copy. Either emit them outside
  the loop or de-duplicate explicitly — **and say which in the implementation**, because updating
  `run_tier1_short_term`'s exact count assertion (`test_validate_pipeline.py:333`) and emitting once
  per run pull in opposite directions.
- On a deployment that does not run ML, the ML checks do not run and do not fail.
- A module with **no registered checks** fails; API-absent / API-disabled paths keep their exit-0
  behaviour and their tests pass unchanged.
- No printed output claims mode-level verification the check did not perform (M2.4).

### M3 — Count values, not rows (INFRA-026)

**Files**: `validate_pipeline.py`
**Depends on**: M1, and lands **together with M2** — M3 alone cannot catch the ML case, because
the ML filter still matches nothing until M2.

1. Add a **per-dataset operational value field** map. This is not one field:

   | Dataset | Operational value field |
   |---|---|
   | runoff | `discharge` |
   | meteo, snow | `value` |
   | short-term forecast, LR forecast | `forecasted_discharge` |
   | long forecast | `q` (`q50` is separate, not a substitute) |
   | skill metric | the metric fields **and** `n_pairs` |

   Counting "any non-null field" would let `norm`, metadata or a single quantile mask a missing
   operational result — which is the exact shape of the snow false-green.
2. Presence checks report **rows AND non-null values** (`80 rows / 7 with values`), and FAIL
   when rows exist but no values do.
3. `check_data_freshness` derives `max_date` from **rows with a non-null value**. Today it uses the
   raw row date: `check_presence` sets `max_date` from `df["date"].max()` with no regard for whether
   the value column is null (`:375-377`), and `check_data_freshness` (`:1061`) consumes exactly that
   field (`:1085-1094`). That is why a 14-day-stale snow series reported "fresh".
   *(Citations re-derived 2026-09-09.)*
4. **Snow is out of scope for this rev (owner, 2026-08-16).** Leave
   `check_snow_operational_values` (`:958`, running to the next definition `check_em_ne_parity` at
   `:1011`) untouched, and do not give snow a staleness verdict.

   The distinction that keeps INFRA-026 closable without it:

   - **"rows exist, all values NULL" ⇒ FAIL — applies to every dataset, snow included.** This
     needs no cadence decision; a dataset with no operational values at all is broken whatever
     its refresh schedule.
   - **"values exist but are old" ⇒ needs the deferred snow/meteo cadence decision**, because
     snow legitimately lags meteo. Until that is settled, snow is **excluded from the
     value-based freshness verdict** and reported informationally only.

   Without this carve-out, deriving freshness from non-null values would start WARNing on snow
   every day on machines that are behaving correctly — reintroducing exactly the alarm-fatigue
   failure mode this plan exists to avoid.
5. **Skill tombstones** (`n_pairs = 0`, null metrics) are legitimate and must not be failed as
   "no values".

**Acceptance**: rows-present/values-absent FAILs; a genuinely populated dataset still passes;
tombstones do not fail.

> rev 2 claimed populated datasets would pass "byte-identically". That was self-contradictory —
> the detail string changes from `N records` to `N rows / M with values` by design. The correct
> criterion is **same status**, not same text; update the tests that assert on the old string.

---

## 4. Verification

- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh validate_pipeline` green, with new fixtures
  for: all-NULL values, norm-only rows, **a module with no *registered* checks** (rev 4 — *not*
  "zero matched checks"; M2.1 distinguishes them deliberately, and an empty-results fixture would
  drive the implementation back toward the guard that breaks the API-absent/disabled tests), and
  tombstones.
- **A `SAPPHIRE_PREDICTION_MODE=BOTH` regression** (rev 4), asserting the new ML day checks appear
  **once**, not once per horizon, and that neither copy is lost to the name-keyed JSON.
- **A deployment-with-ML-disabled case** (rev 4), asserting no ML checks run and none fail.
- **The two locked tests M2 changes are replaced, not deleted** (rev 4):
  `test_tier1_short_term_returns_expected_check_count` (`test_validate_pipeline.py:304-334`) and
  `test_all_forecast_modules_affected` (`:937-963`), each with a comment naming this plan.
- **Regression against recorded reality** — replay the 2026-08-14/16 kghm state and confirm the
  tool now reports what the manual review found:
  - snow **reported as `N rows / 7 with values`** — the row/value split is now visible; no
    staleness verdict is asserted (deferred, § 5)
  - ML **all-NULL FAIL** (was: `0 passed, 0 failed, 0 skipped` → PASS)
  - meteo still **green** (the control — it was genuinely populated through 2026-08-28)
- Run once with the stray shadow directory restored (M1 regression).
- Out-of-loop adversarial review of the diff before PR.

---

## 5. Explicitly deferred — and why

Not "forgotten". Most are blocked on something that does not exist yet — **though rev 4 changes that
for Rule 1: it now has a chosen mechanism under discussion and is deferred for scope and atomic
landing, not for want of a design.** Full requirements are in the out-of-loop review
(`codex_repair_plan_review.md`).

| Deferred | Blocked on |
|---|---|
| **Rule 1** (expectation from `--target`) | `--target` is optional, defaults `short-term`, and accepts only 4 values (`validate_pipeline.py:1668-1673`, `choices=` at `:1670`); it cannot express `maintenance`, `initialize`, or `long-term-operational`. **Amended rev 4:** the rev-3 conclusion *"needs a run manifest, not another CLI string"* was **half right**. What it gets right is that **`--target` cannot carry the expectation** — a *separate* argument can. Whether the expectation should travel as a persisted manifest (**INFRA-028**, the standing owner decision of 2026-08-18) or as an in-memory `--active-modes` CLI argument (**INFRA-052**, filed as an alternative) is an open owner choice; **this plan does not decide it**, and Rule 1 stays deferred either way. What changes is that it is no longer blocked on *building* a manifest as the only conceivable mechanism. *(Citations re-derived 2026-09-09.)* |
| **Rule 1b** (per-dataset completeness by maintenance cadence) | **Maintenance targets never invoke the validator** — `run_maintenance_pipeline` (`run_locally.sh:1705-1767`) contains no `run_api_validation` call, and none of the `run_module_validation` call sites (`:2648`, `:2656`, `:2661`, `:2693`, `:2700`, `:2708`) is on a maintenance target. The production wrapper doesn't either (`bin/run_daily_maintenance.sh` submits to Luigi and never mentions `validate_pipeline`). No `data_through`, no run ledger. rev 2's `min(last maintenance run, window lookback)` also compared a timestamp with a duration — not a definition. *(Citations re-derived 2026-09-09.)* |
| **Rule 2** (input-conditioned checks, `UPSTREAM`) | Needs the status/exit contract above, and the snow case is **not inferable from database rows at all** — the provider evidence lived in module logs. Also the predicates were wrong: EM needs **≥2** qualifying models under `sdivsigma ≤ 0.6 / nse ≥ 0.8 / accuracy ≥ 0.8`; **NE has no skill gate**; monthly EM adds `min_pairs`; Skilled Mean uses a relaxed NSE-positive gate; quarter/season EM is a fixed `LR_Base + LR_SM` aggregate and is not skill-gated. LR tolerates ≤3-day interpolated gaps; ML tolerates configured gaps, interpolation and forward-fill. |
| **INFRA-021 / INFRA-022** (long-term tier) | **Amended rev 4.** ~~Needs env loading before config access; the long-term tier still crashes after this plan.~~ **That premise is gone: env loading shipped in PR #486 (merged).** `_load_deployment_env()` now exists (`validate_pipeline.py:1580`) and `main()` calls it before any config access, returning 1 if it fails (`:1724`); each long-term horizon resolution is additionally guarded — the `try`/`except` around the quarter resolution (`:555`) and around the seasonal one (`:594`) each append a `critical=True` FAIL row instead of raising. So the tier no longer dies on a missing `.env`. What remains deferred is **INFRA-022's gating**: it needs a record of *which modes the run resolved*, and it must not be built by re-deriving the schedule — a re-derivation cannot model the scheduler's tolerance window, per-model `forecast_months`, or manual Luigi overrides. **INFRA-028** (persisted manifest, the standing owner decision) and **INFRA-052** (in-memory `--active-modes`, filed as the alternative) are the two candidate transports; the choice is the owner's and this plan does not make it. Two further constraints for whoever picks this up: `test_long_term_never_skipped` (`apps/validate_pipeline/test/test_validate_pipeline.py:965-985`) deliberately locks today's no-skip behaviour and must be **replaced, not deleted**; and **LTF-007** means a mode the scheduler admits can still be refused by every model, which is a genuine execution failure, not a gated SKIP. *(Citations re-derived 2026-09-09; PR #486 confirmed merged with `gh pr view`.)* |
| Status vocabulary, JSON schema versioning, stable check IDs, baseline/delta on value counts | One coordinated change across `validate_pipeline.py`, `run_locally.sh`, JSON, baselines and tests. |
| Per-cell `(station, model, issue date, lead)` matrices | Needs the retrieval work; also collides with unstable offset pagination (postprocessing reads have no stable `ORDER BY`, `crud.py:66/161/250/354` — ML-007). |
| Snow-vs-meteo completeness semantics | Unresolved factual conflict: same provider, but local gateway maintenance runs **only** `extend_era5_reanalysis.py` and not snow (`run_locally.sh:1072`, inside `run_maintenance_preprocessing_gateway` at `:1065`; the *operational* gateway path at `:717` runs `Quantile_Mapping_OP.py` and `extend_era5_reanalysis.py`), and cutoffs differ (meteo 30-day, snow/ERA5 365-day). **The interim call is already made** — owner, 2026-08-16: skip snow for this rev (§ 7), so nothing here blocks implementation. What stays open is the longer-term completeness semantics, not this plan's scope. *(Citations re-derived 2026-09-09.)* |

---

## 6. Dependency graph

```json
{
  "phases": {
    "M1": { "depends_on": [], "parallel_agents": 1 },
    "M2": { "depends_on": ["M1"], "parallel_agents": 1 },
    "M3": { "depends_on": ["M1", "M2"], "parallel_agents": 1 }
  }
}
```

M2 before M3 is deliberate: M3's value-counting cannot reach the ML case until M2 fixes the
module mapping and queries `day` rows. rev 2's claim that M3 ("P3") was the right standalone
first step was wrong for that reason — **M2's fail-closed guard is the first thing that stops a
green run over an empty check set.**

---

## 7. Open question for the owner — **deferred, not blocking**

Snow/meteo completeness semantics: they share a provider (owner) but not a maintenance target
or a cutoff (code — local gateway maintenance runs only `extend_era5_reanalysis.py`, meteo uses
a 30-day cutoff, snow and ERA5 reanalysis 365-day).

**Owner decision 2026-08-16: skip snow for now.** M3 therefore ships the dataset-agnostic
value-counting fix and carves snow out of the freshness verdict only. Nothing else in this plan
waits on it, so implementation can start.
