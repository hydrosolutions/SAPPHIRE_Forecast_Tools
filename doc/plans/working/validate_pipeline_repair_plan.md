# Plan — minimal fail-closed repair of `validate_pipeline.py`

**Date**: 2026-08-16 (rev 3)
**Target**: `apps/validate_pipeline/validate_pipeline.py`, `apps/iEasyHydroForecast` packaging
**Scope**: **deliberately minimal.** Kill the three false-greens this review actually hit.
Nothing else.
**Closes**: INFRA-023, INFRA-020, INFRA-024 (snow carved out — see M3.4)
**Explicitly does NOT close**: INFRA-021, INFRA-022 (see § 5)

**History**: rev 1 proposed a separate `verify_pipeline_state.py` — rejected by out-of-loop
`codex exec` as a second validator that could go false-green for new reasons. rev 2 proposed a
target-aware, input-conditioned redesign (Rules 1 / 1b / 2) — also rejected, because its
foundations do not exist in the code (see § 5). This rev keeps only what is implementable
today and verifiable against ground truth we already hold.

---

## 1. The three false-greens, and the smallest fix for each

| # | Observed 2026-08-14/16 | Fix |
|---|---|---|
| **INFRA-023** | Validation died at import; module target still exited 0 | M1 — make the tool run |
| **INFRA-020** | ML: `0 passed, 0 failed, 0 skipped` → **PASS**, over 100% NULL forecasts | M2 — a check set that matches nothing is a failure |
| **INFRA-024** | Snow: 6/6 provider tasks errored, `80 records` OK + `all datasets fresh` OK | M3 — count values, not rows |

---

## 2. Governing constraint — **do not add statuses, do not touch the exit contract**

Out-of-loop review established what rev 2 got wrong here, and it decides this plan's shape:

- There is **no `--strict`**. `FAIL` already exits 1 unconditionally
  (`validate_pipeline.py:1270`), and `run_locally.sh` turns a recorded validation failure into
  overall exit 1 (`run_locally.sh:1086`, `:1585`).
- The summary prints only PASS / FAIL / WARN / SKIP and returns non-zero only for FAIL
  (`validate_pipeline.py:1272`). **A new `ERROR` status would render green.**
- `run_locally.sh` consumes only the process return code (`run_locally.sh:1071`), so it
  cannot display new statuses at all.
- JSON is keyed by check name, baselines record only date+target, and deltas compare only
  `record_count` (`:207`, `:241`, `:283`). Tests recognise exactly the four statuses
  (`test_validate_pipeline.py:1338`).

**Therefore: every new failure condition in this plan is reported as `FAIL`.** No `GAP`, no
`UPSTREAM`, no `ERROR`. That sidesteps the entire status/exit/consumer-contract problem, which
is real but is not what is blocking us today.

> **Expect this to turn some currently-green runs red.** That is the point — they are green
> today because nothing was checked. Land M1 first and read one full run before M2/M3, so the
> new reds are understood rather than merely absorbed.

---

## 3. Milestones

### M1 — Make the tool run at all (INFRA-023)

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

1. **Zero matched checks ⇒ `FAIL`**, never PASS. This single change converts the ML
   false-green, and it fails closed for any future filter that stops matching.
2. Fix the ML module mapping: TFT / TiDE / TSMixer presence checks are tagged
   `postprocessing_forecasts`, so `--module machine_learning` matches nothing (`:459`, `:1420`).
3. Query the rows ML actually writes: `horizon_type="day"` regardless of the triggering mode
   (`utils_ml_forecast.py:713`, `:776`), not the requested pentad/decade horizon.
4. **State the provenance limit in the output.** Day rows carry no source-mode marker, so a
   DECAD check can be satisfied by PENTAD leftovers. Print that caveat; do not silently imply
   mode-level verification.

**Acceptance**: `--module machine_learning` runs real checks against day rows and reports the
provenance caveat. Zero matched checks fails.

### M3 — Count values, not rows (INFRA-024)

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
3. `check_data_freshness` derives `max_date` from **rows with a non-null value**. Today it uses
   the raw row date (`:316`, `:349`, `:368` → `:1042`), which is exactly why a 14-day-stale snow
   series reported "fresh".
4. **Snow is out of scope for this rev (owner, 2026-08-16).** Leave
   `check_snow_operational_values` (`:915`–`:949`) untouched, and do not give snow a
   staleness verdict.

   The distinction that keeps INFRA-024 closable without it:

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

- `SAPPHIRE_TEST_ENV=True bash run_tests.sh validate_pipeline` green, with new fixtures for:
  all-NULL values, norm-only rows, zero matched checks, tombstones.
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

Not "forgotten"; each is blocked on something that does not exist yet. Full requirements are in
the out-of-loop review (`codex_repair_plan_review.md`).

| Deferred | Blocked on |
|---|---|
| **Rule 1** (expectation from `--target`) | `--target` is optional, defaults `short-term`, and accepts only 4 values (`:1522`, `:1578`); it cannot express `maintenance`, `initialize`, or `long-term-operational`. Needs a **run manifest**, not another CLI string. |
| **Rule 1b** (per-dataset completeness by maintenance cadence) | **Maintenance targets never invoke the validator** (`run_locally.sh:1854`–`:1929`); the production wrapper doesn't either (`run_daily_maintenance.sh:70`). No `data_through`, no run ledger. rev 2's `min(last maintenance run, window lookback)` also compared a timestamp with a duration — not a definition. |
| **Rule 2** (input-conditioned checks, `UPSTREAM`) | Needs the status/exit contract above, and the snow case is **not inferable from database rows at all** — the provider evidence lived in module logs. Also the predicates were wrong: EM needs **≥2** qualifying models under `sdivsigma ≤ 0.6 / nse ≥ 0.8 / accuracy ≥ 0.8`; **NE has no skill gate**; monthly EM adds `min_pairs`; Skilled Mean uses a relaxed NSE-positive gate; quarter/season EM is a fixed `LR_Base + LR_SM` aggregate and is not skill-gated. LR tolerates ≤3-day interpolated gaps; ML tolerates configured gaps, interpolation and forward-fill. |
| **INFRA-021 / INFRA-022** (long-term tier) | Needs env loading before config access **and** a per-check failure mode, which needs the status contract. The long-term tier still crashes after this plan — known, and it does not affect short-term validation. |
| Status vocabulary, JSON schema versioning, stable check IDs, baseline/delta on value counts | One coordinated change across `validate_pipeline.py`, `run_locally.sh`, JSON, baselines and tests. |
| Per-cell `(station, model, issue date, lead)` matrices | Needs the retrieval work; also collides with unstable offset pagination (postprocessing reads have no stable `ORDER BY`, `crud.py:66/161/250/354` — ML-007). |
| Snow-vs-meteo completeness semantics | Unresolved factual conflict: same provider, but local gateway maintenance runs **only** `extend_era5_reanalysis.py` and not snow (`run_locally.sh:744`), and cutoffs differ (meteo 30-day, snow/ERA5 365-day). **Owner decision needed.** |

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
