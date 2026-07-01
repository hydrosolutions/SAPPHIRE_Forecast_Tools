<!-- STATUS: compute implemented (PR-pending) via workflow wf_ce957733; SAPPHIRE_SKILL_VALUE flag; report+dashboard = Phase-4b follow-up. Reviews: approve/approve. -->

# Phase-4 Implementation Plan — Continuous/Volume Accuracy + Relative Economic Value for `forecast_skill_eval`

Issue tag: `high_prio_gi_draft_forecast_skill_eval_phase4_value_metrics`
Branch: `develop_forecast_skill_eval_phase4` (base `maxat_sapphire_2`; Phase-2/3 merged and present)
Module: `apps/forecast_skill_eval/src/forecast_skill_eval/` (READ-ONLY evaluator — no DB writes)

Both reviews returned **approve / zero blockers**. This synthesis folds in every improvement and risk raised, locks the exact metric formulas (REV verified below), fixes the seasonal-volume unit/naming problem, replaces float-equality edge detection with the repo's `*_undefined` flags, replaces the copy-the-fan-out approach with a shared import + structural-parity test, and expands the `days_in_period` / `expected_periods` test matrices that both reviewers flagged as the highest-bias-risk area.

---

## 1. Summary

Add two **additive, feature-flagged, default-off** metric families to the forecast-skill evaluator, complementing the existing binary-contingency (Phase-1/2) and probabilistic (Phase-3) layers:

- **Part A — Continuous / Volume Accuracy** (`continuous_metrics.py`): per-group `bias`, `MAE`, relative volume error (`rve`), `KGE-2009` (+ its `r`/`alpha`/`beta` components), `NSE`, plus the **headline Apr–Sep seasonal volume error** in true cubic metres (per `code`/`year`, day-weighted, with completeness gating and a cross-year rollup).
- **Part B — Relative Economic Value / cost–loss** (`economic_value.py`): the Richardson (2000) / Wilks (2011, Eq. 8.34) potential economic value `V(α)` of the below-norm decision over an α-grid, with the **analytic** maximum `v_max = H − F` at `α* = s`, derived directly from the already-computed 2×2 counts in `bundle.contingency_metrics`.

Both families are gated behind a **single new flag `SAPPHIRE_SKILL_VALUE`** that mirrors `SAPPHIRE_SKILL_PROB` byte-for-byte (`os.environ.get("SAPPHIRE_SKILL_VALUE", "").lower() in {"1", "true"}`). When the flag is off, every existing output is bit-identical to today's. New `ResultsBundle` fields default to empty frames, so all existing constructor call-sites keep working.

Report/dashboard surfacing is scoped as a **separable follow-up (Phase-4b)**; this plan delivers compute + artifacts CSV/parquet + a one-paragraph status summary only.

---

## 2. Locked metric formulas

### 2.1 Continuous primitives (over per-pair `forecast_value` = fc, `observed_value` = obs; `n` = pair count)

| Metric | Definition | Undefined → `NaN` when |
|---|---|---|
| `bias` | `mean(fc − obs)` | `n == 0` |
| `mae` | `mean(|fc − obs|)` | `n == 0` |
| `rve` | `(sum(fc) − sum(obs)) / sum(obs)` | `sum(obs) == 0` |
| `kge` | `1 − sqrt((r−1)² + (α_k−1)² + (β_k−1)²)` | `n < 2` or `σ_obs == 0` or `σ_fc == 0` or `μ_obs == 0` |
| `kge_r` | Pearson `r = corrcoef(fc, obs)` | as above |
| `kge_alpha` | `α_k = σ_fc / σ_obs` (KGE-2009 variability ratio, **not** Kling-2012 CV ratio) | as above |
| `kge_beta` | `β_k = μ_fc / μ_obs` | as above |
| `nse` | `1 − sum((fc − obs)²) / sum((obs − μ_obs)²)` | `sum((obs − μ_obs)²) == 0` |

- `σ` uses `std(ddof=0)`. **Note (both reviews):** the ratio `α_k = σ_fc/σ_obs` is `ddof`-invariant (numerator and denominator share the same `n`), so `ddof=0` is a parity convention with hydroeval/spotpy, **not** load-bearing. Pin it anyway for reproducibility and pin an explicit non-trivial golden computed with `ddof=0` so a stray `ddof=1` is caught.
- `corrcoef` must be wrapped to suppress the divide-by-zero `RuntimeWarning` when `σ_fc == 0` (constant forecast); returning a full-`NaN` tuple in that case is **intended** (Pearson `r` is genuinely undefined for a constant series — persistence/degenerate groups get no KGE rather than a misleadingly finite score).
- **Redundancy note (Review 1):** `rve == kge_beta − 1` exactly (both are `μ_fc/μ_obs − 1` over the identical sample). This is documented, not a bug — reviewers must not treat `rve` and `kge_beta` as independent signals.

### 2.2 Relative Economic Value — verified derivation

Cost–loss decision with cost `C` (of protective action), loss `L` (if event occurs unprotected), ratio `α = C/L ∈ (0,1)`. Sample base rate `s = (TP+FN)/N`, hit rate `H = POD = TP/(TP+FN)`, false-alarm **rate** `F = POFD = FP/(FP+TN)`.

Normalised mean expenses (per unit loss):

- `E_forecast = α·(H·s + F·(1−s)) + (1−H)·s`
- `E_climate  = min(α, s)`   (always protect vs. never protect, whichever is cheaper)
- `E_perfect  = s·α`

**Value:**

```
        E_climate − E_forecast       min(α, s) − F·α·(1−s) + H·s·(1−α) − s
V(α) = ────────────────────────  =  ──────────────────────────────────────
        E_climate − E_perfect                 min(α, s) − s·α
```

**Verified properties** (checked algebraically during synthesis):

- Perfect forecast (`H=1, F=0`) ⇒ `E_forecast = E_perfect` ⇒ `V(α) = 1` for all α.
- At `α = s`: numerator collapses to `s(1−s)(H−F)`, denominator to `s(1−s)` ⇒ `V(s) = H − F`.
- **Analytic maximum:** `v_max = H − F` at `α* = s` (the Peirce skill score identity). `v_max` MUST be reported analytically, never as `max` over the discrete grid (a coarse grid would understate it). A test asserts this at `α = s`.
- `V(α)` is **not clamped** — genuinely negative values are kept (a skill-negative table yields `V(α) < 0`). Phase-4b display consumers must be told values can be negative (do not apply `max(0, V)` in compute).

**Edge handling (Review 1/2 — use the repo's flags, not float equality):** consume the boolean columns already produced by `metrics.metrics_from_counts` — `base_rate_undefined`, `pod_undefined`, `pofd_undefined` (`metrics.py:~97-99,128`). A row is emitted with `value == NaN`, `v_max == NaN` (counts still recorded) when any of those flags is set or `N == 0`. Do **not** test `s in {0,1}` by float equality.

---

## 3. Context

### 3.1 The additive pattern to mirror (Phase-3)

- **Flag gate** — `orchestrator.run` at `orchestrator.py:135`:
  ```python
  if os.environ.get("SAPPHIRE_SKILL_PROB", "").lower() in {"1", "true"}:
      ...compute...
  else:
      prob_metrics = pd.DataFrame(columns=PROB_METRIC_COLUMNS)
      prob_reliability = pd.DataFrame(columns=PROB_RELIABILITY_COLUMNS)
  ```
- **`ResultsBundle` defaulted fields** — `orchestrator.py:57-63` (`field(default_factory=...)`).
- **Populated in the return** — `orchestrator.py:157-165`.
- **Artifacts empty-guard** — `artifacts.py:49-58`; `_write_table` (`artifacts.py:67-70`) emits both `.csv` and `.parquet`.
- **Summary section** — `_prob_metrics_section` (`artifacts.py:487-530`), appended in `_summary_markdown` at `artifacts.py:140`.
- **CLI passthrough** — `_apply_season_filter` reconstructs the bundle at `cli.py:244-252`; `_filter` (`cli.py:235-242`) no-ops on frames lacking a `season` column.

### 3.2 The reducer skeleton to clone

`compute_probabilistic_metrics` (`prob_metrics.py:580-649`) is the closest analog for Part A: `_ensure_group_columns` (`:710-721`) → four nested slice loops (`:626-639`, generators `:1092-1117`) fanning out `basin × norm_provenance × regime × season` (each with an `"all"` sentinel first) → `_metric_scopes` (`:724-776`) doing `groupby("horizon")` → `for pooled in (False, True)` → group cols `["horizon","model"]` (+`"code"` if not pooled, +`"lead"` if `is_long`) → per-group aggregation → `pd.concat` → reindex to the `*_COLUMNS` constant. Part A **omits** the `_score_pairs`/`_attach_reference_crps` prelude (`:618-621`): its metrics are pure group reductions over `forecast_value`/`observed_value` (always in `PAIR_COLUMNS`, `pairs.py:44-45`), so it is strictly simpler.

**Decision (Review 2):** Part A **imports** the slice generators (`_basin_slices`, `_provenance_slices`, `_regime_slices`, `_season_slices`) and the pooled/lead scope logic from `prob_metrics` rather than copying them, to prevent drift. A structural-parity test asserts continuous_metrics emits byte-identical group keys to prob_metrics on one shared fixture. (AC#8 forbids only an `orchestrator` import; importing `prob_metrics`/`contingency` helpers is allowed and preferred.)

### 3.3 REV ingredients already exist

`metrics.metrics_from_counts` (`metrics.py:54-57`) already computes, per contingency row: `base_rate = (TP+FN)/N` (= **s**), `pod = TP/(TP+FN)` (= **H**), `pofd = FP/(FP+TN)` (= **F**), plus the `*_undefined` flags. `bundle.contingency_metrics` (built `orchestrator.py:204-206`) carries the full 8-key structure + `TP/FP/FN/TN` + `event` + these columns. **Part B consumes this frame directly** (filtered to `event == "below_norm"`) — no re-slicing, guaranteed key-consistency with the binary layer.

> **FAR-vs-POFD trap (Review 2, load-bearing):** REV requires `F = POFD = FP/(FP+TN)` (`metrics.py:57`), **NOT** `FAR = FP/(TP+FP)` (`metrics.py:56`). To make the mis-wire impossible, the REV column is named **`pofd_F`** (not `far_rate_F`), and a dedicated crafted-counts test asserts `pofd_F == pofd` and `pofd_F != far`.

### 3.4 Seasonal-volume source columns

`season` is set per pair by `_season_label` (`pairs.py:557-568`): `"irrigation"` iff target month ∈ `_IRRIGATION_MONTHS = {4,5,6,7,8,9}` (`pairs.py:31`) via `_target_month` (`pairs.py:571-599`). `all_pairs` carries `code, year, season, horizon, period_key, model, forecast_value, observed_value`. There is **no** `days_in_period` helper today — day-weighting needs a new pure helper (D5).

---

## 4. Design Decisions

**D1 — Full 8-key group structure (documented deviation from literal prompt).** The prompt's `(code,horizon,regime,season,norm_provenance,lead)` omits `model` (can't score models without it) and `basin` (needed for joinability). Use the full 8-key structure identical to `prob_metrics._GROUP_KEYS` (`prob_metrics.py:110-119`): `(horizon, model, regime, season, code, basin, norm_provenance, lead)`, POOLED + per-code, per-lead for long-term.

**D2 — Single flag `SAPPHIRE_SKILL_VALUE`** for both families, mirroring `SAPPHIRE_SKILL_PROB` exactly. (Splitting into `SAPPHIRE_SKILL_CONT`/`SAPPHIRE_SKILL_REV` is deferred; the frame schemas do not depend on this and it can be introduced later non-breaking.)

**D3 — REV consumes `bundle.contingency_metrics`** (filtered `event == "below_norm"`), reusing `base_rate`/`pod`/`pofd` + `*_undefined`. Zero re-slicing, guaranteed key alignment.

**D4 — KGE-2009, `std(ddof=0)`** (see §2.1; ratio is `ddof`-invariant, parity-only convention).

**D5 — Day-weighted TRUE-VOLUME seasonal error + new `days_in_period` helper.** `forecast_value`/`observed_value` are period-**mean** discharges in m³/s, so an unbiased seasonal **volume** in cubic metres is `Σ_p (mean_flow_p · days_p · 86400)`. Add a pure helper `days_in_period(horizon, period_key, year)` in `continuous_metrics.py` derived from `_target_month`/`calendar.monthrange` (no new data).

> **Unit/naming fix (Review 1, load-bearing):** the draft's `volume_fc`/`volume_obs` were `m³/s·days` (missing `×86400`) — misleading for a number billed as the allocation headline. Emit **true m³**: columns `season_volume_m3_fc`, `season_volume_m3_obs`. The relative error `seasonal_volume_error = (V_fc − V_obs)/V_obs` is dimensionless and unaffected by the constant, but the absolute columns are now physically correct.
> Equal-weight diagnostic columns are renamed to `mean_flow_fc`/`mean_flow_obs` (mean of period-mean flows, m³/s) — honestly a flow diagnostic, not a volume.

**D6 — New `MIN_PAIRS_FOR_VARIANCE_METRICS = 10`** module-level constant in `continuous_metrics.py` (distinct from `config.min_years`, which is a *years* threshold for percentiles — wrong semantics). Below it, variance-sensitive metrics (`kge*`, `nse`) emit `NaN` (with `n_pairs` recorded); `bias`/`mae`/`rve` still emit for `n ≥ 1`. **Gate is enforced in the reducer**, not the primitive — the primitive only guards `n < 2`, but `corrcoef` of `n = 2` points is always `±1` (meaningless), so the reducer must suppress `kge*`/`nse` for `n ∈ [2, 9]`. A test pins this. Same constant reused by Part B as `min_pairs`.

**D7 — REV headline = `below_norm` only** (the operational allocation decision). The frame carries an `event` column for future extension; only `below_norm` rows are produced.

**D8 — Sample base rate `s = (TP+FN)/N` per group.** H, F, s are all estimated on the identical sample, as Richardson's derivation requires. **For POOLED groups, `s` is the pooled sample rate across stations** — because the pooled contingency counts are themselves pooled, H, F, and s are all computed on the same pooled sample (no mixing of per-station base rates). This is exactly what the derivation assumes.

**D9 — Seasonal-volume horizon gate.** Compute the Apr–Sep rollup only for `{pentad, decade, month}` (targets that tile the irrigation season). **Skip `day`** (short archive, no allocation value). **Skip `quarter`/`season`** (the `season` horizon *is* already the Apr–Sep aggregate — summing double-counts; `quarter` maps to a single month). Emit `n_periods`/`expected_periods`/`season_complete` always so starvation is auditable. Pinned `expected_periods` per horizon over months 4–9: **pentad = 36, decade = 18, month = 6** (tested).

**D10 — Within-group target-period uniqueness guard (Review 1, load-bearing).** The seasonal group key does not double-count leads/models (each is in the key), but the day-weighted sum over `period_key` silently double-counts if `all_pairs` contains two pairs with the same target `period_key` for one group (re-issued forecasts, or multiple issue dates collapsing to one long-term target). Within each seasonal group, **dedupe on `period_key`** (sort by issue date descending if that column exists, else assert uniqueness), and log a ledger entry (`stage="value", reason="duplicate_target_period"`) whenever duplicates are found. Regression-tested with a duplicate `period_key` fixture.

**D11 — `season_complete` is a count gate, not a day gate (Review 2, documented limitation).** `season_complete = (n_periods == expected_periods)`. If the single missing period is a short final sub-period (small day weight), a "complete" season can still slightly mis-weight vs. a full period. Acceptable for a headline diagnostic; documented so Phase-4b does not over-trust `season_complete == True`.

**D12 — Report/dashboard = separable follow-up (Phase-4b).** This plan delivers compute + CSV/parquet + a status summary paragraph only.

---

## 5. Implementation Plan

### 5.1 Files to create

| File | Purpose |
|---|---|
| `src/forecast_skill_eval/continuous_metrics.py` | Part A: pure primitives + `days_in_period` + `compute_continuous_metrics` + `compute_seasonal_volume`; column constants; `MIN_PAIRS_FOR_VARIANCE_METRICS`; `_SEASONAL_VOLUME_HORIZONS`; `_EXPECTED_PERIODS` |
| `src/forecast_skill_eval/economic_value.py` | Part B: `rev_curve` primitive + `compute_economic_value` + `compute_economic_value_summary`; column constants; `REV_ALPHA_GRID` |
| `tests/test_continuous_metrics.py` | Unit tests for Part A primitives + reducers (19999/29999 fixtures) |
| `tests/test_economic_value.py` | Unit tests for Part B primitive + reducer (19999/29999 fixtures) |

### 5.2 Files to modify (additive only)

| File | Change | Constraint |
|---|---|---|
| `orchestrator.py` | Imports; 5 defaulted `ResultsBundle` fields; one `SAPPHIRE_SKILL_VALUE` gate block after the prob block; populate return; ledger `stage="value"` entries | Do NOT touch contingency/baseline/prob blocks or their vars |
| `artifacts.py` | 5 guarded `_write_table` calls; one `_value_metrics_section`; append in `_summary_markdown` | Do NOT change existing writes/sections |
| `cli.py` | Add the 5 new fields to the `ResultsBundle(...)` reconstruction in `_apply_season_filter` | No new argparse flags |
| `tests/test_orchestrator.py` | Flag-on/flag-off gate tests | Additive |
| `tests/test_artifacts.py` | Artifact-write + section tests | Additive |
| `tests/test_cli.py` | Season-filter passthrough test (incl. non-empty assertion under flag-on) | Additive |

**No new argparse flags** — env-flag-gated only, exactly like `SAPPHIRE_SKILL_PROB`.

### 5.3 Reduced output frame schemas

```python
# continuous_metrics.py
CONTINUOUS_METRIC_COLUMNS = (
    "horizon", "model", "regime", "season", "code", "basin",
    "norm_provenance", "lead", "n_pairs",
    "bias", "mae", "rve", "kge", "kge_r", "kge_alpha", "kge_beta", "nse",
)

SEASONAL_VOLUME_COLUMNS = (              # per-(group, year), long
    "horizon", "model", "regime", "code", "basin", "norm_provenance", "lead",
    "year", "n_periods", "expected_periods", "season_complete",
    "season_volume_m3_fc", "season_volume_m3_obs", "seasonal_volume_error",
    "mean_flow_fc", "mean_flow_obs",     # equal-weight diagnostics (D5)
)

SEASONAL_VOLUME_SUMMARY_COLUMNS = (     # cross-year rollup
    "horizon", "model", "regime", "code", "basin", "norm_provenance", "lead",
    "n_years", "seasonal_volume_error_mean", "seasonal_volume_error_median",
)

# economic_value.py
ECONOMIC_VALUE_COLUMNS = (              # one row per (group, alpha), long
    "horizon", "model", "regime", "season", "code", "basin",
    "norm_provenance", "lead", "event", "n_pairs",
    "base_rate_s", "hit_rate_H", "pofd_F", "alpha", "value",
)

ECONOMIC_VALUE_SUMMARY_COLUMNS = (     # one row per group, wide
    "horizon", "model", "regime", "season", "code", "basin",
    "norm_provenance", "lead", "event", "n_pairs",
    "base_rate_s", "hit_rate_H", "pofd_F", "v_max", "alpha_star",
)
```

`ResultsBundle` gains **five** defaulted fields:
```python
continuous_metrics: pd.DataFrame = field(default_factory=lambda: pd.DataFrame(columns=CONTINUOUS_METRIC_COLUMNS))
seasonal_volume: pd.DataFrame = field(default_factory=lambda: pd.DataFrame(columns=SEASONAL_VOLUME_COLUMNS))
seasonal_volume_summary: pd.DataFrame = field(default_factory=lambda: pd.DataFrame(columns=SEASONAL_VOLUME_SUMMARY_COLUMNS))
economic_value: pd.DataFrame = field(default_factory=lambda: pd.DataFrame(columns=ECONOMIC_VALUE_COLUMNS))
economic_value_summary: pd.DataFrame = field(default_factory=lambda: pd.DataFrame(columns=ECONOMIC_VALUE_SUMMARY_COLUMNS))
```

### 5.4 Phased steps

**P1 — Part A primitives** (`continuous_metrics.py`, primitives only): `bias`, `mae`, `relative_volume_error`, `kge_2009`, `nse`, `days_in_period`, plus constants. No reducer yet. Fully unit-tested with hand-computed goldens (incl. the full `days_in_period` matrix in §7.1).

**P2 — Part A reducers**: `compute_continuous_metrics` (imports `prob_metrics` slice/scope helpers per §3.2) and `compute_seasonal_volume` (+ summary, incl. D10 dedupe + D9 gate + `_EXPECTED_PERIODS`). Depends on P1.

**P3 — Part B** (`economic_value.py`): `rev_curve` + `REV_ALPHA_GRID` + `compute_economic_value`/`compute_economic_value_summary` consuming the contingency-count frame via `*_undefined` flags. Independent of P1/P2 — parallelizable.

**P4 — Orchestrator wiring**: imports, 5 `ResultsBundle` fields, one `SAPPHIRE_SKILL_VALUE` gate block, populate return, ledger entries (`min_pairs_gate`, `duplicate_target_period`). Depends on P2 + P3.

**P5 — Artifacts + CLI wiring**: 5 guarded writes, `_value_metrics_section`, `_apply_season_filter` extension (all 5 fields threaded). Depends on P4.

**P6 — Integration tests + full suite**. Depends on P5.

### 5.5 Dependency graph

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": [], "parallel_agents": 1 },
    "P4": { "depends_on": ["P2", "P3"], "parallel_agents": 1 },
    "P5": { "depends_on": ["P4"], "parallel_agents": 1 },
    "P6": { "depends_on": ["P5"], "parallel_agents": 1 }
  }
}
```

P1 and P3 run in parallel; P2 follows P1; P4 joins.

---

## 6. Code Examples (concrete signatures)

### 6.1 `continuous_metrics.py` — primitives + helper

```python
"""Continuous/volume accuracy metrics — pure per-group reducers (Phase-4, Part A).

Self-contained: no orchestrator import, no DB access, no side effects.
Imports the prob_metrics slice/scope helpers to guarantee group-key parity.
Feature-gated by SAPPHIRE_SKILL_VALUE at the orchestrator boundary — this
module is flag-agnostic.
"""
from __future__ import annotations
import calendar
import math
import warnings
from typing import Final
import numpy as np
import pandas as pd

MIN_PAIRS_FOR_VARIANCE_METRICS: Final[int] = 10                     # D6
_SEASONAL_VOLUME_HORIZONS: Final = ("pentad", "decade", "month")    # D9
_EXPECTED_PERIODS: Final = {"pentad": 36, "decade": 18, "month": 6} # D9, Apr-Sep
_SECONDS_PER_DAY: Final = 86_400                                    # D5

CONTINUOUS_METRIC_COLUMNS: Final[tuple[str, ...]] = (...)           # §5.3
SEASONAL_VOLUME_COLUMNS: Final[tuple[str, ...]] = (...)
SEASONAL_VOLUME_SUMMARY_COLUMNS: Final[tuple[str, ...]] = (...)


def bias(fc: np.ndarray, obs: np.ndarray) -> float:
    """Signed mean error mean(fc - obs); NaN if n == 0."""

def mae(fc: np.ndarray, obs: np.ndarray) -> float:
    """Mean absolute error; NaN if n == 0."""

def relative_volume_error(fc: np.ndarray, obs: np.ndarray) -> float:
    """(sum_fc - sum_obs)/sum_obs as a fraction; NaN if sum_obs == 0.
    NB: numerically identical to kge_beta - 1 over the same sample."""

def kge_2009(fc: np.ndarray, obs: np.ndarray) -> tuple[float, float, float, float]:
    """Return (kge, r, alpha, beta), KGE-2009 with std(ddof=0).
    r via numpy.corrcoef inside a warnings.catch_warnings() block that
    suppresses the constant-series divide-by-zero.  Returns an all-NaN tuple
    if n < 2, sigma_obs == 0, sigma_fc == 0, or mu_obs == 0."""

def nse(fc: np.ndarray, obs: np.ndarray) -> float:
    """Nash-Sutcliffe efficiency; NaN if sum((obs-mean_obs)^2) == 0."""

def days_in_period(horizon: str, period_key: int, year: int) -> int | None:  # D5
    """Length in days of a horizon sub-period, computed from calendar.monthrange
    (NEVER hardcode month lengths).  Semantics reuse _target_month:

      pentad : sub-periods 1-5 -> 5 days each;
               sub-period 6    -> monthrange(year, month)[1] - 25
                                  (ranges 3..6: Feb non-leap = 3, 31-day month = 6)
      decade : sub-periods 1-2 -> 10 days each;
               sub-period 3    -> monthrange(year, month)[1] - 20
                                  (ranges 8..11: Feb non-leap = 8, 31-day month = 11)
      month  : monthrange(year, month)[1]
      else   : None (day/quarter/season are gated out upstream)
    """
```

The reducer `_aggregate_continuous(group_frame)` returns one dict per group; it computes `n_pairs` first and, when `n_pairs < MIN_PAIRS_FOR_VARIANCE_METRICS`, forces `kge*`/`nse` to `NaN` while still emitting `bias`/`mae`/`rve`. Guards mirror the `_crpss` idiom (`prob_metrics.py:788-791`).

### 6.2 `economic_value.py` — REV

```python
"""Relative Economic Value (REV) / cost-loss — Richardson (2000) / Wilks (2011).

Consumes the already-computed contingency-count frame (event == 'below_norm'),
so grouping and keys are guaranteed consistent with the binary layer.
Edge detection uses the base_rate_undefined / pod_undefined / pofd_undefined
flags (metrics.py), NOT float equality on s.  Self-contained, flag-agnostic.
"""
from __future__ import annotations
import math
from typing import Final
import numpy as np
import pandas as pd
from forecast_skill_eval.continuous_metrics import MIN_PAIRS_FOR_VARIANCE_METRICS

# 99 interior points; alpha_star = s is appended per row so the analytic peak
# is always sampled even when s < 0.01 or s > 0.99.  Endpoints excluded (V is
# degenerate at alpha in {0, 1}).
REV_ALPHA_GRID: Final[np.ndarray] = np.round(np.arange(0.01, 1.00, 0.01), 2)

ECONOMIC_VALUE_COLUMNS: Final[tuple[str, ...]] = (...)             # §5.3
ECONOMIC_VALUE_SUMMARY_COLUMNS: Final[tuple[str, ...]] = (...)


def rev_curve(s: float, H: float, F: float,
              alphas: np.ndarray) -> tuple[np.ndarray, float, float]:
    """Richardson/Wilks value curve.  Returns (values, v_max, alpha_star).

        V(alpha) = (min(a, s) - F*a*(1-s) + H*s*(1-a) - s) / (min(a, s) - s*a)
        v_max    = H - F   (analytic, NOT max over the grid)
        alpha_star = s

    Any NaN input (H/F/s), or a degenerate denominator, yields value = NaN.
    Values are NOT clamped: genuinely negative V(alpha) is preserved."""


def compute_economic_value(
    contingency_metrics: pd.DataFrame,
    *,
    event: str = "below_norm",
    min_pairs: int = MIN_PAIRS_FOR_VARIANCE_METRICS,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Long V(alpha) frame + wide (v_max, alpha_star) summary.

    Filters event == 'below_norm'.  Reads base_rate/pod/pofd as s/H/F and the
    *_undefined flags for edge detection; N == 0 or any undefined flag -> a row
    is still emitted with counts and value = NaN (never dropped).  Rows with
    n_pairs < min_pairs likewise emit counts + value = NaN.  Empty in -> two
    empty frames with the correct columns.
    Returns (ECONOMIC_VALUE_COLUMNS, ECONOMIC_VALUE_SUMMARY_COLUMNS)."""
```

### 6.3 `orchestrator.py` — gate block (inserted after `orchestrator.py:155`, before the return)

```python
if os.environ.get("SAPPHIRE_SKILL_VALUE", "").lower() in {"1", "true"}:
    continuous_metrics = compute_continuous_metrics(all_pairs)
    seasonal_volume, seasonal_volume_summary = compute_seasonal_volume(
        all_pairs, ledger=merged_ledger,       # D10 dedupe logs here
    )
    economic_value, economic_value_summary = compute_economic_value(contingency)
    for code in _starved_value_groups(continuous_metrics):
        merged_ledger.add(stage="value", reason="min_pairs_gate", code=code)
else:
    continuous_metrics = pd.DataFrame(columns=CONTINUOUS_METRIC_COLUMNS)
    seasonal_volume = pd.DataFrame(columns=SEASONAL_VOLUME_COLUMNS)
    seasonal_volume_summary = pd.DataFrame(columns=SEASONAL_VOLUME_SUMMARY_COLUMNS)
    economic_value = pd.DataFrame(columns=ECONOMIC_VALUE_COLUMNS)
    economic_value_summary = pd.DataFrame(columns=ECONOMIC_VALUE_SUMMARY_COLUMNS)
```

The existing prob block (`orchestrator.py:135-155`), contingency (`:126`), and baselines (`:127-133`) are untouched. New fields appended to the `ResultsBundle(...)` return.

### 6.4 `artifacts.py` — guarded writes (after `artifacts.py:58`)

```python
for frame, stem in (
    (bundle.continuous_metrics, "continuous_metrics"),
    (bundle.seasonal_volume, "seasonal_volume"),
    (bundle.seasonal_volume_summary, "seasonal_volume_summary"),
    (bundle.economic_value, "economic_value"),
    (bundle.economic_value_summary, "economic_value_summary"),
):
    if not frame.empty:
        _write_table(frame, artifact_dir / stem, parquet_available=parquet_available)
```

`_value_metrics_section(bundle)` mirrors `_prob_metrics_section` (`artifacts.py:487-530`): a status line when frames are empty, else counts (n continuous groups, n seasonal-volume rows, n complete seasons, n REV groups, `v_max` range) + artifact filenames + a one-line note that `V(α)` may be negative and `season_complete` is a count gate. Appended at `artifacts.py:140`.

---

## 7. Testing

**Framework:** `SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_skill_eval`. Fixtures use **only** synthetic codes `19999`/`29999` and invented discharge (no-real-codes rule).

### 7.1 Part A primitives (`test_continuous_metrics.py`) — hand-computed goldens

| Case | Input | Expected |
|---|---|---|
| `bias` sign | fc=[3,4], obs=[2,2] | `+1.5` |
| `mae` | fc=[3,1], obs=[2,2] | `1.0` |
| `rve` fraction | fc sum 10, obs sum 8 | `0.25` |
| `rve` edge `sum_obs==0` | obs all 0 | `NaN` |
| `rve` == `kge_beta − 1` | any crafted sample | equal to 1e-12 |
| `nse` perfect | fc==obs | `1.0` |
| `nse` = 0 at mean-forecast | fc = const μ(obs) | `0.0` |
| `nse` edge zero obs-variance | obs all equal | `NaN` |
| `kge_2009` perfect | fc==obs | `(1, 1, 1, 1)` |
| `kge_2009` components (ddof-explicit) | crafted r/α/β | match closed-form (computed with `ddof=0`) to 1e-9 |
| `kge_2009` constant forecast | σ_fc=0 | all-`NaN` tuple, **no `RuntimeWarning`** (assert via `warnings` filter) |
| `kge_2009` edges | n<2 / σ_obs=0 / μ_obs=0 | `NaN` tuple each |
| **`days_in_period` — full matrix (both reviews)** | | |
| pentad 1–5 | any month | `5` |
| pentad 6, 31-day month (Jan) | | `6` |
| pentad 6, 30-day month (Apr) | | `5` |
| pentad 6, Feb non-leap (2021) | | `3` |
| pentad 6, Feb leap (2020) | | `4` |
| decade 1–2 | any month | `10` |
| decade 3, 31-day month (Jan) | | `11` |
| decade 3, 30-day month (Apr) | | `10` |
| decade 3, Feb non-leap (2021) | | `8` |
| decade 3, Feb leap (2020) | | `9` |
| month | Jan / Feb-2021 / Feb-2020 | `31` / `28` / `29` |
| gated horizon | `day`/`quarter`/`season` | `None` |

### 7.2 Part A reducers

- **Group-key parity (Review 2):** feed one shared 2-code × 2-model × pentad+month fixture to both `compute_continuous_metrics` and `compute_probabilistic_metrics`; assert the emitted group-key tuples are **byte-identical** (POOLED rows present, per-lead only for `month`, `lead is None` for `pentad`, `"all"` sentinel buckets emitted).
- **`min_pairs` gate incl. n=2 caveat (Review 1):** a 5-pair group and separately an `n=2` group → `kge`/`nse` are `NaN`, `bias`/`mae`/`rve` finite, `n_pairs` recorded. Confirms the reducer suppresses variance metrics for all `n ∈ [2, 9]` (the primitive alone would let `n=2` through with `r=±1`).
- **Seasonal true-volume day-weighting:** 6 in-season pentads with known day lengths and discharges → assert `season_volume_m3_fc`/`season_volume_m3_obs` equal `Σ(mean_flow · days · 86400)` and `seasonal_volume_error` matches; assert `season_complete == False` when `n_periods < expected_periods`; assert `NaN` error when `V_obs == 0`.
- **`expected_periods` pinned (both reviews):** assert `expected_periods == {pentad:36, decade:18, month:6}` per horizon over Apr–Sep, and that an off-by-one `n_periods` flips `season_complete`.
- **Target-period uniqueness (Review 1, D10):** feed a group with a duplicate `period_key` → assert no double-count (result equals the deduped sum), and a ledger `stage="value", reason="duplicate_target_period"` entry is emitted.
- **Horizon gate (D9):** `day`, `quarter`, `season` produce no seasonal-volume rows; `pentad`/`decade`/`month` do.
- **Cross-year summary:** two complete years → `n_years == 2`, mean/median correct.
- **Empty in → empty out** with correct columns.

### 7.3 Part B (`test_economic_value.py`)

| Case | Assertion |
|---|---|
| `v_max == H − F` (analytic) | crafted TP/FP/FN/TN → `v_max == pod − pofd` to 1e-9; and `v_max ≥ max(V over grid)` (analytic ≥ discrete) |
| `alpha_star == s` | equals `base_rate` |
| `V(α*)` on curve | value at appended `α = s` is finite and equals `v_max` |
| **`pofd_F == pofd` and `!= far`** | crafted counts where `FP/(FP+TN) ≠ FP/(TP+FP)` → guards the FAR/POFD wiring |
| `V` un-clamped | a skill-negative table yields a genuinely negative `V(α)` row |
| `s` undefined (all-normal obs) | `pod_undefined`/`base_rate_undefined` set → all `value==NaN`, `v_max==NaN`, row emitted with `n_pairs`, `base_rate_s==0` |
| `s` saturated (all-below obs) | `pofd_undefined` set → `value==NaN` for α<1, row emitted |
| `N == 0` row | emitted with `value==NaN` (never dropped, never crashes) |
| `min_pairs` gate | thin group emits counts + `value==NaN`, not dropped |
| N-invariance | doubling all counts leaves `V(α)` identical |
| Key alignment | REV rows join 1:1 to `event=='below_norm'` contingency groups |
| **Empty contingency in → empty economic_value out** (Review 2) | correct columns, zero rows, no error |

### 7.4 Integration (`test_orchestrator.py`, `test_artifacts.py`, `test_cli.py`)

- **Flag off (default):** all five new frames empty with correct columns; **`pairs`/`contingency_metrics`/`baselines`/`prob_metrics`/`prob_reliability` and all artifacts bit-identical** to a pre-change run (regression guard on the additive constraint).
- **Flag on** (`monkeypatch.setenv("SAPPHIRE_SKILL_VALUE", "true")`): frames populated; ledger has `stage=="value"` entries for starved groups.
- **Artifacts:** flag on → `continuous_metrics.csv`+`.parquet` (and the other four) written; flag off → absent. Summary always contains the value-metrics section.
- **CLI season filter — non-empty assertion (Review 2, load-bearing):** with the flag **on** and a non-`all` season filter, assert all five value frames survive the `_apply_season_filter` reconstruction **non-empty** (guards against an agent forgetting to thread one of the five fields through `cli.py:244-252`, which would silently reset it to the empty default only under season filtering). `continuous_metrics`/`economic_value` keep only matching `season` rows; `seasonal_volume*` (season-less) pass through untouched via the `"season" not in frame.columns` guard.

---

## 8. Out of Scope

- Report markdown tables/rankings and dashboard tiles (→ Phase-4b; this phase emits CSV/parquet + a status paragraph only).
- Percentile-event REV (only `below_norm`, D7).
- Any change to `sapphire/services/` — read-only evaluator, no DB writes.
- Editing existing contingency/baseline/prob compute paths or their output schemas.
- New CLI argparse flags (env-gated only).
- Kling-2012 KGE variant; climatological base rate for REV; `max(0, V)` clamping.
- Splitting the flag into `SAPPHIRE_SKILL_CONT`/`SAPPHIRE_SKILL_REV` (deferred).

## 9. Acceptance Criteria

1. **Additive/off-by-default:** with `SAPPHIRE_SKILL_VALUE` unset, `pairs`, `contingency_metrics`, `baselines`, `prob_metrics`, `prob_reliability`, and all artifacts are byte-identical to the pre-change branch (regression test + manual diff on a fixture run).
2. **Flag on** produces the five frames with the exact schemas in §5.3, on the full 8-key POOLED×strata structure (per-lead only for long-term).
3. **Scientific correctness** verified by hand-computed goldens: `NSE=0` at mean-forecast, `KGE=(1,1,1,1)` at perfect, `rve` edge `sum_obs=0 → NaN`, `rve == kge_beta − 1`, `v_max = H − F` at `α* = s` (analytic ≥ discrete grid), `pofd_F == pofd ≠ far`, all documented edges → `NaN` (never crash, never clamp).
4. **Units correct:** seasonal columns are true m³ (`× days × 86400`), named `season_volume_m3_*`; the day-weighting golden proves it.
5. **`days_in_period` computed from `calendar.monthrange`** (never hardcoded) and passes the full variable-length sub-period matrix (§7.1), including pentad-6/decade-3 for 31-day, 30-day, Feb-leap, and Feb-non-leap months.
6. **Meaningfulness gates** honored: variance metrics `NaN` below `MIN_PAIRS_FOR_VARIANCE_METRICS` (incl. `n ∈ [2,9]`, enforced in the reducer); seasonal volume restricted to `{pentad,decade,month}` with pinned `expected_periods` completeness flags and within-group `period_key` dedupe; REV edges via `*_undefined` flags → `NaN` rows emitted (never dropped); starved/duplicate groups logged to the ledger with `stage="value"`.
7. **No real station codes or discharge** in any new tracked file or test (`19999`/`29999` only) — grep-clean.
8. Every new function has a unit test; **`SAPPHIRE_TEST_ENV=True bash run_tests.sh` passes with zero failures and zero unexpected skips**.
9. `ruff check` / `ruff format` clean on the two new modules and the three touched files.
10. Both new modules import **no** `orchestrator` symbol (importing `prob_metrics`/`contingency`/`metrics` helpers is permitted and expected).

---

### Orchestration note (CLAUDE.md protocol)

The orchestrator delegates each phase to a Sonnet 4.6 agent, scoped to the file list in §5.1–5.2, with the mandatory clause: *"Do NOT change any existing function signatures, data flow, or control flow; changes are purely additive or modify only the specific behavior described."* Use `isolation: "worktree"` for P4/P5 (orchestrator/artifacts/cli carry side-effect risk). Verify with `run_tests.sh forecast_skill_eval` after each phase; deliberate on the diff (scope, no reorders/reformats, data-flow preserved) before accepting.

Relevant plan-anchor files (all absolute):
`/Users/bea/Documents/GitHub/SAPPHIRE_forecast_tools/apps/forecast_skill_eval/src/forecast_skill_eval/{orchestrator.py, prob_metrics.py, contingency.py, metrics.py, pairs.py, periods.py, artifacts.py, cli.py, config.py}`
