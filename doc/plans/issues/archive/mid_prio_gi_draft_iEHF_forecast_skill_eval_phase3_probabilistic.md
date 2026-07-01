# Phase-3: Probabilistic Forecast Verification for `forecast_skill_eval`

> **Issue file target:** `doc/plans/issues/high_prio_gi_draft_forecast_skill_eval_probabilistic.md`
> **Branch:** `develop_forecast_skill_eval_phase2` (core P1–P3 land in the same PR as Phase-2; P4/P5 are separable follow-up commits — see *PR scoping*)
> **Priority:** high_prio
> **Status:** draft
> **Feature flag:** `SAPPHIRE_SKILL_PROB` (default-off, mirrors `SAPPHIRE_SKILL_LEAD_AWARE`)

---

## Summary

The forecast-skill evaluator currently collapses every forecast to a single deterministic `point_value` and scores only binary contingency events (`below_norm`, percentile low/high, return periods). The source forecasts, however, carry a predictive distribution that the evaluator reads into the raw reader frame and then discards at pair construction.

This phase adds an **additive, feature-flagged** probabilistic verification layer that scores the *uncertainty* of the predictive distribution:

- **CRPS** and **CRPS skill score** (CRPSS) vs an empirical climatology reference — with the reference CRPS computed by the **same quantile-grid estimator** as the forecast CRPS so the skill score is unbiased.
- **Interval coverage / reliability** at the nominal levels each grid supports, plus a Wilson CI on coverage.
- **Sharpness** (interval width, raw and norm-normalised), reported *conditional on* calibration.
- A **coarse rank / reliability table** (explicitly not a fine PIT histogram at 5–7 nodes).
- **Brier score** and **Brier skill score** for interior binary events (`below_norm`), scoring the forecast *probability* derived from the quantile grid.

The existing point-forecast contingency path is not modified. Models without usable quantile bands stay point-only and are logged to the exclusion ledger.

---

## Context

- Module: `apps/forecast_skill_eval/src/forecast_skill_eval` — a **read-only** evaluator. No DB writes anywhere.
- Data flow today: `api_readers` (per-forecast-type readers) → `pairs.build_pairs` (forecast⇄observation matching, one `_ForecastInstance` per row) → `orchestrator.run` (concat all horizons → thresholds → contingency + **three** baselines) → `artifacts.write_artifacts` (CSV/parquet) → `dashboard` (Streamlit/Altair) → report draft.
- The predictive distribution survives the reader stage (the client returns `pd.DataFrame(records)` with all columns) but is dropped at `pairs.py` because `_ForecastInstance` has no quantile field.
- **All line numbers in this plan are indicative only.** Implementing agents MUST locate edit points by **symbol name** (function/class/constant), not by line range — several cited ranges were found to drift (e.g. `read_forecasts` *defines* at `api_readers.py:37`, not `:64`; `run` starts at `orchestrator.py:48`; `baselines.py` builds **three** baselines including `build_operational_proxy_baseline`).

### Quantile availability per source — **VERIFIED (P0 done, 2026-07-01, local dev DB)**

Confirmed by direct query of the postprocessing DB (`docker exec ... psql`), populated-band fraction per `(horizon_type, model_type)`. Band availability is strongly **model-dependent** — the review's caution was correct; gating by band-presence is mandatory.

| Source (reader)          | Table            | Grid (confirmed columns)                          | Verified band population → scorable models |
| ------------------------ | ---------------- | ------------------------------------------------- | ------------------------------------------ |
| `read_forecasts` (short) | `forecasts`      | **4-node** `q05,q25,q75,q95` (point = `forecasted_discharge`; **no q10/q90/q50**) | pentad/decade: TFT/TiDE/TSMixer/NeuralEnsemble **98–99%**, EM **80–90%**; day 50–67% (thin). → **coverage_80 (q10/q90) UNAVAILABLE short-term**; coverage_90 (q05/q95) + coverage_50 (q25/q75) OK. |
| `read_long_forecasts`    | `long_forecasts` | **7-node** `q05,q10,q25,q50,q75,q90,q95` (+ `q,q_obs`) | **Scorable:** EM 94–100%, MC_ALD 100%, NAIVE_MEAN 98–100%, LR_BASE/LR_SM 91–98% (month ~51%). **NOT scorable (0% band):** GBT, SM_GBT, SM_GBT_LR, SM_GBT_NORM. Partial: SKILLED_MEAN 16–47%, LR_SM_ROF month ~40%. coverage_80 available (has q10/q90). |
| `read_lr_forecasts`      | `lr_forecasts`   | **Parametric** `q_mean`, `q_std_sigma` (Gaussian; NO quantile grid) | Point-only in v1. A closed-form Gaussian-CRPS path is a possible follow-up. LR reads via `forecast_type="short"`, so gating must key on band-presence, not `forecast_type` — see Design Decision 5. |

**P0 verdict:** feature is feasible for the headline models (EM at all horizons; MC_ALD/LR_BASE long-term). Model-gating by finite-node count cleanly excludes the 0%-band models. Two forced refinements: (a) `coverage_80` is long-term-only; (b) short-term q50 is absent — insert `forecasted_discharge` as the q50 node (P0 item 3: confirm it is the median, not the mean, per model — still open, low-risk since it only affects the central node).

---

## Problem

Forecast quality is currently reported only through deterministic contingency metrics. A forecast that nails the point value but is grossly over- or under-confident scores identically to a well-calibrated one. Operational hydrologists and model developers need to know whether the predictive intervals are trustworthy (coverage/reliability), how tight they are (sharpness), and how the whole distribution scores against a climatological baseline (CRPS/CRPSS). None of this is computed today even though the data is already fetched and discarded.

---

## Desired Outcome

1. `all_pairs` carries the per-pair quantile band (`fc_q05 … fc_q95`) plus a `fc_grid_id` tag, populated where the source provides it and `NaN`/`""` otherwise — **without changing the existing `forecast_value` / classification / contingency columns or their values**.
2. A new `prob_metrics.py` module computes per-pair probabilistic scores and aggregates them across the **same 8 group keys** the contingency path uses (`horizon, model, regime, season, code, basin, norm_provenance, lead`, with `POOLED` codes).
3. Two new artifact tables are written (only when `SAPPHIRE_SKILL_PROB` is enabled): `prob_metrics.csv/.parquet` (wide, distribution + Brier metrics keyed by group) and `prob_reliability.csv/.parquet` (long, per nominal level for reliability/rank plots).
4. A new **Probabilistic** dashboard view and a **Probabilistic forecast verification** report section (both P4/P5, separable follow-up commits).
5. Models without usable bands stay point-only; every new function has unit tests using synthetic codes `19999`/`29999`; no new hard dependency.

---

## Technical Analysis (locate by symbol, not line)

**Where the distribution is dropped (the additive attach points):**

- **Reader:** `select_point_value` / `_add_point_values` extract only `point_value` (+ note). Quantile columns pass through on the frame but nothing consumes them. Call-sites: `read_forecasts` (`"short"`), `read_lr_forecasts` (`"short"`), `read_long_forecasts` (`"long"`).
- **Pairs:** `_ForecastInstance` captures only `forecast_value`; `_short_instance` / `_long_instance` read only `point_value`; `_pair_row` emits only `PAIR_COLUMNS`. Quantiles discarded here.
- **Orchestrator:** `run` builds `all_pairs`, `thresholds`, `contingency`, three baselines, returns a frozen `ResultsBundle`.

**Structures to mirror (not edit):**

- Group keys / `POOLED`: `contingency.OUTPUT_COLUMNS`; nested slicing in `count_contingencies`; per-lead rows.
- Metric-column convention + Wilson CI: `metrics.METRIC_COLUMNS`, `add_metrics`, `metrics._wilson_interval`.
- Baseline analogue + reusable lag-1 / obs-lookup helpers: `baselines.py` — `build_climatology_baseline`, `build_operational_proxy_baseline`, `build_persistence_baseline`, `_build_obs_lookup`, `_lag1_key`, `_concat_baselines`. **Reuse `build_climatology_baseline`'s exact conditioning set** for the CRPS climatology reference (see Risks).
- Event thresholds/directions for Brier: `events.py` — `EventDef.direction`, `compute_percentile_thresholds`, `compute_return_levels`.
- The point classification rule: `classifier.classify` uses `value < threshold * norm` where `threshold` is a **config parameter** (`config.threshold`) — **not** a literal 0.80.
- Artifact emit: `artifacts.write_artifacts`, summary section.
- Dashboard: loaders in `dashboard/data.py`; sibling-file tolerance in `dashboard/aggregates.py`; view radio, aggregates block, 1:1 diagonal, ranking bars, zero-line, cache pattern in `dashboard/app.py`.

**Downstream-safety verification (confirmed by exploration):** `orchestrator.py` uses `PAIR_COLUMNS` only to build the *empty* frame — appending columns is safe. `events.py` selects columns by name and reindexes to `list(pairs.columns)` — adding columns does not disturb reclassification. Adding trailing defaulted fields to the frozen `ResultsBundle` keeps existing constructor calls valid.

---

## Design Decisions (stated explicitly)

1. **Metrics set (committed):** CRPS, CRPSS-vs-climatology (primary), CRPSS-vs-persistence (secondary, documented as distribution-vs-point); coverage + reliability (`|coverage − nominal|`) at the levels each grid supports + Wilson CI on the widest supported coverage; sharpness_iqr (q75−q25), sharpness_width (outer band) + norm-normalised; a coarse rank/reliability table; Brier + Brier skill score for **`below_norm` only**. **Murphy reliability/resolution/uncertainty decomposition is dropped from this phase** (no dead present-but-NaN schema) — deferred to follow-up.

2. **CRPS estimator (correctness-critical):** approximate CRPS = `2·∫₀¹ pinball_loss(τ) dτ` via trapezoidal weights over the quantile grid, **with explicit tail treatment**: the outer segments `[0, τ_min]` and `[τ_max, 1]` receive a rectangular/linear tail term (extrapolate the outer node value as a flat tail, or linearly extend the outer segment) so that observations beyond the band `(obs < q_min or obs > q_max)` are penalised and over-confident narrow bands are **not** rewarded. The **climatology and persistence reference CRPS use the identical grid+tail estimator** (sample the reference distribution's quantiles on the same levels), so CRPSS is not biased by estimator mismatch. Requires ≥2 finite nodes after isotonic repair; else `NaN`. No new scoring dependency.

3. **Cross-grid comparability:** each pair records `fc_grid_id` (e.g. `"long7"`, `"short5"`). Raw `crps` is **never ranked across grids**; the dashboard CRPSS ranking and any cross-model table are restricted to a single `fc_grid_id` (or facet by it). Documented in the report.

4. **Monotonicity:** a non-decreasing band is enforced by **isotonic sort/clip** (cumulative max of the sorted nodes) before scoring, with a **counted `n_band_repaired` diagnostic** in the ledger — pairs are *not* silently nulled on a quantile crossing. A unit test covers the crossing case.

5. **Model gating by band presence, not `forecast_type`:** LR reads through `forecast_type="short"`, so `QUANTILE_SOURCE_MAP` keyed by `forecast_type` cannot distinguish LR from short-term neural. Gating is by **column presence / finite-node count** on the row (and, defensively, an explicit LR model check). A row-group with no usable band receives NaN scores, is excluded from means, and is logged once to `ExclusionLedger` with reason `no_quantile_band`. A regression test asserts an LR-shaped row (`model="LR"`) yields an empty band even if a stray `q` column is present.

6. **Reader ingestion:** add sibling `select_quantile_band` + `_add_quantile_band` emitting canonical levels (NaN for absent nodes) into a single `quantiles` object column. `select_point_value`/`_add_point_values` untouched.

7. **Output frames:** one **wide** `prob_metrics` (one row per group key; `event="distribution"` sentinel rows carry CRPS/coverage/sharpness/rank; `event="below_norm"` rows carry Brier columns) + one **long** `prob_reliability`. The **NaN-by-row-type contract is explicit and unit-tested**: distribution rows have NaN Brier columns and vice-versa. CRPSS is folded onto the CRPS row (`crps, crps_clim, crpss, crps_persist, crpss_persist`); no separate baseline artifact.

8. **Pair set / gate:** v1 scores the **same `all_pairs` set** the contingency path uses (shares the norm/classifiability gate). This is documented as a known limitation (coverage/CRPS do not intrinsically need `norm`, so a wider pair set is a follow-up); the report states the n is the classifiable subset.

9. **Performance:** the climatology pairwise energy term is O(m²) per group — it is **precomputed once per climatology conditioning group** and reused, never recomputed per pair (cf. commit `ef7975c6`, which fixed an O(n²) summary that hung on event runs). A performance-category test guards this.

10. **No edits to `sapphire/services/`** (colleague-managed, read-only reference only). No DB writes anywhere.

---

## Implementation Plan

### PR scoping

- **Same PR as Phase-2:** P0 (verify), P1 (ingestion), P2 (scoring core), P3 (reducer + orchestrator + artifacts). This is the minimal, testable deliverable: bands in → `prob_metrics.csv` / `prob_reliability.csv` out, behind `SAPPHIRE_SKILL_PROB`.
- **Separable follow-up commits (may land after Phase-2 merges):** P4 (dashboard view) and P5 (report + figures) — bundling a 5-chart dashboard view and a 4-figure report section into an already-large Phase-2 PR violates "keep PRs focused." They depend only on the frozen P3 artifact schema.

### Files to Create

| File | Purpose |
| --- | --- |
| `src/forecast_skill_eval/prob_metrics.py` | Pure per-pair scorers (CRPS+tails, coverage, sharpness, rank, event-probability, Brier) + `_score_pairs` + `compute_probabilistic_metrics` reducer + `build_prob_reliability` + column/level constants |
| `src/forecast_skill_eval/prob_baselines.py` *(optional; may live in `prob_metrics.py`)* | Climatology & persistence **grid-estimator** CRPS references for CRPSS, reusing `baselines` conditioning + lag-1 helpers, with the O(m²) term precomputed per group |
| `tests/test_prob_metrics.py` | Unit tests for every scorer + reducer |
| `tests/test_prob_reliability.py` | Unit tests for the reliability/rank builder |
| `tests/test_prob_baselines.py` | CRPSS sign/reference-conditioning correctness + estimator-consistency tests |

> **Test directory is `apps/forecast_skill_eval/tests/`** (plural). All new and modified test files live there.

### Files to Modify (additive only)

| File | Change |
| --- | --- |
| `api_readers.py` | ADD `QUANTILE_LEVELS`, `QUANTILE_SOURCE_MAP`, `select_quantile_band`, `_add_quantile_band`; call `_add_quantile_band` right after each `_add_point_values` (in `read_forecasts`, `read_lr_forecasts`, `read_long_forecasts`). Leave `select_point_value`/`_add_point_values` unchanged. |
| `pairs.py` | ADD trailing optional `quantiles: Mapping[float,float] | None = None` and `grid_id: str = ""` to `_ForecastInstance`; populate in `_short_instance`/`_long_instance`; emit `fc_q05…fc_q95` + `fc_grid_id` in `_pair_row`; append those to `PAIR_COLUMNS`. |
| `orchestrator.py` | ADD two optional defaulted fields to `ResultsBundle` (`prob_metrics`, `prob_reliability`); in `run`, gated on `SAPPHIRE_SKILL_PROB`, compute both frames after baselines and log `no_quantile_band` / `n_band_repaired` into `merged_ledger`. Flag-off → empty framed defaults. |
| `artifacts.py` | ADD two `_write_table(...)` calls for the new frames; extend the `summary.md` section list. |
| `cli.py` | Read `SAPPHIRE_SKILL_PROB`; pass the two new fields through `_apply_season_filter` / `_filter`. |
| `dashboard/data.py` *(P4)* | ADD `load_prob_metrics` / `load_reliability` mirroring `load_metrics` with sibling-file tolerance. |
| `dashboard/app.py` *(P4)* | Extend view radio with `"Probabilistic"`; add render block; cache loaders. |
| `doc/plans/working/forecast_skill_eval_report_draft.md` *(P5)* | New section + figures + related-document entries. |
| `tests/test_api_readers.py`, `tests/test_pairs.py` | ADD quantile columns to the existing `_forecast` / `_lr_forecast` fixtures (currently only set `forecasted_discharge`) and add ingestion assertions; existing assertions stay green. |

### Ordered Steps (phased per repo protocol)

**P0 — VERIFY (HARD GATE) — ✅ DONE 2026-07-01** (local dev DB, see the verified table in Context above; only item 3 — whether short-term `forecasted_discharge` is the median vs mean per model — remains open, low-risk).
Confirmed, against the **local dev postprocessing DB** (`docker exec sapphire-postprocessing-db psql`):
1. Which quantile columns each table actually exposes.
2. The **populated-band fraction per `(horizon, model_type)`** — i.e. the share of rows with non-NULL `q05…q95`. This is the gate: short-term probabilistic scoring is only trusted/reported for `(horizon, model_type)` combos above an agreed populated-fraction threshold; combos below it are declared point-only in the report.
3. Whether short-term `forecasted_discharge` is the **median** or a **mean** per model (affects inserting it as the q50 node).
Record the confirmed model→band map and populated-fraction table for report §1. Design `_add_quantile_band` to emit NaN for absent nodes so the code is correct regardless, but do **not** publish short-term numbers until (2) is confirmed. Depends on: nothing.

**P1 — Quantile ingestion (plumbing).** Files: `api_readers.py`, `pairs.py` (+ their tests). Purely additive; point path values unchanged. Depends on: P0.

**P2 — Probabilistic scoring core.** Files: `prob_metrics.py` (pure scorers + `_score_pairs`), `prob_baselines.py`, tests. No orchestrator wiring yet. Depends on: P0 (design), parallelisable with P1.

**P3 — Reducer + orchestrator wiring + artifacts.** Files: `prob_metrics.compute_probabilistic_metrics`, `build_prob_reliability`, `orchestrator.py`, `artifacts.py`, `cli.py`. Behind `SAPPHIRE_SKILL_PROB`. Depends on: P1, P2.

**P4 — Dashboard view (separable follow-up).** Files: `dashboard/data.py`, `dashboard/app.py` (+ loader tests). Depends on: P3.

**P5 — Report + figures (separable follow-up).** Files: `report_draft.md`; figure wiring. Depends on: P3, P4.

Dependency graph:

```json
{
  "phases": {
    "P0": { "depends_on": [], "parallel_agents": 0 },
    "P1": { "depends_on": ["P0"], "parallel_agents": 1 },
    "P2": { "depends_on": ["P0"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P1", "P2"], "parallel_agents": 1 },
    "P4": { "depends_on": ["P3"], "parallel_agents": 1 },
    "P5": { "depends_on": ["P3", "P4"], "parallel_agents": 1 }
  }
}
```

---

## Code Examples (concrete signatures)

### `api_readers.py` (additive)

```python
QUANTILE_LEVELS: Final = (0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95)

# Per forecast_type: canonical level -> source column name.
# NOTE: this maps SHORT vs LONG *columns*; it does NOT gate LR — LR also reads
# as "short" and is excluded by band-presence (Design Decision 5).
QUANTILE_SOURCE_MAP: Final[dict[ForecastType, dict[float, str]]] = {
    "short": {0.05: "q05", 0.25: "q25", 0.50: "forecasted_discharge",
              0.75: "q75", 0.95: "q95"},           # q10/q90 absent
    "long":  {0.05: "q05", 0.10: "q10", 0.25: "q25", 0.50: "q50",
              0.75: "q75", 0.90: "q90", 0.95: "q95"},
}

def select_quantile_band(
    row: Mapping[str, Any], forecast_type: ForecastType
) -> tuple[dict[float, float], str, str]:
    """Return ({level: value} for finite source quantiles, note, grid_id).
    Missing/NaN nodes are dropped. Rows with <2 finite nodes -> ({}, note, "").
    grid_id is e.g. 'long7' / 'short5' / '' when band-less."""

def _add_quantile_band(
    data: pd.DataFrame, forecast_type: ForecastType
) -> pd.DataFrame:
    """Add object column 'quantiles' ({level:value}), 'quantiles_note', and
    'fc_grid_id'. Empty frame -> typed empty columns. Additive: does not touch
    point_value / point_value_note / any existing column."""
```

### `pairs.py` (additive)

```python
@dataclass(frozen=True)
class _ForecastInstance:
    # ... existing fields unchanged ...
    quantiles: Mapping[float, float] | None = None   # NEW trailing optional
    grid_id: str = ""                                # NEW trailing optional

# _pair_row emits fc_q05..fc_q95 (NaN when absent) + fc_grid_id.
PAIR_COLUMNS = (*_EXISTING_PAIR_COLUMNS,
                "fc_q05", "fc_q10", "fc_q25", "fc_q50",
                "fc_q75", "fc_q90", "fc_q95", "fc_grid_id")
```

### `prob_metrics.py` (new — pure scorers)

```python
def isotonic_band(
    levels: Sequence[float], quantiles: Sequence[float]
) -> tuple[list[float], list[float], bool]:
    """Drop NaN nodes, sort by level, enforce non-decreasing via cumulative
    max. Returns (levels, repaired_quantiles, was_repaired)."""

def crps_from_quantiles(
    levels: Sequence[float], quantiles: Sequence[float], observed: float
) -> float:
    """Approximate CRPS = 2*integral pinball_loss(tau) dtau via trapezoidal
    weights over the grid, PLUS explicit tail terms on [0, tau_min] and
    [tau_max, 1] so obs beyond the band is penalised. >=2 finite nodes after
    isotonic repair required; else NaN. Same estimator used for references."""

def crps_reference_from_samples(
    sample: Sequence[float], observed: float, levels: Sequence[float]
) -> float:
    """CRPS of an empirical reference distribution, computed by sampling its
    quantiles at `levels` and feeding crps_from_quantiles -- IDENTICAL
    estimator+tails to the forecast, so CRPSS is estimator-consistent."""

def coverage_hit(lower: float, upper: float, observed: float) -> float:
    """1.0 if lower <= observed <= upper else 0.0; NaN if a bound is NaN."""

def interval_width(lower: float, upper: float) -> float:
    """upper - lower; NaN if a bound is NaN."""

def rank_position(
    levels: Sequence[float], quantiles: Sequence[float], observed: float
) -> float:
    """Predictive-CDF value at `observed` by linear interpolation on the grid,
    clamped to [0,1]. Feeds the COARSE reliability/rank table (NOT a fine PIT
    histogram -- 5-7 nodes only). NaN if <2 nodes."""

def event_probability(
    levels: Sequence[float], quantiles: Sequence[float],
    threshold: float, direction: Literal["below", "above"],
) -> float:
    """P(X<threshold)/P(X>threshold) by CDF interpolation. Interior thresholds
    only (below_norm). NaN if <2 nodes."""

def brier_score(forecast_prob: float, observed_event: bool) -> float:
    """(forecast_prob - 1{event})^2. NaN if forecast_prob is NaN."""
```

### `prob_metrics.py` (new — reducer & reliability)

```python
def _score_pairs(pairs: pd.DataFrame, threshold: float) -> pd.DataFrame:
    """pairs + per-pair columns: crps, rank, hit_50/hit_90 (and hit_80 only
    where q10/q90 present, else NaN), width_iqr, width_outer, width_outer_norm,
    below_norm event prob. All-NaN-band rows -> NaN scores. Uses config
    `threshold` (NOT hardcoded 0.80) for the below_norm event, mirroring
    classifier.classify."""

def compute_probabilistic_metrics(
    pairs: pd.DataFrame,
    thresholds: dict,
    clim_ref: dict,           # precomputed per-group CRPS-clim (O(m^2) once)
    events_filter: tuple[str, ...],
) -> pd.DataFrame:
    """Score pairs, add below_norm Brier via event_probability, fold in
    climatology/persistence CRPS references (same estimator) for CRPSS, and
    aggregate (mean over pairs) across the SAME 8-key slice structure as
    count_contingencies (POOLED + per-code; per-lead for long-term).
    event='distribution' rows carry CRPS/coverage/sharpness/rank; event=
    'below_norm' rows carry Brier. NaN-by-row-type contract enforced.
    Ranking restricted to a single fc_grid_id downstream. Columns =
    PROB_METRIC_COLUMNS."""

def build_prob_reliability(pairs: pd.DataFrame) -> pd.DataFrame:
    """Long table: per group key x nominal_level in QUANTILE_LEVELS,
    observed_frequency = P(observed <= forecast quantile at that level) and n.
    Columns = PROB_RELIABILITY_COLUMNS. Coarse-resolution caveat documented."""
```

**Column constants** (Murphy decomposition dropped):

```python
PROB_METRIC_COLUMNS = (
    "horizon", "model", "regime", "season", "code", "basin",
    "norm_provenance", "lead", "event", "fc_grid_id", "n_pairs",
    "crps", "crps_clim", "crpss", "crps_persist", "crpss_persist",
    "coverage_50", "coverage_80", "coverage_90",
    "coverage_ci_lower", "coverage_ci_upper",
    "reliability_50", "reliability_80", "reliability_90",
    "nominal_50", "nominal_80", "nominal_90",
    "sharpness_iqr", "sharpness_width", "sharpness_width_norm",
    "rank_mean", "rank_var", "rank_calibration_error",
    "brier", "brier_ss",
)
PROB_RELIABILITY_COLUMNS = (
    "horizon", "model", "regime", "season", "code", "basin",
    "norm_provenance", "lead", "fc_grid_id", "nominal_level",
    "observed_frequency", "n",
)
```

Brier threshold source: `below_norm` → `config.threshold × pair.norm` (identical rule to `classifier.classify`; **not** a literal 0.80). Percentile-low/high and return-period events are **kept flag-only** this phase — their thresholds fall at/beyond the `[q05,q95]` band edges, so `event_probability` would saturate at 0/1 and degenerate to the yes/no flag (documented in Out of Scope).

### `orchestrator.py` (additive)

```python
@dataclass(frozen=True)
class ResultsBundle:
    pairs: pd.DataFrame
    contingency_metrics: pd.DataFrame
    baselines: pd.DataFrame
    exclusion_ledger: ExclusionLedger
    horizon_summary: tuple[HorizonCoverage, ...]
    # NEW -- defaulted so existing constructors keep working:
    prob_metrics: pd.DataFrame = field(
        default_factory=lambda: pd.DataFrame(columns=PROB_METRIC_COLUMNS))
    prob_reliability: pd.DataFrame = field(
        default_factory=lambda: pd.DataFrame(columns=PROB_RELIABILITY_COLUMNS))
```

Wiring inside `run` (after the three baselines, gated on the flag):

```python
if os.environ.get("SAPPHIRE_SKILL_PROB", "").lower() in {"1", "true"}:
    clim_ref = precompute_climatology_crps(all_pairs)   # O(m^2) once/group
    prob_metrics = compute_probabilistic_metrics(
        all_pairs, thresholds, clim_ref, config.events_filter)
    prob_reliability = build_prob_reliability(all_pairs)
    for grp in _bandless_groups(all_pairs):
        merged_ledger.add(stage="probabilistic", reason="no_quantile_band")
else:
    prob_metrics = pd.DataFrame(columns=PROB_METRIC_COLUMNS)
    prob_reliability = pd.DataFrame(columns=PROB_RELIABILITY_COLUMNS)
```

---

## Testing

Run: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_skill_eval`. All new fixtures use codes `19999`/`29999` and invented quantile vectors — **no real station codes or discharge values in any tracked file**.

**Scoring primitives (`tests/test_prob_metrics.py`):**
- `crps_from_quantiles`: (a) degenerate point band (all nodes equal) → `|q − obs|`; (b) a fixed symmetric band with obs at q50 → **the independently hand-computed trapezoidal-plus-tail value** (pin to the *approximation's own* deterministic result, NOT to the analytic true-CDF CRPS — they differ on a coarse grid); (c) <2 finite nodes → NaN; (d) **tail penalty**: obs far beyond q95 scores strictly worse than obs at q95, and a narrow over-confident band tail-misses worse than a wider calibrated band — locking in the proper-scoring property; (e) **bias bound**: grid-CRPS vs analytic CRPS of a known distribution stays within a stated tolerance.
- `crps_reference_from_samples`: identical estimator path — feeding a reference's own quantiles reproduces its grid-CRPS.
- **Estimator consistency:** a forecast whose band equals the climatology sample's quantiles → `crpss ≈ 0` (this is the test that would FAIL under mismatched estimators; it must pass).
- `isotonic_band`: crossing input (`q75 < q25`) → repaired non-decreasing + `was_repaired=True`; pair is scored, not nulled.
- `rank_position`: obs below q05 → 0.0; above q95 → 1.0; at q50 → ~0.5; single node → NaN.
- `coverage_hit` / `interval_width`: inside/on-boundary/outside; NaN bound → NaN.
- `event_probability`: interior `below_norm` threshold behaves; `P_below + P_above = 1` at the same threshold.
- `brier_score`: (1.0, True)→0; (0.0, True)→1; NaN prob→NaN.

**Reducer, reliability, baselines:**
- `_score_pairs`: all-NaN-band rows → NaN (excluded from means); short-grid rows (no q10/q90) → `coverage_80` NaN but `coverage_50/90` finite; uses `config.threshold` for `below_norm` (parametrise threshold≠0.8 and assert the event matches the contingency event).
- `compute_probabilistic_metrics`: group keys / POOLED / per-lead structure **matches `count_contingencies`** on the same synthetic pairs; NaN-by-row-type contract (distribution vs below_norm) holds; `crpss` sign correct.
- `build_prob_reliability`: perfectly calibrated synthetic ensemble → `observed_frequency ≈ nominal_level`; over-confident → systematic deviation.
- `prob_baselines`: climatology CRPS uses the **same conditioning set** as `baselines.build_climatology_baseline` (assert reference group keys align); persistence CRPS = `|lag1_obs − obs|` (degenerate zero-spread — asserted and documented).
- **Performance category test:** N synthetic groups × M-sample climatology completes under a wall-clock bound, proving the O(m²) term is precomputed once (guards against the `ef7975c6` regression class).

**Ingestion (`tests/test_api_readers.py`, `tests/test_pairs.py`):**
- Existing `_forecast` / `_lr_forecast` fixtures **extended with quantile columns** so ingestion exercises realistic rows.
- `select_quantile_band`: long row → 7 nodes + `grid_id="long7"`; short row → 5 nodes (q50 from `forecasted_discharge`) + `grid_id="short5"`; **LR-shaped row (`model="LR"`) → `{}` + `no_quantile_band` even if a stray `q` column is present**; NaN source node dropped; <2 nodes → empty.
- `_add_quantile_band`: adds `quantiles`/`quantiles_note`/`fc_grid_id`; empty frame → typed empty columns; **`point_value` and all existing columns unchanged**.
- `pairs`: `fc_q05…fc_q95` + `fc_grid_id` present; long pair 7 nodes, short pair 5 (q10/q90 NaN), LR all NaN; **existing `forecast_value`/`fc_class`/contingency columns and their values unchanged** (regression assertion — REQUIRED, see Risks).

**Dashboard loaders (P4):** `load_prob_metrics`/`load_reliability` read sibling CSV; missing file → empty framed DataFrame (old artifact dirs do not crash); `lead` coerced numeric; `event`/`fc_grid_id` synthesised if absent.

**Regression guard (REQUIRED before merge):** existing `test_e2e.py` and contingency/metrics/baselines tests pass unchanged. `pairs.csv` gains trailing `fc_*` columns but existing columns' **values** are identical — a targeted test asserts contingency/metrics/baselines output is unchanged (because `pd.DataFrame(rows, columns=PAIR_COLUMNS)` silently produces all-NaN `fc_*` if `_pair_row` forgets the keys). With `SAPPHIRE_SKILL_PROB` off, the two new frames are empty and no behaviour changes. Zero skips except the sanctioned `sapphire-api-client not installed` guard.

---

## Open questions / verify before implementing

These are the review-flagged uncertainties that P0 (and, where noted, implementation) MUST resolve rather than assume:

1. **Do short-term (`forecasts`) rows actually carry populated `q05,q25,q75,q95`, and for which `model_type`?** In-repo evidence says quantiles are frequently NULL for ensemble-mean/combined and non-quantile models. **Gate:** report and trust short-term probabilistic numbers only for `(horizon, model_type)` above an agreed populated-band fraction (P0 deliverable #2).
2. **Is short-term `forecasted_discharge` the median or a mean?** If a model writes a mean, inserting it as the q50 node can break monotonicity / bias CDF interpolation. Verify per model in P0; isotonic repair mitigates but does not license a wrong-node assumption.
3. **Does `long_forecasts` populate `q10`/`q90` in practice?** `coverage_80` depends on them; if sparsely populated, report `coverage_80` only where present.
4. **Exact conditioning set of `baselines.build_climatology_baseline`** (per-station-per-period vs pooled vs global) — the CRPS climatology reference MUST reuse it so the two "vs climatology" skill scores in the same report share a reference.
5. **Populated-band coverage on the live/dev DB** — since `sapphire_api_client` is not installed locally, P0 relies on the colleague / OpenAPI / a captured response. If none is available before implementation, short-term probabilistic reporting stays behind the flag and out of the report until confirmed.
6. **`fc_grid_id` taxonomy** — confirm no third short-term grid variant exists (e.g. models emitting only q25/q75) before hardcoding `short5`.

---

## Documentation Impact

- `doc/plans/working/forecast_skill_eval_report_draft.md` (P5): new `## Probabilistic forecast verification (predictive distribution)` section, with subsections — (1) **model coverage of quantile bands = the P0 populated-fraction result**, stating band-less/under-populated models are point-only and *excluded* (not zero-skill); (2) CRPS & CRPSS table **faceted by `fc_grid_id`** (never ranking raw CRPS across grids); (3) interval calibration/coverage with Wilson CI; (4) sharpness conditional on calibration; (5) the **coarse** rank/reliability table with an explicit 5–7-node resolution caveat (not framed as a fine PIT histogram); (6) `below_norm` Brier & Brier skill cross-referencing the binary tables, noting tail events are flag-only. Add `fig7_reliability.png`, `fig8_crpss.png`, `fig9_coverage.png`, `fig10_rank.png`; add `prob_metrics.csv` / `prob_reliability.csv` to Related documents; note the `SAPPHIRE_SKILL_PROB` flag.
- Module README / usage doc: new artifacts, the flag, and the Probabilistic dashboard view.
- Plan lifecycle: move `high_prio_gi_draft_forecast_skill_eval_probabilistic.md` → `review_gi_draft_*` when implemented.

---

## Out of Scope

- **LR Gaussian-derived quantiles** from `q_mean`/`q_std_sigma` — explicit opt-in for a later phase; LR stays point-only now.
- **Percentile-low/high and return-period Brier** — thresholds saturate at the band edges (`event_probability` → 0/1), degenerating to the yes/no flag; kept flag-only, documented.
- **Murphy Brier decomposition** (reliability/resolution/uncertainty) — dropped entirely from this phase (no present-but-NaN schema); a committed follow-up if wanted.
- **DRY extraction of the four slice helpers** from `contingency.py` into a shared `grouping.py` — first cut replicates them in `prob_metrics.py`; behaviour-preserving extraction is a stretch goal requiring byte-identical `count_contingencies` verification.
- **Wider (unclassifiable-inclusive) probabilistic pair set** — v1 shares the contingency gate; a norm-independent wider set is a follow-up.
- Any change to `sapphire/services/` (colleague-managed) or to DB write paths.

---

## Dependencies

- **No new hard runtime dependency.** CRPS-from-quantiles (+ tails), coverage, rank, and Brier are implemented with `numpy`/`pandas` (already present). `properscoring` / `scoringrules` were considered and **rejected** to keep the read-only evaluator lightweight and avoid supply-chain/vuln surface; the grid estimator is standard and unit-tested against hand-computed and bounded-analytic cases.
- Reuse `metrics._wilson_interval` for coverage CIs.
- Reuse `baselines._build_obs_lookup` / `_lag1_key` and the `build_climatology_baseline` conditioning for the CRPS references.
- Depends on Phase-2 code already on `develop_forecast_skill_eval_phase2` (same PR for P1–P3).
- P0 VERIFY depends on live/dev-DB or OpenAPI access (locally dependency-gated → design tolerates absent bands via NaN, but short-term reporting is gated on confirmation).

---

## Acceptance Criteria

1. `all_pairs` carries `fc_q05…fc_q95` + `fc_grid_id`; long pairs populate 7 nodes, short pairs 5 (q10/q90 NaN), LR all NaN. **Existing pair columns and their values are unchanged; `pairs.csv` gains only trailing optional columns.** Contingency/metrics/baselines outputs are unchanged (regression test green).
2. With `SAPPHIRE_SKILL_PROB` enabled, `prob_metrics.csv/.parquet` and `prob_reliability.csv/.parquet` are written under `output_dir/run_id`, keyed by the same 8 group columns as `contingency_metrics` (POOLED + per-station) plus `fc_grid_id`, with the listed columns. With the flag off, the frames are empty and no behaviour changes.
3. CRPS (with tail treatment), estimator-consistent CRPSS-vs-climatology, coverage + reliability + Wilson CI at supported levels, sharpness (raw + norm), coarse rank stats, and `below_norm` Brier + Brier skill are computed and correct on synthetic fixtures — including the **estimator-consistency test** (`climatology-equal forecast → crpss ≈ 0`) and the **tail-penalty test** (over-confident tail-miss scores worse).
4. Raw `crps` is never ranked across `fc_grid_id`; the dashboard/report restrict CRPSS comparison to a single grid.
5. Band-less / LR groups are logged to the exclusion ledger with reason `no_quantile_band` and produce no probabilistic rows; quantile crossings are isotonic-repaired and counted, not silently nulled.
6. **P0 populated-band gate satisfied:** the report's short-term probabilistic numbers cover only `(horizon, model_type)` combos confirmed above the agreed populated-fraction threshold; under-populated combos are declared point-only.
7. A **Probabilistic** dashboard view (P4) renders reliability (1:1 diagonal), coverage bars with nominal-target rules, grid-scoped CRPSS ranking with the beats-climatology zero line, sharpness-vs-calibration scatter, and `below_norm` Brier-skill bars — all from the two CSVs; old artifact dirs without them do not crash the dashboard.
8. Report draft (P5) gains the probabilistic section, figures, and related-document entries, including the populated-band coverage statement and the coarse-rank caveat.
9. Every new function has unit tests (including a performance-category test for the O(m²) climatology term); all fixtures use `19999`/`29999`; no real station codes or discharge values in any tracked file; no DB writes.
10. `SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_skill_eval` passes with zero failures and zero unexpected skips (only the sanctioned `sapphire-api-client not installed` guard may skip).
