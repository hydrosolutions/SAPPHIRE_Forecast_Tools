# High priority: monthly long-term forecasts — `horizon_value` carries three meanings, and the dashboard hard-codes lead 1

**Status:** Draft, revision 6 — diagnosed read-only 2026-07-10/11 against the local dev DB and the
Tajik (tjhm) production postprocessing DB. Revisions 2–5 incorporate **four** rounds of adversarial
diagnosis review; revision 6 adds an **implementation-readiness** round (blast-radius inventory +
`codex exec`). **Diagnosis: settled. Implementation readiness: NOT yet — see "Implementation
readiness" and its must-fix gaps.** Every reviewer claim was re-verified by the author; failures in
both directions are in "Disputed reviewer claims." No code written.

**Modules:** `forecast_dashboard`, `postprocessing_forecasts`, `long_term_forecasting`,
`iEasyHydroForecast`, `forecast_skill_eval` (+ read-only observations in `bin/utils/migration_py`).

---

## Summary

Tajik operational hydrologists issue a monthly runoff forecast on the 1st of the month, **for the
month they are standing in**. The dashboard shows them **next month's** forecast instead.

The reported symptom has one sufficient cause (**A**). Beneath it sit eight further defects, four of
which were found only by the adversarial review. One of them (**F**) is a **live bug affecting the
currently-deployed Kyrgyz service**, unrelated to Tajik.

| ID | Defect | Where | Severity | Evidence |
|----|--------|-------|----------|----------|
| **A** | `month_N` config means lead `N-1` for tjhm, lead `N` for kghm; dashboard hard-codes lead 1 | `forecast_dashboard` | Causes the reported symptom | prod + local |
| **C** | `horizon_value` overloaded — **month, quarter AND season** all mix lead / period-number / schema default | `postprocessing_forecasts` | Corrupts history, skill merge, aggregates | prod-observed + local |
| **F** | `month_0` card merges the **main panel's** skill-stats frame into a **lead-0** forecast frame | `forecast_dashboard` | kghm defect (see conditional note) | code + local |
| **H** | Monthly→quarterly aggregation groups without `horizon_value`, blending leads | `postprocessing_forecasts` | 65,769 / 72,431 local quarter keys mix leads | code + local |
| **G** | `month_0` bulletin hydrates from the **main-panel** context, which assumes lead 1 | `forecast_dashboard` | Wrong norms / month length | code |
| **J** | `widgets.format_horizon_info` hard-codes lead 1 **and** takes the year from the production date | `forecast_dashboard` | Visible header shows wrong month + wrong year | code |
| **I** | `forecast_skill_eval` trusts month `horizon_value` as a lead; derives only for quarter/season | `forecast_skill_eval` | Consumes polluted rows | code |
| **D** | 32-month hole in the Tajik monthly series (all of 2024 + 2025) | data / ops | Skill built from 2016–2023 only | prod-observed |
| **E** | The one operational run lacks all ensemble aggregates and `MC_ALD` at leads 1–2 | `postprocessing_forecasts` | Dashboard shows no ensemble row | prod-observed |
| **B** | ~480k local hindcast rows carry raw, un-snapped `valid_from` | local dev DB **only** | Not on prod; low urgency | local |

**Three independent sites hard-code a lead of 1** for the monthly caption/header/bulletin:
`plot_manager.py:32`, `widgets.py:625`, and `bulletin_manager._month_hydration_params` (:374-384,
whose docstring asserts the assumption outright). A fix that changes only one of them leaves the
dashboard internally inconsistent.

**Evidence provenance.** Claims marked *prod-observed* were seen once on the production DB and
**cannot be re-verified from a development machine**. Claims marked *local* were independently
re-run and confirmed by the adversarial reviewer. Claims marked *code* are verifiable by reading.

---

## Reported symptoms

1. **Wrong target month shown** on the Tajik monthly dashboard view. → Defects **A** and **J**.
2. **Bulletin month/year wrong.** → Defects **A**, **G**, **J**, and the `forecast_year` bug below.

*Scope note (corrected in revision 3).* Revision 2 narrowed this symptom too far. It is true that
`resolve_bulletin_header_date()` (`bulletin_manager.py:57-68`) derives the bulletin's own month from
`valid_from`. But the **horizon info pane** does not — `widgets.format_horizon_info` (:581-640)
computes its own target month and year (Defect **J**). So a wrong month/year *is* visible, in
addition to the wrong **saved** bulletin key and the wrong **hydration** parameters (norm, month
length).

---

## Background: where target month start/end is actually derived

Exactly **one** derivation for the monthly horizon, and it is **organization-agnostic**:

`apps/long_term_forecasting/post_process_lt_forecast.py`, `map_forecasted_period_to_calendar_month`

```
:451  target_month = (issue_month + operational_month_lead_time - 1) % 12 + 1
:469  target_year  = issue_year + (target_month < issue_month)
:483  valid_from   = first day of (target_year, target_month)
:490  valid_to     = valid_from + pd.offsets.MonthEnd(0)
```

It consumes the issue **month** and `operational_month_lead_time`. The issue **day** never enters.
Nothing branches on organization. Called unconditionally from `run_forecast.py:395`, no fallback.

Upstream, the installed `lt_forecasting` library emits a **day-count** period
(`valid_from = today + 1 + max(offset - prediction_horizon, 0)`, `valid_to = valid_from + prediction_horizon`).
For monthly modes that raw period is overwritten by the snap (`calendar_month_adjustment` defaults
`True`). For `quarter`/`season` it is `false`; `adjust_forecast_dates_dynamic` /
`adjust_forecast_dates_only` reshape it instead.

Downstream, the record payload stamps **`horizon_value` at `apps/long_term_forecasting/lt_utils.py:350`**
(inside `prepare_long_forecast_records`, :274). `run_forecast.py:409,418` only *passes* the configured
lead into that call — it does not stamp it.

**The DB is keyed on lead, not on mode name.**

---

## Defect A — `month_N` means different leads per organization

Per-mode config JSON at `<config_root>/long_term_configs/<mode>.json`, resolved by
`apps/long_term_forecasting/config_forecast.py:57-68`.

| mode file | tjhm lead | kghm lead |
|---|---|---|
| `month_0.json` | *(absent)* | 0 |
| `month_1.json` | **0** | 1 |
| `month_2.json` | **1** | 2 |
| `month_3.json` | **2** | 3 |

`operational_issue_day`: tjhm = 1 for all monthly modes; kghm = 10 (`month_0`) / 25 (`month_1..3`).

Both conventions are internally coherent — *"the forecast I make on the 1st is for the month I am
standing in"* vs *"the forecast I make on the 25th is for next month."* But **tjhm's `month_1.json`
is semantically kghm's `month_0.json`.**

The dashboard reads mode **names**, not leads:

- `apps/forecast_dashboard/src/db.py:852` — main panel filtered to `get_long_forecasts(station, horizon_value=1)`
- `apps/forecast_dashboard/src/db.py:859-863` — skill tiles filtered to `_op_lead = 1`
- `apps/forecast_dashboard/src/db.py:918` — lead-0 panel rendered only `if "month_0" in supported_modes`
- `apps/forecast_dashboard/dashboard/plot_manager.py:31-34` — caption hard-codes `month_1 → target = issue_month + 1`
- `apps/forecast_dashboard/dashboard/plot_manager.py:301` — m0 visibility gate uses `(max_date.month % 12) + 1`

Tajik's `ieasyhydroforecast_ml_long_term_supported_modes` is
`seasonal_april,month_1,month_2,month_3,quarter` — **no `month_0`**. Therefore Tajik's flagship
lead-0 forecast lands at `horizon_value=0`, is excluded by the `horizon_value=1` filter, and its
panel is gated off. The main panel instead serves `horizon_value=1`, which for Tajik is `month_2`
(lead 1).

**Precision note.** "Tajik lead-0 forecasts are never displayed" is true of *ordinary* lead-0 rows.
It is **not** universally true: per Defect C, some genuinely-lead-0 aggregate rows were written with
`horizon_value=1` and therefore *do* reach the panel — as pollution, not as the intended product.

**Bulletin target-year bug (org-independent, pre-existing).**
`apps/forecast_dashboard/dashboard/data_manager.py:333-361` sets `last_date = max_issue_date + 1 day`
and returns `forecast_year = last_date.year`. For any lead ≥ 1 issued in December the target month is
January of the *following* year, so the saved bulletin year is wrong. `post_process_lt_forecast.py:469`
rolls the year correctly; the bulletin does not.

---

## Defect F — the `month_0` card is annotated with the main panel's skill stats

`apps/forecast_dashboard/src/db.py`:

```python
:859  if not forecast_stats.empty and "horizon_value" in forecast_stats.columns:
:860      _op_lead = 1
:861      _op_mask = forecast_stats["horizon_value"] == _op_lead
:862      if _op_mask.any():                       # <-- CONDITIONAL
:863          forecast_stats = forecast_stats[_op_mask].copy()
...
:919  m0 = ... get_long_forecasts(station, horizon_value=0) ...
:927  m0 = m0.merge(forecast_stats, on=merge_keys, how="left", ...)
```

`forecast_stats` is **not rebound** between :863 and :927. Two failure modes, and the branch decides
which:

- **Lead-1 stats exist** (`_op_mask.any()` true) → the lead-0 `m0` frame is annotated with **lead-1**
  skill metrics.
- **Lead-1 stats absent** → `forecast_stats` stays **unfiltered**, and `m0` merges stats from **all**
  leads indiscriminately.

Neither is correct. *Revision 3 correction: revision 2 claimed the filter always applies. It does
not — the `_op_mask.any()` guard makes this conditional, and the fix must handle both branches.*

Not a no-op: locally, the latest `m0` rows number 467, of which 450 match a lead-1 stats row, and
every one of the 60 stations with latest-`m0` rows has at least one lead-1 stats match.

*Severity:* code-confirmed and locally reproduced. **Not** independently verified on the kghm
production deployment — treat "live in production kghm" as probable, not established.

Any fix to A that resolves a single "operational lead" and reuses one `forecast_stats` frame for
both cards **perpetuates this defect**. Skill stats must be filtered **per displayed lead**, with an
explicit decision about what to render when a lead has no stats (blank, not another lead's).

---

## Defect G — `month_0` bulletin hydrates from main-panel context

`apps/forecast_dashboard/dashboard/bulletin_manager.py`:

- `_horizon_context()` (:306-310) returns `wm.forecast_year` / `wm.forecast_horizon`, populated from
  the **main** panel's `get_bulletin_metadata()`.
- `_on_add_m0()` (:487-544) hydrates the m0 bulletin through `_month_hydration_params()` (:374-384),
  which calls `dm.get_bulletin_metadata("month")` — i.e. the **main-panel** frame, not the m0 frame.
- `_month_hydration_params`'s own docstring states the assumption outright: *"The monthly forecast
  targets the month AFTER it is issued."* That is a **hard-coded lead-1 belief** driving the norm
  lookup and `calendar.monthrange(...)` month length.

---

## Defect J — the visible horizon header hard-codes lead 1 and the wrong year

`apps/forecast_dashboard/dashboard/widgets.py:581-640`, `format_horizon_info`:

```python
:603  production_date = last_date - _dt.timedelta(days=1)
...
:625  target_month_num = (production_date.month % 12) + 1    # hard-codes lead 1
:626  body = _("month: %(month)s %(year)s") % {
:627      "month": month_name(target_month_num, "nominative"),
:628      "year": production_date.year,                       # issue year, not target year
:629  }
```

Two bugs in one expression. The target month assumes a lead of exactly 1 (wrong for tjhm, where the
operational lead is 0). The year comes from the **production date**, so a December-issued lead-1
forecast targeting January renders the **previous** year.

This is the **third** hard-coded lead-1 site, and the only one that is directly visible in the
header pane. Missed by revision 1, revision 2, and adversarial review round 1.

---

## Defect C — `horizon_value` is overloaded across **all three** long-term horizons

### Month

`apps/postprocessing_forecasts/src/api_writer.py:877-881`, `_write_monthly_ensemble_to_api`:

```python
"horizon_value": (
    int(row["horizon_value"])
    if "horizon_value" in row.index and pd.notna(row.get("horizon_value"))
    else month          # <-- calendar month number, NOT a lead
),
```

When the aggregate writer's input frame lacks a `horizon_value` column it stamps the **calendar
month**. `long_forecasts.horizon_value` additionally has `DEFAULT 0` in the service schema, so
writers that omit it produce a third meaning.

### Quarter and season — affected, but current-code vs data-state must be separated

Revision 2 called quarter/season "unaffected" and asked only for an audit. That was wrong — they are
affected. But revision 3's first cut over-claimed; revision 4 corrects it after reading the current
writer (`_write_aggregated_forecasts_to_api`, `api_writer.py:976`, :1066-1090):

- **Quarter, current code (`:1074`):** `horizon_value = quarter_horizon_value()` — the **config lead**
  (`operational_month_lead_time`), *not* quarter-in-year. And `date = valid_from` (`:1098`, no season
  override), so the record's `date` is the **target start, not an issue date**.
- **Season, current code (`:1090`):** `horizon_value = int(row["season_in_year"])`, and `season_in_year`
  is itself **derived from the API `horizon_value` (the lead)** upstream at `data_reader.py:3143`
  (with `horizon_value` deliberately *retained* for season at `:3164-3165`). So this line
  **round-trips the lead** — it is **not** a period-number defect. *(Revision 5 correction: revision 4
  called this a live current-code season defect stamping a period number. That was wrong — the same
  over-claim class that review round 3 caught on quarter. The season current writer is lead-correct;
  the season **data** pollution below has a different, still-unattributed origin.)*

**Important methodology correction.** The classifier
`observed_lead = mod(month(valid_from) - month(date) + 12, 12)` is **invalid for quarter**: because
the quarter writer sets `date = valid_from`, observed lead is `0` *tautologically*, not because of a
lead. So the quarter rows below cannot be read as "lead 0"; the mod column is meaningless there. It
is also unreliable for season wherever `date` is not a true issue date.

Local `flag=0` counts (reproduced). **The `hv` buckets are internally mixed** — the same `hv` value
holds both current-writer lead rows and older data-state rows — so read this as "these buckets need
disambiguation," not as a clean per-meaning split:

| horizon | org | `hv` | rows | reading |
|---|---|---|---|---|
| QUARTER | kyg | 0 | 359 | small residual bucket |
| QUARTER | kyg | 1 | 29,434 | **mixed**: current config lead (kghm quarter lead = 1) *and* `hv = target-quarter` rows |
| QUARTER | kyg | 2 / 3 / 4 | 11,162 / 11,195 / 10,988 | `hv = target quarter`; **cannot** come from `quarter_horizon_value()` (single value) → **old-writer data-state pollution** |
| QUARTER | taj | 0 / 1 | 4,941 / 3,105 | mix of current config lead (0) and older rows |
| SEASON | kyg | 1 | 9,388 | `hv=1` collides genuine lead-1 (4,526) with other rows (4,862) |
| SEASON | kyg | 2 / 3 | 4,106 / 4,183 | genuine leads |
| (small taj + "other"-org quarter/season buckets omitted for brevity) | | | | present but low-count |

So there are (at least) **two** distinct problems the plan must not conflate: (i) **historical
data-state** pollution in the quarter rows (`hv=2,3,4` = target-quarter, from an older writer —
inferred, since the DB carries no writer-version provenance); and (ii) **month** current-code
pollution (`:877-881`) plus month data. The season *current writer* is **not** in this list; the
season *data* mixing at `hv=1` is real but its cause is **open (question 9)**.

**Consequence for the plan:** P0 must define `horizon_value`, `date`, and `horizon_in_year` semantics
for **month, quarter and season** — not month alone. `_resolve_seasonal_horizon_value`
(`db.py:96-102`, used `:706`) and `get_long_forecasts_season()` (which derives `season_in_year` from
`horizon_value`) both select against the season column, whose data is mixed today.

---

## Defect H — monthly→quarterly aggregation blends leads

`apps/postprocessing_forecasts/src/aggregation.py:251-252`, `aggregate_monthly_fc_to_quarterly()`
groups monthly rows by `["code", "year", "quarter_in_year", "model_short"]` — **`horizon_value` is
not a grouping key**. The read side does not partition by lead either:
`read_quarterly_forecasts()` calls `read_monthly_forecasts()` without `horizon_value`
(`data_reader.py:2682-2685`), and `_read_long_forecasts_api()` filters on `horizon_value` only when
one is supplied (`:1100-1102`).

A quarterly value can therefore average a lead-0 January with a lead-2 March. **Locally, 65,769 of
72,431 quarterly grouping keys contain more than one lead.** This is not theoretical.

---

## Defect I — `forecast_skill_eval` trusts month `horizon_value`

`apps/forecast_skill_eval/src/forecast_skill_eval/pairs.py:35`:

```python
LONG_TERM_DERIVE_LEAD_HORIZONS: tuple[str, ...] = ("quarter", "season")
```

Lead is derived only for quarter and season (`pairs.py:250`, `:513-531`). For `month`, the stored
`horizon_value` is taken as the lead (`pairs.py:487`). Month-number-polluted rows (Defect C) are
therefore consumed as if a July row were a 7-month-lead forecast. The module will not repair them
and will not flag them; no month validation path exists.

**"Just add `month` to the tuple" is not a drop-in fix.** `_dedup_long_term()` sets
`include_lead_in_key = horizon == "season"` (`pairs.py:312`), so the dedup key for month is
`(code, period_key, year, model)` with **no lead component**. Adding month to the derive-lead
horizons without also revisiting that key risks collapsing genuine per-lead month rows into one
winner. Whether month rows *already* collapse across leads in dedup is **unverified** — see open
question 8.

---

## Evidence

Aggregate counts only. No station codes, no discharge values.
`horizontype` enum labels are UPPERCASE (`'MONTH'`).

### Production (tjhm postprocessing DB, 2026-07-10) — *observed once; not re-verifiable from dev*

**Snapping — clean.** 13,148 `flag=0` + 3,466 `flag=2` rows, **100% snapped**. **Zero `flag=1` rows.**
Zero un-snapped rows. → Defect B is *not* present on production.

**`horizon_value` vs observed lead** (`flag=0`; sums to 13,148):

| `horizon_value` | observed lead | rows | interpretation |
|---|---|---|---|
| 0 | 0 | 11,082 | schema `DEFAULT 0` + genuine lead-0 |
| 1 | 0 | 110 | calendar month = January |
| 1 | 1 | 157 | genuine lead |
| 2 | 0 | 157 | calendar month = February |
| 2 | 2 | 128 | genuine lead |
| 3..12 | 0 | 1,514 | calendar month |

A `horizon_value=7` row with observed lead 0 cannot be a 7-month-ahead forecast.
Polluted rows: 110 + 157 + 1,514 = **1,781**.

**Who wrote them:** every `horizon_value >= 3` row is exclusively `ENSEMBLE_MEAN`, `SKILLED_MEAN`, or
`NAIVE_MEAN`, each bucket containing only rows issued in that calendar month (`hv=3` → March,
`hv=7` → July), spanning 2016–2023. Consistent with `api_writer.py:877-881`.

**The `hv=0` bucket** (sums to 11,082): **8,529 base-model rows** (`GBT`, `LR_BASE`, `LR_SM`,
`LR_SM_DT`, `LR_SM_ROF`, `MC_ALD`, `SM_GBT`, `SM_GBT_LR`, `SM_GBT_NORM`), 2016-01-01 → 2026-07-01,
all observed lead 0; plus **2,553 aggregate rows** (`ENSEMBLE_MEAN` 287, `SKILLED_MEAN` 1,043,
`NAIVE_MEAN` 1,223), ending 2023-09/2023-10. So the aggregate writer produced rows **both ways**.

**Defect D.** Monthly rows exist for 2016–2023 (12 issue dates/yr; 2023 stops in October), then
**2024 and 2025 are entirely empty**, then a **single** issue date: 2026-07-01. A backfill ending
2023-10 (exactly where aggregate rows die) plus an operational start in 2026-07 — not the failure of
a previously-working pipeline. Gap = 32 months.

**Defect E.** The 2026-07-01 run wrote all three leads for base models (17 stations LR family, 15 GBT
family; counts sum to 394 = 392 `flag=0` + 2 `flag=2`). But `MC_ALD` produced **only `hv=0`**
(11 stations), and there are **no `ENSEMBLE_MEAN` / `SKILLED_MEAN` / `NAIVE_MEAN` rows at all**.

### Local dev DB (holds both orgs) — *independently re-run and confirmed by the reviewer*

- Tajik hindcast (`flag=1`, 522,488 rows): **8.0%** snapped. Kyrgyz (504,972): **96.4%**.
- Tajik `flag=0`: **33.9%** snapped overall; **100% from issue month 2026-03 onward**.
- Un-snapped rows are exactly the raw model period: lead 0 → `valid_from = date + 1`; lead 1 → `+31 d`;
  lead 2 → `+61 d`; all length 30 d — matching tjhm `offset` 30/60/90.
- Ruled out: duplicate writes (**0** tjhm keys with >1 `valid_from`); per-station effect
  (18 stations: 16 mixed, 1 snapped-only, 1 un-snapped-only).
- Day-1 issues, **tjhm `flag=0` only**: **40.4%** snapped across all dates (13,114 rows);
  **48.0%** when further restricted to `date >= '2025-01-01'` (955 rows).
  *(Revision 1 quoted 48% with no scope; revision 2 added the date filter; revision 3 adds the
  `flag=0` + tjhm scope. Both percentages are correct under their own scope.)*
- Local `skill_metrics`, tjhm month lead-0: 2,085 rows, 17 stations, 12 models, avg `n_pairs` 74.2.
- **`horizon_value` ≠ observed lead, month rows, `date >= 2024-01-01`: 234 rows, max date
  2025-12-01, and every one of them is `flag=1` *and* un-snapped. `flag=0` mismatches after
  2024-01-01: zero.** These are **Defect B** artifacts — a raw `valid_from` makes the observed lead
  disagree with `horizon_value` without any writer having mis-tagged anything — and must **not** be
  read as Defect C pollution. *(Adversarial review round 2 reported 983 such rows with max date
  2025-12-25 and used them to refute the P1 sequencing rationale. That figure did not reproduce;
  see "Disputed reviewer claims" below.)*
- **Local-only anomaly:** 1,543 monthly rows at `horizon_value=3` on a single Tajik-prefixed station,
  mostly hindcast, with kghm-style issue days (16/17/21/24/25) — despite tjhm config topping out at
  lead 2. One station appears to have been processed under Kyrgyz config. *(Observed during
  diagnosis; omitted from revision 1.)*

**Defect B origin.** The two CSV importers copy `valid_from` / `valid_to` / `flag` **verbatim**:
`bin/utils/migration_py/long_forecast.py:334-354` (in `_build_record`, defined :310) and
`sapphire/services/postprocessing/app/data_migrator.py:768-776` (in `prepare_month_data`, defined
:730 — **colleague-owned, do not edit**). Neither re-snaps.

*Inference, not proof:* the local source CSVs appear to predate the calendar-snap while the
production ones did not. The CSVs are not on this machine; this is deduced from the DB state and has
not been demonstrated directly.

**B does not corrupt month attribution for tjhm.** Because tjhm issues on day 1,
`valid_from = date + 1 + 30k` still falls inside the correct target month, so `data_reader`
(`month = valid_from.dt.month`) pairs correctly and local `skill_metrics` are healthy. The latent
hazard is for **kghm**: a raw lead-1 row for a day-25 issue lands `valid_from` = Jan 31, attributed to
January though the target is February. That is *why* the snap exists.

B's residual cost is `forecast_skill_eval/.../periods.py:73-79` (`_month_period`), which requires
`start.day == 1` and `end` = month end; raw rows fail its alignment check.

---

## Proposed fix

### 1. Resolve the operational lead from config, per displayed panel (Defects A + F) — **primary**

`apps/iEasyHydroForecast/long_term_horizon_resolver.py` exposes a generic
`_horizon_value_for_mode(config_name)` (:57) reading `operational_month_lead_time` from the per-mode
JSON. **Caution:** `_ensure_supported_mode` (:76-82) **raises** when the mode is absent from
`supported_modes` — so `month_0` must be membership-checked *before* the resolver is called under tjhm.

- Add a **public** `month_horizon_value(mode)` helper; do not reach for the private function.
- Main panel lead = `month_horizon_value("month_1")`; secondary panel lead =
  `month_horizon_value("month_0")`, guarded by `month_0 ∈ supported_modes`.
- **Filter `forecast_stats` separately for each displayed lead** (fixes F). Do not share one frame.
- `plot_manager._format_forecast_info` takes the **resolved lead as an int** and computes both the
  target **month and year** (`issue + lead`, with rollover) — not `issue_month + 1`.

Reproduces kghm's main/m0 leads (1 and 0) while correcting F. For tjhm: main = lead 0, no second card.

**Pre-flight (blocking):** confirm the dashboard process actually has
`ieasyhydroforecast_configuration_path` and `ieasyhydroforecast_ml_long_term_configuration` set, and
that the config directory is mounted into the dashboard container. If it is not, this approach is
unimplementable as written and must be redesigned.

### 2. Bulletin target year and m0 context (Defects A, G)

- `get_bulletin_metadata` must derive `forecast_year` from the **target** period (`valid_from`).
- `_on_add_m0` / `_month_hydration_params` must take the m0 frame's own target month/year, not the
  main panel's. Remove the hard-coded *"targets the month AFTER it is issued"* assumption.

### 3. Stop the `horizon_value` month fallback (Defect C)

`api_writer.py:877-881` must never fall back to `month`. Options (decide in P0):
**(a)** raise when absent; **(b)** resolve the lead from config; **(c)** guarantee upstream carries the
column, and assert. Preference: **(c) + (a)**. Silent synthesis created this defect.

Audit the sibling `_write_aggregated_forecasts_to_api` (`api_writer.py:976`) for the same shape.

### 4. Partition quarterly aggregation by lead (Defect H)

Add `horizon_value` to the grouping keys in `aggregation.py:251` and the corresponding read path,
or explicitly document and enforce a single-lead input contract.

### 5. `forecast_skill_eval` month-lead handling (Defect I)

Either add `month` to `LONG_TERM_DERIVE_LEAD_HORIZONS`, or validate stored month `horizon_value`
against the observed lead and route mismatches to the ledger rather than silently trusting them.

### 6. Re-tag the 1,781 polluted production rows (Defect C, data) — **gated**

Blocked on open question 1 *and* on fix 3 landing first.

### 7. Missing aggregates and `MC_ALD` leads in the 2026-07-01 run (Defect E)

Diagnostic. Root cause unknown — **do not assume it is Defect C.**

### 8. Backfill 2023-11 → 2026-06 (Defect D) — ops, not code

See `doc/prod/historical_backfill_runbook.md`.

### 9. Local-only: re-snap or re-import the raw `flag=1` rows (Defect B). Low priority.

### 10. Minor, found en route

`kghm`'s `supported_modes` advertises `monthly` and `month_4`–`month_9` with no matching JSONs;
requesting one passes the assert (`config_forecast.py:58`) then dies on `open()` (:65). Trim the env
var or add startup validation.

---

## Ownership / coordination

- `sapphire/services/postprocessing/app/data_migrator.py` and the `long_forecasts` schema
  (incl. `horizon_value DEFAULT 0`) are **colleague-managed** — do not edit.
- **P6 (production re-tagging) and any schema/default change require a named owner and an explicit
  handoff**, not a note to "coordinate." Nominate the owner in P0.
- Everything else in `apps/` and `bin/` is in scope.

---

## Implementation readiness (revision 6 — after an implementation-focused review round)

The diagnosis is settled. This section addresses whether the plan is *buildable safely*. It was
produced by a fifth review pass (blast-radius inventory + `codex exec`, both read-only) that asked:
what must we check against, what is the target DB state, what tests come before code, and how do we
avoid breaking working flows or adding new errors. **Verdict of that round: NOT yet
implementation-ready.** The gaps below are must-fix before P1a.

### 1. Target database state (must be ratified in P0, with the colleague)

`long_forecasts` — the target contract for **all three** long-term horizons:

| field | meaning | current violation |
|---|---|---|
| `horizon_value` | lead in whole months, `(year(valid_from)-year(date))*12 + month(valid_from)-month(date)` | month `else month`; season `season_in_year`; quarter old-writer quarter-in-year |
| `date` | **issue date** | quarter writer sets `date = valid_from` (`api_writer.py:1098`) — destroys the issue date |
| `valid_from` / `valid_to` | first/last day of target period | ok (snap) |
| period-in-year | **derived from `valid_from`**, never from `horizon_value` | `db.py:735`, `data_reader.py:3143` derive `season_in_year` from `horizon_value` |

`skill_metrics` — `horizon_value` = lead, `horizon_in_year` = period-in-year (two orthogonal axes,
both legitimately stored).

**The season conflation is a contract decision, not a bug to fix unilaterally.** There is one season
per year (Apr–Sep), so `season_in_year` is degenerate (`aggregation.py:198` hard-sets `1`); the teams
repurposed it / `horizon_value` to carry the **issue lead** (`skill_metrics.py:2110`: "season_in_year
carries the issue lead"). So `db.py:735` may be intentional. P0 must decide whether season keeps that
convention or moves to lead-in-`horizon_value` + `season_in_year=1`, **with the colleague**, because
it changes the shared read/write contract.

### 2. Blast radius — consumers the phase file-lists currently OMIT

Editing a writer without its readers is unsafe. Beyond the 8 files the phases name, these long-term
`horizon_value` consumers must be in scope or explicitly ruled out:

- **A second month writer:** `sapphire/services/postprocessing/app/data_migrator.py:620-672,
  760-770, 1072-1103` (config lead), **colleague-owned**, parallel to `api_writer.py`. Any retag or
  contract change must cover it.
- **Skill producers:** `postprocessing_forecasts/src/skill_metrics.py:1180-1683` (writes per-lead
  `horizon_value`), `ensemble_calculator.py:278-405,527-562` (groups on it).
- **READ-DERIVE (period/lead reverse-engineered from `horizon_value` — the dangerous class):**
  `forecast_dashboard/src/db.py:735`, `postprocessing_forecasts/src/data_reader.py:3143-3147`,
  `forecast_skill_eval/src/forecast_skill_eval/pairs.py:487`.
- **Read-filters that pin a lead:** `long_term_forecasting/data_interface.py` (`WHERE
  horizon_value=1` dependency pin), `dashboard/utils.py`, `dashboard/src/db.py` all-station stats
  dedup, `validate_pipeline/validate_pipeline.py:511-601`.
- **Recalc/nightly:** `postprocessing_operational_long_term.py`, `postprocessing_maintenance_long_term.py`,
  `recalculate_skill_metrics.py` (season `horizon_value=issue_lead`).
- **Migration/import cutoff map:** `bin/utils/migration_py/long_forecast.py:210-275` keys by
  `(horizon_type, horizon_value, code)`.

`forecast_skill_eval` should be treated as the **parity oracle** (it derives lead from the issue date
at `pairs.py:513-560`), not silently re-defaulted — changing its defaults alters already-published
skill artifacts. This reframes Defect I's fix: prefer fixing upstream writers so the stored value is
correct, over changing the oracle. (See P4.)

### 3. Retag feasibility — a hard blocker for quarter (verified on local DB)

Simulating "set `horizon_value := derived lead" and checking the unique key
`(horizon_type, horizon_value, code, date, model_type, valid_from, valid_to)`:

| horizon | rows | would collide | why |
|---|---|---|---|
| QUARTER | 97,462 | **15,935** | writer set `date = valid_from`, so derived lead ≡ 0 for all current-writer rows → they collapse and collide. **The true lead is unrecoverable from the data** — issue date was destroyed. |
| MONTH | (prod) | tbd | run on prod; local month pollution is the raw-period kind, not month-numbered |

**Consequence:** the quarter remediation in P6 **cannot be an in-place UPDATE**. It must re-derive
lead from config (which seasonal/quarter issue config produced the row) or **re-run** the aggregation,
not flip a column. This also means P3 (adding lead as a grouping key) needs the issue date present,
which the current quarter writer does not store — so P3 depends on fixing `date` at write time first.

### 4. Pre-code tests (TDD — write these before touching code)

**Characterization / golden (lock current-correct behaviour so a refactor that changes it fails):**
kghm monthly lead-1 main tile stays one row; kghm m0 uses lead-0 skill; quarter period comes from
`valid_from`; a single-lead quarter/season metric is unchanged by the refactor. Fixtures `17999`/`15999`.

**Regression (fail now, pass after):** the six per-defect cases already listed, **plus**
`quarter_normalizer_preserves_issue_date_and_derives_lead` (issue 2025-03-01, valid 2025-04..06 →
`quarter_in_year=2`, lead 1); `season leads {3,2,1,0} for Jan/Feb/Mar/Apr issues → season_in_year=1`;
`two leads, same quarter period, do not pool`; `aggregated CRPS records include horizon_value`;
`ensemble skill merge uses horizon_value` (lead-0 skilled + lead-1 unskilled must not synthesize a
lead-1 SM from lead-0 skill); `validate_pipeline tolerates absent long-term config without crashing`.

**Existing tests that encode the WRONG (to-be-changed) behaviour and will fight the fix — must be
updated, not treated as green gates:** `test_pp038_writer_reader.py:163-194` (asserts quarter missing
`horizon_value` → 0), `:431-454` (patches the already-normalized helper, so tests nothing);
`test_quarterly_data_reader.py:963-980` (asserts quarter drops `horizon_value`);
`test_seasonal_integration.py:304-398` and `test_quarterly_api_writer.py:161-194` (encode lead in
`season_in_year`). Also `test_pp038_writer_reader.py:42` skips writer tests when the client import is
absent — a **vacuous green**; that skip pattern must not hide the new assertions.

**Cannot be unit-tested (need a seeded throwaway DB or a SELECT-only preflight, marked `db_live`):**
the prod retag collision counts, stale season-row origin, per-container config presence, deployed
OpenAPI/schema compatibility, and post-recalc `n_pairs` distribution.

### 5. Safety, feature-flagging, and staged rollout

Currently-working flows that must not regress: kghm monthly display, operational LT writer
(`run_forecast.py`), the nightly operational+maintenance aggregation jobs, `recalculate_skill_metrics.py`,
validation checks, bulletin generation.

- **P2's "raise on absent `horizon_value`" will crash the currently-green nightly QUARTER/SEASON
  recalc**, because `calculate_quarterly/seasonal_skill_metrics` emit no independent lead column today
  and the shared writer silently fills `0` (`skill_metrics.py:2161`, `file_writer.py:621-659`). It
  must be flag-gated or preceded by populating the column upstream — not shipped raw.
- **Ship behind a default-off flag** (proposed `SAPPHIRE_LTF_LEAD_AWARE_SKILL`, matching the existing
  `SAPPHIRE_SKILL_LEAD_AWARE` pattern). Flag-off must be byte-identical to today so kghm is untouched.
- **Staged order (readers tolerate → writers emit → data last):**
  1. golden tests + DB preflight only;
  2. flag added, default off;
  3. make readers/dashboard/operational code *tolerate* multi-lead rows;
  4. API capability probe for `skill_metrics.horizon_value`;
  5. enable derivation in staging, **no DB retag**, recalc dry-run;
  6. one-deployment/station canary, then full recalc;
  7. only then historical `long_forecasts` remediation, with collision merge/delete rules.

**Reversible** (revert / flag-off): all P1–P4 code, the API probe, the preflight. **One-way doors:**
historical retag/merge, any unique-constraint change, changing published quarter/season ensemble
values (P3), and changing `forecast_skill_eval` default semantics (P4). Treat those with snapshots and
tested reverse predicates.

### Must-fix gaps before P1a (summary)

1. Ratify the target-state contract (§1) in P0, with the colleague on the season/schema parts.
2. Bring the omitted consumers (§2) into scope or explicitly rule each out.
3. Redesign P6 quarter remediation as re-derive/re-run, not in-place UPDATE (§3); make P3 depend on
   fixing the quarter `date` first.
4. Write the golden + regression tests (§4) and fix the tests that lock the wrong behaviour.
5. Add the default-off flag and the staged rollout (§5); do not ship "raise on absent" unflagged.

---

## Phases

### P0 — Decide the long-term `horizon_value` contract + pre-flight (gate, no code)
Define, for **all three** long-term horizons (month, quarter, season), what `horizon_value`, `date`,
and `horizon_in_year` mean — the columns disagree today (month: lead vs calendar month vs default;
quarter: config lead with `date=valid_from`; season: `season_in_year`). Decide whether `horizon_value`
is always lead-in-months. Choose (a)/(b)/(c) for fix 3. Run the config-availability pre-flight for
P1a. Resolve open questions 1 and 9 (8 is already resolved — see below). Nominate the P6 owner.
**Output: a written decision here.**
*(Revision 4: the old "confirm quarter/season are unaffected" wording was stale — they are affected.)*

### P1a — Dashboard **display** lead resolution (Defects A, G, J) — ships first
- **Files:** `apps/iEasyHydroForecast/long_term_horizon_resolver.py`,
  `apps/forecast_dashboard/src/db.py` (forecast-frame selection only),
  `apps/forecast_dashboard/dashboard/plot_manager.py`,
  `apps/forecast_dashboard/dashboard/widgets.py`,
  `apps/forecast_dashboard/dashboard/data_manager.py`,
  `apps/forecast_dashboard/dashboard/bulletin_manager.py` (+ tests)
- **Depends on:** P0 only.
- **Scope:** which forecast rows are displayed, and how the caption / header / bulletin describe
  them. All three hard-coded lead-1 sites. **Does not touch skill-stat filtering.**
- **Why this is safe ahead of P2:** the forecast frame is date-bounded — `get_long_forecasts()`
  queries `start_date = {PREVIOUS_YEAR}-12-20`, `end_date = {CURRENT_YEAR}-12-31` (`db.py:609-620`)
  — and the panel further selects `date.max()`. Production month-number pollution ends in 2023, and
  local `flag=0` month rows have **zero** `horizon_value`/lead mismatches after 2024-01-01. So the
  displayed forecast values cannot be polluted rows.
- **Rollback:** pure read-path change, no data written. Revert the commit.

### P1b — Per-lead skill-stat **filtering** (Defect F, structural) — ships **with** P1a
- **Files:** `apps/forecast_dashboard/src/db.py` (`forecast_stats` handling) (+ tests)
- **Depends on:** P0. **Ships together with P1a.**
- *Revision 4: moved to ship with P1a.* Review round 3 was right that P1a alone — changing the
  displayed forecast lead while leaving stats filtered to the old lead — is an **internally
  inconsistent** state that must not ship. The **structural** fix (filter stats to each card's
  displayed lead; render blank when that lead has no stats; never merge the unfiltered frame) is a
  read-path change with no data dependency, so it ships with P1a.
- Must handle **both** branches of the `_op_mask.any()` guard.
- **What does NOT ship here:** making the underlying metric *values* correct. Those depend on the
  retag/recalc (P6), not on P2. P2 only stops *future* pollution. So after P1a+P1b a card shows the
  right lead's stats, but that lead's metric may still be computed over polluted history until P6.
  This limitation must be stated in the release note, not hidden.
- **Rollback:** revert.

### P2 — Writer correctness (Defect C, all horizons)
- **Files:** `apps/postprocessing_forecasts/src/api_writer.py` (+ tests)
- **Depends on:** P0.
- Covers `_write_monthly_ensemble_to_api` **and** `_write_aggregated_forecasts_to_api`
  (quarter/season), which revision 3 confirms is defective, not merely suspect.
- **Rollback:** revert. Note that option (a) "raise on absent" converts a silent-corruption path into
  a **hard failure of the aggregation step** — validate against a full local pipeline run
  (`bash apps/run_locally.sh all`) before deploy, or the nightly job starts failing.

### P3 — Quarterly aggregation lead partitioning + quarter/season card (Defects H, C-season)
- **Files:** `apps/postprocessing_forecasts/src/aggregation.py`, `src/data_reader.py`,
  `apps/postprocessing_forecasts/src/api_writer.py` (season `:1090`),
  `apps/forecast_dashboard/src/db.py` (`get_long_forecasts_quarter` / `_season`,
  `_resolve_seasonal_horizon_value`) (+ tests)
- **Depends on:** P0.
- Covers: (i) H's lead-blind grouping; (ii) the quarter dashboard card selecting "latest `date`" when
  quarter `date = valid_from` is a target start, not an issue date — so "latest" is not "most recently
  issued"; (iii) the season data-mixing at `hv=1` (open question 9) — **diagnose first**, since the
  season *writer* round-trips the lead correctly (revision 5), so the cause is elsewhere.
- **Rollback:** revert the code; then **recompute** quarter/season aggregates for the affected range.
  Recompute is not a revert — specify the range and the `DELETE … WHERE horizon_type IN ('QUARTER','SEASON')
  AND date BETWEEN …` predicate before executing, and snapshot first.

### P4 — `forecast_skill_eval` month-lead handling (Defect I)
- **Files:** `apps/forecast_skill_eval/src/forecast_skill_eval/pairs.py` (+ tests)
- **Depends on:** P0. Must preserve per-lead strata — see the `_dedup_long_term` caveat and open
  question 8. **Rollback:** revert.
- **Reframed (revision 6):** `forecast_skill_eval` is the **parity oracle** — it can derive lead from
  the issue date (`pairs.py:513-560`). Prefer making upstream writers stamp the correct stored lead
  (P2) over changing the oracle's defaults, since a default change **alters already-published skill
  artifacts**. If the oracle must change, version/flag the output.

### P5 — Aggregates + `MC_ALD` investigation (Defect E)
- Diagnostic only; scope after root cause. **Depends on:** P0.

### P6 — Production re-tagging / re-derivation migration (Defect C, data)
- **Depends on:** P2, open question 1, **named owner** (nominate in P0).
- **NOT an in-place UPDATE for quarter (revision 6).** Verified on local DB: retagging quarter
  `horizon_value` to a data-derived lead collides on **15,935 / 97,462** rows, because the writer set
  `date = valid_from` and thereby **destroyed the issue date** — the true quarter lead is not
  recoverable from the data. Quarter remediation must **re-derive lead from config or re-run the
  aggregation**, not flip the column. Month (month-numbered rows) may be a simpler retag, but the
  collision preflight must be **run on prod first** (local month pollution is a different kind).
- **Rollback (mandatory before execution):** capture the affected `id` list and pre-update values into
  a backup table; author *and test* the reverse predicate; dry-run against a restored snapshot.
- Scope includes month, quarter and season.

### P7 — Backfill 2023-11 → 2026-06 (Defect D)
- **Depends on:** P2, P5 (else the backfill re-creates mis-tagged / aggregate-less rows).
- **Rollback:** snapshot first; then delete by exact issue-date range scoped to
  `horizon_type='MONTH'` **and** the backfilled `model_type` set. Record the row count before and
  after so a partial backfill is detectable.

---

## Acceptance criteria

**P1a (the reported symptom):**
- With `tjhm` config: monthly panel displays the lead-0 (`month_1`) product; caption names the
  **issue month** as target; no second card.
- With `kghm` config: main panel lead 1, `month_0` card lead 0 — behaviour otherwise unchanged.
- The m0 bulletin hydrates from the m0 frame's target month/year (Defect G).
- `format_horizon_info` renders the resolved lead's target month **and the target year** (Defect J).
- A bulletin generated from a December issue at lead ≥ 1 carries the **following** year, in the saved
  key *and* in the visible header.
- No mode-name string literal (`"month_0"` / `"month_1"`) and no `(month % 12) + 1` expression drives
  a lead computation anywhere in `forecast_dashboard`.

**P1b (Defect F):**
- Each card merges skill stats filtered to the lead that card displays. A fixture with both lead-0
  and lead-1 stats for the same code/month/model must not cross-annotate.
- When a card's lead has **no** stats, it renders blank — never another lead's, and never an
  unfiltered merge.

**P2:** the aggregate writer never synthesizes `horizon_value` from a period number; given a frame
without the column it raises; **both** the monthly and the quarter/season writers are covered; a full
local pipeline run completes.

**P3:** a quarterly aggregate built from mixed-lead monthly input either partitions by lead or refuses.

**Project-wide:** `SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero unexpected skips.
No real station codes or discharge values in tests, fixtures, or this file.

---

## Test checklist (see `doc/dev/testing_workflow.md`)

Forecast dates passed as parameters — no `date.today()` in business logic.

1. `month_horizon_value("month_1")` → 0 under tjhm-shaped config, 1 under kghm-shaped.
2. `month_horizon_value("month_0")` under tjhm (mode absent) → guarded, does not raise out of the
   loader (`_ensure_supported_mode` raises; the caller must membership-check first).
3. `_format_forecast_info` lead 0, issue 2026-07-01 → "July"; lead 1 → "August".
4. **Defect J:** `format_horizon_info` lead 0, issue 2026-07-01 → "July 2026"; lead 1, issue
   2026-12-01 → "January **2027**".
5. Lead 1, issue 2026-12-01 → target January **2027** in caption, header, *and* bulletin metadata.
6. Monthly loader selects rows by the **resolved** lead; fixture with a lead-tagged and a
   month-numbered row returns only the former.
7. **Defect F regression (a):** m0 frame merged against lead-0 stats only; main frame against lead-1
   only, when both exist.
8. **Defect F regression (b):** when **no** stats exist for a card's lead, that card renders blank —
   the unfiltered frame is not merged. *(Covers the `_op_mask.any()` false branch.)*
9. **Defect G regression:** m0 bulletin hydration uses the m0 target month, not the main panel's.
10. `month_0` panel hidden when `month_0 ∉ supported_modes`; shown when present.
11. Monthly aggregate writer with a frame lacking `horizon_value` → raises (never writes `month`).
12. Quarter/season aggregate writer with a frame lacking `horizon_value` → raises (never writes
    `quarter_in_year`).
13. Aggregate writers with `horizon_value` present → round-trip unchanged.
14. **Defect H:** quarterly aggregation over mixed-lead months does not average across leads.
15. **Defect I:** a month row whose `horizon_value` disagrees with its observed lead is routed to the
    ledger, not paired.
16. **Defect I, dedup:** genuine month rows at distinct leads survive `_dedup_long_term` as distinct
    pairs (guards against the `include_lead_in_key` collapse).

*All 16 are mock/fixture-testable — none requires a live DB or a real station code.*
kghm regression coverage is the existing monthly dashboard test suite passing unmodified; that is a
gate, not a new test case.

---

## Open questions (resolve in P0)

1. **Non-uniform backfill tagging.** Production shows 157 rows at `hv=1` / observed lead 1, but the
   2026-07-01 run accounts for only 128 (the `hv=2` / lead-2 group reconciles exactly at 128). So
   ~29 lead-1 rows sit on 2016–2023 issue dates. What wrote them? **P6 cannot be authored until this
   is understood** — the naive predicate (`hv = EXTRACT(MONTH FROM valid_from) AND lead = 0`) may be unsafe.
2. Can the dashboard process read the per-mode config JSONs at runtime? (Pre-flight for P1a.)
3. **Define `horizon_value` for all three long-term horizons**, not month alone. Quarter currently
   stores quarter-in-year for aggregate rows; season is polluted at `hv=1`. What should each mean?
   Should it be constrained (CHECK / enum)? Service-schema change — **owner needed**.
4. Is `MC_ALD`-at-lead-0-only expected (config `model_dependencies`) or a defect?
5. Is the 2024–2025 hole expected (LT monthly not yet deployed) or did runs fail silently?
6. What produced the local `horizon_value=3` rows on a Tajik-prefixed station with kghm issue days?
7. Should tjhm's config files eventually be renamed to match the lead convention? *(Deferred: breaks
   `supported_modes`, operator muscle memory, and deployed cron passing `lt_forecast_mode`. The
   config-driven resolver removes the need.)*
8. **`_dedup_long_term` and month — RESOLVED (revision 4).** `_dedup_long_term` is called only when
   `normalized_horizon in LONG_TERM_DERIVE_LEAD_HORIZONS = ("quarter","season")` (`pairs.py:250-251`),
   so it is **not** called for month today — month rows are never collapsed, which is why month is
   safe now. The apparent contradiction at `pairs.py:312` vs `:319-321` never fires for month.
   **Design constraint for P4:** if month is added to the derive-lead horizons, `include_lead_in_key`
   (`pairs.py:312`) must also be extended to month, or genuine per-lead month rows will collapse into
   one winner. This is now a P4 acceptance criterion, not an open question.
9. **Season data-mixing origin (unattributed).** Season `hv=1` mixes ~4,526 genuine lead-1 rows with
   ~4,862 others (local). The season **writer round-trips the lead correctly** (`api_writer.py:1090`
   ← `data_reader.py:3143`), so the mixing is *not* current-writer pollution — its cause is unknown
   (older writer? a non-lead upstream `season_in_year`? multiple seasonal issue configs?).
   `_resolve_seasonal_horizon_value` (`db.py:96-102`, used `:706`) and `get_long_forecasts_season()`
   select against this mixed column. **Diagnose before touching** (P3-iii).

## Latent risks (not defects today — do not fix blind)

- `get_long_forecasts()` (`db.py:609-620`) requests `limit: 1000` over a fixed
  `{PREVIOUS_YEAR}-12-20 … {CURRENT_YEAR}-12-31` window, single page. `get_forecast_stats()` likewise
  fetches one 1000-row page and dedups client-side. Locally the worst case is **526 skill rows for one
  station** (78 stations, none over 1000), so neither truncates today. Both are fragile if station or
  model counts grow. Flagged by adversarial review round 2; verified as *not currently reproducing*.

## Disputed reviewer claims (recorded, not accepted)

- **Round 1** marked *"day-1 issues only 48% snapped"* as REFUTED, reporting 40.4% and stating no
  filter produced 48%. Both numbers are correct under different scopes (see Evidence). The draft's
  fault was an unstated scope, not a false figure. Corrected by qualification, not retraction.
- **Round 2** refuted the P1-independence rationale with *"983 mismatched rows after 2024-01-01, max
  2025-12-25."* Re-running the query yields **234** rows, max **2025-12-01**, **all `flag=1` and all
  un-snapped** — i.e. Defect **B** artifacts, not Defect **C** pollution; `flag=0` mismatches after
  2024 are **zero**. The reviewer conflated B with C. However, its *second* argument — that
  `date.max()` bounds the forecast frame but never `forecast_stats` — is correct and stands. The plan
  was restructured (P1a / P1b) on the strength of that second argument alone.
  **Round 3 explicitly conceded both (a) and (b):** *"Author is right under the documented modulo
  predicate… I explicitly concede the round-2 983-row claim was not a valid refutation"* and *"Round
  1's 'refuted' label was wrong; the 40.4% and 48.0% figures are both valid under different date
  scopes."* Recorded as resolved.
- **Round 3, quarter over-claim (author's own, self-corrected):** revision 3 stated the quarter
  *current writer* stamps quarter-in-year. Round 3 refuted this — the current writer stamps
  `quarter_horizon_value()` (`api_writer.py:1074`); the quarter-in-year rows are **old-writer
  data-state** pollution. Revision 4 separates current-code (season `:1090`) from data-state (quarter
  `hv=2,3,4`). The classifier `mod(month(valid_from) - month(date))` is also **tautological for
  quarter** (writer sets `date = valid_from`) and must not be used to label quarter rows as "lead 0."

---

## Dependency graph

```json
{
  "phases": {
    "P0":  { "depends_on": [], "parallel_agents": 0 },
    "P1a": { "depends_on": ["P0"], "parallel_agents": 2 },
    "P1b": { "depends_on": ["P0"], "parallel_agents": 1 },
    "P2":  { "depends_on": ["P0"], "parallel_agents": 1 },
    "P3":  { "depends_on": ["P0"], "parallel_agents": 1 },
    "P4":  { "depends_on": ["P0"], "parallel_agents": 1 },
    "P5":  { "depends_on": ["P0"], "parallel_agents": 1 },
    "P6":  { "depends_on": ["P2"], "parallel_agents": 1 },
    "P7":  { "depends_on": ["P2", "P5"], "parallel_agents": 0 }
  }
}
```

---

## Revision history

**Revision 6 (2026-07-13)** — added an implementation-readiness review round (blast-radius inventory
+ `codex exec`), triggered by the questions: what to check against, target DB state, tests-before-code,
breakage risk, new-error risk. Added the "Implementation readiness" section: target-state contract
table; ~15 omitted consumers (incl. a **second colleague-owned month writer** `data_migrator.py`, the
skill producers, and the READ-DERIVE season sites); **verified quarter retag collision (15,935/97,462)**
proving quarter remediation cannot be an in-place UPDATE because `date=valid_from` destroyed the issue
date; a pre-code golden+regression test plan plus a list of existing tests that lock the wrong
behaviour; a default-off feature flag and 7-step readers-first rollout; and the P2 unflagged-crash
risk to the nightly quarter/season recalc. Reframed P4 (skill_eval is the parity oracle — fix writers,
not the oracle). Updated P6 to re-derive/re-run rather than UPDATE. **Net: diagnosis settled,
implementation NOT yet ready; 5 must-fix gaps enumerated.** *Process note: the codex pass ran against
the main checkout, from which the draft had just been deleted, so its "gaps in the plan" are inferred
from adjacent artifacts; its code/DB findings (used above) stand regardless and the collision number
was re-verified by the author.*

**Revision 5 (2026-07-11)** — after adversarial review round 4 (scoped confirmatory pass):
- **Corrected the season current-code over-claim.** Revision 4 called `api_writer.py:1090` a live
  season defect stamping a period number. Round 4 refuted it and I verified: `season_in_year` is
  derived from the API `horizon_value` (the lead) at `data_reader.py:3143`, so the writer round-trips
  the lead. Same over-claim class round 3 caught on quarter. Season *data* mixing is real but its
  cause is now open question 9, not the writer.
- Marked the quarter `hv` table buckets as internally **mixed** (round 4: `hv=1` holds both config-lead
  and target-quarter rows); added the small residual buckets I'd omitted.
- Fixed P0's stale cross-reference to open question 8 (already resolved).
- Rescoped P3-iii from "fix the season writer" to "diagnose the season data-mixing first."
- Everything round 4 checked structurally — P1a/P1b split, OQ8 resolution, quarter current-code,
  dedup — was **CONFIRMED**. No plan-structure change in revision 5; evidence-wording only.

**Revision 4 (2026-07-11)** — after adversarial review round 3 (which conceded both disputes):
- **Corrected the quarter/season escalation.** Current quarter writer stamps `quarter_horizon_value()`
  (config lead), not quarter-in-year; the quarter-in-year rows are old-writer data-state pollution.
  Current-code season defect is `api_writer.py:1090` (`season_in_year`). Flagged that the
  `mod(month(valid_from)-month(date))` classifier is tautological for quarter (`date=valid_from`).
- Fixed the stale P0 "confirm quarter/season unaffected" wording; P0 now defines `horizon_value`,
  `date`, `horizon_in_year` for all three long-term horizons.
- **Moved P1b to ship with P1a** (structural per-lead stat filtering is a read-path change with no
  data dependency; P1a alone is internally inconsistent). Metric-value correctness explicitly
  deferred to P6 with a release-note caveat.
- Added the quarter dashboard card (latest-`date` selection is wrong when `date=valid_from`) and the
  season resolver/writer to P3.
- **Resolved open question 8:** `_dedup_long_term` is not called for month today; became a P4 design
  constraint (extend `include_lead_in_key` to month) rather than an open question.
- Recorded round 3's concessions and the author's own quarter self-correction in Disputed claims.

**Revision 3 (2026-07-10)** — after adversarial review round 2:
- **Escalated Defect C to quarter and season.** Revision 2 called them "unaffected" and asked only for
  an audit. Verified: quarter `horizon_value` stores quarter-in-year (1–4) for aggregate rows,
  colliding with genuine leads in the same buckets; season is polluted at `hv=1`.
- **Added Defect J** (`widgets.py:625`) — a third hard-coded lead-1 site, and the visible header's
  year is taken from the production date. Missed by revisions 1–2 and review round 1.
- **Corrected Defect F**: the lead-1 filter is conditional on `_op_mask.any()`; when it is false the
  frame is merged **unfiltered**. Revision 2's "always filters to lead 1" was wrong. Downgraded
  "live kghm bug" to code-confirmed + locally reproduced, not production-verified.
- Corrected the bulletin scope-narrowing from revision 2 (a wrong month/year *is* visible, via J).
- **Split P1 into P1a (display, ships first) and P1b (per-lead skill stats, after P2)**, on the
  reviewer's surviving argument that `date.max()` never bounded `forecast_stats`.
- Added the `flag=0` + tjhm scope to the 40.4% / 48.0% figures.
- Recorded the 234 post-2024 mismatched rows as Defect B artifacts, and documented that review round
  2's 983-row refutation did not reproduce.
- Added `_dedup_long_term` per-lead-strata caveat to fix 5; added open questions 8 and 9.
- Added a "Latent risks" section (API page limits) and a "Disputed reviewer claims" section.
- Concretised rollbacks for P3 and P7; test list expanded to 16 cases.

**Revision 2 (2026-07-10)** — after independent adversarial review (`codex exec`, read-only):
- Corrected citations: `horizon_value` is stamped at `lt_utils.py:350`, not `run_forecast.py:409,418`;
  importer verbatim-copy lines are `long_forecast.py:334-354` and `data_migrator.py:768-776`.
- Narrowed the bulletin symptom (visible header already uses `valid_from`).
- Added the date-filter scope to the 48% day-1 figure; added the 40.4% all-dates figure.
- Downgraded C/D/E from "confirmed" to *production-observed, not re-verifiable from dev*.
- Tightened "lead-0 never displayed" (polluted `hv=1` rows are lead-0).
- **Added Defects F, G, H, I** — F is a live kghm bug the original draft would have preserved.
- Added the local `horizon_value=3` anomaly.
- **Unblocked P1 from the writer fix** (was P2-depends-on-P1); reordered phases.
- Added rollback plans for every data-touching phase and a named-owner requirement for P6.
- Flagged the `_ensure_supported_mode` raise and the config-availability pre-flight as blocking.

**Revision 1 (2026-07-10)** — initial draft.

---

## Notes

- Defect B was initially predicted to exist on production, reasoning that
  `bin/initialize_long_forecast_history.sh` runs the same importer during deployment init. **That
  prediction was wrong** — production has zero un-snapped rows and zero `flag=1` rows. Verifying it
  rather than trusting it is what surfaced Defect C.
- P1 alone resolves both reported symptoms *and* the live kghm skill-annotation bug (F).
- P5 and P7 are arguably more operationally urgent than P1: the Tajik monthly view currently holds
  **one** forecast, with no ensemble row on it.
