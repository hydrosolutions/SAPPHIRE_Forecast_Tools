# High priority: dashboard shows the wrong monthly lead (Tajik target month + kghm skill bug)

**Status:** Draft, revision 7 — **re-baselined against `origin/maxat_sapphire_2` @ `3be4dbf5`**
(the branch we will build on), which corrected several earlier claims made against a divergent branch.
Narrowed to the dashboard read-side. The writer/data side is handled by the existing MIG-008 track.
No code written.
**Module:** `apps/forecast_dashboard` only.
**Convention:** adopts the service owner's resolved convention `horizon_value = operational_month_lead_time`
(`doc/prod/longforecast_quarter_season_hv_convention.md`, 2026-06-22).

---

## Reported symptoms

1. **Wrong target month shown** on the Tajik monthly dashboard view.
2. **Bulletin month/year wrong.**

---

## Root cause (one sentence)

Under the resolved convention, the DB stores each monthly forecast at `horizon_value = its config
lead`. The dashboard **hard-codes the monthly lead to 1** in five places instead of resolving it from
config — so on a Tajik deployment (whose flagship forecast is lead 0) it shows the wrong product, and
on kghm it annotates the current-month card with the wrong lead's skill.

Quarter and season already resolve their lead from config via
`_resolve_quarter_horizon_value` / `_resolve_seasonal_horizon_value` (`db.py:71-102`). **Month is the
one long-term horizon that never got a config resolver.** This issue adds it and routes the five
hard-coded sites through it.

---

## The convention (adopt as given — do not re-decide)

From the owner, 2026-06-22: `horizon_value = operational_month_lead_time`, per-config, no
date-derivation. For month, the *lead value inside each config* is authoritative, not the filename:

| deployment | primary monthly product | secondary |
|---|---|---|
| **kghm** | `month_1.json` → lead 1 (`hv1`), issued day 25 | `month_0.json` → lead 0 (`hv0`), issued day 10 |
| **tjhm** | `month_1.json` → **lead 0** (`hv0`), issued day 1 | none (`month_0` not in `supported_modes`) |

So "the main monthly panel" is *the deployment's primary product*, whose lead differs by org. The
dashboard must resolve it, not assume 1.

---

## The five defects (all verified on `maxat_sapphire_2`)

### A — main panel + caption hard-code lead 1
- `apps/forecast_dashboard/src/db.py:924` — `get_long_forecasts(station, horizon_value=1)` for the
  main panel (signature default also `horizon_value=1` at `:681`); the `month_0` card is gated by the
  literal `if "month_0" in supported_modes:` (`:990`).
- `apps/forecast_dashboard/dashboard/plot_manager.py:31-34` — `_format_forecast_info` computes
  `target_month = (issue_date.month % 12) + 1` for `month_1`.
- Effect: tjhm's `hv0` flagship (issued day 1 for the current month) is filtered out and its panel is
  gated off; the panel instead shows `hv1` = tjhm's `month_2` product (lead 1), captioned as next month.

### F — the `month_0` card is annotated with the main panel's skill-stats frame
- `apps/forecast_dashboard/src/db.py:931-935` filters `forecast_stats` to `_op_lead = 1` **only when**
  `_op_mask.any()`; the frame is **not rebound** before `:991` merges it into the lead-0 `m0` frame.
- Two failure modes: lead-1 stats exist → the lead-0 card shows lead-1 skill; lead-1 stats absent →
  the card merges stats from **all** leads unfiltered. A live kghm bug, independent of Tajik.

### G — the `month_0` bulletin hydrates from the main-panel context
- `apps/forecast_dashboard/dashboard/bulletin_manager.py:499,523` → `_month_hydration_params` (`:386`)
  calls `get_bulletin_metadata("month")` (the **main** panel), not the m0 frame. Its docstring (`:389`)
  asserts "the monthly forecast targets the month AFTER it is issued" — a hard-coded lead-1 belief that
  drives the norm lookup and month-length.

### J — the visible horizon header hard-codes lead 1 and the wrong year
- `apps/forecast_dashboard/dashboard/widgets.py:628-632` — `format_horizon_info` month branch:
  `target_month_num = (production_date.month % 12) + 1`, `"year": production_date.year`. Wrong month
  (assumes lead 1) and wrong year (a December-issued lead-1 forecast targeting January renders the
  previous year). The third independent hard-coded-lead-1 site.

### Bulletin year-rollover (org-independent, pre-existing)
- `apps/forecast_dashboard/dashboard/data_manager.py:361` — `get_bulletin_metadata` returns
  `forecast_year = last_date.year` (issue year). For any lead ≥ 1 issued in December, the target month
  is January of the following year, so the saved bulletin year is wrong.

---

## The design decision this needs (for the plan's P0)

The convention fixes `hv` assignment but not *which card shows what*. Decide: the main monthly panel
shows the deployment's **primary** product (kghm lead 1, tjhm lead 0); the secondary `month_0` card
shows lead 0 **only when a distinct lower lead exists** (kghm yes, tjhm no). Concretely: add a public
`month_horizon_value(mode)` to `long_term_horizon_resolver.py` (mirroring `quarter_horizon_value` /
`seasonal_horizon_value`), and resolve the primary/secondary leads from `supported_modes` +
`operational_month_lead_time`, membership-checking before calling the resolver (it raises on an absent
mode). Flag-gated, default off, byte-identical for kghm.

---

## Re-baseline note — what was investigated and dropped

This issue began as a broad "`horizon_value` overloaded across month/quarter/season" investigation
(6 revisions, 5 adversarial review rounds — see git history of this file and the `develop_…` branch).
Re-verifying against `origin/maxat_sapphire_2` collapsed it:

- **Writer overload (was "Defect C") — already fixed on `maxat`.** Monthly writer falls back to a `0`
  sentinel, not the calendar month (`api_writer.py:896`, with a comment saying calendar month is never
  a valid `horizon_value`); quarter writer stamps `quarter_horizon_value()` (`:1093`); season stamps
  `season_in_year` (= the lead by convention). This was the "P-PIPE" fix from the MIG-008 track, since
  landed.
- **Raw un-snapped `valid_from` (was "Defect B") — not a code issue.** The writer snaps correctly
  (`post_process_lt_forecast.py:483-490`); the raw rows were a local dev-DB artifact only.
- **Quarterly aggregation "blends leads" (was "Defect H") — not a coherent defect.** The owner resolved
  that quarter is a *single product, one lead per deployment*, so the monthly input carries one lead and
  nothing blends. This was the exact "4 calendar quarters" misread the owner corrected.
- **`forecast_skill_eval` "trusts month `horizon_value`" (was "Defect I") — correct by design.** Month is
  excluded from derive-lead, which is right now that the writer stamps a genuine lead. The only residual
  risk is historical polluted rows — a data question, below.
- **Historical data pollution + the Tajik 32-month gap (was "Defects C-data / D / E") — MIG-008 track.**
  Handed off; my prod findings are summarized into
  `doc/prod/longforecast_historical_data_decision_request.md` (addendum 2026-07-13). Not built here.

---

## Acceptance criteria

- With `tjhm` config: the main monthly panel shows the lead-0 product; caption **and** header name the
  **issue month** as target; no second card.
- With `kghm` config: main panel lead 1 (unchanged); `month_0` card + bulletins **corrected** to lead-0
  skill/norms (kyg is a beneficiary, not just protected). The kill-switch
  `SAPPHIRE_LTF_DASH_LEAD_AWARE=false` reproduces today's behaviour **byte-identical to `maxat`**.
  Flag defaults **on** (see the implementation plan's "Flag specification").
- Each card merges skill stats filtered to the lead it displays; blank (never another lead's, never an
  unfiltered merge) when that lead has no stats — covers both `_op_mask.any()` branches (Defect F).
- The m0 bulletin hydrates from the m0 frame's target month/year (Defect G).
- A December-issue lead-1 forecast renders January of the **following** year in caption, header, and
  saved bulletin metadata (Defects J + year-rollover).
- No mode-name string literal or `(month % 12) + 1` expression drives a lead computation anywhere in
  `forecast_dashboard`.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero unexpected skips. No real station
  codes / discharge values.

---

## Test checklist (fixtures `17999` / `15999`; forecast dates passed as parameters)

1. `month_horizon_value("month_1")` → 0 under tjhm-shaped config, 1 under kghm-shaped.
2. `month_horizon_value("month_0")` under tjhm (mode absent) → guarded (membership-checked), does not
   raise out of the loader.
3. `_format_forecast_info` lead 0, issue 2026-07-01 → "July"; lead 1 → "August".
4. `format_horizon_info` lead 0, issue 2026-07-01 → "July 2026"; lead 1, issue 2026-12-01 →
   "January 2027" (Defect J: month + year).
5. Main-panel loader selects rows by the **resolved** lead, not literal 1.
6. Defect F (a): m0 merged against lead-0 stats only; main against lead-1 only, when both exist.
7. Defect F (b): when no stats exist for a card's lead, it renders blank (the `_op_mask.any()` false
   branch does not merge the unfiltered frame).
8. Defect G: m0 bulletin hydration uses the m0 target month, not the main panel's.
9. `month_0` panel hidden when `month_0 ∉ supported_modes`; shown when present.
10. kghm regression: existing monthly dashboard tests pass unmodified under flag-off.

---

## Provenance

Diagnosis history (broad `horizon_value` investigation, revs 1–6, five `codex exec` review rounds,
prod + local DB evidence) is in this file's git history on branch `develop_ltf_monthly_horizon_value`
and summarized in the implementation plan. Revision 7 narrows to the verified dashboard scope.
