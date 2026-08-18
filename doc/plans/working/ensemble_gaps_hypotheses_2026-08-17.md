# Targeted § 9.6 investigation — four ensemble/skill gaps, with hypotheses

**Date**: 2026-08-17
**Trunk**: `maxat_sapphire_2` @ `f4034e52`
**Scope**: deliberately *not* the full § 9.6 sweep. Only the four observations from the
2026-08-14/17 local review that currently have **no explanation**. The rest of § 9.6 either
re-derives what we already established or is blocked by INFRA-021.

**Method note**: § 9.6 cannot be run automatically — INFRA-021 crashes
`validate_pipeline --target long-term` before any check executes, on both orgs. Everything
below is manual API observation plus code reading.

---

## Q1 — tjhm 2026-07-01: members present, **no Naive Mean**

**Observed** (`/long-forecast/?horizon_type=month`, tjhm codes, after
`maintenance:postprocessing_long_term` PASSed with the flag ON and changed nothing):

```
2026-07-01: 17 stations with members (8 member models each), 10 with any ensemble → 7 with NONE
2026-08-01: 17 stations with members,                        16 with any ensemble → 1 with NONE
```

**Why it needs explaining.** The owner established that **EM** can legitimately be empty when
fewer than two models clear the skill gate. That explanation does **not** extend to
**Naive Mean**, which (per out-of-loop code review) has *no* skill gate and uses all
non-baseline models. With 8 members present, a Naive Mean should be formable.

**Hypotheses, ranked:**

| # | Hypothesis | Discriminating evidence |
|---|---|---|
| **H1a** | Naive Mean is gated by a **min-n / `min_pairs`** floor (monthly default 4), not by skill. Stations with thin history (tjhm starts 2021) fall under it. | Find the min-pairs gate in the monthly ensemble path; check `n_pairs` for the 7 affected stations vs the 10 that succeeded. |
| **H1b** | The 8 members are **all "baseline"** by the code's definition, so "all non-baseline models" leaves too few. | Read the non-baseline model set; compare against the 8 model names actually present. |
| **H1c** | Ensembles are keyed **per (code, lead)** and those 7 stations lack members *at the configured lead* (tjhm `month_1` = lead **0**) even though they have members at other leads. | Group the 07-01 member rows by `horizon_value` per station; check whether the 7 lack lead-0 members specifically. |
| **H1d** | Under `SAPPHIRE_SKILL_LEAD_AWARE=true` the gap detector selects **only the configured operational issuance**, and something about 07-01 puts it outside that selection. | Read the flag-ON branch of the long-term gap detector. |

**My prior**: H1c, then H1a. H1c would also explain why 08-01 has only 1 station missing —
different lead coverage on a more recent date.

---

## Q2 — kghm 2026-08-10: a **configured** issuance with 61 stations of members and **zero** ensembles

**Observed** (kghm codes only, after separating orgs — the endpoint has no org filter):

```
2026-06-10:  61 members,  57 with ensemble,   4 missing
2026-06-25:  61 members,  56 with ensemble,   5 missing
2026-07-07:  61 members,   0 with ensemble,  61 missing
2026-07-23:  61 members,   0 with ensemble,  61 missing
2026-08-10:  61 members,   0 with ensemble,  61 missing
```

**Why it needs explaining.** kghm issue days are **10** (`month_0`, lead 0) and **25**
(`month_1..3`, leads 1–3). So `07-07` and `07-23` are **not** configured issue days — they look
like *run dates*, which matches an unresolved stamping inconsistency noted on 2026-08-15
(today's run stamped its month_0 forecast `2026-08-10`, the configured day, while July rows
carry `07-07`/`07-23`). But **`08-10` IS a configured issuance**, and it has no ensembles.

**Hypotheses, ranked:**

| # | Hypothesis | Discriminating evidence |
|---|---|---|
| **H2a** | Monthly ensembles are built only for the **primary** lead (the dashboard's `month_1`, kghm lead 1, issued on the 25th). A date carrying **only lead-0** rows (the 10th) therefore never produces one. | Check whether 06-10 (which *does* have ensembles) carries leads other than 0; read which leads the monthly ensemble builder iterates. |
| **H2b** | The `date` stamped on a row is sometimes the **run date** and sometimes the **configured issue day**; the ensemble builder joins on the configured day and misses run-date-stamped rows. | Find where the long-forecast `date` is assigned in `run_forecast.py`; compare 06-10/06-25 (worked) against 07-07/07-23 (didn't). |
| **H2c** | Skill/min-n gates fail wholesale at that date. | Would have to suppress **EM, Naive Mean and Skilled Mean simultaneously** — implausible given Naive Mean's lack of a skill gate; listed for completeness. |

**My prior**: H2b explains 07-07/07-23; H2a explains 08-10. They may be two different causes,
which is exactly why they must not be filed as one issue.

---

## Q3 — kghm monthly **EM starved**: 22 rows vs ~95 for every other monthly model

**Observed** (kghm, both review stations, after a full-history recalc from 2000 with the flag
ON — distribution byte-identical before and after):

```
EM            hv=0:17, hv=1:4, hv=3:1     = 22 rows
GBT           hv=0:24, hv=1:23, hv=2:24, hv=3:24   ≈ 95
LR_Base       hv=0:24, hv=1:24, hv=2:24, hv=3:24   ≈ 96
… every other model ≈ 95
```

**Hypotheses, ranked:**

| # | Hypothesis | Discriminating evidence |
|---|---|---|
| **H3a** | **PP-030** excludes EM from re-derivation at boundaries, so EM is written only by the operational path and never replenished by the recalc. | Read the PP-030 exclusion (`exclude_models=["EM"]` in `recalculate_skill_metrics.py`) and establish exactly what it skips. |
| **H3b** | Monthly EM requires **≥2 models past the skill gate**, and monthly skill rarely has two qualifying models. | Count, per (code, target period), how many models clear the monthly thresholds. |
| **H3c** | Monthly EM additionally applies a **horizon-specific `min_pairs`** on top of the default gate. | Locate the monthly branch of the ensemble calculator and its min-pairs argument. |

**My prior**: H3a is the mechanism for *why a recalc doesn't fix it*; H3b/H3c explain *why it
was sparse to begin with*. These are compatible, not competing — and if H3a holds, EM sparsity
is **expected behaviour of the recalc**, not a defect, which would close the question.

---

## Q4 — quarter/season EM parity (§ 9.6.5 asserts `EM = mean(LR_Base, LR_SM)`)

**Claim to verify** (from out-of-loop review, unverified by me): quarter/season EM is a **fixed
`LR_Base + LR_SM` aggregate and is explicitly not skill-gated**, unlike short-term and monthly EM.

**Why it matters.** If true, quarter/season EM should exist wherever *both* LR_Base and LR_SM
exist, regardless of skill — which makes it a clean, falsifiable check, and means any absence is
a real defect rather than a gating outcome. It also means the § 9.6.5 parity test is the only
ensemble check in the review that is **not** confounded by admission rules.

**Hypothesis H4**: quarter/season EM composition is fixed at `[LR_Base, LR_SM]` and bypasses
`filter_for_highly_skilled_forecasts`. **Falsified if** the quarter/season path calls the skill
filter, or if the composition is data-dependent.

---

## What this investigation must not do

- **Not** re-derive established results: LR-008 (verified both orgs), the tjhm lead map, ML
  input-dependence, PP-045's mechanism.
- **Not** conflate organisations. The postprocessing tables have **no organisation column** and
  the read APIs expose no org filter, so every query must be filtered to a config-derived
  station list. I already made this mistake once today and it produced a wrong reading.
- **Not** treat an absent ensemble as a defect before establishing which gate applies to that
  specific aggregate — EM, NE, Naive Mean and Skilled Mean have **different** admission rules.
