# Derive monthly discharge norms locally from the multi-year daily runoff archive (governance-gated)

**Priority:** mid — removes the iEH-HF monthly-norm bottleneck that leaves Tajik
month/quarter/season `norm` cells empty; not blocking (PREPQ-009 already writes the observed
triad without it).
**Module:** `apps/preprocessing_runoff` (fair game).
**Depends on:** PREPQ-009 (the long-horizon writer + write path this extends).
Split out of PREPQ-009 as its **fix #4** ("out of scope — file separately").
**Found:** 2026-07-08, from PREPQ-009 diagnosis + domain review.

## Background

PREPQ-009 established that the long-horizon writer sources the monthly climatological **norm**
from the iEH-HF SDK (`get_norm_for_site(code,"discharge",norm_period="m")`), and that for the
Tajik org that call returns **zero monthly norms for every station** — even though all 17
stations have decades of local daily discharge in SAPPHIRE. PREPQ-009 decouples row existence
from the norm (writes `previous`/`current` from local data, `norm=None`), but the `norm` /
percent-of-norm columns stay empty until iEH-HF is populated **or** the norm is derived
elsewhere.

The same local daily archive that already yields `previous`/`current` (via
`monthly_mean_threshold_80`, ~70 yrs of history for the affected stations) can yield the
monthly norm — removing the iEH-HF dependency entirely and filling the empty columns.

## Proposed change

Add an option to derive the 12 monthly discharge norms per station from the multi-year local
daily runoff archive, used as a fallback when the iEH-HF norm is absent (or, if the archive is
trusted, as the primary source). Reuse the same per-month coverage rule per historical year,
and record `count` = number of years used per month.

**This must not ship until a governance gate is satisfied** — a locally-derived normal must
never masquerade as official iEH-HF climatology. Before any derived norm appears in a bulletin,
a hydrology-lead-approved spec must fix and document:
- approved **reference period** (which years);
- per-month **completeness threshold** (min days/year and min years);
- **years-used count** per station/month (stored as `count`);
- handling of **station moves, datum changes, rating-curve changes, regulated-flow changes,
  outliers**;
- explicit **units** and the **aggregation formula**;
- a **provenance flag** + **version/date**, labelled **"SAPPHIRE-derived operational normal"**
  unless the institution formally adopts it as official climatology.

Interaction with FD: once norms are derived, the month/quarter/season `norm` cells become
numeric and the **FD-016** "N/A" labeling simply stops triggering — no further FD change needed.

## Acceptance criteria
- A documented, hydrology-lead-**approved** derived-norm spec (the governance checklist above)
  exists and is referenced from the implementation.
- Unit tests (`run_tests.sh preprocessing_runoff`, `today` explicit, placeholder code `19999`):
  derived monthly norms match the agreed formula over a fixture archive; the completeness rule
  drops under-covered months/years; `count` reflects years used; provenance flag is set on
  written records.
- When iEH-HF returns a valid 12-value norm, that remains the source (derivation is
  fallback) — unless the approved spec chooses local-primary, in which case document why.
- No real station codes/discharge in code, tests, or fixtures.

## Notes
Adjacent but distinct from the **SDK-sourced** norm work
(`review_gi_draft_infra_monthly_norms_from_sdk.md`, `mid_prio_gi_draft_prepg_yearly_norm_
recalculation.md`) — this issue is specifically about deriving norms **locally** from SAPPHIRE
daily discharge, not fetching them from iEH-HF. Related: PREPQ-009 (writes `norm=None` today),
FD-016 (N/A labeling that this makes unnecessary once norms exist).
