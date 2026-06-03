# Runoff long-horizon hydrograph P5 implementation prompt — dashboard handoff stub

> Paste the section between "--- BEGIN PROMPT ---" and "--- END PROMPT ---"
> to the documentation agent. P5 is documentation-only. Plan at commit
> `ec03c44`; writer at commit `aeceebe`; P3 evidence at commit `fc22f6d`.
> P4 and P5 can dispatch in parallel.

--- BEGIN PROMPT ---

You are a documentation agent on the SAPPHIRE forecast tools project.
Your role is **Phase 5 only** of the long-horizon runoff hydrograph
plan at
`doc/plans/issues/high_prio_gi_draft_runoff_long_horizon_hydrograph.md`.
This is documentation-only — you write one handoff note that names
the downstream dashboard scope and explicitly preserves the
out-of-scope items.

## What you are doing

**Goal**: Create
`doc/plans/working/runoff_long_horizon_hydrograph_dashboard_handoff.md`
as a forward-pointer for a future dashboard plan that wires the new
monthly + seasonal hydrograph triads into the dashboard reader.

**Files you may modify (exhaustive)**

- `doc/plans/working/runoff_long_horizon_hydrograph_dashboard_handoff.md`
  (CREATE)

You may NOT modify any other file. No production code, no tests,
no edits to the plan document, the decisions artifact, prior
evidence files, or anywhere else.

## What the handoff note must contain

The handoff is a short Markdown file (~40-80 lines) with the
following sections, in this order, verbatim where stated:

```markdown
# Runoff Long-Horizon Hydrograph — Dashboard Handoff

This note marks the end of the preprocessing-side work for
long-horizon (monthly + seasonal) runoff hydrograph rows. The
downstream dashboard plan is responsible for displaying the
new triad fields to operators and analysts.

## What this plan delivered

The preprocessing-side writer at
`apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`
(commit `aeceebe`) now produces, for each configured station
that has monthly norms in iEH HF:

- **12 monthly hydrograph rows per target year** with
  `horizon_type="month"` and the full `(norm, previous, current)`
  triad. `previous` and `current` are arithmetic means of the
  daily SAPPHIRE runoff records for the same calendar month in
  `Y-1` and `Y`, subject to the per-month threshold rule
  (D-Q6: ≥80% of calendar days populated with non-null finite
  values, otherwise the cell is `None`).
- **1 seasonal hydrograph row per target year** with
  `horizon_type="season"`, `date="{Y}-04-01"`,
  `horizon_value=1`, `horizon_in_year=1`. The seasonal
  `(norm, previous, current)` fields are arithmetic means of
  the six April-September monthly fields, subject to the
  strict-completeness rule (D1: if any one of the six monthly
  values is `None`, the seasonal field is `None`).

Stations whose iEH HF monthly norm call returns zero values or
raises are logged at WARNING and skipped (commit `aeceebe`).

## Downstream scope (next plan, not this plan)

The downstream dashboard plan updates the forecast dashboard's
monthly and seasonal data loaders so the new triad fields
appear in the existing UI:

- **`_get_data_monthly`** in `apps/forecast_dashboard/`
  (currently returns an empty hydrograph overlay DataFrame
  for the monthly tab). It should pull
  `/preprocessing/hydrograph/?horizon=month&code=<station>` and
  surface `norm`, `previous`, and `current` per month for the
  active station and target year.
- **`_get_data_season`** in `apps/forecast_dashboard/`
  (currently returns an empty hydrograph overlay DataFrame for
  the season tab). It should pull
  `/preprocessing/hydrograph/?horizon=season&code=<station>`
  and surface `norm`, `previous`, and `current` for the
  one-and-only April-September seasonal row per active station
  per target year.

Visualization style is the dashboard plan's call.
Recommendations: mirror the snow hydrograph display style
(min-max envelope or norm band, previous-year line,
current-year line, in-progress current shown as partial
trajectory). Reference the snow display plan (now archived
under `doc/plans/issues/archive/`) for component patterns.

## Explicitly out of scope (do NOT expand)

- **Quarter hydrograph triad**: no quarter records are stored
  or written by preprocessing. The preprocessing enum lacks
  `quarter` at
  `sapphire/services/preprocessing/app/models.py:6-13`, and
  the reservoir quarter card already reads monthly data through
  upstream PR #341. Do NOT add a quarter writer in this plan
  or the downstream dashboard plan.
- **API/schema changes**: the shared `Hydrograph` table at
  `sapphire/services/preprocessing/app/models.py:70-73` already
  has `norm`, `previous`, and `current` and the service already
  exposes shared `/hydrograph/` POST/GET endpoints; no service
  edits are needed for the dashboard work.
- **Operator wrapper edits**: the new wrapper
  `bin/yearly_runoff_hydrograph_aggregation.sh` lives in P4 of
  this plan and is not in the downstream dashboard plan's
  scope.

## Pointers for the downstream plan author

- **Live data shape**: see the P3 evidence at
  `doc/plans/working/runoff_long_horizon_hydrograph_e2e_evidence.md`
  (commit `fc22f6d`) for the actual record counts and value
  ranges from the operator's local stack. As of 2026-06-02,
  53 of 63 configured kghm stations produced 12-month + 1-season
  triad rows for target year 2026 (636 monthly + 53 seasonal =
  689 records); the remaining 9 stations are an operator-side
  iEH HF data gap.
- **In-progress year semantics**: for any target year where the
  current calendar date falls inside April-September, the
  seasonal `current` will be `None` (D1 + D2). The dashboard
  must handle this gracefully — show the seasonal previous-year
  / climatology band but explicitly note that the current-year
  seasonal mean is not yet defined.
- **Per-month thresholds**: a station with chronic data gaps in
  some months but solid coverage in others will write
  meaningful means for the well-covered months and `None` for
  the sparse ones (D-Q6). The dashboard should NOT compute its
  own fallback values for these cells; trust the writer's
  `None` decisions and render the cell as missing.

## End of preprocessing-side work

After P4 (operator wrapper + Luigi task retirement) and P5
(this handoff note) commit, the runoff long-horizon hydrograph
plan is complete. The downstream dashboard plan is the next
deliverable but is out of scope for this plan.
```

The shape above is the recommended structure. Adjust wording
sparingly for clarity, but keep:

- The "What this plan delivered" subsection citing commits
  `aeceebe` and `fc22f6d` verbatim.
- The "Downstream scope" subsection naming exactly
  `_get_data_monthly` and `_get_data_season` as the next
  scope.
- The "Explicitly out of scope" subsection preserving the
  three non-goals (quarter writes, API/schema changes,
  operator wrapper edits in dashboard plan).

## Hard constraints (non-negotiable)

1. **Do NOT modify any file outside the handoff path above.**
2. **Do NOT instruct future implementation agents to edit
   `apps/forecast_dashboard/` in THIS plan.** The handoff
   names the scope; the downstream plan owns the
   implementation.
3. **Do NOT include real station codes** in the handoff
   (none should naturally appear; this is here as a guard).
4. **Do NOT commit, push, branch, stage, or stash.** The
   orchestrator commits after deliberation.
5. **Do NOT add commit-hash-cited references for commits that
   don't exist yet.** P4's commit hash is unknown at handoff
   time — refer to P4 by phase name, not by hash. P1, P2,
   P1-fix, and P3 evidence commits all exist and can be
   cited.

## Self-review before returning

1. **Scope check**: `git diff --stat` shows exactly one new
   file at the path above; no other files touched.
2. **Verbatim section names** are present: "What this plan
   delivered", "Downstream scope (next plan, not this plan)",
   "Explicitly out of scope (do NOT expand)", "Pointers for
   the downstream plan author", "End of preprocessing-side
   work".
3. **No real station codes**: grep the file for any 4-5 digit
   string that isn't a year or `19999`.
4. **No instruction to edit forecast_dashboard**: grep the
   file for `Edit ` or `modify ` or `change ` patterns; the
   only acceptable verbs in the downstream-scope section are
   "should update", "should pull", "should surface", etc.

## Deliverable format

Return a single short Markdown report (under ~50 lines):

1. **Summary** — 1-2 sentences: handoff note created;
   downstream scope named; out-of-scope items preserved.
2. **File created** — full path.
3. **Scope check** — confirm exactly one new file; no other
   files touched.
4. **Section check** — confirm the 5 verbatim section names
   are present.
5. **Out-of-scope check** — confirm the 3 non-goals (quarter,
   API/schema, operator wrapper) are preserved in the
   handoff text.
6. **Sensitive-data check** — confirm no real station codes.
7. **Coordination items** (optional) — anything the
   orchestrator should know.

## What success looks like

- One new handoff file at the specified path.
- The file names `_get_data_monthly` and `_get_data_season`
  as the downstream dashboard plan's scope.
- The file explicitly preserves the 3 out-of-scope items.
- No production code changes.
- No instruction to edit `apps/forecast_dashboard/` in this
  plan.

If you encounter an ambiguity (e.g. the recommended structure
above feels too prescriptive for the file's purpose), STOP
and escalate to the orchestrator with a specific question.
Do NOT improvise heavily — the handoff is a small, scoped
artifact.

--- END PROMPT ---
