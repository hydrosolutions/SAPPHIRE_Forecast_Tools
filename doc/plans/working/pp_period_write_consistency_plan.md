# Period-write consistency: PP-045 close-out and PP-060 — implementation plan

**Status:** Draft, not started. Subject to CLAUDE.md's mandatory out-of-loop review
before any phase executes.

**Base:** `origin/maxat_sapphire_2` @ `c9e91f83` (PR #448 merged).

**C8's gate is OPEN as of 2026-08-18** — the parallel session's work landed in PR #448
and the checkout is clean, so T0.2, T1.2 and T4 are unblocked. The gate has opened and
closed twice during this plan's life; **re-check immediately before writing a registry
row** rather than trusting this line. The earlier closure was residual risk 6 arriving
within the hour it was written.

**Scope.** Close out PP-045 honestly, land the two follow-up tickets it owes, and
implement **option (a)** for the write divergence filed as PP-060 — the contract was
decided 2026-08-18 (§4 T2). Option (b) survives only as historical alternative; where
this plan mentions it, that is the record of what was considered, not live work.

**Scope honesty.** Tiers 0 and 1 are documentation only. **Tier 3 changes runtime code**
in `apps/postprocessing_forecasts/` and possibly `apps/forecast_dashboard/`, on paths
that are scheduled and user-invoked. The operational forecast path is working correctly
today and must not change; §2 exists to make that *checkable* — gathering evidence
rather than asserting safety. Note C2 is explicit that the checks are evidence, not
proof.

**Anchoring convention.** Locations are anchored by **symbol or heading plus a quoted
phrase**; line numbers are secondary hints only and are known to drift in this repo.
Grep the quoted phrase, never seek to a line.

---

## 0. What is actually open, verified 2026-08-18

### 0a. Correctness debt already on trunk

1. **PP-045 carries a claim the probe contradicts — but read the scope carefully.**
   §B2 states the DAY archive "has not been queried"; it has been. **On 2026-08-18** a
   read-only probe found, for 2026-07-25/07-31/08-05/08-10: per-model PENTAD rows
   present, NE rows present, DAY inputs present (426 rows each for TSMIXER/TIDE/TFT, 71
   distinct codes), `ENSEMBLE_MEAN` absent, and only 15 of 71 codes covered.

   **What that refutes is "the archive is empty today" — not the historical cause.**
   The probe establishes state on 2026-08-18. It does not establish *when* those rows
   arrived, *which writer* produced them, or what the archive held during the run window;
   `forecasts` has no provenance column and the upsert updates in place. Cause C1 asserts
   that no usable row existed *at aggregation time*, and that remains **unresolved**, not
   disproved. Any wording stronger than that is unsupported — an earlier draft of this
   plan said the diagnosis was "disproved" and attributed the rows to the 08-13/08-14
   hindcast catch-up; neither is established. The refutation was recorded in PR #445 and
   in PP-060 — **never in PP-045 itself.** (Both of those records also over-stated it as a
refutation of the cause; T0.1 must not repeat that.)

2. **PP-045's §G checklist is stale in both directions.** It is supposed to be the
   single live checklist (contract C7 of the previous plan). It currently lists as
   outstanding several items that shipped:

   | §G item | Actually |
   |---|---|
   | `doc/prod/` runbook for the backfill | **Done** — PR #441 |
   | CLI docstring fix 1 (maintenance parenthetical) | **Done** — PR #438 |
   | CLI docstring fix 2 (real precondition, three filters) | **Done** — PR #438 |
   | README recovery procedure | **Done** — PR #441 |
   | `doc/data_flow_short_term.md` recovery path | **Done** — PR #441 |
   | "§E P0 (configuration) — NOT run" | **Run** 2026-08-18 |
   | "§E's database steps — NOT run" | A direct read-only SQL probe was run 2026-08-18; **not** in the §E P2/P3 form specified |

   And one item is mis-scoped: it directs a fix to
   `doc/dev/review_checklist_local_2026-08-14_kyg.md`, which is **gitignored and
   untracked** and holds operational data. P3 corrected the tracked *template* instead;
   the §G item was never updated to say so.

3. **`doc/plans/module_issues.md` PP-045 row** still reads *"Option B implemented
   (branch `fix_postprocessing_boundary_gap`)"*. That branch merged as PR #425 on
   2026-07-23 and no longer exists.

4. **The write-divergence issue is PP-060 — an id collision, resolved by yielding.**
   It was filed as `PP-059` in PR #445 and merged to trunk without an index row (C8's
   gate was closed). A parallel session then allocated **PP-059 to a different issue**
   ("Remove monthly EM"), **which has since landed in the registry via PR #448** — so the
   collision is now settled fact, not a pending clash. Owner decision 2026-08-18:
   **this issue yields and becomes PP-060**; PP-059 stays with the parallel session's
   work. Renamed here across the issue file and this plan in one commit, so no
   intermediate state has the two documents disagreeing. `PP-060` was free on trunk and
   in the working copy at the time, and remains free and unindexed on trunk `c9e91f83`
   after PR #448 landed the parallel session's rows — **still re-verify before T0.2
   writes the row.**

### 0b. Owed but never written

5. **PP-046 update.** Its draft frames the yearless-dedup collapse as a risk "for any
   future multi-year caller". `recalculate_skill_metrics.py::_run_short_term_recalc` is
   an **existing** one, so the defect manifests today. May justify re-rating.
6. **Detect-and-report ticket.** No such file exists under `doc/plans/issues/`.

### 0c. Owner decisions (the four T2 gates) — three settled 2026-08-18, one open

7. **The kyg criterion** (PP-045's last acceptance item) — **still OPEN.** The only
   unsettled T2 gate.
8. ~~**PP-060's contract**~~ — **DECIDED: option (a)** (§4 T2.1).
9. ~~**The dashboard `write_csv` carve-out**~~ — **DECIDED: carve it out**; T3.1 runs as
   its own change (§4 T2.2).
10. ~~**PP-045's carried API-only-by-default flag**~~ — **DECIDED: confirmed**, CSV
    writing stays off by default (§4 T2.4).

**Open items outside these four gates**, so that "only kyg is open" is not misread as
"nothing else needs a decision": **C9** (whether the dashboard's swallowed failures
should now surface) is an explicit product choice, and **PP-060's priority** is still
marked owner-to-confirm.

### 0d. Housekeeping

11. Seven scratchpad worktrees and seven merged branches from this sequence remain. A
    prior cleanup attempt was declined; not re-attempted here without an explicit ask.

---

## 1. Implemented / documented / drift

| Item | Implemented? | Documented? | Drift |
|---|---|---|---|
| Backfill CLI + docstring contract | Yes (#425, #438) | Yes — runbook, README, data-flow (#441) | none |
| Operator runbook, write path | Documented, **write path prohibited** pending a tested rollback | Yes (#441 §7) | Rollback SQL still unwritten — deliberate |
| PP-045 diagnosis (cause C1) | — | Asserted in §B2/§H as the live hypothesis | **Unresolved, not refuted.** The 2026-08-18 probe refutes only "the archive is empty today". Tier 0 restates it |
| PP-045 §G as sole live checklist | — | Asserted | **Stale both ways** (§0a.2) |
| Recalc/operational write divergence | Present in code | Filed as PP-060 (#445) | Unfixed; **contract decided 2026-08-18 (option a)**, implementation pending T3.2 |
| Detect-and-report | No | Recommended in PP-045 §F | Ticket never filed |

---

## 2. Hard contracts

**C1 — Never change `save_forecast_data`'s defaults; change the call sites.**
`write_csv=True` and `require_api=False` are inherited silently by every caller. A
default change is invisible at each call site, cannot be reviewed locally, and would
alter callers nobody examined. Passing explicit arguments at the two or three call sites
that need them is reviewable and leaves every other caller provably untouched. **If a
phase proposes editing the defaults, reject it.**

**C2 — The operational path is the definition of "correctly working". Gather evidence
it is unchanged; do not claim proof.** For any Tier 3 change:
- Parse `postprocessing_operational.py` before and after with `ast`, strip docstrings,
  and compare. **This proves only that that file did not change.** Its behaviour depends
  on `data_reader`, `ensemble_calculator` and `file_writer`; a change in any of those
  alters operational behaviour with an identical AST. Treat it as *scope evidence*, and
  when a shared module is edited, name explicitly which operational behaviour could move
  and cover it with a test.
- Full module suite green. **No existing test may be deleted or weakened, and no
  unrelated test may be edited.** A test whose asserted behaviour is *intentionally*
  changed by the phase may be updated — narrowly, with the change called out in review.
  (Option (a) cannot satisfy a blanket "no existing test edited" rule: existing tests
  assert the recalc's forecast-save happens. An earlier draft of this contract made
  option (a) inadmissible by accident.) **Adding tests is required** — see C5.
- Neither check substitutes for the other.

**C3 — Do not remove `exclude_models=["EM"]` on its own.** PP-030 introduced it because
boundary-date misalignment produced EM rows with `n_pairs` of 1-2. Removing it restores
that defect unless the alignment is fixed first. Contract option (b) is therefore
strictly larger than it looks and must carry PP-030's fix or an explicit waiver.

**C4 — One divergence axis per change, one PR per axis.** PP-060 lists eight. Fixing
several at once makes any regression un-bisectable on a path that is both scheduled and
user-invoked.

**Explicit exception — atomic responsibility removal.** Option (a) removes the recalc's
forecast-write responsibility entirely, which collapses all eight axes in a single edit.
That is admissible *because* it is one decision, not eight fixes — but it forfeits
bisectability, so it must land alone, with nothing else in the PR, and with the seeding
question in T3.2 answered first. Without this exception C4 would make option (a)
inadmissible.

**C5 — Every behaviour change ships with a test that fails before and passes after.**
A behaviour change with only a passing test proves nothing about what changed.

**C6 — Documentation before code, for an evidential reason.** Changing which entrypoint
writes which rows changes the database state, which is the evidence PP-045's diagnosis
rests on. Correct the diagnosis first or the next investigation re-derives it against a
moving target. **Tier 3 must not start before Tier 0 lands.**

**C7 — `sapphire/services/` is read-only here.** The absent provenance column on
`forecasts` (PP-060 open question 3) would make this class of defect diagnosable, but it
is a colleague-managed schema change and needs its own discussion. No migration in this
plan.

**C8 — `module_issues.md` is edited once per phase that needs it, on a clean tree,
re-reading the file to allocate ids.** **The gate is OPEN as of 2026-08-18** (PR #448
landed; checkout clean), and the §0a.4 id collision is settled. It has opened and closed
twice already, so T0.2, T1.2 and T4 must each re-check rather than trust this line.

**C9 — The dashboard change alters failure visibility.** The scoped recalc's failures
are currently swallowed as non-fatal, and the container runner prints rather than raises.
Making them visible is probably right, but it is a **deliberate product decision**, not a
side effect of a CSV fix. Decide it explicitly.

**C10 — No real station codes.** Placeholder `19999` only in anything committed. The DB
observations in §0a.1 are aggregate counts by design.

---

## 3. Tiering rationale

Tier 0 is first because it is zero-risk and removes a **wrong diagnosis from a live
High-priority issue** — the single most misleading artefact currently on trunk. It also
satisfies C6's precondition for any later code work.

Tier 1 (the detect-and-report ticket) is separated from Tier 0 only because it is new
authorship rather than correction; it has no dependency on the code decisions and can
run in parallel.

Tier 2 is a decision gate with no work in it. It exists so that Tier 3 cannot start on a
guess.

Tier 3 is last and is deliberately split so the smallest, highest-value fix (the
dashboard CSV inheritance) can land without waiting for the larger contract to be
*implemented*. It still waits for T2, because the carve-out itself is a T2 decision —
what it skips is T3.2, not the decision.

---

## 4. Phases

### T0.1 — Correct PP-045's diagnosis and reconcile §G

- **Goal:** PP-045 stops presenting an unresolved cause as the live diagnosis, stops
  saying the archive is unqueried, and its live checklist matches reality.
- **Files:** `doc/plans/issues/review_gi_draft_pp_missed_boundary_period_gap.md` only.
- **Depends on:** nothing.

Steps:

1. §B2 — add the 2026-08-18 database probe: what was queried and what was found
   (aggregate counts only, C10). State the scope precisely: **the probe refutes "the
   archive is empty today"; it does not resolve what the archive held at aggregation
   time.** Keep the log-evidence findings unchanged — they remain true. **Do not assert
   when or by which writer the rows arrived**: there is no provenance column, the upsert
   updates in place, and no evidence ties them to the 08-13/08-14 catch-up. The
   defensible sentence is: *on 2026-08-18 the DAY, per-model and NE rows were present and
   EM was absent; when they were created, by which writer, and whether they existed
   during the run window are unknown.*
2. §H — move C1 from "leading hypothesis" to **unresolved**, not to "refuted". Restate
   the observed anomaly: per-model and NE rows present, **EM absent**, 15 of 71 codes
   covered. Record that the cause is **not attributable from the table** (no provenance
   column, in-place upsert) and that PP-060 documents candidate writers without selecting
   one.
3. §G — reconcile against §0a.2: tick what shipped in #438/#441/#442 with its PR
   reference; re-scope the review-checklist item to the tracked template that P3 actually
   corrected; mark the §E database steps as *run in a different form than specified*,
   naming what was and was not covered (the §E P2 derived-frame probe was **not** run —
   a direct SQL query was).
4. Add a Corrections-log row for the retired C1 claim, per the existing C1 contract in
   that file.

- **Acceptance:**
  - No statement in the issue asserts C1 as the live cause, or that the archive is
    unqueried.
  - Every §G item is either ticked with evidence, restated, or explicitly still open.
  - Evidence tags intact: the probe result is **PROVEN** (direct query); any statement
    about *which writer* produced the rows is **INFERRED** at best.
  - One file changed.

### T0.2 — Index rows

- **Goal:** `module_issues.md` matches reality.
- **Files:** `doc/plans/module_issues.md`. **Depends on:** T0.1 (so the PP-045 row can
  cite the corrected state). **Gate:** clean tree (C8).

Steps: correct the PP-045 row (merged via PR #425; current status; the C1-scope
correction made in T0.1); add the row for the write-divergence issue **under whatever id
survives the collision in §0a.4**, re-verifying against the file at execution time.

- **Acceptance:** both rows accurate; no other row touched; `git diff` one file.

### T0.3 — PP-046 update

- **Goal:** PP-046 stops reading as latent.
- **Files:** `doc/plans/issues/mid_prio_gi_draft_pp_get_latest_forecasts_yearless_key.md`.
- **Depends on:** **T2** — see the sequencing caution below; option (a) would immediately
  invalidate the text this phase writes about the recalc.

Steps: record `_run_short_term_recalc` as an existing multi-year caller — unbounded read
(no `start_year`/`end_year`), then `save_forecast_data`, then the yearless dedup — so the
collapse manifests today rather than hypothetically. **Also record
`postprocessing_maintenance.py` as a second existing multi-year caller** — it reads
combined forecasts without date bounds and later writes merged state — which the first
draft of this plan missed entirely. Note the interaction with the
`SAPPHIRE_SKILL_METRICS_START_YEAR` filter, and that the full combined CSV receives a
broader frame than the API payload. Flag the priority for owner re-rating; **do not
re-rate unilaterally.**

**Sequencing — resolved.** T2 chose option (a), so the recalc *will* stop being a caller
once T3.2 lands. Write this text so it survives that: state the maintenance caller
unconditionally, and the recalc caller as "as of trunk `6e28647a`; removed by PP-060
option (a), see T3.2". T0.3 therefore runs after T2 (which it now does) and does not need
revisiting when T3.2 lands.

- **Acceptance:** PP-046 no longer says "future" caller; cross-references PP-060.

### T1 — File the detect-and-report ticket

- **Goal:** the condition becomes visible regardless of cause.
- **Files:** one new `doc/plans/issues/<priority>_gi_draft_pp_*.md`, **and its
  `module_issues.md` row** — `doc/plans/README.md` treats that file as the unified
  registry and expects indexing at discovery, so the row is part of this phase, not a
  deferred afterthought. It is node **T1.2** in the graph. Allocate the id under C8,
  re-reading the file, and coordinate with T0.2 so the two index edits do not collide.
- **Depends on:** nothing for the draft (parallel with T0); T1.2 depends on T0.2.

Content requirements, carried from PP-045 §F so they are not re-derived: report-only, in
`maintenance:postprocessing_forecasts`; **no writes, no exit-code change** (PP-051/PP-055
own the exit contract — do not entangle). State the honest cost: maintenance does not
already own the machinery — `read_combined_forecasts` takes no date bounds, the 13-month
cutoff lives in `gap_detector` relative to the max observed date, and maintenance never
enumerates *expected* boundary dates. A detector needs an expected-boundary calendar, an
active code/model history so retired stations do not alarm, and archive-availability
logic. **Do not describe it as small.**

Add, from this sequence's evidence: the detector should distinguish *absent per-model
rows* from *present rows with EM absent*, because those have different causes and the
latter is not the PP-045 signature.

- **Acceptance:** a junior developer could implement it without re-deriving the cost
  analysis; the two signatures are distinguished.

### T2 — Owner decisions (gate; no work)

> **DECIDED 2026-08-18 — option (a), and CSV output is being deprecated repo-wide.**
> The two are linked: with CSV writing going away, the recalc's only remaining *forecast*
> side effect is the API forecast write, so removing it is a far smaller conceptual
> change than when it also owned two CSV artefacts. (It still writes skill metrics to the
> API — that is its job and is untouched.) **Decisions 1, 2 and 4 are settled as of
> 2026-08-18; only 3 — the kyg criterion — remains open.** See "Consequences of the
> decision" after the list.

1. **PP-060 contract — DECIDED: (a)**, the recalc stops writing period forecasts.
   Option (b) (write the same row set as operational) is **not** being pursued, which
   also means **PP-030 is not pulled in** — the `exclude_models=["EM"]` exclusion stays
   exactly as it is, because under (a) the recalc writes no forecast rows at all. C3
   remains in force for anyone later revisiting (b).

   **Option (a) is one edit but not a small change** — an earlier draft of this plan
   called it "the removal of one call", which understated it badly. Deleting that call
   removes, from *every* short-term recalc: combined and `_latest` CSV generation **and**
   the forecast API write. Now that (a) is chosen, these are **verification items for
   T3.2**, not inputs to the decision:
   - **What actually seeds combined-forecast API state on a new deployment?** Determine
     this empirically; do not assume the recalc does (see the consequences section — the
     evidence suggests its write may already be a no-op on a fresh site). Both
     initialization implementations (`apps/run_locally.sh`, `apps/pipeline/pipeline_docker.py`)
     *finish* with the recalc and have no subsequent operational or backfill step, and
     new-site backfill (`bin/initialize_site_backfill.sh`) also ends on a `BOTH` recalc —
     so **if** the recalc's write is what seeds that state, (a) removes it with no
     replacement. Whether it seeds anything is the open question; answer it before
     building a replacement.
   - **What about the tooling that hardcodes those CSVs?** DB reset and the postprocessing
     `data_migrator` reference them by name, and `doc/prod/backfill_ml_fromfile.md`
     identifies them as what puts short-term ML into the dashboard.
   - **Which documents go stale?** PP-045's writer inventory and §H matrix, the backfill
     CLI docstring, and the review-checklist template all state that the recalc writes
     period rows. Option (a) invalidates each — including text T0.3 would have just
     written.
2. **Whether the dashboard `write_csv` fix is carved out** — **DECIDED 2026-08-18:
   yes, carve it out.** T3.1 runs as its own change rather than waiting for the CSV
   deprecation to absorb it. The §6 dependency graph's default scenario therefore holds
   as written and needs no re-derivation. Scope per "T3.1's status" below: permanently
   the `save_skill_metrics` CSV, plus the combined CSVs on an interim basis until T3.2
   removes that call.
3. **The kyg criterion** — run the full kyg pipeline when available, or waive in writing.
4. **PP-045's API-only-by-default flag** — **DECIDED 2026-08-18: confirmed.** The
   backfill CLI keeps `--write-csv` off by default. Consistent with the CSV deprecation,
   and it means the flag needs no change now; it retires with the CSVs themselves.

**Gating, stated once so the graph and prose agree:** T3.2 is gated on **decision 1**
and T3.1 on **decision 2** — both settled 2026-08-18, so Tier 3's *owner-decision* gates
are cleared. It is **not** otherwise unblocked: C6 still requires all of Tier 0 to land
first, which the graph encodes. T3.1 runs,
so §6's default scenario applies as written and the re-derivation note there is now
hypothetical.

### Consequences of the 2026-08-18 decision

**PP-060 shrinks — by two axes, not three.** Of its eight, exactly two are CSV-shaped:
**CSV writing** and **what each artefact receives**. Both are retired by the deprecation
rather than fixed. (An earlier draft also counted the CSV-backed consistency check; that
is a detail of T3.1, not one of the eight rows.) **Six survive**: the EM row set,
`require_api`, year scope, station scope plus virtual stations, empty-observation
behaviour, and operator controls.

**T3.1's status — option (a) does NOT retire it.** The carve-out existed to stop a scoped
recalc rewriting shared CSVs, and there are two:
- the **combined** forecast CSVs, written via `save_forecast_data` — option (a) removes
  that call from the recalc entirely, so these are retired by (a);
- the **skill-metric** CSV, written via `save_skill_metrics`, which the recalc still calls
  because writing skill metrics is its job. **Option (a) leaves this one live**, so a
  station-scoped dashboard recalc can still overwrite the shared skill-metric CSV with a
  single station's data.

T3.1 is therefore superseded only once the CSV deprecation removes the skill-metric CSV
write. Until then it is a live defect. **DECIDED 2026-08-18 (T2.2): carve it out** — T3.1
runs as its own change, scoped permanently to `save_skill_metrics` (option (a) handles
the combined CSVs via T3.2). The alternative, letting the deprecation absorb it, was
declined. The dependency graph's default scenario therefore applies as written; §6's
re-derivation note is now hypothetical and says how to
re-derive it if not.

**The seeding concern is WEAKER than an earlier draft claimed — verify it before acting
on it.** That draft asserted the recalc's API forecast write is what seeds a new site.
The pipeline *shape* is confirmed — `bin/initialize_site_backfill.sh` has three phases,
ends with the recalc, and nothing after it writes forecasts — but the seeding claim is
**not established**, and the evidence points the other way:
- the init flow regenerates **LR forecasts only**;
- the recalc obtains ML rows by *reading* the already-populated `forecasts` API, so on a
  purged site there is nothing for it to read;
- `api_writer` drops LR rows before the combined write and returns false when nothing
  non-LR remains — so on a fresh site that write plausibly does nothing at all;
- `doc/prod/first_deploy_checklist.md` promises only LR state and skill metrics from a
  fresh deployment.

**Action before T3.2:** establish empirically what a fresh site ends up with in
`forecasts`, and whether removing the recalc's write changes it. If it changes nothing,
option (a) needs no seeding step and the main objection to it disappears. Do not build a
seeding step on the strength of the earlier claim.

**Two deprecation hazards, filed here because no other plan owns them.** Neither is
loud: both log, and both let the run exit zero, so they surface as wrong data rather than
as a failure — the same shape as the seeding gap, a green run with a wrong result,
discovered much later:
1. **CSV fallback readers become stale-data traps.** `data_reader.read_combined_forecasts`
   is API-first with a fallback already marked deprecated in code. Once writing stops it
   serves frozen data when the API is down instead of failing. Remove each fallback in
   the same change as its writer.
2. **`bin/reset_sapphire_db.sh` invokes `data_migrator.py --type combinedforecast`**,
   which reads `combined_forecasts_pentad.csv` / `combined_forecasts_decad.csv` by name.
   **The API cannot be the replacement source here** — the reset drops the database volume
   *before* starting the API, so the restarted API is backed by the empty database it is
   meant to refill. This path needs a pre-reset export, a backup/restore, or a
   regeneration step. Left as is: a stale file repopulates stale rows; an absent file is
   warned about and skipped, leaving the **combined period rows** empty and the migrator
   exiting zero. (Not the whole `forecasts` table — the reset also runs
   `--type forecast`, which can populate DAY rows.)
These belong to the deprecation effort, not to PP-060; record them wherever that work is
tracked. They are noted in `CLAUDE.md` § Data I/O Transition.

### T3.1 — Dashboard scoped recalc must not rewrite the shared skill-metric CSV

**Scope narrowed by decision (a) — with one interim exception.** Option (a) removes the
recalc's `save_forecast_data` call, so the *combined* forecast CSVs stop being at risk
from this path **once T3.2 lands**. In the default ordering T3.1 runs *first*, so it must
still cover them in the meantime; its steps below mark that work interim and T3.2 step 5
deletes it. The artefact that survives permanently, and the reason T3.1 exists at all,
is `save_skill_metrics`, whose CSV write is unconditional and which the recalc still
calls. **That artefact is T3.1's permanent scope; the combined CSVs are temporary scope**
it carries only until T3.2 removes the call ahead of it.

- **Goal:** a single-station user action stops rewriting shared CSVs — after (a), that
  means the skill-metric CSV.
- **Files:** the call sites — `apps/forecast_dashboard/src/vizualization.py`,
  `recalculate_skill_metrics.py`, and possibly a new parameter on
  `file_writer.save_skill_metrics` (see step 1). **Not** `save_forecast_data`'s
  defaults (C1).
- **Depends on:** T0 (C6), T2.2.

**There are TWO shared CSV artefacts at risk, not one.** An earlier draft of this phase
saw only the combined forecast CSVs:

- `save_forecast_data` writes the combined and `_latest` CSVs — gated by `write_csv`,
  which the recalc inherits as `True`.
- **`save_skill_metrics` writes the skill-metric CSV with no `write_csv` parameter at
  all** — its `atomic_write_csv` call is unconditional. A station-scoped recalc
  therefore overwrites the shared skill-metric CSV with one station's data, and there is
  currently *no* way for a caller to opt out. This is arguably the more serious half,
  because skill metrics are what ensemble admission gates on.

Steps:

1. Decide the mechanism. **After decision (a) the only artefact in scope is the
   skill-metric CSV** — the combined CSVs leave via T3.2. (If T3.1 lands *before* T3.2,
   also pass `write_csv=False` on the scoped `save_forecast_data` call as an interim
   guard; drop that half once T3.2 removes the call.) For the skill-metric CSV, either
   add a `write_csv` parameter to `save_skill_metrics` **defaulting to `True`** so no
   existing caller
   changes (C1's principle applied to a new parameter), or stop the dashboard invoking a
   writing path at all. Prefer whichever leaves the unscoped recalc byte-identical,
   since T3.2 will revisit that path.
2. **A `write_csv=False` on `save_skill_metrics` must also suppress the CSV-backed
   consistency check**, which re-reads that CSV to verify the write. Left enabled on an
   API-only run it would verify a scoped frame against stale or absent shared CSV state
   and report a spurious mismatch. Suppress both together.
3. Tests (C5). **Primary, and the only ones that outlive T3.2:** a scoped recalc does not
   touch the shared **skill-metric** CSV; an unscoped recalc's behaviour is unchanged; the
   consistency-check suppression is exercised with `SAPPHIRE_CONSISTENCY_CHECK=true`; and
   the skill **API write and its failure return are preserved** — this phase removes a CSV
   side effect, not the write. *Interim only, if T3.1 lands before T3.2:* the same
   assertions for the combined and `_latest` CSVs. Delete those two when T3.2 removes the
   `save_forecast_data` call, rather than leaving tests asserting a path that no longer
   exists.
4. Decide C9 explicitly: should the dashboard's swallowed failures now surface? If yes,
   it is a **separate** change (C4).

- **Acceptance:** C2 evidence; the scoped path provably writes **no shared skill-metric
  CSV** (and, while T3.2 is outstanding, no combined CSV either); the skill API write
  still happens and still reports failure;
  unscoped behaviour byte-identical; the new parameter (if added) defaults to current
  behaviour; one axis only.

### T3.2 — Implement the chosen PP-060 contract

- **Goal:** the recalc stops writing period forecasts (option (a), decided 2026-08-18).
- **Files:** `recalculate_skill_metrics.py` — the `save_forecast_data` call. The *code*
  edit is one call, but the phase also touches the existing tests that assert that call
  and the documents that describe the recalc as a period-row writer. One decision, not
  one file.
- **Depends on:** T2.1 (settled), T3.1 in the default scenario.

Steps:

1. **Answer the seeding question first** (§T2 consequences): establish empirically what a
   fresh site ends up with in `forecasts`, and whether removing this call changes it. If
   it changes nothing, proceed. If it does, the replacement seeding step must be
   implemented or the risk explicitly accepted in writing **before** this lands.
2. Remove the call. It lands **alone**, under C4's atomic-responsibility-removal
   exception.
3. Update the existing tests that assert the forecast-save occurs — narrowly, per C2.
   Existing tests require two forecast-save calls, four recalc output CSVs, saved
   forecast contents, and forecast-save failure propagation; these are intentional
   expectation changes, not weakened coverage.
4. Refresh the documents that state the recalc is a period-row writer: PP-045's writer
   inventory and §H matrix, the backfill CLI docstring, and
   `doc/dev/review_checklist_local_template.md`. Otherwise the fix ships beside
   documentation asserting the opposite.
5. Drop T3.1's interim combined-CSV assertions, now that the call is gone.

**PP-030 is not in scope.** Under (a) the recalc writes no forecast rows, so
`exclude_models=["EM"]` stays exactly as it is. Only option (b) would have required
reconciling it.

- **Acceptance:** the recalc performs no forecast write; its skill-metric write and
  failure return are unchanged; C2 evidence; no document left asserting the superseded
  writer inventory.

### T4 — Record the decisions and close PP-045

- **Goal:** the thing this plan is named for. Three of T2's four decisions — the PP-060
  contract, the dashboard carve-out and the API-only default — are now recorded in §T2,
  in PP-060, and (for the API default) in PP-045's §G checklist. **The kyg decision is
  the one still unwritten**, because it is the one still unmade. An earlier draft of this plan had no such phase — its stated
  close-out goal was unreachable from its own dependency graph.
- **Files:** `doc/plans/issues/review_gi_draft_pp_missed_boundary_period_gap.md`;
  `doc/plans/module_issues.md`.
- **Depends on:** T2 (the decisions), T0.1 (a corrected file to write into), **T0.2 and
  T1.2** (both edit `module_issues.md`, and C8 forbids concurrent registry edits — the
  three are serialised), and whichever Tier 3 phases the owner runs.

Steps:

1. Record the **kyg** decision in §G — run, waived, or downgraded — with the rationale
   and the decider named. **This is the only T2 decision still unrecorded, because it is
   the only one still unmade.**
2. ~~Record the API-only-by-default decision~~ — already done: PP-045 §G, 2026-08-18.
3. Record PP-060's contract decision (option (a)) and its effect on §H's matrix —
   recorded in PP-060 and this plan; §H's matrix still needs the update.
4. Propose the status transition and the `review_gi_draft_*` → archive move.
   **Do not execute without approval** (owner-owned, per the preceding plan's C2).
5. Final `module_issues.md` pass so the PP-045 row matches the issue's Status.

- **Acceptance:** every §G item ticked, waived with a named decider, or explicitly
  carried forward; index row matches; no decision recorded only in a PR description —
  the failure that produced §0a.1.

---

## 5. Residual risk

1. **The 15-of-71 code gap is unexplained.** The scoped dashboard recalc is a candidate
   but nothing is established. A bounded read-only probe before T2 would sharpen the
   contract decision; without it, (a)/(b) is decided partly on inference.
2. **Rows cannot be attributed to a writer.** No provenance column, and the upsert
   updates in place, so T3's effect on the observed state cannot be confirmed by
   inspecting the table afterwards. Plan verification through logs and tests, not through
   the data.
3. **Changing writers changes the evidence base** — the reason for C6. If Tier 3 lands
   before Tier 0, the next investigator sees a state produced by the fix and diagnoses
   the fix.
4. **The recalc is invoked from more places than first counted**, one of them
   interactive. Beyond the dashboard, the Luigi initialization task and the annual Luigi
   schedule, there are `apps/run_locally.sh` targets (standalone, yearly and
   initialization flows), the `bin/yearly_skill_metrics_recalculation.sh` wrapper, and
   `bin/initialize_site_backfill.sh`. Long-term-only call sites can be excluded; **the
   short-term ones cannot**, and option (a) affects every one of them. Enumerate them
   before Tier 3, not during.
5. **PP-030 is a trap for option (b).** The EM exclusion is load-bearing.
6. **C8's gate can close again** if a parallel session dirties `module_issues.md`.
7. **This plan's own §0 inventory will go stale**, exactly as PP-045's §G did. Re-verify
   at execution rather than trusting the table.
8. **There may be further unconditional CSV writes.** The skill-metric CSV was missed in
   the first draft of T3.1 and found only in review. Before implementing T3.1, grep
   `file_writer.py` for every `atomic_write_csv` call and check which are gated — do not
   assume the three known ones are all of them.
9. **Option (a) *may* strand a new deployment — unverified, and the evidence now points
   away from it.** The concern is that initialization completes with no combined-forecast
   API state and no step that creates it. But the recalc reads ML rows from `forecasts`,
   which a purged site does not have, and the writer drops LR rows, so its write may
   already be a no-op on a fresh site (see the T2 consequences). Verify before treating
   this as a blocker. If real, the risk is invisible on an existing deployment and appears
   only on the next fresh install — the worst place to discover it.
10. **This plan has twice understated a change's blast radius** — first calling option (a)
    "the removal of one call", then covering only one of three shared CSVs. Both were
    caught in review, not by the author. Treat any "this is a small change" claim in
    Tier 3 as unverified until someone has enumerated the call sites.

---

## 6. Dependency graph

Prerequisites and conditionals not expressible in the graph:

- **T3 requires all of Tier 0 landed** (C6), including T0.3 — the graph now encodes this
  rather than leaving T0.3 out of T3.1's dependencies.
- **T0.2, T1.2 and T4 all edit `module_issues.md`** and therefore require a clean tree
  (C8) *and* must not run concurrently with each other. The graph serialises them; the
  C8 gate is an external condition it cannot express.
- **T3.1 runs** (T2.2, decided 2026-08-18), so the graph below applies as written. The
  following is retained only against a future reversal: if T3.1 were ever dropped,
  **re-derive the graph** —
  **re-parent its Tier-0 dependencies onto T3.2 and T4** — that is, T3.2 becomes
  `["T0.1", "T0.2", "T0.3", "T2"]`. Simply deleting the T3.1 edges would leave T3.2 with
  no Tier-0 dependency at all and silently break C6. The JSON below is one named
  scenario, not a representation of both branches.
- Option (b) is not being implemented; its PP-030 prerequisite (C3) applies only if the
  contract is ever revisited.

```json
{
  "phases": {
    "T0.1": { "depends_on": [], "parallel_agents": 1 },
    "T0.2": { "depends_on": ["T0.1"], "parallel_agents": 1 },
    "T0.3": { "depends_on": ["T2"], "parallel_agents": 1 },
    "T1.1": { "depends_on": [], "parallel_agents": 1 },
    "T1.2": { "depends_on": ["T1.1", "T0.2"], "parallel_agents": 1 },
    "T2":   { "depends_on": ["T0.1"], "parallel_agents": 1 },
    "T3.1": { "depends_on": ["T0.1", "T0.2", "T0.3", "T2"], "parallel_agents": 1 },
    "T3.2": { "depends_on": ["T2", "T3.1"], "parallel_agents": 1 },
    "T4":   { "depends_on": ["T0.1", "T0.2", "T1.2", "T2", "T3.1", "T3.2"], "parallel_agents": 1 }
  }
}
```

Changes from the first draft, all forced by review: **T0.3 now depends on T2**, because
option (a) would immediately invalidate the text it writes; **T1 is split** into the
draft (T1.1) and its index row (T1.2), which the graph previously omitted altogether;
**T3.1 depends on all of Tier 0** including T0.3, so C6 is enforced by the graph and not
only by prose; **T4 depends on T0.2 and T1.2** so the three registry-editing phases are
serialised (C8); and **T4 exists at all** — the close-out this plan is named for was
previously unreachable from its own graph.

---

## References

- `doc/plans/issues/review_gi_draft_pp_missed_boundary_period_gap.md` (PP-045), §G and §H
- `doc/plans/issues/high_prio_gi_draft_pp_recalc_backfill_write_divergence.md` (PP-060)
- `doc/plans/issues/mid_prio_gi_draft_pp_get_latest_forecasts_yearless_key.md` (PP-046)
- `doc/prod/backfill_period_forecasts_runbook.md` — write path prohibited pending rollback
- `doc/plans/working/pp045_issue_draft_update_plan.md` — the preceding plan; its P0/P2/P3
  phases are complete
- PRs: #425, #438, #439, #440, #441, #442, #445
- Conventions: `CLAUDE.md` § Orchestration Protocol, § Multi-Model Review;
  `doc/plans/README.md`
