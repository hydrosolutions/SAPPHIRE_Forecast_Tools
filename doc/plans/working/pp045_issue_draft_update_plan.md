# PP-045 — issue-draft update: implementation plan

**Status:** **P0 and P2 DONE** (PR #438). P1, P3, P4, P5 open. Revised three times: twice
against out-of-loop `codex exec` reviews (the first returned NOT SAFE TO COMMIT on 2
Critical + 26 Important findings; the confirm-fixes pass cleared 30 of 37 and drove
changes to C8 and P0), and once — this revision — after the **§8 analysis** closed the
question P1 previously punted on.

**Base:** `origin/maxat_sapphire_2` @ `f4034e52`. The 2026-08-17 re-assessment
(`31370164`) that P1/P4/P5 depend on **is now merged**; §0's blocking dependency is
discharged.

**Target artefact:** `doc/plans/issues/review_gi_draft_pp_missed_boundary_period_gap.md`
(PP-045), 1074 lines on trunk.

**Owner-locked scope:** bring PP-045's issue draft to a state where it is internally
consistent and where a Review → Complete decision is defensible.

**Scope honesty — read this before calling the plan "docs-only".** The *repository patch*
is documentation and string literals only; no runtime behaviour changes anywhere. But the
plan as a whole is **not** side-effect-free: P0 and P4 read shared operational logs and
databases, and P4 step 5 schedules a **real database write** against a live deployment,
conditionally and behind owner authorisation. Any summary of this plan that says
"documentation work only" is wrong.

**Not in scope:** fixing PP-046, PP-047 or PP-048; any change to
`apps/postprocessing_forecasts/` runtime behaviour; building the detect-and-report
feature (this plan updates/creates its ticket, it does not implement it).

**Anchoring convention (deliberate).** Locations are anchored by **section heading or
symbol name plus a quoted phrase**, with line numbers as a secondary hint only. Any
"trunk L…" hint in §2 refers to the **pre-`31370164`** file and is now off by the
re-assessment's insertions — treat those numbers as historical, not navigational. The
sibling plan
`doc/plans/working/pp051_recalc_write_failure_plan.md` records anchor drift as a recurring
failure mode in this repo's plan documents; the first revision of *this* plan carried four
wrong anchors of its own, caught in review. **An executing agent must grep for the quoted
phrase, never seek to a line number.**

---

## 0. The base dependency, stated plainly

**DISCHARGED 2026-08-17.** The re-assessment — sections "A. Code re-verification" through
"G. Remaining checklist to move Review → Complete", referred to below as §A–§G — merged in
PR #438 (`31370164`, now an ancestor of `f4034e52`). P1, P4 and P5 depended on it; they no
longer block. Branch new phase work off current trunk, **not** off the old
`docs_pp045_status_2026-08-17` branch, which is merged and must not be reused.

Retained because it still governs how the phases read: P1 rewrites prose so it agrees with
§A6, P4 edits §B/§C/§G, and P5 works the §G checklist — so all three assume §A–§G is
present in the file they open. It now is.

**The finding P1 exists to propagate**, restated so this plan is standalone: PP-045's
Summary asserts that short-term per-model PENTAD/DECADE rows are created *only* by the
operational code path. That is false on trunk. `recalculate_skill_metrics.py::_run_short_term_recalc`
reads with no year bounds (`:191-194`; the reader's `start_year`/`end_year` default to
`None` = unbounded, `src/data_reader.py:2746-2751`) and then calls
`file_writer.save_forecast_data(config, modelled)` (`:233`). `backfill_period_forecasts.py`
is a third writer; the un-wired `apps/machine_learning/reaggregate_day_to_periods.py` a
fourth. Confirmed by direct code read and by two independent `codex exec` reviewers.

---

## 1. Separating implemented / documented / drift

| Item | Implemented? | Documented? | Drift |
|---|---|---|---|
| Option B backfill CLI (`backfill_period_forecasts.py`) | **Yes** — merged PR #425, `cd97db57`, 2026-07-23 | Only inside PP-045, `module_issues.md`, and *gitignored* local review checklists | **No tracked operator-facing doc exists.** No `doc/prod/` runbook, no README section. P3 closes this. |
| `save_forecast_data(write_csv=, require_api=)` | **Yes** | In-code docstring only | none |
| 23 locked tests in `tests/test_backfill_period_forecasts.py` | **Yes** (23 methods, statically verified) | PP-045 §Verification | Green status is **REPORTED, not proven** — no durable test log exists. |
| Maintenance cannot heal a zero-`combined` date | n/a (this is the defect) | §A3, re-verified 2026-08-17 | none |
| "Only operational writes period rows" | — | Asserted in PP-045 Summary/Problem | **Drift — false.** P1 closes this. |
| The four stranded tjhm days are a PP-045 reproduction | — | Implied by `module_issues.md` and local checklists | **Probably not one.** §8: they look like an input-availability case (nothing ran for 20 days), which PP-045's tool cannot fix. Corroborated, not proven — P4 falsifies. |
| "A re-run heals it / it is permanent across a year boundary" | — | Asserted in PP-045 `## Problem` | **Both need restating.** §8 replaces them with a precondition + per-entrypoint matrix. The issue names only operational's year-scoped read as the cross-year cause; recalc has a *different* one (PP-046's dedup), and the permanence is not unconditional. P1 step 4. |
| kyg end-to-end verification | **No** | DEFERRED in Acceptance Criteria | Blocked on infra since 2026-07-23; P5 forces a decision. |

---

## 2. Defects in the artefact, confirmed by direct inspection

Quoted phrases are exact. Unless noted, each was verified against the file at `8e3fc1bc`.

**D1 — Self-contradiction.** `## Summary` contains "are created **only** by the
operational" (trunk L15). §A6 refutes it. A reader who stops at the Summary — which is
what `module_issues.md` links to — acquires a wrong model of the system.

**D2 — The entry-point count is itself wrong.** `## Context` opens "The
`postprocessing_forecasts` app has two entry points:" (trunk L24). The module has **six
top-level production scripts** with a `__main__` block — short- and long-term operational,
short- and long-term maintenance, `recalculate_skill_metrics.py`, and
`backfill_period_forecasts.py` (test utilities elsewhere in the tree also have `__main__`
blocks and are not counted). P1 must **correct the count**, not merely add a
writers-versus-entry-points distinction — the first revision of this plan made exactly
that mistake.

**D3 — Stale recovery inventory.** `## Problem` contains "**Current recovery options
(both manual / out-of-band):**" (trunk L98), listing a `SAPPHIRE_FORECAST_DATE`
operational re-run and the un-wired `reaggregate_day_to_periods.py`. It omits
`backfill_period_forecasts.py` — the tool this issue exists to deliver, merged three weeks
earlier. Most actively harmful staleness in the file: an operator following the Problem
section reaches for the wrong tool.

**D4 — Obsolete decision framing, in three places.**
`### Approach — OPEN DECISION FOR THE HUMAN OWNER` (trunk L168) and
`### Recommendation (for the owner to weigh, not a decision)` (trunk L221) present A/B/C
as live; the decision was taken 2026-07-17 and shipped, and the section saying so
(`## Decision & Workplan (Option B — backfill entrypoint)`, trunk L238) sits 70 lines
below. **Third instance:** `## Desired Outcome` still reads "The remediation depth is the
open decision below (A/B/C)."

**D5 — Two sections still defer tickets that are already filed.**
`### Latent defect found while mapping (record, do not necessarily fix here)` ends "worth
a separate ticket" → filed as **PP-046**.
`## Secondary anomaly (triage — recommend SEPARATE ticket)` → filed as **PP-048**. Both
are indexed in `doc/plans/module_issues.md`.

**D6 — Three competing "what remains" lists, no stated authority.** Since `31370164`
merged, current trunk carries all three: `## Documentation Impact` (four unchecked boxes),
`## Acceptance Criteria` (one unchecked — the kyg deferral), and `§G`. Nothing tells a
reader which to work from. (Before that merge there were two; the distinction no longer
matters, since the file P1 opens has all three.)

**D7 — An unprovable attribution is being treated as settled.** `module_issues.md` and the
local review checklists attribute the stranded boundary days to PP-045. §C lists six
candidate causes; five mean they are not PP-045. Critically, **no probe of current state
can establish which inputs or configuration existed during the historical run** — see P4.
The plan must not promise a resolution it cannot deliver.

---

## 3. Hard contracts every phase must preserve

**C1 — Preserve the audit trail, with an explicit split.** The issue is the record for a
*merged* PR, so:
- **Preserve verbatim, banner above, no edits inside:** `## Implementation Plan` through
  `### Dependency graph`, and `## Verification (2026-07-23)`. A diff showing deletions
  inside these ranges is rejected.
- **Correct in place, with a dated correction line:** `## Summary`, `## Context`,
  `## Problem`, `## Desired Outcome`. These are the reader's entry point; strikethrough
  there is worse for the reader than a clean sentence plus a recorded correction. Every
  such correction appends one line to a new `## Corrections log` at the end of the file
  giving date, the superseded claim, and why it was wrong. **The original claim survives
  in the log, not in the body.** (The first revision left C1 protecting only the first
  group while P1 silently replaced Summary/Problem text — an internal contradiction.)

**C2 — Do not change `Status:` and do not rename the file.** The status word and the
`review_gi_draft_*` → archive move are the owner's call; `doc/plans/README.md` owns the
status vocabulary (`Open → Draft → Ready → In Progress → Complete`), and `review_` is a
*filename/workflow stage*, not a second competing status list. Note the artefact-level
inconsistency this creates: the target file's header currently reads `**Status**: Review`,
a value that vocabulary does not contain. Flag it for the owner at P5; do not resolve it
as a side effect of this plan.

**C3 — `doc/plans/module_issues.md` is edited once, in P5, on a clean tree.** A parallel
session held uncommitted changes to it on 2026-08-17, including an entry claiming the
`PP-056` identifier which is unused on trunk. Any ID allocation re-reads the *current*
file. **No other phase may edit it** — P4 in the first revision violated this.

**C4 — P2 is literal-only, and the tests are not the proof of that.** In
`backfill_period_forecasts.py`, only the module docstring and `argparse`
`help=`/`description=` string literals may change. **Scope evidence is a reviewed diff
confirming every changed line is inside a string literal** — token- or AST-level if the
diff is large. Passing tests are a *regression check*, not scope proof: unmodified green
tests cannot demonstrate that no behavioural line changed, only that covered behaviour
still works. Both are required; neither substitutes for the other. If any test needs
editing, scope was exceeded — stop and escalate.

**C5 — No real station codes.** Placeholder `19999` only in anything committed.

**C6 — Preserve evidence tags, and never upgrade inference to proof.** §A–§G tag claims
PROVEN / INFERRED / REPORTED. Rewritten prose inherits the tag of the claim it carries. A
tag may be downgraded only by evidence that actually bears on it — see C8 and P4.

**C7 — One live checklist, and historical boxes are frozen.** After P1, §G is the sole
authority. `## Documentation Impact` and `## Acceptance Criteria` become **frozen**
historical snapshots as of 2026-07-23: their checkboxes are never ticked again. Any item
in them still outstanding — notably the kyg deferral — is **migrated into §G**, and §G is
where it is resolved. (The first revision left it ambiguous whether P5 ticked the old
boxes or not.)

**C8 — Write-safety contract, binding on P4 step 5 and any future backfill run.** The CLI
does **not** write only the dates under investigation: `--horizon pentad` still reads and
re-upserts *every period of the whole selected year for every configured station*, with EM
recomputed from current skill metrics. Therefore, before any real run:

1. **Name the target deployment explicitly** and confirm it is the intended one.
2. **Enforce a maintenance window — a point-in-time check is not enough.** A "no writer is
   running right now" check does not hold exclusion across snapshot → write → read-back.
   Disable the cron/orchestrator entries for `postprocessing_forecasts` operational,
   maintenance and recalc (and confirm no other session holds the tunnels) **for the whole
   duration**, and re-enable only after the read-back completes. Record when the window
   opened and closed.
3. **Snapshot the complete write set** — every `(code, date, model)` row of the affected
   horizon and year, values included — not merely the dates of interest.
4. **Build a rollback manifest, not just a snapshot.** Restoring a snapshot of
   pre-existing rows cannot undo rows the backfill *inserted*, because those keys had
   nothing to restore. The manifest must therefore partition the intended write set into
   **keys that already existed** (rollback = restore prior values) and **keys that did
   not** (rollback = delete). Write and test both commands *before* the run; an untested
   rollback is not a rollback.
5. **Verify by comparing the full submitted payload against the read-back**, key and
   value. Reading back the four dates of interest proves nothing about the rest of the
   year.
6. **Do not treat "row counts unchanged on a second run" as idempotence** — that is a
   count check, not a value check.

If any of 2 or 4 cannot be satisfied, the run does not happen. An unauthorised or
un-rollback-able whole-year write is a worse outcome than leaving the question open.

---

## 4. Phasing rationale

P0 runs first because it preserves the only evidence that can ever distinguish cause C4
(input absent *then*, present *now*) — no query of current state can recover it. All three
short-term writers — operational, maintenance and `recalculate_skill_metrics.py` — use
`TimedRotatingFileHandler(when="midnight", backupCount=30)`.

**Corrected 2026-08-17 after running P0 — the earlier urgency framing was wrong.** This
plan previously called P0 time-critical, reasoning from `backupCount=30` to roughly 30
days of retention. That is not how it behaves: rotation happens only on days the job
actually runs, so the 30 retained backups span **2026-04-01 → 2026-08-16**, about four and
a half months. Evicting the 2026-07-24 file would take ~30 further run-days. P0 remains
first — it is cheap and it must precede anything that writes — but it is **not** a race.
The lesson is worth keeping: this premise survived three adversarial review passes and was
corrected only by looking at the directory.

P1 and P2 are independent of each other and run in parallel. P3 depends on P2 because the
corrected docstring is the source text the runbook quotes; writing the runbook first
propagates a wrong precondition into operator-facing documentation, which is the expensive
direction of that error.

P4 is isolated because it is the only phase gated on contended external resources and on
owner authorisation, and because its conclusions are inherently weaker than the plan first
assumed (D7).

P5 is last: every action in it is owner-owned and effectively irreversible.

**Deliberate deviation from §G's own priorities:** §G files the Summary/Problem prose fix
as "non-blocking". P1 elevates it. It is the cheapest item in the plan, actively
misleading today (D1/D3), and upstream of every later phase's quoted text.

---

## 5. Phases

### P0 — Preserve expiring evidence (do this first, today)

- **Goal:** capture logs before they rotate out. Read-only; no analysis yet.
- **Files:** none in the repo. Output goes to an out-of-repo archive location.
- **Depends on:** nothing. **Blocks:** P4's ability to reach a conclusion.

Steps:

1. Copy the postprocessing logs covering 2026-07-20 → 2026-08-17 for **both** tjhm and
   kghm to durable storage outside the repo. Capture **all three** short-term writers'
   logs — `log_operational*`, `log_maintenance*` **and `log_recalc*`**. The recalc log is
   not optional: `recalculate_skill_metrics.py` is itself a writer of period rows (§0), so
   it is a candidate producer of the rows that *do* exist, and it has the same 30-backup
   retention. `apps/logs/` is gitignored; nothing here is committed.
2. Record which rotated files existed and which were already gone — an absence recorded
   now is itself evidence; an absence discovered in P4 is indistinguishable from never
   having looked.
3. Do **not** analyse yet. Analysis is P4 step 2.

- **Acceptance:** an inventory listing exists naming every file captured and every gap in
  the sequence, dated.

**STATUS: DONE — 2026-08-17.** 155 files (2.8 MB) copied to
`~/sapphire_evidence/pp045_2026-08-17/logs/`, with `INVENTORY.md` alongside. Kept outside
any repo: the source is gitignored because it carries operational data. Three results,
recorded here because they change later phases:

- **The run-day sequence is itself evidence.** `log_operational.2026-07-24` (mtime Jul 24)
  is followed directly by `log_operational.2026-08-13` (mtime Aug 13) — a **20-day gap
  with no runs**, spanning all four stranded pentad issue days (07-25, 07-31, 08-05,
  08-10). This is direct corroboration for **C1**, not proof of it: absence of a rotated
  file is strong but not conclusive, and LR rows exist for all four days, so something ran
  later — most plausibly an LR hindcast. P4 must still confirm from contents.
- **Retention is not a race** — see §4.
- **Contamination trap.** The live `log_operational` (no date suffix) has mtime
  2026-08-17 14:10, which is this session's `run_tests.sh`: the backfill tests write into
  `apps/logs/`. Do not read the live file as operational history, and snapshot before
  running the suite.
- **Org attribution is not available from filenames.** The same log paths are reused for
  whichever `.env` was active, so tjhm/kghm separation requires log *contents*.

### P1 — Reconcile the draft with itself

- **Goal:** the narrative sections state only true things; historical sections are visibly
  historical; §G is the single live checklist.
- **Files:** `doc/plans/issues/review_gi_draft_pp_missed_boundary_period_gap.md` only.
- **Branch base:** current trunk (`f4034e52` or later). The old
  `docs_pp045_status_2026-08-17` branch is merged and must not be reused (§0).
  **Depends on:** nothing further.

Steps:

1. `## Summary` — replace "are created **only** by the operational code path" with the
   multi-writer statement, cross-referencing §A6. The boundary-day mechanism the Summary
   describes is still correct; keep it. Log the correction (C1).
2. `## Context` — **correct the count**: "two entry points" is false. State the actual
   executable scripts and distinguish *entry points* from *writers of period rows*,
   pointing at §A6 for the inventory (D2).
3. `## Problem` — retitle "**Current recovery options (both manual / out-of-band):**",
   dropping "both", and add `backfill_period_forecasts.py` as the primary option. Demote
   the two existing bullets to fallbacks, keeping their caveats verbatim — the CSV-rewrite
   side effect and the raw-SQL bypass are both still accurate.
4. `## Problem` — replace the absolute language in the "Net effect" bullets ("no re-run
   heals a missed period", "never recreated at all") with the corrected statement in
   **§8** — the precondition plus the per-entrypoint matrix, **not** a single universal
   rule. Split the tags per C6: the *mechanism* is PROVEN and should be written plainly;
   the *attribution* of the observed gaps to input-unavailability remains **INFERRED**
   until P4 runs, and must stay tagged that way. The earlier draft of this plan punted
   here; its successor over-corrected by saying "write the conclusion, not a hedge",
   which would have laundered an inference into a fact. Do neither.
5. `## Problem` — the self-heal bullet asserts "provided (a) the DAY archive still holds
   those issue dates". Keep it; append a pointer to §C's C1–C6 table, which enumerates the
   ways proviso (a) fails. Do not restate the table (C7).
6. `## Desired Outcome` — "The remediation depth is the open decision below (A/B/C)" is
   stale; the decision was taken and shipped (D4).
7. `### Latent defect found while mapping` — prepend: filed as PP-046. Keep the mechanism
   text; it is the clearest statement of that defect anywhere.
8. `## Secondary anomaly (triage — recommend SEPARATE ticket)` — prepend: filed as PP-048;
   retitle to drop "recommend SEPARATE ticket".
9. Insert the historical banner below `## Implementation Plan`, covering through
   `### Dependency graph`: decision taken 2026-07-17, Option B shipped PR #425 2026-07-23,
   A/B/C not open. **No edits below the banner** (C1).
10. `## Documentation Impact` and `## Acceptance Criteria` — mark frozen as of 2026-07-23;
    live checklist is §G. Migrate the unchecked kyg criterion into §G (C7).
11. Add the `## Corrections log` section required by C1.

- **Acceptance criteria:**
  - No statement above `## Confirmed on trunk 2026-08-17` contradicts §A–§G — verified by
    reading the two halves against each other, not by spot-check.
  - The entry-point count is correct. The stale "open decision" language is corrected in
    `## Desired Outcome`; the two `OPEN DECISION` / `Recommendation` headings inside the
    C1 preserve-verbatim range are **left untouched** and are disarmed by the banner
    instead. (Correcting all three would breach C1 — the first revision of this plan
    demanded exactly that.)
  - Exactly one checklist is live; the other two say so and are frozen.
  - `git diff --stat`: one file. `git diff`: no deletions inside the C1 preserve-verbatim
    ranges.
  - Every in-place correction has a `## Corrections log` entry (C1).
  - Evidence tags intact; the stranded days still read REPORTED (C6).

### P2 — CLI contract corrections

- **Goal:** close §D1/§D2 of the re-assessment plus one further defect found in review.
- **Files:** `apps/postprocessing_forecasts/backfill_period_forecasts.py`.
- **Depends on:** nothing. Parallel with P1.

Steps:

1. Module docstring, "maintenance recalculates skill metrics, not period forecasts"
   (`:3-8`): **both halves are wrong.** Maintenance *reads* skill metrics
   (`recalculate_skill_metrics.py` recalculates them) and *does* write period rows —
   refreshed individual, NE and EM. Anchor by name: the `refresh_parts` build in
   `postprocessing_maintenance.py`, blocks `7a` (stale individual/NE), `7b` (NE gaps),
   `7c` (EM gap-fill + stale EM, comment "requires skill metrics"), and the direct write
   whose comment reads "bypasses get_latest_forecasts". State the real limitation: it
   cannot discover a date with zero `combined` rows (early return on empty combined; the
   `detect_missing_ensembles` call omits `modelled_forecasts`, which
   `src/gap_detector.py` uses to widen the universe), and its write set never emits fresh
   per-model rows for a newly-discovered date.
2. Replace the implied "DAY rows must exist" precondition with the accurate one, **kept
   distinct from the later API-side filter**: for the aggregation to produce a row, the
   merged archive must yield, for that issue date, a row that survives the boundary drop
   *and* the in-period `target` filter (`src/data_reader.py:2386-2439`) — supplied either
   by the DAY archive or by a retained pre-cutover period-archive row
   (`_merge_archives_by_day_cutover`, `src/data_reader.py:2087-2158`). **Separately**, a
   surviving row still reaches the API only if `forecasted_discharge` is non-null
   (`src/api_writer.py:413-423`). A retained period row is not automatically exempt from
   either filter. The first revision conflated these two stages.
3. Name the upstream tools, **with** the caveat that `fill_ml_gaps.py` detects only gaps
   between consecutive existing dates and therefore cannot see an empty archive, a leading
   gap, or a trailing gap.
4. **New defect found in review:** `--start-date` / `--end-date` are documented as
   inclusive date bounds, but the implementation uses only their *years*
   (`for year in range(start.year, end.year + 1)`) and reprocesses each selected year in
   full. Sub-year bounds are silently ignored. Correct both the docstring and the two
   `help=` strings so an operator is not misled into believing the range narrows the work.

- **Acceptance criteria:**
  - Reviewed diff confirms every changed line lies inside a string literal (C4).
  - `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` — zero
    failures, zero unexpected skips, `tests/test_backfill_period_forecasts.py`
    **unmodified** (C4, regression check).
  - `--help` renders; the year-semantics correction is visible in it.

**STATUS: DONE — 2026-08-17.** All four steps applied.

- **C4 scope proof, mechanised.** Rather than eyeballing the diff, both versions were
  parsed with `ast`, docstrings stripped and every `help=`/`description=` value replaced
  with a placeholder; the resulting trees are **identical**. That is positive evidence no
  executable code changed, which a reviewed diff only approximates.
- **Regression check:** 1832 passed, 1 xfailed, 0 failed, 0 skipped — identical to the
  trunk baseline, with `tests/` unmodified (`git status` clean for that path).
- **`--help` verified** to render the year-semantics warning.
- Note for whoever runs this next in a fresh worktree: `run_tests.sh` first reported
  "Errored — could not be verified" because the worktree had no venv. It did **not**
  report a false pass. Run `uv sync --all-extras` in the module directory first.

### P3 — Operator-facing documentation

- **Goal:** the recovery tool stops being invisible in *tracked* documentation. PP-045's
  `## Desired Outcome` makes "the behavior is documented" part of done.
- **Files:** new `doc/prod/<name>.md`; `apps/postprocessing_forecasts/README.md`;
  `doc/data_flow_short_term.md`; `doc/dev/review_checklist_local_template.md`.
- **Depends on:** P2.

Steps:

1. New `doc/prod/` runbook. **Its first section must be "does this tool apply?", per §8** —
   not the commands. An operator whose pipeline simply did not run will otherwise run the
   backfill, see exit 0, and believe the gap is repaired when nothing was written. State:
   the CLI can only re-aggregate inputs that exist. **The check is merged-archive coverage,
   not DAY-row presence** — a retained pre-cutover period-archive row also suffices, and the
   reader falls back to the period archive when there are no DAY rows at all, so "no DAY row
   ⇒ hopeless" is wrong and contradicts §8 and the P2 docstring. Point the operator at the
   §E P2 derived-frame probe, or at establishing that the date lies after the relevant DAY
   cutover. If coverage really is absent, the fix is upstream (`fill_ml_gaps.py` /
   `hindcast_ML_models.py`, with the leading/trailing-gap caveat) and this tool will not
   help. Give the two cases where it *is* the right tool — inputs present but boundary-day
   postprocessing missed, and cross-year recovery where its per-year iteration avoids the
   PP-046 collapse — while noting it is the most controlled cross-year option, not the only
   one (§8 table).
   Then, the operational detail: exact commands with environment
   selection; **one year per internal aggregation/save call** — note that the CLI accepts
   a multi-year range and isolates each year itself, so separate invocations are *not*
   required (the first revision implied they were) — and why the isolation exists
   (yearless-key dedup at `src/file_writer.py:120-122`, before the year filter at `:129`);
   that `--horizon` should be scoped to the affected horizon because each touched year is
   a whole-year read and save for all configured stations; API-only default and what
   `--write-csv` changes; that EM is recomputed against *current* skill metrics so a
   historical backfill is not a replay; the dry-run's limits (it logs totals, never dates
   or codes — the `"%s DRY-RUN: would write %d row(s) (%s); save skipped."` message);
   failure handling; and the full **C8 write-safety procedure**, including that read-back
   must cover the whole submitted payload rather than the dates of interest, because
   PP-047 means `_write_combined_forecast_to_api` can return `True` over a zero or partial
   server write (`src/api_writer.py:445-466`) and `require_api=True` therefore does not
   prove persistence. Name PP-047 so the coupling is discoverable if it is later fixed.
2. `apps/postprocessing_forecasts/README.md` — recovery section; none exists. Link the
   runbook rather than duplicating it.
3. `doc/data_flow_short_term.md` — add the backfill as the recovery path for stranded
   period rows.
4. `doc/dev/review_checklist_local_template.md` (**tracked**) — correct two substantive
   errors: "short-term per-model PENTAD/DECADE rows are written **only** by [the
   operational path]", and the diagnostics row reading "per-model period rows present but
   EM/NE absent ⇒ PP-045", when PP-045 concerns *missing per-model rows* and EM has an
   independent skill gate.
5. **Do not edit the dated local checklists.** `doc/dev/review_checklist_local_20*.md` is
   gitignored (`.gitignore:269`) and untracked — `doc/dev/review_checklist_local_2026-08-14_kyg.md`
   is not in the repository and contains operational data. Correct the tracked template
   only; the owner may optionally annotate their local copy by hand. Never `git add -f`
   one of these. (The first revision instructed editing that file and scheduled a blanket
   decision over all dated copies — both wrong.)

6. **Author the C8 rollback commands — they do not exist.** C8 requires a manifest
   partitioning the intended write set into pre-existing keys (restore prior values) and
   absent keys (delete), with **both commands written and tested before any run**. Nobody
   has written them; the earlier revision said "document C8" and silently assumed they
   existed. Either produce them here, against a scratch target, or state in the runbook
   that rollback is undefined and the procedure must not be run. Do not paper over it.
7. **Mark the procedure unexercised — in the defensible form.** Say: *no maintenance
   window or rollback manifest is documented in the tracked record of the 2026-07-23
   write.* Do **not** assert "C8 has never been executed" — the record is silent on
   whether any such precaution was taken, and silence is not proof of absence. Either way
   the runbook must not read as a validated procedure; documenting something more careful
   than what was previously done is correct, implying it has been proven is not.

- **Acceptance criteria:**
  - An operator who has never read PP-045 can, from `doc/prod/` alone: decide whether the
    tool applies at all (§8) and, **if rollback is concrete**, recover a stranded boundary
    day knowing a full-payload read-back is required.
  - The "does this tool apply?" section precedes the commands.
  - The applicability test is merged-archive coverage, not DAY-row presence (step 1).
  - No runbook statement contradicts the P2 docstring — check them against each other
    explicitly; they assert the same facts.
  - **Rollback resolves one of two ways, and the acceptance differs.** Concrete and tested
    ⇒ the write path is documented and runnable, and the recovery criterion above applies.
    Declared undefined ⇒ the runbook is complete only as a **diagnosis** document; the
    write path is documented as prohibited, and P3 does **not** claim an operator can
    recover a gap. The earlier draft allowed "undefined" while still claiming recovery —
    an unusable, prohibited procedure cannot satisfy a recovery criterion.
  - `git status` shows no gitignored checklist staged.

### P4 — Probe, within its evidentiary limits

- **Goal:** establish **what is true now** about the stranded days, and recover whatever
  the preserved logs can say about what was true then. Not more than that.
- **Files:** the issue file, sections §B / §C / §G. **No other file** — in particular not
  `module_issues.md` (C3).
- **Split by prerequisite (revised 2026-08-17, after P0).** Step 2 — analysing the
  preserved logs — needs **no tunnels and no DB access at all**, and P0 has already
  supplied its input. It can run today. Steps 1 and 3–5 need the live databases. Do not
  report P4 as wholly blocked; the log-only half is available now and may settle the C1
  question on its own.
- **Narrowed by §8, but not settled by it.** §8 makes input-unavailability (C1) the leading
  hypothesis — corroborated by P0's 20-day run gap and the owner's experience — so P4 leans
  toward *falsification* rather than open discovery. It does **not** license concluding C1;
  §8 tags that attribution INFERRED precisely because no live check has run.
  One check §8 makes worthwhile: did `recalculate_skill_metrics.py` run inside the gap
  window (`log_recalc*` is in the P0 archive)? Recalc *can* emit fresh per-model rows, so a
  recalc that ran while inputs were present should have re-emitted them.
  **Read that signal weakly.** Its saved frame is unfiltered *by observations* only — the
  payload still passes the yearless dedup, the two-year filter and the `api_writer` drops,
  the run is scoped to configured codes/models, and it early-returns if observed or
  modelled is empty; PP-047 also means a reported success need not have persisted. And
  because C1 is **date-specific**, rows appearing for some dates and not others is fully
  compatible with C1 — it is *not* evidence against it, as an earlier draft of this bullet
  claimed.
- **Depends on:** P0 (logs) and P1. Steps 1 and 3–5 are **externally blocked** on tunnel
  availability and
  owner authorisation.

**Evidentiary ceiling — the constraint that shapes this phase.** A probe of current
configuration and current database contents establishes *present recoverability*. It
**cannot** establish which inputs or configuration existed during the historical run. The
filter logs are aggregate counts for a whole run and frequently do not name dates or
codes. Therefore P4 **may not** conclude "the cause was C_n" from present state alone,
**may not** detach the observations from PP-045 on that basis, and **may not** clear the
REPORTED tag except for the specific sub-claims the evidence actually reaches. Doing so
would present inference as proof and violate C6. This was the first revision's most
serious error.

Steps:

1. Run §E's P0–P3 **read-only**, independently on tjhm and kghm. §B is two observations,
   not one confirmed shared cause: the kghm record self-labels its attribution as
   underdetermined and separately records an LR hindcast, so LR row presence carries no
   information about when those rows were created.
2. Analyse the P0 archive for the aggregate filter signals, recording for each whether it
   fired, with what count, and whether it can be attributed to specific dates. Where it
   cannot, say so.
3. Record the result as a **present-state finding** against the C1–C6 table, with an
   explicit statement of what remains undetermined about the historical run. Downgrade
   REPORTED only for sub-claims the evidence reaches.
4. If the present state shows the days are **not currently recoverable** (C1–C5
   conditions), record that PP-045's tooling cannot heal them and that the owning module
   is elsewhere — as a *finding*, not as a re-attribution of historical cause. Any
   consequent edit to `module_issues.md` is deferred to P5 (C3).
5. **Only** if the present state shows the days *are* recoverable and the question is
   whether the write path drops them, **and** with explicit owner go-ahead: execute the
   §E P4 write verification under the **full C8 procedure**. `--horizon pentad`, dry-run
   first for shape, full write-set snapshot, full-payload read-back parity, value-level
   (not count-level) idempotence check.

- **Acceptance criteria:**
  - Every §E step recorded as run / not-run with its result — no silent omissions.
  - Every conclusion is scoped "as of <date>, present state" unless a preserved log
    supports a historical claim, in which case the log is cited.
  - The residual unknown about the historical run is stated explicitly rather than
    resolved by assertion.
  - If step 5 ran: the C8 checklist is reproduced with each item ticked and evidenced.

### P5 — Close out

- **Goal:** a defensible Review → Complete decision, and the follow-ups landed.
- **Files:** the issue file; `doc/plans/module_issues.md`; PP-046's draft; one new
  `gi_draft_*` file.
- **Depends on:** P1, P2, P3, P4.

Steps:

1. Resolve the kyg deferral — now migrated into §G (C7). The criterion as written requires
   **the full kyg short-term pipeline** end-to-end. Running only the backfill against local
   kghm data is **not** equivalent evidence, and still needs a configured API/database plus
   write authorisation; the first revision wrongly offered it as a no-server substitute.
   Real options: run the full pipeline when kyg is available, or formally waive/downgrade
   the criterion with written rationale. **Owner decides.**
2. Land the follow-ups per §7.
3. Propose the status transition and the archive move. **Do not execute without approval**
   (C2); flag the `Status: Review` vocabulary mismatch at the same time.
4. `doc/plans/module_issues.md` — the PP-045 row still reads "Option B implemented (branch
   `fix_postprocessing_boundary_gap`)". Update to merged-via-PR-#425 and reflect P4's
   finding. **Last action, clean tree only** (C3).

- **Acceptance criteria:**
  - Every §G item ticked or carrying a written waiver naming the decider.
  - `module_issues.md` PP-045 row matches the issue file's Status.
  - Frozen historical checklists untouched (C7).

---

## 6. Residual risk — do not summarise away in the PR description

1. **~~The base is unmerged.~~ RETIRED 2026-08-17** — `31370164` merged in PR #438. Branch
   new work off current trunk; the old branch must not be reused.
2. **Anchor drift.** Line numbers here are hints only; the first revision of this plan
   shipped four wrong ones (`file_writer.py:118`/`:124` for what is `:120`/`:129`; a
   dry-run log line; a checklist line). Grep the quoted phrase.
3. **P4 is not schedulable** — contended shared infrastructure. The plan must not be
   reported as blocked overall because P4 is blocked; P0–P3 deliver most of the value.
4. **Present-state probes cannot prove historical cause.** The central question — why
   those days were never written — may be permanently unanswerable if the logs have
   rotated. P0 is the only mitigation and it is time-critical.
5. **Whole-year, all-station blast radius.** Any real backfill run rewrites every period of
   the selected year for every configured station, with EM recomputed from current skill.
   C8 exists for this. Note specifically that a snapshot alone does **not** make the run
   reversible — rows the backfill *inserts* have no prior value to restore, which is why
   C8 requires a rollback manifest partitioned into pre-existing versus absent keys.
6. **~~Historical logs may already be gone.~~ RETIRED 2026-08-17** — P0 ran and the
   archive spans 2026-04-01 → 2026-08-16. The risk was real in principle and false in
   fact. Kept visible rather than deleted, because the *reasoning* that produced it
   (inferring retention from `backupCount=30` instead of listing the directory) is the
   recurring error, and it survived three adversarial review passes.
7. **Gitignored operational records.** The dated local checklists contain operational data
   and are untracked by design. Any instruction to edit or commit them is a data-handling
   violation; P3 step 5 is the guard.
8. **P1 edits the audit record of a merged PR.** C1's split rule plus the corrections log
   is the mitigation; a phase that "tidies" the preserved sections destroys the trail.
9. **`module_issues.md` conflict risk** is real and recurring — a parallel session held it
   dirty during the 2026-08-17 session. C3 costs nothing to obey.
10. **P4 could invalidate PP-045's framing.** If the days turn out to belong to another
    module, P5's index row must say something quite different from what is currently
    expected — and, per the evidentiary ceiling, it may have to say "undetermined".

---

## 7. Follow-up filing (P5)

**Allocate any new ID against the current `module_issues.md`, not trunk's** — `PP-056` is
unused on trunk but claimed by an uncommitted entry in a parallel working copy (C3).

1. **Update PP-046 — do not file a new ticket.** `mid_prio_gi_draft_pp_get_latest_forecasts_yearless_key.md`
   already covers the class: "Anything that feeds `save_forecast_data` more than one
   calendar year at once writes only the latest year's rows". But it frames the risk as a
   footgun "for any future multi-year caller". **An existing caller already does this:**
   `recalculate_skill_metrics.py::_run_short_term_recalc` reads unbounded (`:191-194`) and
   saves through `save_forecast_data` (`:233`), which dedups to at most one row per
   `(code, period_in_year, model_short)` (`src/file_writer.py:120-122`) before the
   two-year filter (`:129`). Net: at most one row per period-in-year reaches the API
   regardless of how many years were read. Rows are upserted rather than deleted, so the
   mechanism is a silent **under-write**, not data loss — though rows missing from older
   years stay missing. This converts PP-046 from latent to actively manifesting and may
   justify re-rating its priority. Filing a second ticket would duplicate it.
2. **New ticket — detect-and-report for stranded boundary days.** Report-only, in
   `maintenance:postprocessing_forecasts`; no writes, no exit-code change. Cost note for
   the ticket, stated honestly: maintenance does **not** already own the machinery.
   `read_combined_forecasts` / `_read_combined_forecasts_api` (`src/data_reader.py:791-847`)
   take no date parameters — the read is unbounded; the 13-month cutoff lives inside
   `gap_detector.detect_missing_ensembles` as
   `cutoff = max_date - pd.DateOffset(months=max_lookback_months)`, relative to the maximum
   *observed* date; and maintenance builds its universe only from existing pairs, never
   enumerating *expected* boundary dates. A zero-row detector therefore needs an
   expected-boundary calendar, an active code/model history so retired stations do not
   alarm, and archive-provenance logic so an upstream gap is reported as such. Do not
   describe this as small. Do not attribute the exit contract to PP-051/PP-055 — those
   concern skill-metric writer outcomes, not maintenance's exit code.

---

## 8. What actually heals a missed period (analysis, 2026-08-17)

This section closes the question P1 step 4 previously punted on. It is the substantive
content P1 and P3 must carry, so it is stated once here and referenced, not repeated.

**Revised after out-of-loop review.** The first draft of this section stated a single
universal rule — "any re-run that re-aggregates heals if and only if inputs exist; the
asymmetry is not script-specific". That was **wrong**, and wrong in a way that erased the
distinction PP-045 is built on: maintenance re-aggregates and still cannot heal. The
corrected form separates a precondition that *is* universal from an ability that is not.

### The two-part rule

> **Precondition (universal *for the app path*).** No entrypoint that goes through the
> normal reader can emit a period row unless the merged archive still yields a usable
> input row for that issue date — one surviving the boundary drop and the in-period
> `target` filter, with a non-null discharge at the API write. A gap whose inputs were
> never produced is unhealable by any of them. **The raw-SQL reaggregator is outside this
> precondition** — it builds its own aggregation from DAY records and upserts directly,
> which is exactly why it is not a like-for-like substitute.
>
> **Ability (entrypoint-specific).** Meeting the precondition is not sufficient. Each
> entrypoint has its own limit, and **maintenance cannot write a fresh per-model row for
> a missed date at all.**

Note the distinction the table draws between *reach of the read* and *reach of what
actually lands* — an earlier draft conflated them for recalc.

| Entrypoint | Re-aggregates? | Emits a *fresh* per-model row? | Reach of what actually lands | Its own limiter |
|---|---|---|---|---|
| `postprocessing_operational.py` (boundary day) | whole current year | **Yes** | current year | Year-scoped read ⇒ never touches prior years |
| `postprocessing_maintenance.py` | only for dates it discovers | **No** | **Mixed, and not uniformly bounded.** Gap and stale-quantile detection are limited to `gap_detector`'s lookback (default 13 months back from the max `combined` date), but the **stale-EM scan is unbounded** — it filters `combined` directly, with no cutoff, so it can reach the whole history | Universe built solely from existing `combined` rows; early-returns on empty `combined`; `refresh_parts` never emits fresh non-stale individual rows (its individual-model writes require an existing stale key; genuinely new rows are NE/EM only). Writes direct to the API, bypassing `get_latest_forecasts` |
| `recalculate_skill_metrics.py` | unbounded (all years) | **Yes** | **only the latest year and latest-1** — the read reach is *not* the emission reach | Start-year filter drops pre-`SAPPHIRE_SKILL_METRICS_START_YEAR` rows; then yearless dedup + two-year filter (PP-046) collapses any `period_in_year` also present in a later year. Early-returns if observed **or** modelled is empty |
| `backfill_period_forecasts.py` (PP-045) | per year, ascending | **Yes** | any year in range | Its per-year iteration is precisely what defeats PP-046 |
| operational with `SAPPHIRE_FORECAST_DATE` | that year | **Yes** | one chosen year | **The chosen date must itself be a boundary date** or the entry gate skips the run entirely; also rewrites that year's combined CSVs from scratch |
| `reaggregate_day_to_periods.py` (raw SQL, un-wired) | its own DAY grouping | **Yes — individual *and* NE rows** | any | Outside the app path: ignores the source `target` (recomputes `date + 1`), skips the `api_writer` LR-drop/null-drop/dedup, sets `horizon_value=0`. It emits no EM of its own, but its SQL does not exclude source EM rows, so "never writes EM" is **not** code-guaranteed |

Two consequences worth stating explicitly, because the earlier draft got both wrong:

- **Cross-year is limited by two different mechanisms, not one.** For operational it is the
  year-scoped read; for recalc it is PP-046's dedup. The issue currently blames only the
  former. Also, "cross-year gaps stay unhealed" is **not unconditional** — a prior-year row
  for a `period_in_year` that is *absent* from later years survives the dedup and the
  two-year filter, so it can be written.
- **The backfill CLI is not the only cross-year option**, only the most controlled one. A
  `SAPPHIRE_FORECAST_DATE` operational re-run reaches a chosen historical year, and the
  raw-SQL reaggregator reaches any year. P1 keeps both as documented fallbacks.

### The old-heals / recent-does-not asymmetry

Mechanism (**PROVEN** from code): dates before each (code, model)'s first DAY issue date
are served from the retained migrated **period archive** via
`_merge_archives_by_day_cutover`; those rows carry `target = date + 1` (set by both the
migrator and the normal writer) and so pass the in-period filter and re-emit unchanged.
Dates in the DAY era require a real DAY row.

Attribution to the observed cases (**INFERRED, not verified**): that the specific gaps
operators have seen fall on the far sides of that cutover. Corroborated by P0's 20-day run
gap and by the owner's operational experience (2026-08-17: every recent gap observed was on
a day the pipeline did not run). **No live-data check has been run** — first-DAY dates per
(code, model) are still unqueried; that is P4. Do not let P1 or P3 render this as settled.

### Hypothesis tested and REFUTED — do not reintroduce it

An earlier reading proposed the recalc path is *observation-gated*: because skill metrics
need measured discharge, a period that has not completed gets skipped, producing the
asymmetry. **The code does not do this.** Recorded so it is not re-derived:

- The observation merge — `pd.merge(simulated, observed[["code","date","discharge_avg",
  "delta"]], on=["code","date"])`, no `how=` ⇒ inner — produces `skill_metrics_df`, which
  feeds **only the skill statistics**. It never filters the frame returned for saving.
- `recalculate_skill_metrics.py` passes `exclude_models=["EM"]` (PP-030), taking the
  "Skipping EM ensemble derivation (excluded)" branch where
  **`joint_forecasts = simulated.copy()`**. Note this is *recalc-specific*: without EM
  exclusion, the joined frame does influence generated EM rows.
- The only row-removing operation on `simulated` inside the function is the start-year
  filter (`SAPPHIRE_SKILL_METRICS_START_YEAR`, default `today.year - 20`, compared by
  calendar year), which cuts off *very old* data — the opposite direction from the
  observed asymmetry.

**Wording caution.** Say the recalc frame is unfiltered *by observations*. Do **not** call
it "the unfiltered saved frame": what reaches the database is `simulated_latest`, after the
yearless dedup and two-year filter, then the `api_writer` LR-drop, null-drop and dedup.
The earlier draft's "unfiltered" wording was materially false for the write.

### What this means for PP-045's own tool

**The backfill CLI does not address the most common real-world cause of the symptom.** When
the pipeline did not run, there is nothing to aggregate. Its genuine value is narrower:

1. **Inputs exist but the boundary-day postprocessing was missed** — ML produced its DAY
   forecasts, postprocessing failed or was skipped.
2. **Cross-year recovery where inputs exist** — where its per-year iteration avoids the
   PP-046 collapse that defeats an unbounded recalc. This is its strongest justification,
   though not an exclusive capability (see the table).

If inputs are absent the operator's next step is upstream — regenerate the ML DAY forecasts
(`fill_ml_gaps.py` / `hindcast_ML_models.py`), with the standing caveat that
`fill_ml_gaps.py` sees only gaps *between* existing dates and may miss a leading or
trailing one. **Establish coverage before reaching for the backfill**, or the run reports
success having written nothing new.
## 9. Dependency graph

Prerequisites not expressible in the graph, and binding regardless of it: **all phases
except P0, P2 and P3 require commit `31370164` in the branch base** (§0); **P4 requires**
the P0 log archive, DB/tunnel availability, and owner authorisation, plus C8 for its
step 5; **P5 requires** a clean `module_issues.md` tree (C3) and, for its kyg option, a
configured kyg deployment with write authorisation.

```json
{
  "phases": {
    "P0": { "depends_on": [], "parallel_agents": 1 },
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": [], "parallel_agents": 1 },
    "P3": { "depends_on": ["P2"], "parallel_agents": 1 },
    "P4": { "depends_on": ["P0", "P1"], "parallel_agents": 1 },
    "P5": { "depends_on": ["P1", "P2", "P3", "P4"], "parallel_agents": 1 }
  }
}
```

---

## References

- Issue draft: `doc/plans/issues/review_gi_draft_pp_missed_boundary_period_gap.md`
- Shipped fix: PR #425, `cd97db57` (2026-07-23); commits `cce5922a`, `62bbba65`
- 2026-08-17 re-assessment: commit `31370164`, branch `docs_pp045_status_2026-08-17`
- Related: PP-046 (yearless key — updated by §7, not duplicated), PP-047 (write reports
  success on zero/partial), PP-048 (decade EM freeze), INFRA-024 / INFRA-026 (validator
  cannot see this class of gap)
- Conventions: `CLAUDE.md` § Orchestration Protocol, § Multi-Model Review;
  `doc/plans/README.md` (status vocabulary — see C2 on the disagreement);
  `doc/dev/agent_review_workflow.md`
