# PP-045 — issue-draft update: implementation plan

**Status:** Draft, not started. Revised twice against out-of-loop `codex exec` reviews
(read-only, fresh context). The first pass returned NOT SAFE TO COMMIT with 2 Critical and
26 Important findings (P4 causal overclaiming and P4 write-safety were the Criticals); the
confirm-fixes pass cleared 30 of 37 and drove a second revision, chiefly to C8 and P0.

**Base:** `origin/maxat_sapphire_2` @ `8e3fc1bc`, **plus** the 2026-08-17 re-assessment
commit `31370164` on the unmerged branch `docs_pp045_status_2026-08-17`. See §0 — this is
the plan's largest dependency, and P1/P4/P5 are void without it.

**Target artefact:** `doc/plans/issues/review_gi_draft_pp_missed_boundary_period_gap.md`
(PP-045), 624 lines on trunk, 1074 after `31370164`.

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
symbol name plus a quoted phrase**, with line numbers as a secondary hint only. Line
numbers in the target file are invalidated the moment `31370164` merges. The sibling plan
`doc/plans/working/pp051_recalc_write_failure_plan.md` records anchor drift as a recurring
failure mode in this repo's plan documents; the first revision of *this* plan carried four
wrong anchors of its own, caught in review. **An executing agent must grep for the quoted
phrase, never seek to a line number.**

---

## 0. The base dependency, stated plainly

The 2026-08-17 re-assessment — sections "A. Code re-verification" through "G. Remaining
checklist to move Review → Complete", referred to below as §A–§G — is **committed but not
merged**: branch `docs_pp045_status_2026-08-17`, commit `31370164` (parent `8e3fc1bc`),
one file changed, +453/−3.

Consequences an executing agent must respect:

- **P1, P4 and P5 all depend on it.** P1 rewrites prose so it agrees with §A6; P4 edits
  §B/§C/§G; P5 works the §G checklist. Only **P2 and P3 are genuinely independent** — the
  first revision of this plan wrongly claimed P2–P5 were.
- **Branch P1 off `docs_pp045_status_2026-08-17`, not off trunk**, or the two edits
  conflict in the same file. Treat plan and re-assessment as one review unit.
- If the owner rejects or materially revises the re-assessment, P1 must be re-planned,
  not patched.

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
| The four stranded tjhm days are a PP-045 reproduction | — | Implied by `module_issues.md` and local checklists | **Unproven, and may be unprovable** — see §2 D7 and P4. |
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

**D6 — Competing "what remains" lists, no stated authority.** On **trunk**, two:
`## Documentation Impact` (four unchecked boxes) and `## Acceptance Criteria` (one
unchecked — the kyg deferral). On the **combined base** (trunk + `31370164`), three, with
§G added. Nothing tells a reader which to work from. The defect is real on the base this
plan executes against; it is not a trunk-only defect.

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

P0 runs **first and immediately**, before any other work: it preserves evidence that
expires. All three short-term writers — operational, maintenance and
`recalculate_skill_metrics.py` — use `TimedRotatingFileHandler(when="midnight",
backupCount=30)`, so logs age out on a rolling basis. Note the retention is a
*rotation count*, not a wall-clock TTL: 30 backups equals ~30 days only if rollover
happens daily, which it does not on days the job never runs. Either way, 2026-07-20 is
28 rollovers back as of 2026-08-17 — at the edge. Deferring preservation until P4 — as
the first revision did — risks
losing the only evidence that can ever distinguish cause C4 (input absent *then*, present
*now*), because no query of current state can recover it.

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

### P1 — Reconcile the draft with itself

- **Goal:** the narrative sections state only true things; historical sections are visibly
  historical; §G is the single live checklist.
- **Files:** `doc/plans/issues/review_gi_draft_pp_missed_boundary_period_gap.md` only.
- **Branch base:** `docs_pp045_status_2026-08-17` (§0). **Depends on:** nothing further.

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
4. `## Problem` — qualify the absolute language in the "Net effect" bullets ("no re-run
   heals a missed period", "never recreated at all"). Those were written when operational
   was believed to be the only writer; a recalc run is now known to re-save period rows,
   and the raw-SQL script can too. Add the qualification; do **not** attempt to re-derive
   what the corrected net effect is — that needs analysis this plan has not done, so state
   the uncertainty and leave it.
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

### P3 — Operator-facing documentation

- **Goal:** the recovery tool stops being invisible in *tracked* documentation. PP-045's
  `## Desired Outcome` makes "the behavior is documented" part of done.
- **Files:** new `doc/prod/<name>.md`; `apps/postprocessing_forecasts/README.md`;
  `doc/data_flow_short_term.md`; `doc/dev/review_checklist_local_template.md`.
- **Depends on:** P2.

Steps:

1. New `doc/prod/` runbook. Must contain, at minimum: exact commands with environment
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

- **Acceptance criteria:**
  - An operator who has never read PP-045 can recover a stranded boundary day from
    `doc/prod/` alone, including knowing a full-payload read-back is required.
  - No runbook statement contradicts the P2 docstring — check them against each other
    explicitly; they assert the same facts.
  - `git status` shows no gitignored checklist staged.

### P4 — Probe, within its evidentiary limits

- **Goal:** establish **what is true now** about the stranded days, and recover whatever
  the preserved logs can say about what was true then. Not more than that.
- **Files:** the issue file, sections §B / §C / §G. **No other file** — in particular not
  `module_issues.md` (C3).
- **Depends on:** P0 (logs) and P1. **Externally blocked** on tunnel availability and
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

1. **The base is unmerged.** If `31370164` is rejected or revised, P1/P4/P5 are void.
   Mitigation: branch off `docs_pp045_status_2026-08-17`; review both as one unit.
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
6. **Historical logs may already be gone.** 30 rotated backups puts 2026-07-20 at the
   edge as of 2026-08-17. P0 may find the evidence already lost — record that outcome
   explicitly rather than leaving it indistinguishable from "not yet checked".
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

## 8. Dependency graph

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
