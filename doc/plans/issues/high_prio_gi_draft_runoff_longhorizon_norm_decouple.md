# Long-horizon hydrograph: decouple row existence from iEH-HF monthly norm

**Priority:** high — currently breaks the Tajik monthly/quarterly/seasonal bulletin
(empty last-year runoff column for ~all stations).
**Module:** `apps/preprocessing_runoff` (fair game). Idempotency sub-item touches
`sapphire/services/preprocessing` (colleague-managed) — see Phase P3, coordinate.
**Found:** 2026-07-07, read-only end-to-end diagnostic. Line numbers from branch
`develop_forecast_skill_eval_phase4`.

## Summary

In the forecast dashboard the Tajik monthly bulletin renders an **empty last-year
runoff** hydrograph table. Root cause is a **code defect layered on a genuine upstream
data gap**:

- `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py` is the writer of
  `MONTH`/`QUARTER`/`SEASON` hydrograph rows (run yearly by
  `bin/yearly_runoff_hydrograph_aggregation.sh`). Pentad/decade come from a separate
  daily local-data pipeline with no norm dependency. *(Caveat: "sole writer of these
  horizons" is asserted from the current tree; confirm no other job/migration writes
  MONTH/QUARTER/SEASON before relying on it operationally — see Open questions.)*
- `write_station_monthly_hydrograph` fetches the monthly discharge **norm** from the
  iEH-HF SDK (`:192`) and, if it is not exactly 12 values, **skips the entire station**
  (`:203` `return []`). An empty monthly result then cascades to skip season+quarter
  (`:375` `if not monthly_records: continue`).
- But `previous`/`current` — the **last-year runoff the dashboard actually shows** — are
  computed from **local** daily SAPPHIRE runoff (`monthly_mean_threshold_80`,
  `build_monthly_records` `:153-162`), **independent of the norm**.

So a missing climatological norm discards locally-computable observations. The bulletin's
empty table = this coupling defect. The empty `norm` column = the genuine gap.

## Evidence (diagnostic, read-only)

- DB `hydrographs` (local dev DB): Tajik (prefix-17) = 17/17 distinct codes at
  DAY/PENTAD/DECADE, but **1/17** at MONTH/QUARTER/SEASON. Kyrgyz ~52 codes at month.
  Rows **missing**, not NULL. The 16 uncovered stations have ~70 yrs of local daily
  discharge, current to now.
- **Live iEH-HF SDK probe over the Tajik tunnel:** `get_norm_for_site(code,"discharge",
  norm_period="m", automatic=False)` returns **length 0 for all 17 stations** — including
  the single station that still has DB rows (**inferred** to be stale from an earlier run
  when it transiently had 12 monthly norms — the code proves "a clean run today writes
  none," not how that one row was originally created; treat as a hypothesis until the
  production DB + scheduled-job history confirm it). Manual **daily/decadal** norms (length 36)
  exist for 15 of them; `automatic=True` errors for all. So the gap is precisely:
  **manual monthly discharge norms are not populated in Tajik iEH-HF for any station.**
- Ruled out (both reviewers required this): **station-list filtering** — all 16 codes
  are `data_source="ieh_hf"`, none manual-excluded, all present in today's
  `config_station_selection.json`, so all are resolved and attempted; **partial run** —
  Jun-11 aggregation logs iterate cleanly through all 17, each logging `got 0 - skipping`,
  no crash.

## Terminology: the norm classifier (parametrize this exactly in tests)
A **valid monthly norm** is *a sequence of exactly 12 finite numeric values* → status
**written**. Every other *successful* SDK return is **norm-absent** (write local
`previous`/`current`, preserve any stored norm) — **never** a crash. A *raised* SDK call is
**sdk-failed** (skip station). Malformed *successful* payloads must **not** be reclassified
as sdk-failed. The exhaustive mapping (each row a test param):

| SDK return | Status |
|---|---|
| list/tuple of exactly 12 finite numerics | **written** |
| `None` | norm-absent |
| `[]` | norm-absent |
| partial list (len 1, 11, 13, …) | norm-absent |
| len-12 containing `None` | norm-absent |
| len-12 containing `NaN` / `inf` / `-inf` | norm-absent |
| len-12 containing string/object | norm-absent |
| bare string (incl. len-12 string) | norm-absent |
| dict (incl. 12 keys) | norm-absent *(unless the real SDK shape intentionally supports dict — resolve Open-question #4 first)* |
| non-iterable scalar/object | norm-absent |
| SDK **raises** (transport / tunnel / auth / API) | **sdk-failed** |

## Proposed fix

### 1. Decouple row existence from norm availability (primary)
When the monthly norm is **norm-absent** (as defined above — SDK call *succeeded* but
returned no valid 12-value set), still build and write the 12 monthly records — plus the
season and quarter records — with the locally-computed `previous`/`current`, and with the
norm **preserved** per #2 (not blindly nulled). Season/quarter means already null-out
gracefully (all-or-nothing at `:238` / `:301`). `norm=None` is tolerated by the API schema
and dashboard (`schemas.py:58/82`, `models.py:72` nullable; skill-eval drops
non-finite/non-positive norms so no false below-norm pairs).

Keep a `logger.warning` so the climatology gap stays visible. On an **sdk-failed**
station, do **not** write a norm-less row — skip and preserve for the next run so a
transient failure can't degrade good data.

### 2. Preserve existing norm on a norm-absent write (BLOCKER — the crux, must ship with #1)
**Verified 2026-07-07:** the naive "omit the `norm` key" approach does **not** work.
`crud.create_hydrograph` (`crud.py:88`) is an upsert that calls `item.model_dump()`
**without** `exclude_unset`/`exclude_none` (`:91`); `HydrographCreate.norm` defaults to
`None` (`schemas.py:82`); and for an existing row it runs a blind
`for k,v in data.items(): setattr(existing, k, v)` (`:107-110`) whenever `_has_changes`
fires (it will, because `previous`/`current` changed). So an omitted key arrives as
`norm=None` and **overwrites the stored numeric norm**. A mock asserting the pre-client
dict omits `norm` gives false confidence — it never exercises this path.

Two workable mechanisms; the draft **requires one of them to be chosen and proven by a
full client→API→service test** (not a pre-client mock):

- **(A) Service-side preservation — preferred, but colleague-managed.** In the upsert,
  don't overwrite `norm` when the incoming value is `None` (e.g. `COALESCE`, or exclude
  `None` fields from the `setattr` loop, or `model_dump(exclude_none=True)` for the
  update branch). Correct and protects **all** writers, not just this job. Crosses into
  `sapphire/services/preprocessing/` → **open a discussion first**; cannot ship
  unilaterally.
- **(B) Writer-side read-merge — stays in `apps/`, confirmed feasible.** Before writing
  norm-absent records, **read the existing** MONTH/QUARTER/SEASON rows for that station and
  copy their stored numeric `norm` into the outgoing records, so the upsert re-writes the
  same value instead of nulling it. **Verified 2026-07-07:** the client the writer already
  holds exposes `read_hydrograph(horizon, code, start_date, end_date, skip, limit=100)
  -> pd.DataFrame` (`sapphire_api_client/preprocessing.py:158`, `GET /hydrograph/`) — no
  service change needed. Notes for the implementer: default `limit=100` is ample for these
  horizons (12 month / 4 quarter / 1 season rows per year) but pass an explicit window;
  an **empty DataFrame** (first-ever run, no prior row) correctly leaves `norm=None`; the
  extra per-station read is cheap and the re-write of the same value is idempotent.

Pick (A) if the colleague can turn it around quickly (cleaner, systemic, protects **all**
writers); otherwise (B) — which unblocks immediately with no cross-team dependency.
Do **not** ship #1 without the chosen mechanism — the SDK currently returns 0 for every
station, so an unguarded run would wipe any good norm on the very next execution.

### 3. Surface skip/failure counts + a *degraded-success* signal
Today an SDK exception → `[]`, and `write_long_horizon_hydrograph` pops the station from
`attempted_station_codes` (`:375-376`), so the job can exit "success" while silently
skipping many stations. Returning `[]` for **both** "sdk-failed" and "norm-absent, nothing
to write" is exactly the ambiguity to remove. Track three distinct tallies —
**written / norm-absent / sdk-failed**.

Exit code: **non-zero iff there was ≥1 sdk-failed station** (unexpected, retryable);
norm-absent is expected and does **not** fail the run. This needs a status-carrying return
(see the relaxed scope guard in P1) — a bare `list` can't express it.

**But exit-code logic alone is not enough** (domain review): a norm-absent run that writes
rows exits 0, which would let a silently-degraded climatology persist for the next bulletin
cycle. So on any norm-absent stations, emit a **conspicuous degraded-success summary**, not
just per-station warnings — e.g. a single top-level line:
`DEGRADED: monthly discharge norms unavailable for N/M stations; observed runoff written;
norm and percent-of-norm unavailable.` It must surface in **(i)** the cron email / log tail,
**(ii)** a small run-summary artifact or maintenance report, and **(iii)** ideally an
admin/dashboard banner. Counts only — no per-station codes in the shared summary (see #6).

### 4. (Medium-term, separate issue) Local monthly-norm derivation — **governance-gated**
Optionally derive monthly norms from the multi-year local daily runoff archive (the same
source already yielding `previous`/`current`). For a resource-constrained service this is
likely preferable to manual iEH-HF entry **if the daily archive is trusted** — but a
derived normal must **never masquerade as official climatology**. Before any derived normal
appears in a bulletin, a **governance gate** (hydrology-lead approval) must fix and document:
- approved **reference period**; per-month **completeness threshold**; **years-used count**
  per station/month;
- handling of **station moves, datum / rating-curve changes, regulated-flow changes,
  outliers**; explicit **units** and **aggregation formula**;
- a **provenance flag** + **version/date**; labelled **"SAPPHIRE-derived operational
  normal"** unless the institution formally adopts it as official climatology.
Removes the iEH-HF norm bottleneck entirely. **Out of scope for this issue** — **filed
separately as PREPQ-010** (`mid_prio_gi_draft_runoff_local_monthly_norm_derivation.md`),
carrying this governance checklist as its acceptance gate.

### 5. Bulletin labeling (crosses into `forecast_dashboard`)
Decoupling only helps if the missing comparator is shown honestly. Where the norm is
absent, render the norm and percent-of-norm as an explicit **"N/A — monthly norm
unavailable"** — **not** `0`, not an empty cell, not a generic dash (any of which reads as
a data error and erodes forecaster trust). Add a footnote/tooltip: *"Observed runoff is from
local daily discharge; the long-term monthly norm is not populated, so percent-of-norm is
not calculated."* **Ownership:** this touches `apps/forecast_dashboard` (bulletin render),
a different module from the writer — **filed as sibling issue FD-016**
(`mid_prio_gi_draft_fd_month_norm_na_labeling.md`); do not bundle it into the
`preprocessing_runoff` change.

### 6. Log hygiene (counts-only; redacted diagnostics)
The shared run summary and cron output must reference **counts only** (`N/M stations`), never
station codes. Any station-level diagnostics go to a **restricted debug artifact** or use
**redacted/hashed** identifiers. Access-restrict and rotate/scrub the existing code-bearing
logs (see Operational follow-up).

### 7. Future follow-up — bounded retry on transient API errors (not scheduled)
`API_FAILED` (exit 5) currently fires on the first `_API_READ_WRITE_ERRORS`
(`SapphireAPIError`, `ConnectionError`, `Timeout`) for a station. `SapphireAPIError` (HTTP
4xx/5xx — e.g. the PREPQ-008 stale-worker 422) and all-stations-failed are genuinely critical
and should fail loudly; but a lone `ConnectionError`/`Timeout` on this **annual** job can be a
transient blip. A bounded retry (2–3 attempts with backoff) on the network-error subset —
*before* marking `API_FAILED` — would remove false failures without masking real ones. Left as
a small hardening; not worth its own issue unless timeout noise actually appears. Severity-split
(fail on `SapphireAPIError`, warn-and-continue on pure timeout) is a more nuanced alternative,
deferred for the same reason.

Two low-severity nits from the PR-readiness review, tracked but not scheduled:
- **`Decimal` norms** are classified NORM_ABSENT (`Decimal` is not `numbers.Real`) and `_json_safe`
  wouldn't normalise a `Decimal` read from an existing row. Harmless unless the iEH-HF SDK can
  return `Decimal`-like numerics (it returns JSON floats today) — if that ever changes,
  accept/convert them and add a test; otherwise document "JSON-like numeric scalars only".
- **Malformed duplicate same-month rows** in the read-merge: `_read_existing_month_norms` is
  last-row-wins in service date order and there's no test pinning that. Add a focused unit test if
  you want the behaviour locked.

## Operational follow-up (not code) — runbook required
The rerun is safe **only** with a runbook (domain review). Populate 12 manual monthly
discharge norms per Tajik station in iEH-HF **or** adopt #4, then run
`yearly_runoff_hydrograph_aggregation.sh` once to backfill all 17 (upsert on
`(horizon_type, code, date)` overwrites the pre-existing single-station rows).

- **Confirm the true cause first (cheapest check):** query the iEH-HF norm source directly,
  behind the **same** tunnel, grouped by station × `automatic` flag × `norm_period` — this
  separates "truly absent monthly manual norms" from a wrong instance / org-scope / station
  mapping. Do this before entering or deriving any norms.
- **Confirm the symptom on the production DB** (all DB evidence here is from the dev DB):
  `SELECT horizon_type, count(DISTINCT code) FROM hydrographs WHERE code LIKE '17%' GROUP BY 1;`
- **Before rerun:** export/snapshot the affected rows; confirm target year; confirm the
  **deployed** schema/API accepts all horizons (cf. PREPQ-008 — `quarter` was rejected by a
  behind-the-times deployed image); check tunnel/SDK health; record the selected-station
  count and the norm-availability classification.
- **During rerun:** exit non-zero **only** on SDK/transport failure; existing numeric norms
  preserved (fix #2).
- **After rerun:** verify expected distinct-station counts for month/quarter/season;
  spot-check that a pre-existing numeric norm **survived**; confirm the dashboard renders
  blank percent-of-norm (not a crash); capture the run summary / degraded-success line.
- **Note the job is normally annual (cron Jan 1):** an off-schedule manual rerun is fine but
  should be logged and the runbook steps followed; don't leave the cron in a surprised state.
- **Sensitive logs (restrict + reduce):** the Jun-11 logs at
  `<taj-data-root>/logs/runoff_hydrograph_aggregation/` contain real station codes in skip
  warnings — **access-restrict and rotate/scrub before sharing**. Future norm logs should use
  **counts only**; any station-level diagnostics go to a restricted debug artifact or use
  redacted/hashed identifiers (see #6).

## Phases

### P0 — Decide the norm-preservation mechanism (gate, no code)
- **Goal:** choose fix #2 mechanism **(A)** service-side preservation or **(B)**
  writer-side read-merge.
- **Status (2026-07-07):** sub-question (ii) **resolved** — the client **does** expose
  `read_hydrograph(...)` (`preprocessing.py:158`), so **(B) is feasible entirely in `apps/`
  with no service change.** Only (i) remains: whether the colleague wants to also do the
  cleaner systemic (A) in `crud.py`. **Recommendation:** ship **(B)** now to unblock (no
  cross-team dependency), and optionally file **(A)** separately as defense-in-depth for the
  service upsert (it protects any future writer, not just this job).
- **Depends on:** none. **Agents:** 0 (orchestrator + colleague decision).
- **Output:** the chosen mechanism, recorded here; unblocks P1's write path.

### P1 — Decouple + norm preservation
- **Goal:** month/quarter/season rows written for every attempted (non-sdk-failed) station
  even when the monthly norm is absent; an existing numeric norm is **never** overwritten
  by a norm-absent run.
- **Files:** `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`,
  `apps/preprocessing_runoff/test/`. If mechanism **(A)**:
  `sapphire/services/preprocessing/app/crud.py` — **colleague-managed, coordinate**.
- **Depends on:** P0. **Agents:** 1 (Sonnet, `isolation: "worktree"`).
- **Scope guard:** behavioral changes confined to `write_station_monthly_hydrograph`,
  `build_monthly_records`, `write_long_horizon_hydrograph` (+ the chosen preservation
  mechanism). Do **not** touch the daily-runoff read path, the 80% coverage rule, or the
  season/quarter mean helpers. **Relaxation:** the "no signature change" rule is lifted
  *only* to introduce a status-carrying return / small status enum
  (`written | norm_absent | sdk_failed`) needed by P3 — that is the ambiguity being fixed.
  Edit `sapphire/services/` **only** if mechanism (A) was chosen and coordinated.
- **Acceptance (all with placeholder code `19999`, no real codes/values; `today` passed
  explicitly everywhere — never `date.today()`):**
  1. **Decouple (exact identities, not just counts):** norm-absent → exactly **12 month +
     1 season + 4 quarter** rows, correct `date`/`horizon_value`/`horizon_in_year`, **no
     duplicate keys**; `previous`/`current` **equal the local aggregates** for
     representative months; **all `norm` fields `None`** when no stored norm exists, while
     local fields stay populated where coverage permits. (A row-count-only assert passes a
     field-corrupting bug — assert identities.)
  2. **BLOCKER — full-path preservation (service upsert):** the repo **has** the harness —
     in-memory SQLite FastAPI TestClient at
     `sapphire/services/preprocessing/tests/conftest.py:39`; the upsert under test is
     `crud.py:88` (`model_dump()` + blind `setattr`). Test at the **endpoint** level: seed
     a row with a numeric norm, then **POST** the same `(horizon_type, code, date)` with
     changed `previous`/`current` and **absent/null norm**, then **GET** and assert:
     exactly **one** row remains for the key, `previous`/`current` **changed**, and the
     **original numeric norm survived**. Run under
     `run_tests.sh service:preprocessing`. The contract is **final persisted behavior**,
     mechanism-agnostic (A or B). If the external `sapphire-api-client` can't be driven
     in-process, use the TestClient POST→GET directly (**not** a pre-client dict/mock). If
     mechanism **(B)** is chosen, **additionally** add a writer test with a **stateful fake
     client** proving the writer reads+merges the existing norm before writing.
  3. **Transitions (exact, same key):** absent→later-valid: key goes `norm=None`→numeric.
     valid→later-absent: key **keeps** prior numeric norm while `previous`/`current` update.
     **sdk-failed** (SDK raises): **zero** writes for that station across **month+season+
     quarter** — no partial row set.
  4. **API smoke only:** a `norm=None` row round-trips the API path without crashing
     (percent-of-norm not computed). Render/label behavior lives in **P-FD** — keep this a
     thin API/hydration smoke, or drop it as duplication of P-FD.

### P2 — (was mechanism A) folded into P1 if chosen; otherwise N/A
- Service-side preservation, if selected in P0, ships **within P1** under coordination.
  No separate phase.

### P3 — Skip/failure tallies + degraded-success signal
- **Goal:** run summary reports **written / norm-absent / sdk-failed** tallies; exit
  **non-zero iff ≥1 sdk-failed** station; norm-absent never fails the run. On any
  norm-absent stations, emit the conspicuous **`DEGRADED: …N/M…`** summary (fix #3) to the
  cron/log tail **and** a run-summary artifact/maintenance report (counts only).
- **Files:** `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py` (+ wherever the
  run-summary artifact/maintenance report is emitted).
- **Depends on:** P1 (needs the status-carrying return). **Agents:** 1 (Sonnet).
- **Acceptance:** mixed-batch test (one written, one norm-absent, one sdk-failed) asserts
  **tallies keyed by the status enum**, rows written **by station class**, **non-zero exit
  iff ≥1 sdk-failed**, and that the degraded run emits the top-level DEGRADED summary — with
  **no station code appearing anywhere in that summary** (not merely the headline), counts
  only. Ordering-insensitive assertions. No regression to P1 behavior.

### P-FD — Bulletin labeling (sibling, `forecast_dashboard`)
- **Goal:** where the monthly norm is absent, the bulletin shows norm & percent-of-norm as
  explicit **"N/A — monthly norm unavailable"** + footnote (fix #5).
- **Files:** `apps/forecast_dashboard/` (bulletin render) — **different module/owner from
  the writer.** May be split to its own FD issue; link it here. Run under
  `run_tests.sh forecast_dashboard` as a **normal unit/render test**, not a skipped
  external-server integration test.
- **Depends on:** P1 (norm-less rows must exist to render). **Agents:** 1 (Sonnet).
- **Acceptance (render/output assertion, `today` explicit):** given a month row for `19999`
  with `norm=None` and finite observed/`previous`/`current`, hydration/render must —
  (a) not crash; (b) preserve/display observed runoff; (c) set norm-derived percentage
  fields to `None`/unavailable; (d) render the explicit unavailable text + footnote;
  (e) **never** render norm as `0`, blank, `-`, `"None"`, or `"nan"`.

## Edge-case test checklist (add to P1/P3 — each an explicit case)
- **Leap-year DOY / Feb-29** for month, quarter, and season records (`MID_MONTH_DOY`,
  quarter/season start-DOY).
- **80%-coverage boundary** tested **separately** for the previous (full) year and a
  **sparse current** partial year — just-below vs just-at threshold.
- **Current target month** forced to `current=None` with an explicit `today` (the
  `target_year==today.year and month==today.month` branch).
- **Quarter/season all-or-nothing per field** when *some* constituent months are
  **norm-absent** vs **local-data-absent** — distinguish the two, assert per-field null.
- **Empty resolved station list** → deterministic exit/log (clean exit, zero rows).
- **All stations sdk-failed** → non-zero exit, zero rows, correct counts.
- **Mixed batch** → non-zero exit under the "≥1 sdk-failed" rule.
- **Norm-absent AND no local data** → *decide and test*: are the 17 rows still written with
  all triad fields (`norm`/`previous`/`current`) `None`? (State the chosen behavior.)
- **Re-run idempotency** → identical inputs twice ⇒ no duplicate rows and **no spurious
  `_has_changes`/update** signal.
- **DB isolation** → each service test starts from a clean SQLite DB and asserts the row
  count for the tested key.
- **Determinism** → `today` threaded as a parameter everywhere; monkeypatch any dashboard
  code that reads the real current year.
- **Ordering-insensitive** assertions for batches and log/summary contents.
- Placeholders only (`19999`); no real codes/discharge in code, fixtures, logs, or summary.

## Acceptance criteria (issue-level)
- A clean run writes month/quarter/season rows for **all** attempted (non-sdk-failed)
  stations; the dashboard monthly bulletin last-year runoff populates from local data;
  `norm` cells stay empty until iEH-HF (or #4) provides norms.
- **No existing numeric norm is overwritten by a norm-absent run — proven through the full
  client→API→service path, not a pre-client mock** (the P1.2 blocker test).
- Norm-dependent displays show explicit **"N/A — monthly norm unavailable"** + footnote
  when `norm=None` (P-FD) — never `0`, blank, or a bare dash.
- Exit code is non-zero iff a transport/SDK failure occurred; norm-absent is a clean exit,
  **but** a degraded run emits the conspicuous `DEGRADED: …N/M…` summary (cron/log tail +
  run-summary artifact), counts only.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff` — 0 fail /
  0 unexpected skip. (If (A) chosen: also `bash run_tests.sh service:preprocessing`;
  P-FD: `run_tests.sh forecast_dashboard`.)
- No real station codes, discharge values, or credentials in code, tests, fixtures, **or the
  shared run summary/logs** (counts only; station-level diagnostics redacted/restricted).

## Open questions (resolve before/within P0–P1)
1. **Preservation mechanism:** (A) service `crud.py` change vs (B) writer-side read-merge.
   ~~Does the client expose a hydrograph read?~~ **Resolved: yes** —
   `read_hydrograph(...)` (`preprocessing.py:158`), so (B) needs no service change.
   Remaining: colleague's call on whether to also do (A). **Recommend (B) now + optional (A)
   follow-up.**
2. **"Sole writer" of MONTH/QUARTER/SEASON:** confirm no migration/other job writes these
   horizons, so decoupling fully explains and fixes the symptom.
3. **"Stale single-station row":** confirm on the production DB + scheduled-job history
   (the code alone proves "clean run writes none," not the row's origin).
4. **Malformed-payload shape:** what does the SDK actually return for a partially-defined
   station (partial list? dict? `None`?) — pin the norm-absent classifier to real shapes.
5. **True cause of the norm gap:** run the iEH-HF norm-source query (station × `automatic` ×
   `norm_period`) behind the tunnel to separate "monthly manual norms genuinely absent" from
   a wrong-instance / org-scope / station-mapping issue — before entering or deriving norms.

## Dependency graph
```json
{
  "phases": {
    "P0":   { "depends_on": [], "parallel_agents": 0 },
    "P1":   { "depends_on": ["P0"], "parallel_agents": 1 },
    "P3":   { "depends_on": ["P1"], "parallel_agents": 1 },
    "P-FD": { "depends_on": ["P1"], "parallel_agents": 1 }
  }
}
```

## Notes
Reviewed four times (2026-07-07): (1) data-pipeline/architecture + operational-hydrologist
on the diagnosis; (2) a second **architecture** pass on the draft — verified against the
service code that the original "omit the `norm` key" guard is insufficient (`crud.py:91`
`model_dump()` with no `exclude_unset`; blind `setattr` at `:107-110`), reworking fix #2 to
require proven service-side-preservation **or** writer-side read-merge backed by a full
client→API→service test, plus the "valid norm" definition, malformed→norm-absent, a
status-carrying return, concrete exit semantics, and softened "sole writer"/"stale row"
claims; (3) a **domain (operational-hydrologist/sysadmin)** pass — verdict
*sound-with-changes*, adding the degraded-success signal (fix #3), explicit N/A bulletin
labeling (fix #5 / P-FD), the derived-norm **governance gate** (fix #4), the rerun
**runbook**, the iEH-HF-source confirmation query, and **log hygiene** (fix #6); (4) a
**test-strategy/TDD** pass — verdict *needs-strengthening*, confirming the SQLite service
harness (`sapphire/services/preprocessing/tests/conftest.py:39`) makes the P1.2 blocker a
real endpoint POST→GET test, tightening every acceptance to assert **identities not counts**
(exact keys, transitions, tallies-by-status, no-code-in-summary), adding the parametrized
**norm classifier** table and the **edge-case checklist**, de-duplicating P1.4 vs P-FD
(API smoke vs render contract), and pinning cross-module test placement. Diagnostic detail
in memory: `tajik_monthly_bulletin_empty_lastyear_runoff`.
