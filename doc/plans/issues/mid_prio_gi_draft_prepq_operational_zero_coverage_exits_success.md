## Operational runoff preprocessing reports success when iEasyHydro HF returns no discharge for any station, and never back-fills a missed day (PREPQ-024)

**Status**: Draft (2026-10-05)
**Module**: `apps/preprocessing_runoff`
**Priority**: **Medium** — a missed pentad on a production deployment was the visible
effect, but the data was genuinely absent upstream; the defects are the missing signal
and the missing back-fill, not data corruption.
**Labels**: `preprocessing_runoff`, `silent-success`, `observability`, `data-gap`, `operational`
**Discovered**: 2026-10-05, production kghm, a pentad issue day (`maxat_sapphire_2`).
**Related**: **LR-010** — the downstream half of this incident (misleading CRITICAL);
**P-062** — downstream blast radius; **PREPQ-019** — same silent-success family
(different code path); **PP-051 / ML-021 / INFRA-031** — family and the "nothing verifies
a run produced what it owed" framing; `review_gi_draft_infra_new_deployment_initialization.md`
— notes operational mode fetches only yesterday, no gap detection.

Two defects in one file, both established by the 2026-10-05 incident:

- **(A)** Zero (or very low) station coverage exits 0; the only signal is a WARNING.
- **(B)** The operational fetch window is yesterday-only, so a day missing at fetch time
  is never recovered by an operational run.

---

## Symptom

iEasyHydro HF had no WDDA (daily average discharge) values for 2026-10-03 and
2026-10-04 (weekend, data not entered). Preprocessing DB `runoffs` DAY rows ended
2026-10-02 (57 stations, complete). The 05:00 UTC operational run logged:

```
Operational mode: fetching data from 2026-10-04 onwards.
Fetching data from 2026-10-04 00:01:00+... to ...
[API] COUNT VALIDATION: API reports total_count=100, total_pages=1
[DATA] process_hydro_HF_data: Final processed records: 0 from 58 hydro sites
[API] Response WDDA (...): 0 records from 0/62 sites (0.0%)
WARNING - [API] High data loss for WDDA: 100.0% of sites (62/62) returned no data
No daily average data found for the given date range.
...
[OUTPUT] Preprocessing completed successfully
```

The strings above are the code's (`src.py:3810`, `:3817`, `:1812`); the log excerpt is
otherwise abridged. Note "COUNT VALIDATION" (`:1812`) and "Final processed records"
(`:1432`) are DEBUG-level, so they appear only with debug logging on.

Container exit 0; Luigi `PreprocessingRunoff` DONE. The failure surfaced one stage later
as LR's CRITICAL "API database is now behind CSV" (LR-010) and a failed pentad workflow
(P-062).

Evidence the data was absent in HF rather than a fetch bug: the previous evening's 19:00
local maintenance run (30-day smart lookback) fetched 52584 WDDA records from 56/58
sites up to 2026-10-04 13:00 and still produced nothing for 2026-10-03.

A manual re-run of `bin/run_pentadal_forecasts.sh` the same afternoon failed
identically: the operational fetch only covers yesterday, so 2026-10-03 cannot be
recovered by it.

## Root cause

Defect A — no coverage outcome (verified by reading the code 2026-10-05):

- `apps/preprocessing_runoff/src/src.py:1438` `log_data_retrieval_summary`; the only
  reaction to loss is `if missing_pct > 20: logger.warning("[API] High data loss ...")`
  at `:1474-1479`. It returns nothing and raises nothing.
- `src.py:2083-2085`: an empty WDDA frame logs "No daily average data found for the
  given date range." at INFO and returns an empty DataFrame — treated as normal.
- `apps/preprocessing_runoff/preprocessing_runoff.py:648-653`: the success line
  `[OUTPUT] Preprocessing completed successfully` is logged unconditionally, then
  `if ret is None: sys.exit(0) else: sys.exit(1)` where `ret` is the CSV writers' return.
  Both CSV writers already `sys.exit(1)` on failure (`preprocessing_runoff.py:516-536`),
  so this final `if ret is None` branch effectively always exits 0. No coverage-based
  exit exists anywhere.
- No test in `apps/preprocessing_runoff/test/` references `log_data_retrieval_summary`
  or "High data loss" (grep, 2026-10-05).

Defect B — fixed one-day operational window. This is a deliberate, completed design, not
an oversight: `doc/plans/issues/archive/gi_draft_preprunoff_operational_modes.md` (`:33-35`,
`:124`) specifies operational mode as yesterday's WDDA plus today's WDD, for speed
(documented runtime "~5-10 seconds (expected) / ~100 seconds (actual)", `:35`). The
change proposed below alters that design:

- `src.py:3807-3817`: `mode == "operational"` sets
  `start_date = pd.Timestamp.now().normalize() - pd.Timedelta(days=1)`, then fetches
  yesterday 00:01 -> now. No gap detection against stored data.
- Maintenance mode (`src.py:3828` onward) uses `get_maintenance_lookback_days()` and
  `calculate_fetch_ranges` (`src.py:3302`) per site, so it back-fills; operational does not.
- Consequence: a day missing at operational fetch time is filled only by the 19:00
  maintenance run, and only if the data has been entered by then. If it is entered
  later, the next pentad issue day's operational run still starts at yesterday.

Grep of `bin/` and `apps/pipeline/pipeline_docker.py` (2026-10-05): the `bin/` wrappers
only invoke the module; no consumer of the one-day window was found.

## Why it matters

- A 100%-coverage-loss day is indistinguishable, at the exit-code level, from a normal
  day. Operators must find one WARNING in a roughly 300 KB log.
- `bin/handover_healthcheck.sh:292` flags discharge as stale only when age is more than
  7 days, so a 2-day gap reports "discharge data current".
- The pentad needs the three days before the issue date; a gap on the issue day means a
  missed pentad for every station, with no earlier alert.

## Proposed fix (to be planned)

**Options only — owner decision required. Nothing below is implemented.**

For (A), reuse before building: maintenance mode already has a coverage check,
`run_post_write_validation` with `reliability_threshold`
(`apps/preprocessing_runoff/preprocessing_runoff.py:565-582`, maintenance-only). Extending
it to operational mode is the first candidate, ahead of a new mechanism.

1. A distinct non-zero exit code (or structured "coverage" outcome) that the pipeline and
   `bin/handover_healthcheck.sh` can surface. Trade-off: a hard fail also blocks ML tasks
   that require `PreprocessingRunoff` (`RunMLModel.requires()`,
   `apps/pipeline/pipeline_docker.py:740-743`) unless P-062 / Luigi semantics are
   addressed; the repo's own LR-010 trap applies.
2. WARN-level alert only (e.g. a healthcheck line derived from per-day station counts in
   `runoffs`), exit stays 0.
3. Constraint on any option: weekends and holidays with legitimately no data entered must
   not page someone daily. A threshold must distinguish "no data entered yet" from
   "fetch broken" (the HF `total_count` vs processed count in the log is a candidate
   discriminator; Not verified as reliable).
4. Edge: a zero-coverage signal should fire only on pentad/decad issue days, or only when
   the newest stored day is at least N days old, so weekends with no data entry do not
   alert daily.

For (B) (a change to the deliberate design cited above): operational mode fetches from the per-site latest stored date, bounded to at
most N days, instead of fixed yesterday. N and reuse of `calculate_fetch_ranges` are
owner/design decisions.

**Contracts not to break:**

- Maintenance-mode behaviour (`src.py:3828` onward) unchanged.
- CSV and API write paths unchanged.
- A partial-coverage day (some stations missing) must still write what it has.

## Acceptance criteria

- Zero coverage (mock SDK returns no values for all sites): outcome follows the option
  the owner selects; the run is distinguishable from a normal day without reading the log.
- Partial coverage (some sites return data): rows for those sites are written exactly as
  today; no new failure.
- Window computation with a 2-day gap in stored data (test with a fixed "now"; do not use
  `datetime.now()` in the test): the operational start date reaches back to the gap,
  bounded by N; with no gap it equals today's yesterday-only behaviour.
- Maintenance mode produces identical fetch ranges to before the change.
- Operational runtime does not regress materially against the documented baseline
  (~5-10 s expected, ~100 s actual per the archived operational-modes issue); measure
  before and after.
- A weekend-style fixture (no data for the whole window, no prior gap) does not escalate
  beyond the owner-chosen severity.
- Placeholder station codes only (`<code>`, `15xxx`); no real discharge values.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff` green, zero skips.

## Interim workaround

Operator guidance is in `doc/prod/handover_kghm_version_5.md`, Chapter 4 "Data
freshness" (pentad warning): do not re-run the pentad until the missing days are
entered in iEasyHydro HF; ask forecasters to enter them. Then (`:901-909`) run
`bin/daily_preprunoff_maintenance.sh "$ENV_FILE"` BEFORE re-running the pentad: without
it the re-run fetches only yesterday and still misses the day before yesterday.

## Reproduction

Not verified locally. Production sequence: leave iEH HF without WDDA for the two days
before a pentad issue day, then run operational `preprocessing_runoff`. Expected
(observed 2026-10-05): "0/N sites (0.0%)", the "High data loss" WARNING, "completed
successfully", exit 0. A local reproduction should mock the SDK to return no WDDA
values and assert the exit status.
