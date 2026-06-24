# Long-Term Forecast `horizon_value` Convention Plan

Branch: `fix_migration_long_forecast_multihorizon`  
Scope: MIG-008 after the convention was resolved on 2026-06-22.  
Constraint: no implementation in this pass; investigation used dry-runs and aggregate-only DB reads.

## Review Outcome (2026-06-22): NO-GO for P3/P4 as originally written

A critical review empirically falsified the cleanup premises. P0/P1/P2/P5 and the P4 mechanism are
sound; the destructive cleanup is not safe as specified. Incorporated changes:

- **M1**: The `SEASON hv1` April block is **NOT** a duplicate of `hv0` (anti-join overlap = 0; it is a
  distinct Kyrgyz April-1 series, not reproducible from current configs). The "delete because hv0 has
  it" premise is false -> **do not delete**.
- **M2**: `QUARTER hv1`-January + `hv2/3/4` are a coherent **old calendar-quarter, 12-model ensemble**
  (~55k real, non-reproducible rows), not "unexplained/stale" -> deletion needs owner sign-off.
- **M3**: Destructive steps now require **exact reviewed SQL + dry-run row-count diffs**, using the
  DB's **uppercase** `model_type` (`LR_BASE`/`LR_SM`), not the config's `LR_Base`/`LR_SM`.
- **M4**: A **service-owner / modeller sign-off gate** is now a hard dependency of P3 and P4 (both
  mutate the colleague-owned `long_forecasts` table). See
  `doc/prod/longforecast_historical_data_decision_request.md`.
- **S1**: Default to **quarantine / per-row scoped dump**, not hard delete; the whole-table P0 backup
  is not a surgical rollback.
- **S3**: `MONTH hv4..12` (a few models, from 2006 -> the `month_1..9` backfill) is **out of cleanup
  scope** -- "not produced by current config" does not mean "stale".

## BLOCKER + scope expansion (2026-06-22): postprocessing ensemble pipeline writes the old convention

A consumer-impact audit (the second review iteration) found that `QUARTER hv1-4` and `SEASON hv1` are
**not deprecated history** -- they are the live output of the `apps/postprocessing_forecasts`
quarterly/seasonal **ensemble** pipeline, which writes a *different* `horizon_value` convention:

- `api_writer._write_quarterly_ensemble_to_api` (`api_writer.py:917`) -> `client.write_long_forecasts()`
  (`:1107`) with **`horizon_value = quarter_in_year`** (calendar quarter 1-4, `:1043-1051`).
- `api_writer._write_seasonal_ensemble_to_api` (`:938`) -> same table with **`horizon_value = 1`
  hardcoded** (`:1067`).
- Operationally invoked: `postprocessing_operational_long_term.py` / `_maintenance_long_term.py` ->
  `file_writer.save_quarterly/seasonal_forecast_data` -> `file_writer.py:686/718`.
- `MODEL_TYPE_MAP` emits `EM` / `Naive Mean` / `Skilled Mean` -- the "12-model ensemble" in QUARTER is
  this pipeline's product.

Consequence: cleaning `QUARTER hv1-4` / `SEASON hv1` without changing this pipeline is **futile** (the
next operational run regenerates them), and the cleanup premise ("deprecated old data") was wrong.

**Decision (2026-06-22): cover the postprocessing ensemble pipeline (option a).** Extend the
config-lead convention to `apps/postprocessing_forecasts` so the ensemble writers emit
`horizon_value` per the deployment's quarter/season config lead (quarter: kyg 1 / taj 0; season per
issue: kyg Jan/Feb/Mar/Apr = 3/2/1/0, taj Apr = 0) instead of `quarter_in_year` / hardcoded-1.

**Re-sequencing**: a new phase **P-PIPE** (fix the ensemble pipeline) becomes a hard prerequisite of
the data cleanup. P3 (re-stamp/delete) and P4 (Tajik hv0) must NOT run until P-PIPE ships, or they
are immediately undone. P-PIPE needs its own planner + reviewer pass (see the planner handoff).

## Resolved Convention

`horizon_value = operational_month_lead_time` from each long-term config. The
config is authoritative per bucket. There is no date-derived `horizon_value` and
no calendar-quarter mapping.

- Month: `hv = lead`. Kyrgyz `month_0..3 -> hv0..3`; Tajik filenames are off by
  one, so `month_1.json` carries the authoritative lead `0`.
- Quarter: one quarterly product per deployment. Kyrgyz lead `1` -> `QUARTER hv1`;
  Tajik lead `0` -> `QUARTER hv0`.
- Season: one config per issue month, `hv = months before the April target`.
  Kyrgyz Jan/Feb/Mar/Apr -> `hv3/hv2/hv1/hv0`; Tajik April-only -> `hv0`.

Writer evidence:

- Service migrator reads `operational_month_lead_time` at
  `sapphire/services/postprocessing/app/data_migrator.py:669` and writes
  `self.horizon_value` at `sapphire/services/postprocessing/app/data_migrator.py:768-770`.
- Operational forecast path uses `get_operational_month_lead_time()` for DB
  dependency and save paths at `apps/long_term_forecasting/run_forecast.py:269`
  and `apps/long_term_forecasting/run_forecast.py:409`.
- From-file importer reads the config lead at
  `bin/utils/migration_py/long_forecast.py:251-273`, accepts `month`, `quarter`,
  and `season` at `bin/utils/migration_py/long_forecast.py:258-268`, and stamps
  payload records from the mode config at
  `bin/utils/migration_py/long_forecast.py:341-348`.

No `sapphire/services/**` code change is planned.

## Findings

### 1. Per-Deployment Config Audit

Tajik config root:
`/Users/bea/Documents/GitHub/taj_data_forecast_tools/config/long_term_configs`

| Mode | Horizon type | Lead / hv | Config evidence | Hindcast CSVs |
|---|---:|---:|---|---:|
| `month_1` | month | 0 | `month_1.json:14` | 9/9 present |
| `month_2` | month | 1 | `month_2.json:14` | 9/9 present |
| `month_3` | month | 2 | `month_3.json:14` | 9/9 present |
| `quarter` | quarter | 0 | `quarter.json:9`, `quarter.json:12` | 2/2 present |
| `seasonal_april` | season | 0 | `seasonal_april.json:11`, `seasonal_april.json:13` | 2/2 present |

Tajik quarter is confirmed as `lead=0`, and Tajik seasonal April is confirmed as
`lead=0`. There are no Tajik Jan/Feb/Mar seasonal configs, which matches the
resolved Tajik April-only convention.

Kyrgyz config root, located from
`/Users/bea/Documents/GitHub/kyg_data_forecast_tools/config/.env_bea_kghm`:
`/Users/bea/Documents/GitHub/kyg_data_forecast_tools/config/long_term_configs`

| Mode | Horizon type | Lead / hv | Config evidence | Hindcast CSVs |
|---|---:|---:|---|---:|
| `month_0` | month | 0 | `month_0.json:13` | 9/9 present |
| `month_1` | month | 1 | `month_1.json:13` | 9/9 present |
| `month_2` | month | 2 | `month_2.json:13` | 9/9 present |
| `month_3` | month | 3 | `month_3.json:13` | 9/9 present |
| `quarter` | quarter | 1 | `quarter.json:8`, `quarter.json:11` | 2/2 present |
| `seasonal_january` | season | 3 | `seasonal_january.json:10`, `seasonal_january.json:12` | 2/2 present |
| `seasonal_february` | season | 2 | `seasonal_february.json:10`, `seasonal_february.json:12` | 2/2 present |
| `seasonal_march` | season | 1 | `seasonal_march.json:10`, `seasonal_march.json:12` | 2/2 present |
| `seasonal_april` | season | 0 | `seasonal_april.json:10`, `seasonal_april.json:12` | 2/2 present |

Kyrgyz seasonal January, February, and March configs and hindcast CSVs are
present locally. There is no local modeller block. If any deployment server lacks
these seasonal CSVs, that server gap is owned by the long-term modeller.

### 2. Existing Local DB Reconciliation

Aggregate-only query against `sapphire-postprocessing-db`, table
`long_forecasts`:

| Horizon | hv | Rows | Sites | Models | Issue date range | Valid range |
|---|---:|---:|---:|---:|---|---|
| QUARTER | 1 | 32,486 | 79 | 12 | 2000-03-25..2026-05-25 | 2000-04-01..2026-08-31 |
| QUARTER | 2 | 13,849 | 78 | 12 | 2000-04-01..2026-04-01 | 2000-04-01..2026-06-30 |
| QUARTER | 3 | 13,675 | 78 | 12 | 2000-08-01..2026-07-01 | 2000-08-01..2026-09-30 |
| QUARTER | 4 | 13,529 | 78 | 12 | 2000-10-01..2025-10-01 | 2000-10-01..2025-12-31 |
| SEASON | 0 | 3,671 | 79 | 2 | 2000-04-01..2026-04-21 | 2000-04-01..2026-09-30 |
| SEASON | 1 | 5,932 | 62 | 2 | 2000-03-25..2026-04-01 | 2000-04-01..2026-09-30 |
| SEASON | 2 | 2,847 | 73 | 2 | 2000-02-25..2026-02-25 | 2000-04-01..2026-09-30 |
| SEASON | 3 | 2,904 | 73 | 2 | 2000-01-25..2026-01-25 | 2000-04-01..2026-09-30 |

Issue-month distribution:

- `QUARTER hv1` contains issue months Jan and Mar-Sep. The Mar-Sep `LR_BASE` and
  `LR_SM` rows exactly match the Kyrgyz `quarter` CSV aggregates, so that part is
  valid Kyrgyz `QUARTER hv1` under the resolved convention.
- `QUARTER hv1` also contains January calendar-quarter rows, and `QUARTER hv2`,
  `hv3`, and `hv4` contain calendar-quarter-style rows. The colleague specified
  only Kyrgyz `hv1` and Tajik `hv0`; `QUARTER hv2/3/4` have no convention
  explanation. These are stale old-convention rows and should be cleaned after a
  backup and final aggregate confirmation.
- `SEASON hv3` matches Kyrgyz January source aggregates exactly.
- `SEASON hv2` matches Kyrgyz February source aggregates exactly.
- `SEASON hv1` contains correct Kyrgyz March rows, plus an extra April block
  (`2,890` rows, `61` sites) that is stale under the resolved convention.
- `SEASON hv0` exactly matches Tajik April plus Kyrgyz April source aggregates:
  `3,671` rows, `79` sites, `2` models. That bucket is correct and should be kept.

Cleanup is needed for the stale local rows:

- `QUARTER hv2`, `QUARTER hv3`, and `QUARTER hv4`.
- `QUARTER hv1` rows that are not the Kyrgyz rolling-quarter product from the
  current `quarter` config, especially issue-month January and non-current
  quarter models.
- `SEASON hv1` issue-month April rows, after confirming `SEASON hv0` contains the
  correct April rows.

No cleanup should remove correct Kyrgyz `QUARTER hv1` Mar-Sep rows or correct
Kyrgyz `SEASON hv1/hv2/hv3` March/February/January rows.

### 3. Held Tajik Quarter Write

Current local DB has no `QUARTER hv0` aggregate rows. The Tajik `quarter.json`
config is `horizon_type=quarter`, `operational_month_lead_time=0`, so Tajik
quarter belongs in `QUARTER hv0`.

Dry-run evidence from the from-file importer:

```text
MODE=full-import (target empty)
TARGET_TABLE=long_forecasts
CUTOFF=none
DISCOVERED_MODE_COUNT=1
DISCOVERED_MODES=['quarter']
SOURCE_ROW_COUNT=4876
FILTERED_ROW_COUNT=4876
DISTINCT_STATION_COUNT_REDACTED=17
MODE_INVENTORY mode=quarter model=LR_Base status=ok source_rows=2482 filtered_rows=2482 date_min=2000-03-01 date_max=2026-04-01 distinct_codes=17
MODE_INVENTORY mode=quarter model=LR_SM status=ok source_rows=2394 filtered_rows=2394 date_min=2000-03-01 date_max=2026-04-01 distinct_codes=17
DRY RUN: no POSTs attempted.
```

Plan decision: proceed with the Tajik quarter from-file backfill after the DB
cleanup preflight and backup. It writes a disjoint `QUARTER hv0` bucket and does
not overwrite existing `QUARTER hv1..4` rows because `horizon_value` is part of
the natural key. The stale `hv1..4` cleanup remains necessary so consumers do
not see obsolete quarter buckets.

### 4. Importer Verification

No importer `horizon_value` code change is expected. A no-write sentinel
verification using `code=19999` confirmed that `_load_mode_config()` and
`_build_record()` carry the config lead into the payload:

| Deployment | Mode | Record horizon type | Record hv |
|---|---|---:|---:|
| Tajik | `quarter` | quarter | 0 |
| Tajik | `seasonal_april` | season | 0 |
| Kyrgyz | `quarter` | quarter | 1 |
| Kyrgyz | `seasonal_january` | season | 3 |
| Kyrgyz | `seasonal_february` | season | 2 |
| Kyrgyz | `seasonal_march` | season | 1 |
| Kyrgyz | `seasonal_april` | season | 0 |

A small regression test is worth adding because it locks the resolved convention
and prevents a future date-derived or calendar-quarter mapping from reappearing.
The test should use synthetic temp configs and sentinel `19999` only, with no API
POST.

### 5. Server Parity

The historical backfill runbook requires a long-term configured-mode diagnostic
and DB acceptance queries (`doc/prod/historical_backfill_runbook.md:123-128`,
`doc/prod/historical_backfill_runbook.md:1900-1926`). It also states that the
generic P7 command runs `month_[1-9]` and `seasonal_*`, while `quarter` is
skipped unless a long-term owner provides a deployment-specific command
(`doc/prod/historical_backfill_runbook.md:1828-1883`).

Therefore server rollout must not stop at local cleanup. Each deployment server
needs the same aggregate reconciliation, backup, cleanup, Tajik `QUARTER hv0`
backfill where applicable, and acceptance queries. If a server lacks Kyrgyz
seasonal Jan/Feb/Mar configs or CSVs, that is a modeller-owned hindcast
production gap; otherwise it is actionable by this repo/data-ops path.

## Phased Plan

### P0 - Freeze Evidence and Backups

**Goal**: Freeze the convention evidence, take DB backups before any write, and
prepare idempotent aggregate verification queries.

**Files**:

- `doc/plans/archive/longforecast_hv_convention_plan.md` for the planning record.
- DB backup artifacts outside git, per deployment operator practice.
- No `sapphire/services/**` files.

**Depends on**: none.

**Agents**: 1 data-ops agent. It runs read-only aggregate SQL and backup commands;
no implementation agent.

**Acceptance criteria**:

- Backup exists for each target DB before cleanup/backfill.
- Aggregate query output records only counts, site counts, model counts, and date
  ranges; no station codes or discharge values.
- The target deployment is identified before any write.

### P1 - Server Config and Hindcast Parity Audit

**Goal**: Repeat the local config/source audit on each deployment server and
classify gaps as repo-owned or modeller-owned.

**Files**:

- Deployment config trees under each server data repo, especially
  `config/long_term_configs/*.json`.
- Deployment hindcast CSV directories under
  `intermediate_data/long_term_predictions/<mode>/<model>/`.
- No service files.

**Depends on**: P0.

**Agents**: 2 parallel read-only agents, one for Tajik and one for Kyrgyz.

**Acceptance criteria**:

- Tajik confirms `quarter hv0` and `seasonal_april hv0`.
- Kyrgyz confirms `quarter hv1` and seasonal Jan/Feb/Mar/Apr `hv3/hv2/hv1/hv0`.
- Every configured model has a matching hindcast CSV, or the missing CSV is
  explicitly recorded as a modeller-owned blocker.
- No blocker remains for local data: Kyrgyz seasonal Jan/Feb/Mar CSVs are present.

### P2 - Importer Regression Guard

**Goal**: Add a focused no-write regression test that encodes the resolved
convention and proves the importer stamps config lead, not date-derived hv.

**Files**:

- `apps/iEasyHydroForecast/tests/test_initialize_long_forecast.py` or the nearest
  existing migration test file for `migration_py.long_forecast`.
- No production importer change expected.
- No `sapphire/services/**`.

**Depends on**: P1.

**Agents**: 1 test agent.

**Acceptance criteria**:

- Synthetic temp configs cover Tajik-style `quarter hv0`, Kyrgyz-style
  `quarter hv1`, and season `hv3/hv2/hv1/hv0`.
- Synthetic records use sentinel `19999` only and do not POST.
- Test fails if `horizon_value` is derived from dates or calendar quarters.
- Focused tests pass.

### SIGNOFF - Owner / modeller decision gate (hard dependency of P3 and P4)

**Goal**: Obtain an explicit per-dataset decision (keep / quarantine / re-stamp / delete) from the
postprocessing-service owner + long-term modeller before any mutation of `long_forecasts`.

**Files**: `doc/prod/longforecast_historical_data_decision_request.md` (the request + evidence).

**Depends on**: P1.

**Agents**: none (human decision). No code or DB action.

**Acceptance criteria**:

- A recorded decision for Dataset A and Dataset B.

**DECISION RECEIVED (2026-06-22, service owner / modeller):**

- **Dataset A** (`SEASON hv1` April-1 series): **re-stamp/re-migrate** to the correct April bucket
  (`hv0`).
- **Dataset B** (`QUARTER hv1`-Jan + `hv2/3/4`): **delete the deprecated models**; **re-stamp the
  currently-configured models** (`LR_BASE`/`LR_SM`) into the correct quarter bucket (`hv1`).

### P3 - Local Reconciliation (re-stamp + targeted delete, per the SIGNOFF decision)

> **SUPERSEDED for ensemble handling (2026-06-22, P-PIPE review MF-A).** PP0 chose to **regenerate**
> the EM/Naive/Skilled ensembles, so P3 step 2's blanket
> `DELETE ... model_type NOT IN ('LR_BASE','LR_SM')` would delete rows that P-PIPE regenerates -- do
> NOT run it as written. The ensemble cleanup is now **re-derived post-regen, scoped to the
> old-convention signature** (season: `hv1` with `date==valid_from/target-start`; quarter: `hv2/3/4`
> orphans + old calendar-`hv1`), per
> `doc/plans/archive/ppipe_postprocessing_ensemble_hv_plan.md` ("Cross-plan cleanup reconciliation").
> The pre-regen counts below (2,890 / ~41,225 / 10,665) are **stale** and must be re-measured against
> the post-regen state. The **raw `LR_BASE`/`LR_SM` re-stamp (step 3) still stands** as a cleanup op
> (P-PIPE never moves raw LR). Sequencing: P-PIPE ships -> deploy -> full-history recalc -> verify ->
> re-derive + reviewer-approve predicates -> execute.

**Goal**: Apply the owner's decision: preserve configured-model history by re-stamping it to the
correct bucket, delete only the deprecated-model rows. No blanket deletes.

**Files**:

- Local `sapphire-postprocessing-db` only.
- Reviewed SQL committed as run notes (aggregate summaries only; no station codes).
- No source code files. No `sapphire/services/**`.

**Depends on**: P0, P1, **SIGNOFF** (satisfied). (P2 also passes.)

**Agents**: 1 data-ops agent, executing **only pre-reviewed SQL** (no execution-by-prose), each step
inside a transaction, with a per-step scoped dump first.

**Exact operations (reviewed; uppercase `model_type` as stored in the DB; collision-verified):**

1. **Dataset A re-stamp** -- move the Kyrgyz April-1 seasonal series into the correct April bucket:
   `UPDATE long_forecasts SET horizon_value=0 WHERE horizon_type='SEASON' AND horizon_value=1 AND extract(month from date)=4;`
   2,890 rows; **0** unique-key collisions with existing `SEASON hv0` (verified). Leaves the correct
   `SEASON hv1` March rows untouched.
2. **Dataset B delete deprecated models** -- across all quarter buckets:
   `DELETE FROM long_forecasts WHERE horizon_type='QUARTER' AND model_type NOT IN ('LR_BASE','LR_SM');`
   ~41,225 rows. Safe for the current rolling product: in `hv1` the deprecated-model rows are **100%
   January** (verified), so the Mar-Sep `LR_BASE`/`LR_SM` rolling product is untouched.
3. **Dataset B re-stamp configured models** -- consolidate the LR quarter history into the correct
   Kyrgyz bucket:
   `UPDATE long_forecasts SET horizon_value=1 WHERE horizon_type='QUARTER' AND horizon_value IN (2,3,4) AND model_type IN ('LR_BASE','LR_SM');`
   10,665 rows; **0** unique-key collisions with existing `QUARTER hv1` (verified). The `hv1`-January
   `LR_BASE`/`LR_SM` rows are already in the correct bucket and need no move.

End state: `QUARTER` retains only `hv1` (all `LR_BASE`/`LR_SM`: the rolling Mar-Sep product + the
re-stamped Jan/Apr/Jul/Oct LR history); `hv2/3/4` are emptied. `SEASON hv1` keeps only the correct
March rows; the April series lives in `hv0`. (Tajik `QUARTER hv0` arrives in P4.)

**Acceptance criteria**:

- Each step: a **dry-run row-count diff** matching the verified counts (2,890 / ~41,225 / 10,665)
  before execution, run inside a transaction.
- **Concrete, op-specific rollback (SF-1)** -- a scoped row dump is NOT a valid reversal for an
  in-place `horizon_value` UPDATE:
  - Op 2 (DELETE): scoped dump of the deleted rows; reversal = re-insert.
  - Ops 1 and 3 (re-stamp UPDATEs): capture `(id, old horizon_value)` for every affected row first;
    reversal = inverse `UPDATE long_forecasts SET horizon_value=<old> WHERE id=<captured id>` (NOT a
    re-insert, which would PK-collide / duplicate since the row still exists with the new hv).
  - The P0 whole-table backup is the coarse backstop for all three.
- **(N-2)** The per-step scoped dumps / id maps contain real station codes + discharge -- they live
  **outside the repo** and are **never committed** (same as the P0 backup).
- Keep-set still present afterward: Kyrgyz `QUARTER hv1` Mar-Sep `LR_BASE`/`LR_SM`; Kyrgyz
  `SEASON hv3/hv2/hv1` Jan/Feb/Mar; combined April `SEASON hv0`.
- Post-state aggregate query shows no `QUARTER hv2/3/4`, no non-`LR_BASE`/`LR_SM` quarter models, no
  `SEASON hv1` April rows; `MONTH` untouched (out of scope, S3).
- **(N-1)** After op 1, `SEASON hv0` holds **multiple April issue-days** per target year (existing
  Tajik day-1, Kyrgyz days 16/17/21/25, plus the re-stamped Kyrgyz day-1 series) -- mechanically fine
  (disjoint keys, 0 collisions), but confirm with the consumer audit that a SEASON skill aggregator
  does not double-count multiple issue-days per target year (see consumer-impact follow-ups below).

### P4 - Tajik Quarter `hv0` Backfill

**Goal**: Write the held Tajik quarter source rows into the correct
`QUARTER hv0` bucket via the from-file importer.

**Files**:

- Tajik source config and CSV inputs under
  `/Users/bea/Documents/GitHub/taj_data_forecast_tools/`.
- Local or server `long_forecasts` table, depending on rollout target.
- No importer code change expected.

**Depends on**: P0, P1, **SIGNOFF** (P4 writes to the colleague-owned `long_forecasts` table).
Prefer P3 completed first on the same DB to avoid consumer confusion from stale quarter buckets. The
write is mechanically safe regardless (the UNIQUE natural key makes `QUARTER hv0` disjoint from
`hv1..4`), but it still requires the owner gate before mutating their DB.

**Agents**: 1 data-ops agent.

**Acceptance criteria**:

- Final `--dry-run --mode quarter` still reports `SOURCE_ROW_COUNT=4876`,
  `FILTERED_ROW_COUNT=4876`, and `DISTINCT_STATION_COUNT_REDACTED=17` locally,
  or an explained deployment-specific server count.
- Real run writes/upserts `QUARTER hv0` only.
- Post-write aggregate query shows `QUARTER hv0` with 2 models and the expected
  redacted station count/date span.
- `QUARTER hv1` Kyrgyz rows remain unchanged.

### P5 - Kyrgyz Correct-Bucket Confirmation

**Goal**: Confirm Kyrgyz quarter and seasonal rows are complete in their correct
buckets after cleanup and do not need re-migration locally.

**Files**:

- Kyrgyz source config and CSV inputs under
  `/Users/bea/Documents/GitHub/kyg_data_forecast_tools/`.
- Target `long_forecasts` table for aggregate reads.
- No source code files.

**Depends on**: P3.

**Agents**: 1 data-ops agent.

**Acceptance criteria**:

- `QUARTER hv1` Mar-Sep aggregate matches the Kyrgyz `quarter` source CSV
  aggregate for current configured models.
- `SEASON hv3/hv2/hv1/hv0` match Kyrgyz Jan/Feb/Mar/Apr source aggregates, with
  Tajik April coexisting in `SEASON hv0` where the local stack contains both
  deployments.
- If a server is missing any Kyrgyz seasonal source CSVs, the phase is blocked
  on modeller hindcast production, not on importer code.

### P6 - Server Rollout and Parity Verification

**Goal**: Apply the same convention cleanup/backfill process to deployment
server DBs and leave local/server state aligned.

**Files**:

- Server data repos and DBs for Tajik and Kyrgyz.
- `doc/prod/historical_backfill_runbook.md` may need a follow-up doc patch if the
  operational runbook should explicitly call out quarter `hv0/hv1` handling.
- No `sapphire/services/**`.

**Depends on**: P2, P3, P4, and P5.

**Agents**: 2 data-ops agents can work in parallel after P2-P5: one Tajik server
operator and one Kyrgyz server operator.

**Acceptance criteria**:

- Each server has a DB backup before cleanup/backfill.
- Tajik server has correct `QUARTER hv0` and `SEASON hv0`.
- Kyrgyz server has correct `QUARTER hv1` and seasonal `hv3/hv2/hv1/hv0`.
- No server retains unexplained `QUARTER hv2/3/4` rows after cleanup.
- Aggregate acceptance queries match local expectations or have documented,
  deployment-specific differences.

## Ownership and Blockers

Actionable now by this repo/data-ops path (no `long_forecasts` mutation):

- Add the importer regression guard (P2). **Done** -- 5 tests added, module green (746 passed).
- Send the owner/modeller decision request (`doc/prod/longforecast_historical_data_decision_request.md`).

Gated on the SIGNOFF decision (these mutate the colleague-owned `long_forecasts` table):

- Any local reconciliation / quarantine / deletion of pre-convention rows (P3).
- The Tajik `QUARTER hv0` from-file backfill (P4) -- mechanically safe, but still a write to the
  owner's DB.
- The per-server reconciliation/backfill (P6), which must **re-derive predicates from each server's
  own aggregates** (never replay local deletes) and re-confirm M1/M2 there.

Blocked on modeller only if found on a server:

- Missing Kyrgyz `seasonal_january`, `seasonal_february`, or `seasonal_march`
  hindcast CSVs. Locally, those configs and CSVs are present, so there is no
  current local modeller blocker.

Out of scope:

- Any `sapphire/services/**` changes.
- Date-derived `horizon_value`.
- Calendar-quarter remapping.
- Committing real station codes, discharge values, or sensitive env contents.

## Dependency Graph

```json
{
  "phases": {
    "P0": { "depends_on": [], "parallel_agents": 1 },
    "P1": { "depends_on": ["P0"], "parallel_agents": 2 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "SIGNOFF": { "depends_on": ["P1"], "parallel_agents": 0, "type": "owner_modeller_decision_gate" },
    "P-PIPE": { "depends_on": ["SIGNOFF"], "parallel_agents": 1, "note": "fix postprocessing_forecasts ensemble pipeline to config-lead hv; needs own planner+reviewer pass" },
    "P3": { "depends_on": ["P0", "P1", "SIGNOFF", "P-PIPE"], "parallel_agents": 1 },
    "P4": { "depends_on": ["P0", "P1", "P3", "SIGNOFF", "P-PIPE"], "parallel_agents": 1 },
    "P5": { "depends_on": ["P3"], "parallel_agents": 1 },
    "P6": { "depends_on": ["P2", "P3", "P4", "P5", "SIGNOFF", "P-PIPE"], "parallel_agents": 2 }
  }
}
```

Note: P2 (importer regression test) is the only phase shipped. SIGNOFF is satisfied. **P-PIPE (fix the
postprocessing ensemble pipeline) is now the gating prerequisite** for all `long_forecasts` mutation
(P3/P4/P6) -- without it the cleanup is regenerated by the next operational run.
