## A `Quantile_Mapping_OP.py` failure silently suppresses snow ingestion (PREPG-024)

**Status**: Draft (2026-09-04) — **exit contract decided, see "Exit contract" below**
**Module**: `apps/preprocessing_gateway` (`Dockerfile:43`), surfaced via
`bin/daily_gateway_maintenance.sh`, `apps/pipeline/pipeline_docker.py` and `apps/run_locally.sh:681-683`
**Priority**: **Medium** — snow is removed by a fault it has no dependency on. Structural,
deterministic on any org where quantile mapping fails.
**Labels**: `preprocessing_gateway`, `snow`, `coupling`, `silent-success`
**Found**: 2026-09-04, local kghm/tjhm investigation.
**Related**: **PREPG-009** (the snow script's own exit-0-on-total-failure). **INFRA-046** (parked at Low pending
PREPG-023; may be closed rather than built) is the **sibling one layer out**: a gateway container failure withholds the Luigi
marker, so `RunMLModel` / `RunAllMLModels` / `RunLongTermForecast` are blocked even when only the
*ensemble* stage failed and none of them reads ensemble data (marker logic at `:384-428`). Same family — one over-broad failure unit — but a different mechanism in different
files: INFRA-046 is the marker/task graph, this is the in-container `&&` chain. Cross-reference, do not
merge. **INFRA-023** owns the
maintenance-wrapper swallowed-exit-code class — the gateway instances and the
full wrapper surface are recorded there, not here; see the signal table below.

---

## Observation

The gateway container runs its three scripts as a single `&&` chain:

```dockerfile
# apps/preprocessing_gateway/Dockerfile:43
CMD ["sh", "-c", "uv run Quantile_Mapping_OP.py && uv run extend_era5_reanalysis.py && uv run snow_data_operational.py"]
```

`apps/run_locally.sh:681-683` mirrors it with `break` on first failure. `Quantile_Mapping_OP.py` runs
**first** and terminates the chain via `sys.exit(1)` on nine paths. Two fire on kghm and were observed
on consecutive days:

| Date | Log line | Exit path |
|---|---|---|
| 2026-09-03 | `No temperature data found in the ensemble forecast files.` | `Quantile_Mapping_OP.py:302-303` |
| 2026-09-04 | `Control member download failed for HRU <x> due to ValueError` | `Quantile_Mapping_OP.py:819-821` |

**Cleanest evidence** — the local runner logs show the chain launching only its first script:
`apps/logs/run_locally_20260904_101318.log:15-31` and `run_locally_20260903_140947.log:15-53`
(`Running: preprocessing_gateway/Quantile_Mapping_OP.py` → error → `preprocessing_gateway failed
(exit 1) after 3s`). Prefer these over the module log: `apps/preprocessing_gateway/logs/log` *does*
contain six `Processing snow data` lines at **10:28**, but those are a standalone diagnostic
invocation made while investigating, not the chain resuming. `log.2026-09-03` likewise contains a
standalone ERA5 run at 15:26. Compare timestamps against the runner log, not the presence of a string.

## Corrected premise

The first draft asserted that all three scripts are independent. **That is false, and the error was
load-bearing** — it is recorded here rather than quietly removed, because it changes the fix.

- **ERA5 extension genuinely depends on quantile mapping.** `extend_era5_reanalysis.py:586-615`
  reads `{hru}_P_control_member.csv` / `_T_control_member.csv` from `OUTPUT_PATH_CM`; QM writes
  exactly those files at `Quantile_Mapping_OP.py:871-872`. `test_api_coverage_gaps.py:700-706`
  documents the required QM→extend ordering. `QM && extend` is correct and must stay.
- **Snow is independent.** It reads a different Data Gateway endpoint (`snow-operational` via
  `get_operational`) and writes a different sink (`/snow/` vs `/meteo/`). Nothing it consumes is
  produced by QM or by the ERA5 extension.
- **But snow is not free to run in parallel either.** QM deletes every file in the shared
  `OUTPUT_PATH_DG` at `Quantile_Mapping_OP.py:705-715` when not in debug, and snow downloads into
  that same directory. Concurrent execution would race.

So the defect is narrower than first stated: **snow — and only snow — is removed by a fault it has
no dependency on**, and it must be run sequentially, not concurrently.

## Signal per path — "the failure stays loud" is not uniformly true

| Path | On QM failure |
|---|---|
| Container / Luigi (`pipeline_docker.py:384-428`) | non-zero → retry, notify, raise, **no marker** |
| `run_locally.sh` | recorded FAIL → final exit 1 |
| **`bin/run_preprocessing_gateway.sh`** — the canonical 03:00 cron entry (`update_deployment_checklist.md:802-803`) | **exits 0.** Submits via `docker compose run`, never captures the status, ends on two `echo`s; its Luigi CLI also keeps the default `task_failed=0` |
| `bin/daily_gateway_maintenance.sh` (`:120-141`) — legacy/manual, **not** the cron path | **exits 0** (logs a WARNING) |

**Consequence, and it limits what this issue can deliver:** fixing the `&&` chain makes the failure
visible to Luigi and to `run_locally.sh`, but **cron still sees 0** on the scheduled path. Snow
failures therefore remain invisible to an operator until `run_preprocessing_gateway.sh` also
captures its compose status *and* Luigi is configured to return non-zero. That wrapper class is
owned by **INFRA-023** (which now records the full surface); it is **not** in scope here. Say so
explicitly rather than implying this fix restores operator visibility on its own.

## Measured on the kghm server, 2026-09-04 — priority settled at Medium

Quantile mapping fails there **occasionally, not every run**: 2 days (2026-08-31, 2026-09-04) across
the retained cron logs, unlike locally where it fails every time. Snow is therefore not permanently
unreachable on that server, and this stays **Medium**.

**It still cost five days of snow, and it took both defects to do it.** Snow last wrote on
2026-08-30 (CSV mtimes 09:04; the API's newest non-null `value` is 2026-09-07, which is that run's
*forecast tail*, not a later run):

| Date | What happened |
|---|---|
| ≤ 2026-08-30 | snow ran normally |
| 2026-08-31 | quantile mapping failed → chain broke → snow never ran (**this issue**) |
| 2026-09-01…03 | quantile mapping fine, but the upstream 09-01 hole failed the fetch (**PREPG-009**) |
| 2026-09-04 | quantile mapping failed again |

Neither issue alone explains the stall — useful evidence that they are genuinely separate.
`Processing snow data` appears **zero** times in every retained gateway cron log, on failing and
succeeding days alike.

**Measurement trap, recorded because it caught us twice.** The max date is *not* a freshness signal:
each fetch writes ~8 days of forecast ahead, so a stale file still shows a future date. Worse, a
plain max against the API returns **2026-12-31**, because the yearly recalculation writes norm rows
across the whole calendar year with a NULL `value` — the INFRA-026 norm-only-rows trap. Use the file
mtime, or the newest row whose `value` is non-null.

## Exit contract — DECIDED (owner, 2026-09-04)

Out-of-loop review established that **a graded exit taxonomy cannot be honoured by current
orchestration**: `pipeline_docker.py:384-428` treats every non-zero status except `124` identically
(retry → notify → raise → no marker), and `run_locally.sh:1831-1904,2345-2350` reduces any recorded
failure to exit 1. A "warn-level" code would still page and still fail Luigi.

**Decision: single non-zero aggregate.** Run the meteo branch as today (`QM && extend`), then run
snow unconditionally, and exit non-zero if *either* branch failed, naming the failed stage and the
succeeded/failed counts in the log. Luigi behaviour and the marker contract are unchanged:

```
Snow data processing complete: 0/6 succeeded
FAILED stages: snow (6/6 tasks)
-> exit 1
```

Caller semantics are **explicitly not** to be changed here. Partial-vs-total distinguishability is
out of scope; it would reopen the Luigi retry/notify and marker contract and nothing in the evidence
requires it.

## Acceptance criteria

- A `Quantile_Mapping_OP.py` failure no longer prevents `snow_data_operational.py` from executing.
- A run in which QM fails and snow succeeds still writes the snow rows to `/snow/`, **and still
  exits non-zero** — the QM failure must not be masked by snow's success.
- `extend_era5_reanalysis.py` still runs **only** after a successful QM. Do not add a criterion
  requiring it to run regardless; it would read `{hru}_*_control_member.csv` that QM never wrote.
- Snow does not run concurrently with QM (shared `OUTPUT_PATH_DG`, deleted at `:705-715`).
- The `Dockerfile` CMD and `run_locally.sh` stay behaviourally equivalent to each other — if only one
  changes, the local runner stops predicting production, which is how this went unnoticed.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green, zero skips.
- Orchestration tests covering **both implementations and both failure points** — a `run_locally.sh`
  test alone can pass with the Docker CMD still defective, and vice versa. Four cases: QM fails and
  extend fails, each against the container command and the local runner; assert snow still ran and
  the aggregate status stayed non-zero.

## Contract not to break

- **The failure must stay loud.** Running snow unconditionally must not collapse to "exit 0 because
  snow worked". PREPG-017 and PREPG-009 both warn against converting a loud failure into a silent one.
- **Do not reorder the scripts.** QM must precede the ERA5 extension (real dependency) and the ML
  consumers that read its output.
- Do not change `Quantile_Mapping_OP.py`'s own exit codes here — the nine `sys.exit(1)` sites are
  load-bearing for PREPG-010's retry contract and PREPG-017's stay-loud requirement.
- The Luigi marker semantics at `pipeline_docker.py:570-573` are shared with
  `ExternalPreprocessingGateway`'s daily-reuse check (`:459-472`, dispatched `:147-155`); keep that
  path working or the gateway re-runs on every pipeline invocation.
