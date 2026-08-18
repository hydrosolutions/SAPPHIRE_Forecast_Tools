## `preprocessing_gateway` reports PASS with all six snow tasks errored (PREPG-009)

**Status**: Draft (2026-08-14)
**Module**: `apps/preprocessing_gateway` (`snow_data_operational.py`), surfaced via
`apps/run_locally.sh`
**Priority**: **Medium** — silent-success on a fully failed sub-task. Not data loss, but the
operator signal is wrong, and the companion validation defect (INFRA-024) means nothing
downstream catches it either.
**Labels**: `preprocessing_gateway`, `snow`, `error-handling`, `silent-success`
**Found**: 2026-08-14, local kghm review on `maxat_sapphire_2` @ `8e3fc1bc`.
**Related**: **INFRA-024** (validation passes on the resulting norm-only rows — the two
together produce an entirely green run over a total snow outage). Same silent-success family
as PP-051 / PP-054 / LR-010.

> **Provenance correction (2026-08-16).** The checkout moved from `maxat_sapphire_2` to
> `fix_lr010_lr011_write_contract` at **2026-08-14 16:00** (git reflog), so every run from the
> full-history recalc onward executed on that branch (now `849c8736`), **not** on trunk as the
> line above states. That branch's diff vs trunk touches only
> `apps/linear_regression/linear_regression.py`, `apps/iEasyHydroForecast/forecast_library.py`,
> their tests, and docs — **none of the files this issue concerns** — so the finding holds
> identically on trunk. Recorded for accuracy of the audit trail, not because the conclusion changes.

---

## Observation

`snow_data_operational.py` logged **six ERROR pairs** — every HRU × variable combination
(SWE, HS, RoF for two HRUs) — then finished normally:

```
ERROR - Error getting snow data from Data Gateway for HRU <X>, SWE: Failed to get data from
        api/calculations/snow-operational/… : {"message": "Operational data for HRU <X> is not
        available for date 2026-08-09 00:00:00!", "success": false}
ERROR - Failed to get snow data for HRU <X>, SWE
… x6 …
INFO  - Snow data processing complete (6 tasks)
```

Result: `preprocessing_gateway completed in 1m 42s` → **PASS**, exit 0. Zero snow values
were written; the stored SWE series still ends at its previous non-null date.

The **upstream cause is not ours** — the SAPPHIRE Data Gateway reports operational snow data
unavailable from the requested date. That is a data-availability condition to report, not a
bug to fix here. The defect is that it is reported as success.

## Why "6 tasks complete" is the wrong summary

`Snow data processing complete (6 tasks)` counts tasks *attempted*, not *succeeded*. An
operator reading the tail of the log sees a completion line and a PASS. The six ERROR lines
are 15 lines above it and are not reflected in any exit code, summary, or downstream check.

## Proposed fix

1. Track succeeded/failed counts and emit a summary that states both
   (`Snow data processing complete: 0/6 succeeded`).
2. Decide and document the exit contract. Options, in the repo's existing idiom
   (`maintenance:preprocessing_runoff` already uses distinct codes: 2 = no records,
   4 = SDK norm failure, 5 = API failure):
   - all tasks failed → non-zero;
   - some failed → non-zero or a distinct "partial" code;
   - upstream "data not available yet" → possibly a warn-level code, since it is expected
     near the start of a season and should not page anyone.
   **The distinction between "upstream has no data yet" and "we failed to fetch data that
   exists" is the important one** — they deserve different codes.
3. Ensure `run_locally.sh` surfaces whichever code is chosen (it already special-cases
   exit 2/4/5 for the runoff maintenance target).

## Acceptance criteria

- A run in which all snow tasks fail does **not** report PASS.
- A run with partial failure is distinguishable from full success and from total failure.
- An upstream "not available for date" response is classified distinctly from a transport or
  auth failure.
- Existing fully-successful runs behave byte-identically.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green.

## Contract not to break

- Do not make a snow outage abort the whole gateway run: `Quantile_Mapping_OP.py` and
  `extend_era5_reanalysis.py` succeeded in this same invocation and their output is needed
  downstream. The exit signal must communicate partial failure without discarding good work.

## Incidental hygiene finding (not the subject of this issue)

The Data Gateway **API key is written to the logs in cleartext** as a query parameter
(`…&api_key=<key>`), present in 30 local log files. `apps/logs` is gitignored
(`.gitignore:255`, zero tracked files) so nothing has reached the repository, but the same
lines are produced on deployed servers where logs may be collected or shipped. Worth a
separate small issue: redact the `api_key` parameter before logging the URL.
