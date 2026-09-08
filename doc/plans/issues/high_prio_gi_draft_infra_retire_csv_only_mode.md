## Retire the CSV-only mode: a missing API becomes an error (INFRA-049)

**Status**: Draft (2026-09-08) — **owner decision recorded, scope needs one confirmation before work starts**
**Module**: cross-module (`apps/`), plus `CLAUDE.md` and `doc/dev/testing_workflow.md`
**Priority**: **High** — the current policy documents a mode the owner says is no longer maintained
or tested, so the repo's own guidance is misleading, and a silent CSV-only fallback is a
stale-data trap.
**Labels**: `infra`, `api`, `deprecation`, `silent-success`, `documentation`
**Found**: 2026-09-08, while scoping **PREPG-026**. The owner's decision: *"api enabled must be true
nowadays, all other behaviour is deprecated and no longer tested and working… for me, no api is an
error now."*
**Related**: **PREPG-026** (a failed snow API write does not fail the task) — that issue is the
narrow, immediately useful half and should ship first; this one is the policy change around it.

---

## The decision

`SAPPHIRE_API_ENABLED=false` and the CSV-only path it selects are **deprecated**. The API is the
only supported and maintained sink for pipeline data. An absent or disabled API is an **error**, not
a fallback.

`CLAUDE.md:165-172` currently says the opposite — it lists "the documented `SAPPHIRE_API_ENABLED=false`
mode" as a *transitional exception, not a violation*, and `doc/dev/testing_workflow.md:93` **requires**
every module to carry a test exercising the API-disabled path. Both must change, and they are the
reason this cannot be a quiet code edit.

## The distinction the work turns on — get this wrong and the gateway breaks

"We no longer write CSV" is true of **pipeline data I/O**. It is **not** true of the repository as it
stands, and the difference is load-bearing:

| CSV use | Example | Under this issue |
|---|---|---|
| **Pipeline data to the API** — forecasts, skill metrics, observations, snow | `write_snow_to_api` and its siblings | API only; disabled/absent = error |
| **Intermediate on-disk handoffs between modules** | `{hru}_P_control_member.csv`, `{hru}_T_control_member.csv` written by `Quantile_Mapping_OP.py:957-958` and **read by `extend_era5_reanalysis.py:586-615`** | **KEEP** — not API data |
| ERA5 reanalysis working files | `extend_era5_reanalysis.py`, `get_era5_reanalysis_data.py` | **KEEP** — not API data |
| Migration / operator exports | `bin/export_runoff_period_history.sh`, `data_migrator.py` inputs | **KEEP** — CLAUDE.md already exempts these |
| Presentation-boundary output | dashboard/bulletin CSV | **KEEP** |

**PREPG-024 exists because that control-member handoff is a real dependency.** A blanket "no CSV"
change would break the meteo chain this repo just finished protecting. Any implementer must treat
"CSV" as a question about *what the data is*, not about the file format.

## Measured scope, so the size is not a surprise

Counted on trunk 2026-09-08:

- **140** production references to `SAPPHIRE_API_ENABLED` across 9 modules
- **601** test references
- **60** production `to_csv()` calls across 9 modules — **most of which are legitimate** per the table
  above and must survive

This is not a one-line change and cannot be one PR.

## Open question — confirm before starting

`write_snow_to_api` returns a bare `False` for **five different situations** (`dg_utils.py:1122`,
`:1128`, `:1141`, `:1145`, plus a raised `SapphireAPIError`), which conflate *"deliberately not
writing"* with *"tried and failed"*. Sibling writers in other modules follow the same shape.

**Does "no api is an error" mean:**
- (a) only `SAPPHIRE_API_ENABLED=false` and a missing client become errors, or
- (b) also every currently-silent write failure — i.e. PREPG-026 generalised to every module?

(b) is the larger and more useful reading, and it is what actually protects an operator from a green
run over a stale database. It is assumed below; say so if (a) was meant.

## Staging

Four phases. **Each is a separate PR**, and phases 2-4 are gated on the one before.

**P1 — PREPG-026 first, unchanged and unwidened.** Make a genuine snow API delivery failure fail the
task, leaving the disabled path exiting 0 for now. Small, already scoped, and it fixes the reported
symptom (a local run reports PASS while the dashboard stays stale). It also proves the shape the
other modules will copy.

**P2 — the policy documents.** Rewrite `CLAUDE.md:165-172` to say the API is the only supported sink
for pipeline data, that `SAPPHIRE_API_ENABLED=false` is deprecated and slated for removal, and that
the exemptions in the table above remain exemptions. Update `doc/dev/testing_workflow.md:93` so a
CSV-fallback test is no longer required — and say what replaces it, or the next module author will
have no instruction. **Docs first, deliberately**: the policy must be written down before code
starts enforcing it, or every module change looks like a violation of the standing rule.

**P3 — make it loud, not fatal.** Emit a clear deprecation warning wherever `SAPPHIRE_API_ENABLED=false`
or an absent client is detected, in every module. No exit-status change. This surfaces any deployment
or CI job still relying on the mode *before* it starts failing, which the 601 test references suggest
is worth knowing.

**P4 — make it an error.** Convert the disabled/absent paths to failures, module by module, and
retire the CSV-fallback tests P2 stopped requiring. Sequence per module, not all at once, and run the
full suite between each — 601 test references will not all be mechanical.

## Contract not to break

- **Do not touch the intermediate on-disk handoffs.** The control-member CSVs and ERA5 working files
  are inter-module dependencies, not deprecated pipeline I/O. PREPG-024 and the ERA5 chain depend on
  them.
- **Do not remove a CSV *reader* while its writer still runs**, and do not remove a writer while a
  reader still depends on it. CLAUDE.md's existing warning about fallback readers becoming
  stale-data traps applies in both directions.
- `bin/reset_sapphire_db.sh` invokes `data_migrator.py --type combinedforecast`, which reads
  `combined_forecasts_pentad.csv` / `_decad.csv`. **The API cannot be the replacement source there** —
  the reset drops the database volume before the API starts. That path needs its own answer before
  those files stop being written.
- The conceptual model is CSV-only and is out of scope.
- Do not let P4 land without P3 having run on the deployments for at least one full cycle.

## Acceptance criteria

- `CLAUDE.md` and `doc/dev/testing_workflow.md` no longer describe the CSV-only mode as supported,
  and say what a module author should do instead.
- With the API reachable, every module behaves exactly as it does today.
- With the API unreachable and the flag true, the affected module exits non-zero (P1 for snow, P4
  elsewhere).
- With the flag false, P3 warns and P4 fails — and no module silently writes CSV and reports success.
- The intermediate handoffs listed above still work: `Quantile_Mapping_OP.py` still writes the
  control-member CSVs and `extend_era5_reanalysis.py` still reads them.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` green, zero unexpected skips, after each phase.
