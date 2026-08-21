# PREPQ-015: Long-horizon hydrograph: an SDK-raise still drops the whole station

**Status**: Draft (2026-08-20)
**Module**: `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`
**Priority**: High — affected stations get **zero** month/quarter/season rows, reproducing the
exact symptom PREPQ-009 fixed (empty last-year runoff, no percent-of-norm in the long-term monthly
bulletin), via a different branch of the same classifier. **CONFIRMED 2026-08-21**: a read-only
probe against live kyg iEH-HF found 4/62 stations hitting `SDK_FAILED` on the current, unfixed
code, on every run.
**Labels**: `preprocessing_runoff`, `long-horizon`, `data-loss`, `silent-skip`
**Found**: 2026-08-20, filed as a follow-up while implementing INFRA-037.
**Related**: **PREPQ-009** (archived, `issues/archive/high_prio_gi_draft_runoff_longhorizon_norm_decouple.md`,
merged PR #409 `c894edcd`) fixed this asymmetry for the `NORM_ABSENT` branch; this issue is the
residual `SDK_FAILED` branch it left standing. **PREPQ-014** (Low, Draft) explains *why* the SDK
raises for these stations; this issue's fix does not depend on that issue landing first. **Revised
2026-08-21 (second pass): all reclassification/grading is dropped.** Three designs to reclassify a
raise into `NORM_ABSENT` were proposed and each was refuted — see "Grading mechanisms considered and
rejected" below. This issue now ships only the simpler, provably safe fix: a raised lookup keeps
status `SDK_FAILED` and also writes the station's records. It does not distinguish a structural
(virtual-station) failure from a transient one, and does not stop the permanent alarm for
structurally normless stations — see "Accepted cost" below.
**Implemented 2026-08-21** on branch `fix_prepq015_longhorizon_sdk_failure_writes_rows`: all four
"The contract" bullets and both PART-2 review corrections (a new sole-`API_FAILED` test, and the
retained/labeled `..._with_zero_records` synthetic test) applied exactly as specified.
`cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff` — 462 passed, 2 skipped
(the same two pre-existing `test_src.py` placeholders as the clean baseline; net +1 from the new
sole-`API_FAILED` test). Full suite (`bash run_tests.sh`, all app modules + all five services) —
zero failures, zero unexpected skips. `ruff check`/`ruff format --check` clean on both changed
Python files; `bash -n` clean on the changed shell script. Awaiting review before status changes.

---

## Current behaviour: three statuses, one asymmetric outcome

`write_station_monthly_hydrograph` (`sync_long_horizon_hydrograph.py:337-417`) classifies the
iEH-HF monthly-norm SDK call via `_lookup_monthly_norms`/`_classify_monthly_norms` (`:277-308`):

| Classification | SDK behavior | Rows written? | Stored norm preserved? |
|---|---|---|---|
| `VALID` | returns 12 finite numbers | Yes | n/a (fresh norm used) |
| `NORM_ABSENT` | returns successfully, not 12 finite numbers | Yes — same actuals pipeline as `VALID` | Yes, via `_read_existing_month_norms` read-merge (`:369-375`) |
| `SDK_FAILED` | **raises** | **No** — `records=[]` (`:354-366`) | n/a — nothing written |

The station's `previous`/`current` monthly actuals come from a **separate** SDK call
(`shh._fetch_sdk_period_actuals`, called at `:386-388`, feeding `shh.period_actuals`/
`_period_actual`, `sync_short_horizon_hydrograph.py:259-302`), independent of the norm lookup that
raised; that call already fails soft into a local-daily fallback rather than raising
(`sync_short_horizon_hydrograph.py:552-560`). So a raised norm lookup withholds data it has no
bearing on — the exact coupling PREPQ-009 already broke for `NORM_ABSENT`.

## The contract

- `write_station_monthly_hydrograph`'s `SDK_FAILED` early return (`:354-366`) is removed. The
  `NORM_ABSENT` check that fires the `_read_existing_month_norms` read-merge (`:369-375`) gains
  `SDK_FAILED` as a second matching classification — same read-merge, same reasoning: prefer a
  previously stored numeric norm over `None`. The warning log (`:356-362`) is reworded to say the
  run is **continuing** for this station, not "skipping" (the docstring at `:348` says the same
  thing and needs the same correction — see "Documentation" below).
- The final status computation (`:412-417`) gains a third, explicit branch: `SDK_FAILED`
  classification maps to `LongHorizonStationWriteStatus.SDK_FAILED`, not the ternary's `else`.
  Status becomes orthogonal to record existence — `SDK_FAILED` now carries the same 12 monthly
  records `NORM_ABSENT` carries.
- `write_long_horizon_hydrograph`'s `SDK_FAILED` skip/pop block (`:572-579`) is removed entirely.
  Every status takes the same path from there: `all_records.extend(monthly_records)` (`:581`), run
  the seasonal and quarterly writers normally, retain the station in `attempted_station_codes`,
  append to `completed_station_codes` (`:600`) only after all three writes succeed without raising.
- Exactly ONE terminal status is recorded per station. If the monthly write returns `SDK_FAILED` but
  a LATER write in the same iteration raises one of `_API_READ_WRITE_ERRORS`, the existing `except`
  block (`:602-614`) appends `API_FAILED` — this
  must remain the ONLY status recorded for that station; do not also leave the earlier append
  (`:601`) in place. Assert `len(station_statuses) == len(attempted_station_codes)` to catch an
  accidental double-append.

### Status matrix (post-fix)

| Norm lookup outcome | Status | Records written |
|---|---|---|
| returns 12 finite values | `LongHorizonStationWriteStatus.WRITTEN` | yes |
| returns a non-12-finite value | `LongHorizonStationWriteStatus.NORM_ABSENT` | yes |
| raises | `LongHorizonStationWriteStatus.SDK_FAILED` | yes — the fall-through this issue adds |
| any later SAPPHIRE API failure | `LongHorizonStationWriteStatus.API_FAILED` (sole terminal status, overrides) | partial |

Enum members verified at `sync_long_horizon_hydrograph.py:65-69`: `WRITTEN`, `NORM_ABSENT`,
`SDK_FAILED`, `API_FAILED`. There is no `VALID` member on this enum — an earlier revision of this
matrix named one that does not exist. (`_NormClassification`, a separate internal enum at `:59-62`,
does have a `VALID` member; it is not what gets returned to callers.)

### Systemic-outage guard

`_exit_code_for_long_horizon_summary` (`:639-651`) stays unchanged: `API_FAILED >= 1` returns 5,
`SDK_FAILED >= 1` (zero `API_FAILED`) returns 4, otherwise 0. INFRA-037 already records a FAIL row
whenever `lt_rc == 4` (`apps/run_locally.sh:918-934`), and `print_summary`/`main()` already fold
that into a non-zero process exit (`:923-931`, `:2270`) — "any `SDK_FAILED` exits non-zero" already
holds today; this fix does not touch the exit-code function.

### Stale-norm policy (accepted trade-off, not fixed here)

`_read_existing_month_norms` (`:311-334`) is the read-merge both `NORM_ABSENT` and (after this fix)
`SDK_FAILED` rely on. It reads only rows for `target_year` — no cross-year fallback — so what it
preserves is a **previously stored norm on a target-year row**, not "possibly a prior-year norm";
on the first run of a new year every value is genuinely `[None] * 12`, not "preserved but stale".
It keys by `horizon_in_year` with last-row-wins on duplicates, does not re-validate finiteness on
read, and only read-merges MONTH rows (season/quarter norms are separate fields this function does
not touch). Net effect — a last-known target-year norm can mix with fresh current-year observations
with no staleness marker — is the existing, accepted policy, unchanged by this fix.

## Grading mechanisms considered and rejected

Three designs were proposed to reclassify a raised norm lookup into `NORM_ABSENT` for stations
presumed structurally normless (virtual sites, which have no norm to fetch). **All three were
refuted; none is part of this fix.** Recorded so none is re-attempted without reading the
refutation.

| # | Design | Refutation |
|---|---|---|
| 1 | Grade on the exception itself | `_get_site_uuid_for_site_code` (`ieasyhydro_sdk/sdk_endpoint_definitions.py:90-109`) returns `None` for ANY non-200 from the site-lookup endpoint, which becomes a `None` path (`:118-126`) and raises the identical `ValueError('No path provided or the provided path is None')` (`sdk_base.py:64`). A 500/502/401 is indistinguishable from an unregistered site — this would mask a real outage as "no norm, expected." |
| 2 | Grade on the deployment's local virtual-station config | The trigger is still that same ambiguous exception — the config only narrowed WHICH stations could be downgraded, not what caused the downgrade. Also the local config is provably stale: it omits one of the four affected stations, and the station library flags that same station non-virtual, while the SDK's authoritative list includes it. |
| 3 | Grade on the SDK's authoritative `get_virtual_sites()` list | A successful discovery call proves list membership at that moment, not the health of a later norm lookup against a DIFFERENT endpoint. Worst case: discovery succeeds, non-virtual stations complete, the norm endpoint then starts returning 503, a listed station's lookup raises, it is graded absent, local actuals fall back, writes succeed, and the run exits 0 during a live outage — made likelier because hydrological sites precede virtual ones in work-list order (`iEasyHydroForecast/setup_library.py:1533`). Also "virtual" does not imply "cannot have a norm": PREPQ-014 documents that virtual/hydrological overlap can resolve normally, so an overlapping station hitting a genuine 503 would be silently graded absent too. |

**The design that would be correct — explicitly out of scope here.** On each raised lookup,
re-probe at that moment: if the probe itself raises, it is an outage (`SDK_FAILED`); if it succeeds
and the station is absent from the hydrological registry, the raise was structural; if it succeeds
and the station is present, the norm endpoint specifically failed (`SDK_FAILED`).
`resolve_sdk_station_codes()` (`sync_long_horizon_hydrograph.py:689`) already fetches site data and
discards it, so the data this design would need is partly available already. Not implemented here —
this issue ships the simpler fall-through fix only.

## Accepted cost (known limitation)

With no grading, a deployment whose stations are structurally normless reports exit 4 and a
`long-horizon sync` FAIL row on EVERY run, indefinitely. **This is accepted, not solved.** The
`LONG-HORIZON RUN SUMMARY` counts line (`total_attempted=… written=… norm_absent=… sdk_failed=…
api_failed=…`) already lets a log reader distinguish "a few stations failing" (small `sdk_failed`)
from "a total outage" (`sdk_failed` near `total_attempted`, or `api_failed > 0`) — that is the only
mitigation this issue provides against alarm fatigue.

## Empirical confirmation

A read-only probe called `_lookup_monthly_norms` once per station in the long-horizon work list
against live kyg iEH-HF: **62 attempted — 53 `VALID`, 5 `NORM_ABSENT`, 4 `SDK_FAILED`**, all 4
raising the identical `ValueError`, reproducing consistently across this probe and the 2026-08-18
field report (same 62/53/5/4/0 cardinality). A separate check confirmed all 4 are members of the
SDK's own `get_virtual_sites()` list. **What this does and does not establish**: it confirms SDK-side
virtual-list *membership* for the 4 stations; it does NOT verify their *absence* from the
hydrological registry (a separate live query this probe did not make). This fix performs no grading
and depends on neither fact — the confirmation is kept here only as background for why exit 4
currently recurs for these four stations, not as justification for any classification this fix
performs.

## Out of scope

- `backfill_discharge_aggregation.py`'s status-blindness to `SDK_FAILED`/`API_FAILED` (it discards
  the writer's return value entirely, `:109`) — pre-existing, needs its own issue if unacceptable.
- Changing `_read_existing_month_norms`'s stale-norm behavior — see "Stale-norm policy" above.
- Correcting the local virtual-station config (which omits one of the four stations) and the
  station library (which flags that same station non-virtual) — both wrong relative to the SDK's
  own list, which includes it; an operator/owner data-fix, not a code change here (see PREPQ-014).
  This fix reads neither file and does not need either corrected.
- Any form of reclassifying `SDK_FAILED` into `NORM_ABSENT` — see "Grading mechanisms considered
  and rejected" above. Re-attempting one requires refuting the specific failure recorded there.
- Extending the SDK/service to produce actual norms for virtual sites — a separate upstream request
  (`doc/prod/iehhf_virtual_station_norms_request.md`), not this issue's job.

**Operational impact (unfixed today)**: an `SDK_FAILED` station gets zero month/quarter/season rows
for that run — indistinguishable in the long-term monthly bulletin from a station never configured,
reproducing PREPQ-009's original symptom for this subset.

## Acceptance criteria

- [ ] `WRITTEN` and `NORM_ABSENT` behave exactly as today (regression guard) — 12/17 records, norm
      handling unchanged.
- [ ] A raised lookup keeps status `SDK_FAILED` and now ALSO writes the station's 17 monthly/
      seasonal/quarterly records via the same fall-through path every other status uses (the
      contract this issue adds).
- [ ] `_read_existing_month_norms`'s read-merge (stale-norm policy) applies to `SDK_FAILED` the same
      way it already applies to `NORM_ABSENT` — a previously stored target-year norm is preserved,
      not clobbered with `None`.
- [ ] Exit-code precedence, unchanged: any `API_FAILED` → 5 regardless of `SDK_FAILED` count; any
      `SDK_FAILED` with zero `API_FAILED` → 4; a run with zero `SDK_FAILED`/`API_FAILED` → 0.
- [ ] Exactly one terminal status per station: a later `API_FAILED` overrides an earlier
      `SDK_FAILED` append, never both — assert `len(station_statuses) ==
      len(attempted_station_codes)`.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff` — zero failures,
      zero unexpected skips.

## Tests that must be deliberately rewritten, not silently deleted

All six verified present in `apps/preprocessing_runoff/test/test_sync_long_horizon_hydrograph.py`.
Deleting one without a same-shape replacement is a regression, not a cleanup.

| Test | Old assertion | New assertion |
|---|---|---|
| `test_skips_station_when_sdk_raises` (`:757`) | `records == []`, `write_hydrograph.assert_not_called()` | 12 records written, `assert_called_once()` |
| `test_skips_station_when_sdk_raises_logs_at_warning_with_station_and_error` (`:783`) | WARNING log present | same — only the surrounding "skips" framing changes |
| `test_valid_then_norm_absent_preserves_norms_but_updates_local_values_then_sdk_failed` (`:806`) | `sdk_failed_records == []`, no new writes | 17 new records written, previously-preserved norm intact |
| `test_orchestrator_continues_after_skipped_station` (`:1099`) | 17 records for the surviving station only, `write_hydrograph.call_count == 3` | 34 records across both stations, `call_count == 6`, 12/1/4 shape check repeated per station |
| `test_orchestrator_skip_has_metadata_but_no_attempt_completion_or_failure` (`:1286`) | `records == []`, all three code lists empty | 17 records; station in `attempted_station_codes` AND `completed_station_codes`; absent from `failed_station_codes` |
| `test_main_exits_four_when_all_sdk_failed_even_with_zero_records` (`:1394`) | name assumes zero records | **CORRECTED (implemented 2026-08-21)**: retained, not dropped or renamed — kept as an explicitly-labeled SYNTHETIC unit test of `main()`'s exit-code/logging logic in isolation, since `main()` only reads `station_statuses`/`attempted_station_codes` and does not enforce the length-equality invariant itself. A comment in the test records that `write_long_horizon_hydrograph` can no longer construct this exact input, and that `test_main_exits_four_when_sdk_norm_lookup_fails` already covers exit 4 with the now-realistic non-zero-records shape. |

**CORRECTION (implemented 2026-08-21) — new test added, not in the original six:** a norm
lookup that raises (`SDK_FAILED`) followed by a LATER SAPPHIRE API write failure (seasonal or
quarterly) was previously untested. Added
`test_orchestrator_sole_api_failed_status_when_sdk_raises_then_seasonal_write_fails`, asserting:
sole `API_FAILED` status (never both `SDK_FAILED` and `API_FAILED`), exactly one status recorded
for that station, the station absent from `completed_station_codes`, the 12 partial monthly
records that were written before the later failure, and exit code 5.

Also strengthen `test_mixed_batch_carries_station_statuses` (`:862`) to assert record counts (17
per station) and the total record count, not just `station_statuses`.

## Documentation this fix will invalidate

- `write_station_monthly_hydrograph`'s own docstring (`:348`): "Only an SDK exception skips the
  station" — no longer true.
- The `SDK_FAILED` warning log text (`:357`): "skipping" — must say the run continues.
- `apps/preprocessing_runoff/README.md:176`: "skipped" framing — needs a rewrite, not a word swap.
- `bin/yearly_runoff_hydrograph_aggregation.sh:17-19`: same "skipped" claim; also currently omits
  `SDK_FAILED` from its description entirely, an opportunity to describe both classifications.
