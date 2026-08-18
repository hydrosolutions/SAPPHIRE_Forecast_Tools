## `recalculate_nan_forecasts` logs "Nan Values are replaced" whatever it achieved — including a failed API write (ML-019)

**Status**: Draft (2026-08-18)
**Module**: `apps/machine_learning/recalculate_nan_forecasts.py`
**Priority**: **Medium** — no data is harmed; the operator signal is wrong. It is the module's
only end-of-run statement, and it is emitted on paths where nothing was replaced and on paths
where the API write failed. Owner to confirm.
**Labels**: `machine_learning`, `reporting`, `silent-success`
**Found**: 2026-08-18, local kghm (kyg) end-to-end review on `maxat_sapphire_2` @ `a304ffb0`.
**Related**: PP-051 / PP-054 / PREPG-009 / LR-010 (silent-success family), **INFRA-030**
(skipped modules leave no summary line). **Not** INFRA-029: this module uses a named logger
with an explicit `setLevel(DEBUG)` (`:35-36`), so unlike the postprocessing entry points its
output is actually emitted.

---

## Observation

On kyg, `maintenance:machine_learning` (PENTAD, TFT) ended with:

```
Recalculating forecasts for codes [3 codes]
Min missing date: 2026-08-17   Max missing date: 2026-08-18
Wrote 72 hindcast rows to API for TFT PENTAD
Wrote 18 recalculated forecasts to API (out of 72 hindcast rows)
Nan Values are replaced. Exiting recalculate_nan_forecasts.py
```

The run did real work — it found exactly the 3 affected stations and wrote 18 of 72 hindcast
rows. But **33 null values survived** at issue date 2026-08-18 (3 stations × their full 11-day
horizon, verified by before/after database counts: 605 rows, 572 non-null, 33 null, unchanged
across the run). The closing statement asserts the opposite.

## Mechanism

`recalculate_nan_forecasts.py:461-464` emits the same two lines — once through the logger, once
through `print` — with no reference to any outcome:

```python
logger.info("Nan Values are replaced. Exiting recalculate_nan_forecasts.py\n")
...
print("Nan Values are replaced. Exiting recalculate_nan_forecasts.py\n")
```

Every path that reaches the end of the function emits them, including:

- **Partial replacement** — the observed case. `changed_mask` (`:399`) counts only rows whose
  flag moved out of `[1, 2]`; the rest stay NaN and are never mentioned.
- **API write failure** — `:455-459` logs `"API write unsuccessful; data persisted only in CSV"`,
  and five lines later the run announces success. The two statements contradict each other, and
  the second is the one an operator scanning the tail of a log will read.
- **Per-code failure** — `:415-424` catches an exception from `update_forecast`, logs
  `"skipping code, NaN records preserved"`, and deliberately does not re-raise so the remaining
  codes still run. Correct behaviour; it also lands on the same closing line.
- **Nothing replaced at all** — if `replaced_rows` is empty the API block (`:429`) is skipped
  entirely, no count is logged, and the closing line still claims replacement.

The genuinely informative line already exists — `"Wrote %d recalculated forecasts to API (out of
%d hindcast rows)"` (`:437-441`) — but it is emitted **only** when the API write both ran and
returned success. Note its denominator is `len(hindcast)`, the hindcast row count, not the
number of NaN candidates, so "18 out of 72" does not mean "18 of 72 gaps closed".

The early guards are correctly quiet: `:294-295` returns before the closing line when there is
nothing to recalculate (though at DEBUG, so it is invisible at default level), as do the
emptiness and hindcast-failure guards at `:239`, `:252`, `:329` and `:333`.

## Desired outcome

The closing statement reports what happened. Something equivalent to:

```
recalculate_nan_forecasts: 3 code(s) with NaN candidates, 72 hindcast rows,
18 row(s) replaced, 33 NaN row(s) remaining, API write: OK
```

with "0 row(s) replaced" and "API write: FAILED" appearing where they apply. The counts needed
are already computed: `codes_with_nan`, `len(hindcast)`, `len(pd.concat(replaced_rows))`, the
surviving `flag in [1, 2]` count, and `api_write_ok`.

## Implementation sketch

1. Track the outcome counts already in scope and build one summary line at `:461`.
2. When `api_write_ok` is False, say so in that line rather than only in the preceding warning.
3. When nothing was replaced, say "0 replaced" — do not claim replacement.
4. Do **not** change the exit code under this issue. Whether a partial replacement should fail
   the run is a separate decision that touches `run_locally.sh`'s maintenance contract; record
   it, do not fold it in here.

Minor, same file: `console_handler` is created and formatted at `:33-34` but never added to the
logger (`:37-38` attaches only `file_handler`), so console visibility depends on propagation to
the root logger's handlers. Tidy it in the same patch or leave it — but do not "fix" it by
attaching the handler without checking for duplicate console output first.

## Testing

- [ ] Unit test: with a forecast frame where only some `flag in [1, 2]` rows are replaced by the
      hindcast, the closing summary reports the replaced count and the surviving NaN count.
- [ ] Unit test: with `_write_ml_forecast_to_api` returning `False`, the closing summary reports
      the API write as failed.
- [ ] Unit test: with an empty `replaced_rows`, the closing summary does not claim replacement.
- [ ] Regression: the early-return guards still emit no closing summary.
- [ ] `SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning` — zero failures, zero skips.

## Out of scope

- The exit-code contract for partial replacement.
- Why the 3 stations had no usable input in the first place — see **ML-020**.
- `fill_ml_gaps.py`'s own reporting (it correctly logs per-code missing ranges).

## Acceptance criteria

- [ ] The module's final line reflects candidates found, rows replaced, rows still NaN and the
      API write outcome.
- [ ] No path claims replacement when nothing was replaced.
- [ ] The API-failure warning and the closing line agree.
