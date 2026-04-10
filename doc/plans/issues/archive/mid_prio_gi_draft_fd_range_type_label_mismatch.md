# Fix range_type label mismatch and inverted math in summary table

**Status**: Review — implementation complete, 214 unit tests pass (2026-04-09)
**Module**: forecast_dashboard
**Priority**: Mid
**Labels**: `forecast_dashboard`, `bug`

---

## Summary

The `"min[delta, %]"` range option in the forecast dashboard works correctly in hydrograph plots but is silently broken in the summary table. A label mismatch causes the branch in `processing.py` to be dead code, and the scalar sub-branch also has inverted math.

## Context

The dashboard offers three range-type options for forecast uncertainty bands:
1. `"delta"` — use the skill-metric delta as ±offset
2. `"Manual range, select value below"` — user-specified percentage
3. `"min[delta, %]"` — intersection of delta and percentage bands (narrowest envelope)

The **hydrograph plots** (`vizualization.py`) handle all three correctly. The **summary table** routes through `processing.py:calculate_forecast_range`, which has a different label (`"max[delta, %]"`) that never matches the widget value.

## Problem

### 1. Dead branch — label mismatch

The UI widget (`widgets.py:166`) emits `"min[delta, %]"`. The hydrograph functions (`vizualization.py:2397`, `vizualization.py:2747`) check for `"min[delta, %]"` — correct.

But `processing.py:1246` checks for `"max[delta, %]"`:

```python
elif range_type == _("max[delta, %]"):   # never matches widget value
```

When the user selects `"min[delta, %]"`, execution in `calculate_forecast_range` falls through all branches to the `else` fallback, which computes plain `±delta` — ignoring the percentage slider entirely.

### 2. Inverted math in scalar sub-branch

Even within the dead branch, the two sub-branches compute opposite things:

| Sub-branch | Lower bound | Upper bound | Semantic |
|-----------|------------|------------|----------|
| Widget path (`hasattr`) | `np.maximum(fc-δ, (1-%)fc)` | `np.minimum(fc+δ, (1+%)fc)` | **Narrow** envelope (correct) |
| Scalar path (`else`) | `np.minimum(fc-δ, (1-%)fc)` | `np.maximum(fc+δ, (1+%)fc)` | **Wide** envelope (inverted) |

The reference implementation in `vizualization.py` uses the narrow-envelope math (`np.maximum` for lower, `np.minimum` for upper). The scalar sub-branch has min/max swapped.

### 3. Tests test the dead branch

`test_processing.py:test_max_delta_pct_scalar` calls `calculate_forecast_range` with the string `"max[delta, %]"` and a scalar slider — exercising the dead branch with the inverted math. The test passes, but it tests behavior that never occurs from the UI.

## Desired Outcome

- `processing.py` uses `"min[delta, %]"` to match the widget label
- Both sub-branches use `np.maximum` for lower / `np.minimum` for upper (narrow envelope), matching `vizualization.py`
- Summary table and hydrograph plots show consistent range bands when `"min[delta, %]"` is selected
- Tests updated to reflect correct label and math

---

## Technical Analysis

### Affected code (line numbers as of 2026-04-09, after LR-only fix commit)

| File | Line(s) | Issue |
|------|---------|-------|
| `apps/forecast_dashboard/src/processing.py` | 1247 | `"max[delta, %]"` should be `"min[delta, %]"` |
| `apps/forecast_dashboard/src/processing.py` | 1253-1257 | Scalar sub-branch: `np.minimum`/`np.maximum` should be `np.maximum`/`np.minimum` |
| `apps/forecast_dashboard/tests/test_processing.py` | 130-138 | Uses wrong label `"max[delta, %]"`; asserted values reflect inverted math |
| `apps/forecast_dashboard/tests/test_lr_only_fixes.py` | 141-153 | Uses wrong label; asserted values reflect wrong semantics |

**Note:** The LR-only fix (FD-001) replaced all `forecast_table['delta']` with `delta_offset` in this function. The code snippets below reflect the current state with `delta_offset`.

### Reference implementation (correct)

`vizualization.py:2397-2401` (both v2 and legacy are identical):

```python
elif range_type == _("min[delta, %]"):
    forecasts['fc_lower'] = np.maximum(
        forecasts['forecasted_discharge'] - delta_offset,
        (1 - range_slider / 100.0) * forecasts['forecasted_discharge'])
    forecasts['fc_upper'] = np.minimum(
        forecasts['forecasted_discharge'] + delta_offset,
        (1 + range_slider / 100.0) * forecasts['forecasted_discharge'])
```

**Semantic:** `np.maximum` for lower bound picks the tighter (higher) lower bound; `np.minimum` for upper bound picks the tighter (lower) upper bound. Result: the intersection of delta-band and percentage-band — the narrowest range that both constraints agree on.

### Widget definition (correct, no change needed)

`widgets.py:166`:
```python
range_selector = pn.widgets.Select(
    options=[_("delta"), _("Manual range, select value below"), _("min[delta, %]")],
    value=_("delta"),
)
```

### Data flow

```
widgets.py:166                    "min[delta, %]"
    ↓ (via plot_manager.py)
    ├── vizualization.py:2397     elif == "min[delta, %]"    ✓ matches (range_slider=scalar)
    ├── vizualization.py:2747     elif == "min[delta, %]"    ✓ matches (range_slider=scalar)
    └── processing.py:1247        elif == "max[delta, %]"    ✗ never matches → else fallback
```

### Caller analysis

`calculate_forecast_range` has one production caller:
- `vizualization.py:2983` inside `create_forecast_summary_table`
- `range_type` arrives as a widget object (unwrapped to string at line 1232-1233)
- `range_slider` arrives as a widget object → `hasattr(range_slider, 'value')` is True

The hydrograph functions receive `range_slider.value` (scalar) via `plot_manager.py:125` (`_common_plot_kwargs`), so they have no `hasattr` branching. The summary table path passes the widget object via `plot_manager.py:161` (`update_forecast_tabulator`).

In production, only the widget sub-branch of the `max[delta, %]` branch would execute (if the label matched). The scalar sub-branch is only exercised by tests.

### Behavioral impact

| Scenario | Hydrograph (vizualization.py) | Summary table (processing.py) |
|----------|------------------------------|-------------------------------|
| User selects `"delta"` | ±delta (correct) | ±delta (correct) |
| User selects `"Manual range"` | ±% (correct) | ±% (correct) |
| User selects `"min[delta, %]"` | Intersection/narrow (correct) | **±delta only** (falls to else) |

The summary table has **never** applied the percentage cap when `"min[delta, %]"` is selected. This is a silent behavioral bug affecting all deployments since the option was introduced.

---

## Implementation Plan

### Files to Modify

| File | Changes |
|------|---------|
| `apps/forecast_dashboard/src/processing.py` | Fix label and scalar math |
| `apps/forecast_dashboard/tests/test_processing.py` | Update label and expected values |
| `apps/forecast_dashboard/tests/test_lr_only_fixes.py` | Update label and expected values |

### Fix

**Single phase, single agent.** All changes are in the same three files with no cross-file dependencies.

**`processing.py:1247`** — Change label:
```python
# Before:
    elif range_type == _("max[delta, %]"):
# After:
    elif range_type == _("min[delta, %]"):
```

**`processing.py:1253-1257`** — Fix scalar sub-branch math (swap `np.minimum`↔`np.maximum`):
```python
# Before (inverted — produces wider envelope):
        else:
            forecast_table['fc_lower'] = np.minimum(forecast_table['forecasted_discharge'] - delta_offset,
                                                    (1 - range_slider / 100.0) * forecast_table['forecasted_discharge'])
            forecast_table['fc_upper'] = np.maximum(forecast_table['forecasted_discharge'] + delta_offset,
                                                    (1 + range_slider / 100.0) * forecast_table['forecasted_discharge'])
# After (correct — produces narrower envelope, matches vizualization.py):
        else:
            forecast_table['fc_lower'] = np.maximum(forecast_table['forecasted_discharge'] - delta_offset,
                                                    (1 - range_slider / 100.0) * forecast_table['forecasted_discharge'])
            forecast_table['fc_upper'] = np.minimum(forecast_table['forecasted_discharge'] + delta_offset,
                                                    (1 + range_slider / 100.0) * forecast_table['forecasted_discharge'])
```

**`test_processing.py:test_max_delta_pct_scalar` (lines 130-138)** — Fix label, comment, and values:
```python
# Before (uses dead label, asserts inverted math):
    def test_max_delta_pct_scalar(self, identity_gettext, forecast_table):
        # max[delta, %] with 5% → pct = 5.0, delta = 10.0
        # For row 0: delta range = [90, 110], pct range = [95, 105]
        # fc_lower = min(90, 95) = 90; fc_upper = max(110, 105) = 110
        result = processing.calculate_forecast_range(
            identity_gettext, forecast_table, "max[delta, %]", 5
        )
        assert result["fc_lower"].iloc[0] == pytest.approx(90.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(110.0)

# After (correct label, narrow-envelope math):
    def test_min_delta_pct_scalar(self, identity_gettext, forecast_table):
        # min[delta, %] with 5% → pct = 5.0, delta = 10.0
        # For row 0: delta range = [90, 110], pct range = [95, 105]
        # Intersection: fc_lower = max(90, 95) = 95; fc_upper = min(110, 105) = 105
        result = processing.calculate_forecast_range(
            identity_gettext, forecast_table, "min[delta, %]", 5
        )
        assert result["fc_lower"].iloc[0] == pytest.approx(95.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(105.0)
```

**`test_lr_only_fixes.py:test_max_delta_percent_missing_delta` (lines 141-153)** — Fix label, docstring, and values:
```python
# Before:
    def test_max_delta_percent_missing_delta(self, identity_gettext):
        """max[delta, %] with missing delta → percentage range still applies. ..."""
        ...
        result = processing.calculate_forecast_range(
            identity_gettext, df, "max[delta, %]", 10
        )
        assert result["fc_lower"].iloc[0] == pytest.approx(90.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(110.0)

# After:
    def test_min_delta_percent_missing_delta(self, identity_gettext):
        """min[delta, %] with missing delta → zero-width range.

        With delta_offset=0 and slider=10%: delta band=[100,100], pct band=[90,110].
        Intersection (narrow): max(100, 90)=100, min(100, 110)=100 → zero-width.
        """
        ...
        result = processing.calculate_forecast_range(
            identity_gettext, df, "min[delta, %]", 10
        )
        assert result["fc_lower"].iloc[0] == pytest.approx(100.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(100.0)
```

---

## Testing

### Test Cases

- [ ] `"min[delta, %]"` with delta and percentage → narrower envelope (intersection)
- [ ] `"min[delta, %]"` with missing delta → zero-width range (delta band is zero)
- [ ] `"min[delta, %]"` with widget object `range_slider` → same result as scalar
- [ ] Summary table and hydrograph show same range values for `"min[delta, %]"`
- [ ] Existing `"delta"` and `"Manual range"` tests still pass unchanged

### Testing Commands

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard
```

---

## Risks

- **R1**: Existing deployments where users have selected `"min[delta, %]"` will see a behavior change in the summary table — from pure ±delta to the correct intersection band. This is a bug fix, not a regression.
- **R2**: No translation files map between `"min[delta, %]"` and `"max[delta, %]"`, so localized deployments are also affected by the dead branch. The fix applies uniformly.

## Out of Scope

- Adding a `"max[delta, %]"` (wider envelope) option to the widget — could be useful but is a feature, not a fix
- Refactoring `calculate_forecast_range` to eliminate the `hasattr` branching

## Dependencies

- Requires FD-001 (LR-only deployment fixes) to be applied first — that fix introduced `delta_offset` in `calculate_forecast_range`. The code snippets in this plan assume `delta_offset` is present. FD-001 is already implemented (Review status as of 2026-04-09).

## Acceptance Criteria

- [ ] `processing.py` branch label matches the widget value `"min[delta, %]"`
- [ ] Both sub-branches produce the narrow (intersection) envelope
- [ ] Tests updated with correct label and expected values
- [ ] All forecast_dashboard tests pass
