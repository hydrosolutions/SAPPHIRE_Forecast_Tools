# FD-011: Horizon selector widget passes translated strings as API enum values

**Status**: Draft
**Module**: forecast_dashboard
**Priority**: High
**Labels**: `forecast_dashboard`, `i18n`, `widgets`, `lr-visibility`
**Assigned**: @maxatp

---

## Summary

The horizon selector widget wraps its options in `_()` (gettext), so `wm.horizon_selector.value` returns a translated string. This string is used both as a comparison key against hardcoded English (`if horizon == "pentad":`) and as the `horizon_type` value sent directly to the postprocessing API. If a translator ever adds entries for `msgid "pentad"` or `msgid "decade"`, all horizon-dependent branching silently breaks and API calls fail with 422 validation errors.

## Context

Discovered during review of FD-010 (lr-visibility parameter mismatch). The issue is pre-existing and affects the entire dashboard, not just lr-visibility.

Currently benign because neither the `en_CH` nor `ru_KG` `.po` files contain standalone `msgid "pentad"` or `msgid "decade"` entries, so `_()` falls back to the identity function and returns the English string. The code works by accident, not by design.

## Problem

### Widget definition (`dashboard/widgets.py:82–92`)

```python
def create_horizon_selector():
    horizon_types = [_("pentad"), _("decade")]
    horizon_selector = pn.widgets.Select(
        name=_("Select forecast horizon:"),
        options=horizon_types,
        value=_("pentad"),
    )
    return horizon_selector
```

When `pn.widgets.Select` receives a plain list as `options`, `.value` returns the list element itself — there is no label/value distinction. So `.value` is whatever `_("pentad")` evaluated to at widget creation time.

### Impact chain if a translation is added

1. `_("pentad")` returns e.g. `"пятидневка"` in Russian
2. `wm.horizon_selector.value` returns `"пятидневка"`
3. `if horizon == "pentad":` at 30+ locations in `vizualization.py` silently falls through to `else` (decad branch)
4. API POST sends `"horizon_type": "пятидневка"` — rejected by the `HorizonType` Pydantic enum with a 422 error

### API schema (`sapphire/services/postprocessing/app/models.py:9–17`)

```python
class HorizonType(str, Enum):
    DAY = "day"
    PENTAD = "pentad"
    DECADE = "decade"
    MONTH = "month"
    QUARTER = "quarter"
    SEASON = "season"
```

Only lowercase English strings are accepted. The GET endpoint (`horizon: str = None`) is unvalidated, but the POST endpoint enforces `HorizonType` via `LRVisibilityBulkCreate`.

## Desired Outcome

`wm.horizon_selector.value` always returns the raw API enum string (`"pentad"` or `"decade"`) regardless of the UI language. Display labels are translated; stored values are not.

---

## Technical Analysis

### Root Cause

`pn.widgets.Select` supports two modes:
- **List of strings**: `options=["a", "b"]` — `.value` returns the string itself (used as both label and value)
- **Dict of label→value**: `options={"Label A": "a", "Label B": "b"}` — `.value` returns the dict value, display shows the key

The widget currently uses a list of translated strings, conflating display labels with API values.

### Fix

Change the widget options from a list to a dict:

```python
def create_horizon_selector():
    horizon_types = {_("pentad"): "pentad", _("decade"): "decade"}
    horizon_selector = pn.widgets.Select(
        name=_("Select forecast horizon:"),
        options=horizon_types,
        value="pentad",
    )
    return horizon_selector
```

After this change, `.value` returns `"pentad"` or `"decade"` (raw English), while the dropdown displays the translated label.

### Scope

Only one file needs to change: `apps/forecast_dashboard/dashboard/widgets.py` (the `create_horizon_selector` function). No changes needed in `vizualization.py` — the 30+ `if horizon == "pentad":` comparisons will continue to work because `.value` now always returns English.

---

## Implementation Steps

- [ ] **Step 1**: In `create_horizon_selector` (`dashboard/widgets.py:82–92`), change `options` from a list to a dict and set `value` to the raw string `"pentad"`.
- [ ] **Step 2**: Verify that any other selector widgets in `widgets.py` that use `_()` for options follow the same dict pattern (check `pentad_selector`, `decad_selector`, `station_selector`).
- [ ] **Step 3**: Test that the horizon selector still displays translated labels and that `.value` returns raw English strings in both `en_CH` and `ru_KG` locales.

---

## Testing

### Manual

1. Start the dashboard in English — verify dropdown shows "pentad"/"decade", `.value` returns `"pentad"`/`"decade"`
2. Switch to Russian locale — verify dropdown shows translated labels, `.value` still returns `"pentad"`/`"decade"`
3. Toggle visibility checkboxes, click Save Changes — verify lr-visibility API calls succeed (no 422)

### Unit (if widget tests exist)

- Create the widget, assert `horizon_selector.value == "pentad"`
- Change selection, assert `horizon_selector.value == "decade"` (not a translated string)

---

## Documentation Impact

No documentation update required — this is an internal widget fix with no user-facing behavior change.

## Out of Scope

- Auditing all other `_()` usages in widget options across the dashboard (could be a follow-up)
- Adding API-side tolerance for translated strings (the API schema is correct; the client should send valid values)

## Dependencies

None — this is independent of FD-010 and can be implemented at any time.

## Acceptance Criteria

- [ ] `wm.horizon_selector.value` returns `"pentad"` or `"decade"` (raw English) regardless of UI locale
- [ ] Dashboard dropdown still displays translated labels
- [ ] All horizon-dependent branching in `vizualization.py` continues to work
- [ ] lr-visibility save/read API calls succeed in both locales

---

## References

- `apps/forecast_dashboard/dashboard/widgets.py:82–92` — widget definition
- `apps/forecast_dashboard/src/vizualization.py:3209` — `horizon = wm.horizon_selector.value`
- `apps/forecast_dashboard/src/vizualization.py:3775` — `"horizon_type": horizon` in API POST
- `apps/forecast_dashboard/src/gettext_config.py` — `_()` translation function
- `sapphire/services/postprocessing/app/models.py:9–17` — `HorizonType` enum
- FD-010: discovered during review of lr-visibility parameter mismatch
