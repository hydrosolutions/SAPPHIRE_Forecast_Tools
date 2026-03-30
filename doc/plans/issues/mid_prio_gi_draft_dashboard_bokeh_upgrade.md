# DASHBOARD: Upgrade Bokeh to fix WebSocket origin validation

**Priority:** Mid
**Owner:** Max
**Status:** Draft
**Created:** 2026-03-27

## Summary

Bokeh 3.4.3 in `forecast_dashboard` has a WebSocket origin validation bypass
(CVE-2026-21883). The fix requires upgrading to >=3.8.2, which is a significant
version jump (3.4 -> 3.8) with potential API changes.

## Vulnerability

- **CVE:** CVE-2026-21883 (GHSA-793v-589g-574v)
- **Severity:** Moderate
- **Package:** bokeh
- **Current version:** 3.4.3
- **Fix version:** >=3.8.2
- **Affected file:** `apps/forecast_dashboard/uv.lock`

### Impact Assessment

The vulnerability allows Cross-Site WebSocket Hijacking (CSWSH) of Bokeh
server applications. The `match_host` function incorrectly validates origins
that start with an allowed pattern (e.g., `example.com.evil.com` matches
`example.com`).

**Mitigating factors:**
- Only affects Bokeh server mode (not static HTML, embedded plots, or Jupyter)
- The forecast dashboard runs on an internal network
- Exploitation requires the attacker to know the server URL

## Risk of Upgrade

Bokeh 3.5+ introduced changes that may affect the dashboard:
- Layout system changes
- Deprecated APIs removed in later versions
- Widget behavior changes

This needs hands-on testing of the dashboard after the upgrade.

## Steps

1. Bump `geopandas` floor in `pyproject.toml`: change `"bokeh>=3.4.3"` or
   equivalent to `"bokeh>=3.8.2"`
2. Run `uv lock --upgrade-package bokeh`
3. Run tests: `SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard`
4. **Manual testing:** Start the dashboard locally and verify:
   - [ ] Dashboard loads without errors
   - [ ] All plots render correctly
   - [ ] Widgets (dropdowns, date pickers, etc.) function
   - [ ] Data updates work

## Acceptance Criteria

- [ ] Bokeh >=3.8.2 in lock file
- [ ] Dashboard tests pass
- [ ] Manual smoke test of all dashboard views
- [ ] Dependabot alert #28 resolved
