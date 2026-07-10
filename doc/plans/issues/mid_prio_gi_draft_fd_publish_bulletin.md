# Publish Bulletin — generate shareable JSON links from the dashboard

**Status**: Draft
**Module**: forecast_dashboard
**Priority**: Medium
**Labels**: `feature`, `forecast_dashboard`, `bulletin`

---

## Summary

Add a Bulletin-tab UI where a user selects one or more horizons and a subset of
stations, clicks "Generate links", and receives one shareable capability URL per
horizon — each returning a frozen JSON snapshot of the full Excel-equivalent
bulletin data for the selected stations, expiring when the next period of that
horizon begins.

## Context

Third-party organizations need to pull SAPPHIRE bulletin data as JSON. Per the
design (`doc/plans/publish_bulletin_api_design.md`), the **dashboard assembles**
the snapshot (the computed Excel fields — % of norm, volumes, norms, header
dates — do not exist in the `bulletins` DB table; they are computed here on the
`SapphireSite` objects), POSTs it to the new service endpoint, and shows the
returned URLs.

This is the **apps side**. It depends on the service contract from
`mid_prio_gi_draft_pp_bulletin_share_api.md`:
- `POST /bulletin/share` body `{horizon, year, horizon_value, expires_at,
  payload, station_codes}` → `{token, url, expires_at}`.
- Public `GET /public/bulletin/{token}` returns the `payload` verbatim.

## Problem

There is no UI or logic to export bulletin data for external sharing. The
existing horizon and station selectors are single-value `pn.widgets.Select`
(`dashboard/widgets.py:89-109`, `126-143`) and cannot express a multi-horizon /
multi-station selection.

## Desired Outcome

- A "Publish bulletin" card on the Bulletin tab: horizon multi-select + station
  multi-select + "Generate links" button + a results pane.
- On click: for each selected horizon, assemble the full snapshot for the
  selected stations, POST it, and display the returned URL.
- Stations with no bulletin data for a horizon are omitted and reported to the
  user; a horizon with zero resulting stations produces no link (with a warning).
- Service failure surfaces an error and produces no partial links.

---

## Technical Analysis

### Current Implementation

**Selectors** (`dashboard/widgets.py`):
- `create_horizon_selector` `89-109` — `pn.widgets.Select`, options
  `{label: "pentad"|"decade"|"month"|"season"}`, `.value` is a single string.
- `create_station_selector` `126-143` — `pn.widgets.Select`, `groups=station_dict`
  (grouped by basin); `.value` is a label like `"<code> - <river> <punkt>"`; the
  code is extracted via `.value.split()[0]` (see `widget_manager.py:200`).
- Multi-select template already in repo: `create_model_checkbox`
  (`widgets.py:162`, a `pn.widgets.CheckBoxGroup`).

**Widget manager** (`dashboard/widget_manager.py`):
- Selectors instantiated: `station_selector` `54`, `horizon_selector` `59`.
- `self.last_date, self.forecast_horizon, self.forecast_year` set in `__init__`
  `63-65` from `dm.get_bulletin_metadata(self.horizon_selector.value)` and re-set
  on change `226-229`. **These singletons track the CURRENT single horizon** — do
  NOT rely on them for multi-horizon; call `dm.get_bulletin_metadata(h)` per
  selected horizon.
- New widgets go in the "BULLETIN TAB WIDGETS" block (~`142-158`) as `wm.<...>`.

**API client** (`src/db.py`):
- Config: `api_gateway_url = os.getenv("API_GATEWAY_URL", "http://localhost:8000")`
  `24`; `API_BASE = f"{api_gateway_url}/api"` `25`; `API_TIMEOUT = 30` `26`.
- `_save_data(service_type, data_type, records)` `178-198` — POSTs
  `json={"data": records}`, `raise_for_status()`, returns None.
- **No auth header is sent anywhere** (verified). The share POST rides the same
  open proxy as existing writes — no key handling needed for MVP.

**Layout** (`src/layout.py`): active builder is `define_tabs_2` (`326+`). Bulletin
tab at `435-455`: a `pn.Column` with a "Forecast bulletin" `pn.Card` (`438-445`)
and a collapsed "Download bulletin" `pn.Card` (`446-453`). Insert the new card as
a third sibling (after `453`, before the closing `)` at `454`). Sidebar:
`define_sidebar_2` `46-54`.

**Horizon metadata** (`dashboard/data_manager.py`):
- `get_bulletin_metadata(horizon)` `333-361` → `(last_date, forecast_horizon,
  forecast_year)`. `last_date` = `forecasts_all['date'].max() + 1 day` = **start
  of the current period**. `forecast_horizon` = period-in-year index.

**Period-boundary utilities** (`apps/iEasyHydroForecast/tag_library.py`,
importable in tests via conftest sys.path):
- `get_date_for_pentad(pentad_in_year, year)` `672-730` → `'YYYY-MM-DD'` first day
  of pentad (1-72); **does NOT roll the year past 72 — handle wraparound**.
- `get_date_for_decad(decad_in_year, year)` `733+` (1-36).

**Snapshot source — `SapphireSite` attributes** (`src/site.py`):
- Identity: `code` `40`, `station_label` `47`, `basin_ru` `52`,
  `river_name_ru`/`punkt_name_ru` `43-44`.
- Short-term (`get_forecast_attributes_for_site` `197-230`): `forecast_expected`,
  `forecast_lower_bound`, `forecast_upper_bound`, `forecast_delta`,
  `forecast_sdivsigma`, `forecast_mae`, `forecast_accuracy`, `perc_norm`.
- Monthly (`get_monthly_forecast_attributes_for_site` `232-270`) & seasonal
  (`272-311`): `forecast_expected` (monthly), `forecast_q_min/q_max`,
  `forecast_v_min/v_max`, `forecast_norm`, `perc_norm`.
- These are hydrated by `bulletin_manager._load_bulletin_from_api` +
  `_populate_forecast_attributes` (in `dashboard/bulletin_manager.py`).

### Root Cause (what's missing)

No multi-select UI, no per-horizon snapshot assembler, no expiry helper, no
share-POST client, no results rendering.

---

## Implementation Plan

### Approach

Reuse the existing bulletin hydration pipeline to build the payload: per selected
horizon, call `_load_bulletin_from_api(horizon, year, horizon_value,
dm.sites_list)` (which already runs `_populate_forecast_attributes`), filter to
the selected station codes, and serialize each site's attributes into a JSON row.
Compute `expires_at` from `get_bulletin_metadata` + a new period-boundary helper.
POST via a new thin `src/db.py` client that returns the response body. Keep the
assembler and expiry helper as **pure functions** (no Panel import) so they are
unit-testable; keep the button handler in the widget/manager layer.

Station multi-select is populated from the full station pool (grouped, like
`station_selector`); at generation, only stations with bulletin data for a given
horizon are included, the rest are reported skipped. (Alternative — populate only
from current bulletin sites — noted as a possible refinement; default to the full
pool + skip-reporting for flexibility.)

### Files to Create

| File | Purpose |
|------|---------|
| `apps/forecast_dashboard/dashboard/bulletin_publish.py` | Pure helpers: `assemble_bulletin_snapshot(...)`, `compute_next_period_start(...)`, `serialize_site(...)`; plus a `BulletinPublisher`-style handler if kept separate from `BulletinManager` |
| `apps/forecast_dashboard/tests/test_bulletin_publish.py` | Tests for the helpers + button handler |

### Files to Modify

| File | Changes |
|------|---------|
| `dashboard/widgets.py` | Add `create_publish_horizon_multiselect`, `create_publish_station_multiselect`, `create_generate_links_button`, `create_publish_results_pane` |
| `dashboard/widget_manager.py` | Instantiate the new widgets (~`142-158`); wire the button to the handler |
| `src/db.py` | Add `_post_bulletin_share(payload) -> dict` (POST, returns `resp.json()`) |
| `src/layout.py` | Add a "Publish bulletin" `pn.Card` in `define_tabs_2` Bulletin tab (after `:453`) |

### Implementation Steps

- [ ] Step 1: `compute_next_period_start(horizon, forecast_horizon,
  forecast_year, forecast_date)` → UTC datetime (pentad/decade via
  `get_date_for_pentad/decad` with 72/36 wraparound; month via
  `date(year, month+1, 1)` with Dec→Jan rollover; season/quarter from calendar).
- [ ] Step 2: `serialize_site(site, horizon)` → dict with the correct field set
  per horizon (short-term vs month/season) + identity fields.
- [ ] Step 3: `assemble_bulletin_snapshot(horizon, selected_codes, dm,
  forecast_date)` → `{payload, skipped_codes}` using `_load_bulletin_from_api` +
  filtering; `payload` includes `{horizon, year, horizon_value, valid_from,
  valid_to, generated_at, expires_at, stations:[...]}`.
- [ ] Step 4: `_post_bulletin_share(payload)` in `src/db.py`.
- [ ] Step 5: Widgets (horizon `CheckBoxGroup`/`MultiSelect`, station
  `MultiSelect` grouped by basin, button, results `pn.pane.Markdown`).
- [ ] Step 6: Button handler — for each selected horizon: assemble → POST →
  collect URL; render URLs + skipped report; skip empty horizons; on any POST
  failure show an error and render no partial links.
- [ ] Step 7: Layout card + widget_manager wiring.
- [ ] Step 8: Tests.

### Code Examples

```python
# dashboard/bulletin_publish.py  (pure — no panel import)

def compute_next_period_start(horizon, forecast_horizon, forecast_year, forecast_date):
    """Return the UTC datetime at which a link for this horizon expires:
    the first day of the NEXT period. forecast_date passed in per the
    Forecast Date Rule (no date.today() here)."""
    ...

def serialize_site(site, horizon) -> dict:
    base = {"code": site.code, "station_label": site.station_label,
            "basin": getattr(site, "basin_ru", ""),
            "river": getattr(site, "river_name_ru", ""),
            "model": getattr(site, "forecast_model", "")}
    if horizon in ("month", "season"):
        base.update(q_min=site.forecast_q_min, q_max=site.forecast_q_max,
                    v_min=site.forecast_v_min, v_max=site.forecast_v_max,
                    norm=site.forecast_norm, perc_norm=site.perc_norm,
                    forecasted_discharge=getattr(site, "forecast_expected", None))
    else:
        base.update(forecasted_discharge=site.forecast_expected,
                    fc_lower=site.forecast_lower_bound,
                    fc_upper=site.forecast_upper_bound,
                    delta=site.forecast_delta, sdivsigma=site.forecast_sdivsigma,
                    mae=site.forecast_mae, accuracy=site.forecast_accuracy,
                    perc_norm=site.perc_norm)
    return base
```

```python
# src/db.py
def _post_bulletin_share(payload: dict) -> dict:
    url = f"{API_BASE}/postprocessing/bulletin/share/"   # confirm trailing slash vs service route
    resp = requests.post(url, json=payload, timeout=API_TIMEOUT)
    resp.raise_for_status()
    return resp.json()   # {token, url, expires_at}
```

> Note: the service route is `POST /bulletin/share` (no trailing slash in the
> service). Confirm the gateway proxy path (`/api/postprocessing/bulletin/share`)
> matches during integration.

---

## Testing

### Test Cases

- [ ] `compute_next_period_start`: pentad rolls 72→next year pentad 1; decade
  rolls 36→1; month rolls Dec→Jan; returns start-of-next-period for mid-range
  cases. (Parametrized; injected `forecast_date`.)
- [ ] `serialize_site`: short-term produces the pentad/decade field set;
  month/season produce volumes + norm; identity fields present.
- [ ] `assemble_bulletin_snapshot`: selected codes present → included; selected
  code with no bulletin data → in `skipped_codes`, not in `payload.stations`;
  payload carries horizon/year/period + expiry.
- [ ] Button handler: N selected horizons → N POSTs → N URLs surfaced (faked
  `_post_bulletin_share`); a horizon with zero stations → no link + warning;
  POST raises → error shown, no partial links.

### Testing Commands

```bash
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard
```

### Test conventions

- Pure helpers: importable without Panel; use `tests/conftest.py` fixtures +
  `tag_library` (already on sys.path). See `tests/conftest.py:1-201`.
- Button handler (imports panel): use the `sys.modules`-stub bootstrap from
  `tests/test_bulletin_edit_persistence.py:28-55` and mock
  `src.db._post_bulletin_share` with the `mock_api_response` factory
  (`conftest.py:130-141`).

### Manual Verification

1. `bash apps/run_locally.sh` (or the dashboard entry point); open the Bulletin
   tab.
2. Add a few stations to the bulletin (existing flow) for one or more horizons.
3. In "Publish bulletin", select ≥2 horizons + some stations → "Generate links".
4. Confirm one URL per horizon appears + a skipped-station report for any without
   data. Open a URL and confirm it returns the expected JSON; confirm it 410s
   after expiry (or with a manually-past `expires_at`).

---

## Documentation Impact

- [ ] User guide (`doc/user_guide.md`) — how to publish/share bulletin links.
- [ ] Module README (`apps/forecast_dashboard/README.md`) — new feature + the
  `API_GATEWAY_URL` reliance.
- [ ] `doc/data_flow_short_term.md` / `_long_term.md` — bulletin sharing path.
- [ ] Configuration docs — none new on the dashboard side (`API_GATEWAY_URL`
  already exists); the public base URL lives service-side.
- [ ] Claude memory — note the payload-source pipeline reuse if it becomes a
  stable pattern.

---

## Out of Scope

- Link management UI (list/revoke) — generate-only MVP.
- Per-organization scoping.
- Any change to `sapphire/services/` (that is the companion services issue).

## Dependencies

- **Blocks on** `mid_prio_gi_draft_pp_bulletin_share_api.md` (the `POST
  /bulletin/share` contract + public GET). Helpers/UI can be built and unit-tested
  against a faked client first; end-to-end verification needs the service.

## Acceptance Criteria

- [ ] Multi-horizon + multi-station selection UI on the Bulletin tab.
- [ ] "Generate links" produces one URL per selected horizon; empty horizons
  produce none (with a warning); skipped stations are reported.
- [ ] Payload contains the full Excel-equivalent field set per horizon.
- [ ] `expires_at` = start of next period, correct across pentad-72 / decade-36 /
  December rollovers.
- [ ] Service failure shows an error and yields no partial links.
- [ ] `SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` passes, zero
  unexpected skips.
- [ ] No station codes / discharge values committed in code, fixtures, or docs.

---

## References

- Design: `doc/plans/publish_bulletin_api_design.md`
- Services companion issue: `doc/plans/issues/mid_prio_gi_draft_pp_bulletin_share_api.md`
- Payload pipeline: `dashboard/bulletin_manager.py` (`_load_bulletin_from_api`,
  `_populate_forecast_attributes`), `src/site.py:197-365`
