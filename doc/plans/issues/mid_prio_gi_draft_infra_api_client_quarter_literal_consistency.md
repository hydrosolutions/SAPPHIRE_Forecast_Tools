## sapphire-api-client `horizon_type` Literals diverge — `quarter` missing on most write/read paths (INFRA-019)

**Status**: Resolved 2026-06-12 — fixed upstream in `sapphire-api-client` v0.5.0 (`4fd543e`); pin bumped 0.4.0 → 0.5.0 across all consuming modules and landed on `maxat_sapphire_2` via PR #373. Upstream now exposes a single `HorizonTypeLiteral` source of truth in `validators.py` (includes `quarter`), with `VALID_HORIZONS = set(get_args(HorizonTypeLiteral))` derived from it so type hints and runtime validation cannot drift.
**Module**: `sapphire-api-client` (external repo: `hydrosolutions/sapphire-api-client`) + every SAPPHIRE module that pins it
**Priority**: **Medium** (hygiene / latent footgun — no current operational failure traces to it)
**Labels**: `api-client`, `enum`, `consistency`, `tech-debt`, `cross-module`
**Discovered**: 2026-06-12 while investigating PREPQ-008 (quarterly hydrograph `422`).
**Related**:
- **PREPQ-008** ([`high_prio_gi_draft_runoff_quarter_horizon_type_rejected.md`](high_prio_gi_draft_runoff_quarter_horizon_type_rejected.md)) — the investigation that surfaced this. PREPQ-008 was **not** caused by this divergence (the failing write path posts raw dicts and does not validate client-side), so this is filed separately as agreed (decision D3 = defer).
- **MIG-003** ([`high_prio_gi_draft_migration_horizon_type_case_coercion.md`](high_prio_gi_draft_migration_horizon_type_case_coercion.md)) — sibling `horizon_type` consistency bug class.
- sapphire-api-client re-pin procedure (memory: `sapphire_api_client_repin_procedure.md`) — bumping the pin touches ~17 files / 10 modules.

---

## Summary

The pinned `sapphire-api-client` (rev `7bd349172ef24576b654a7b78f38734de3f2e657`) declares the `horizon_type` parameter as a `Literal[...]` on several methods, but the allowed value sets **disagree about `quarter`**. Most paths omit it; one includes it. The authoritative server-side enum (`sapphire/services/preprocessing/app/models.py` `HorizonType`) includes all of `day, pentad, decade, month, quarter, season, year`. The client should match.

| File:line (under `sapphire_api_client/`) | Method | Includes `quarter`? |
|---|---|---|
| `postprocessing_base.py:99` | (base write) | **yes** |
| `preprocessing.py:108` | `prepare_runoff_records` | no |
| `preprocessing.py:213` | `prepare_hydrograph_records` | no |
| `short_term.py:113` | (short-term write) | no |
| `postprocessing.py:94` | (read) | no |

## Why it matters (and why it is only Medium)

- **Latent footgun**: any caller that *does* route through `prepare_hydrograph_records` / `prepare_runoff_records` (the validating helpers) with `horizon_type="quarter"` will be rejected **client-side** with a confusing `Literal` error, even against a correct server. Today the long-horizon writer bypasses these helpers (posts raw dicts), so no operational path currently trips it — hence Medium, not High.
- **Inconsistency is a correctness hazard**: `quarter` is a first-class horizon in the server enum and in postprocessing; the client's read path (`postprocessing.py:94`) silently can't express it either.

## Proposed fix direction (develop later)

> The client is an **external repo** — changes happen upstream in `hydrosolutions/sapphire-api-client`, then the pin is bumped here. Coordinate; do not vendor-patch the installed `.venv` copy.

1. Upstream: add `"quarter"` to the four diverging `Literal[...]`s so all `horizon_type` Literals equal the server `HorizonType` value set (`day, pentad, decade, month, quarter, season, year`). Consider deriving them from a single shared constant to prevent future drift.
2. Cut a new client rev; re-pin across all consuming modules per the re-pin procedure (`uv lock --upgrade-package sapphire-api-client`) — ~17 files / 10 modules incl. transitive (forecast_dashboard, pipeline, preprocessing_station_forcing).
3. Run the full suite per module after re-pin.

## Acceptance criteria

- [x] All `horizon_type` Literals in the client match the server `HorizonType` value set (incl. `quarter`) — unified behind `HorizonTypeLiteral` in v0.5.0.
- [x] New client rev cut upstream (v0.5.0, `4fd543e`) and pin bumped across all consuming modules (PR #373).
- [x] `SAPPHIRE_TEST_ENV=True bash run_tests.sh` green across affected modules after re-pin (all 8 client-consuming modules pass; 40/40 CI checks green on #373).
