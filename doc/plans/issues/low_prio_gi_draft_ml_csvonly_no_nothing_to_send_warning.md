# ML-026: CSV-only ML runs (`sapphire-api-client` not installed) emit no "nothing to send" warning

**Status**: Draft (2026-09-09)
**Module**: `apps/machine_learning` (`make_forecast.py`)
**Priority**: Low — `SAPPHIRE_API_AVAILABLE=False` is a documented and required
*test* scenario (see Reachability), not a documented *production* deployment
mode: `apps/machine_learning/pyproject.toml:10-31` declares `sapphire-api-client`
as a required (non-optional) dependency, and the production
`apps/machine_learning/Dockerfile:33-34` installs it via `uv sync --frozen
--no-dev` from the committed lockfile, which fails the build rather than
silently omitting it. The client-absent path is only reachable in a tolerated
incomplete or developer environment (e.g. a local venv where the private
`sapphire-api-client` git dependency failed to install) — not in the shipped
production image. When it is reached, the consequence is the same class of
silent-success outcome ML-021/ML-019/ML-022 were filed to close, which is why
this is Low rather than dropped — not Medium, since no repository evidence
shows it reachable in a deployed environment.
**Labels**: `ml`, `api`, `silent-success`, `csv-fallback`
**Found**: 2026-09-08/09, out-of-loop review of ML-021 (PR #503). Listed as
"deliberately not addressed" defect 3 in
[`review_gi_draft_ml_forecast_api_write_silent_success.md`](review_gi_draft_ml_forecast_api_write_silent_success.md)
(`## Deliberately not addressed`); this file expands it to a standalone issue.

---

## Defect

`write_pentad_forecast` (`make_forecast.py:151-231`) and `write_decad_forecast`
(`:234-314`) gate the entire API write path behind
`if SAPPHIRE_API_AVAILABLE:` (`:182`, `:265`). `SAPPHIRE_API_AVAILABLE` reflects
whether the `sapphire_api_client` package **imported successfully**
(`scr/utils_ml_forecast.py:44-58` — `True` on success, `False` on `ImportError`);
it is a different flag from `SAPPHIRE_API_ENABLED`, the runtime opt-out.

As part of ML-021, `_write_ml_forecast_to_api` was deliberately changed to check
emptiness **first**, before anything about the API's state, and log it at
WARNING instead of the old INFO (`scr/utils_ml_forecast.py:757-763`) —
specifically because the root logger is capped at WARNING
(`scr/utils_ml_forecast.py:38`, `logging.getLogger().setLevel(logging.WARNING)`),
so the old INFO line reached no log at all. That fix works whenever
`_write_ml_forecast_to_api` is actually called.

**It is never called when `SAPPHIRE_API_AVAILABLE` is `False`.** The outer
`if SAPPHIRE_API_AVAILABLE:` guard in `make_forecast.py` skips the function
entirely — not just the write, but the emptiness check inside it. `api_write_ok`
stays at its initialized default of `True` (`:180`/`:263`), the CSV writes still
run (unconditionally, unaffected by this issue), and the function returns
`True`. If there is nothing to forecast — e.g. an empty `rivers_to_predict` —
**no warning of any kind is emitted**, because the code path that would emit it
never runs.

The CSV path itself is **not** a no-op: it still reads the old archive CSV
(`pentad_<MODEL>_forecast.csv` / `decad_<MODEL>_forecast.csv`), concatenates
the (possibly empty) new data, normalizes columns and dates
(`normalize_ml_csv_columns`), deduplicates on
`["forecast_date", "date", "code"]`, and rewrites the file unconditionally
(the `# --- 2. Write to CSV (archive/fallback) ---` block in each of
`write_pentad_forecast` and `write_decad_forecast`). So the
archive is not guaranteed byte-for-byte "unchanged from the previous run" — a
rewrite through this pipeline may reformat, reorder, or (via the dedup step)
drop rows even when no new forecast row is added, and the read-concat-write can
fail outright when no valid prior archive exists (caught by the surrounding
`try`, logged as an error, not raised). What is accurate is narrower: the
archive is left in an **apparently-present** state either way — there is no
distinguishing "ran and forecast nothing" from "ran and forecast something" by
looking at whether the file exists, and no warning marks the nothing-to-forecast
case.

**This is narrower than "`SAPPHIRE_API_ENABLED=false`".** If the client library
*is* installed (`SAPPHIRE_API_AVAILABLE=True`) but disabled via
`SAPPHIRE_API_ENABLED=false`, the outer guard passes, `_write_ml_forecast_to_api`
*is* called, and its emptiness check runs and logs the WARNING before the
disabled check is even reached (`:757` precedes `:770-773`) — that combination
is **not** affected by this issue. The gap is specific to the client library
being absent, not to the API being turned off.

## Reachability — a documented, required *test* scenario; not a documented *deployment* mode

- `SAPPHIRE_API_AVAILABLE=False` (client not installed) is an explicitly
  documented and required **test** scenario, not an inferred edge case:
  `doc/dev/testing_workflow.md:107` shows
  `test_returns_false_when_api_unavailable` patching `SAPPHIRE_API_AVAILABLE` to
  `False` as one of two required API-failure tests, distinct from
  `test_returns_false_when_api_disabled` (`:111`, which patches
  `SAPPHIRE_API_ENABLED` instead). CLAUDE.md's "Zero Skips Policy" separately
  names `SAPPHIRE_API_AVAILABLE`-gated skips as the **only** acceptable skip
  pattern in this codebase, confirming the client-absent case is a first-class,
  supported *test* state, not an accident.
- **This is a statement about test coverage, not about production deployment.**
  `apps/machine_learning/pyproject.toml:10-31` lists `sapphire-api-client` as a
  required project dependency (not `optional-dependencies`), and the production
  `apps/machine_learning/Dockerfile:26-34` installs it from the committed
  `uv.lock` via `uv sync --frozen --no-dev` — `--frozen` fails the build rather
  than silently proceeding without a locked dependency. Nothing in this repo
  documents "client library not installed" as a *supported production
  deployment shape*; the closest documented CSV-only mode,
  `SAPPHIRE_API_ENABLED=false` (client installed, API disabled), is a different
  condition not affected by this issue (see above). The client-absent path is
  therefore a **tolerated incomplete or developer environment** — e.g. a local
  `.venv` where the private `sapphire-api-client` git dependency failed to
  install — not a shape the shipped Docker image can be in. How often a
  developer runs `make_forecast.py` directly in such an environment with
  `rivers_to_predict` also empty is not established here.

## Desired outcome

The "nothing to forecast" condition should be visible whenever it happens,
including when the client library is absent — either by having
`make_forecast.py` check emptiness before the `SAPPHIRE_API_AVAILABLE` guard (so
the WARNING fires regardless of API availability), or by adding an explicit
WARNING in the `else` branch of the guard when there is nothing to forecast. The
CSV write behavior must not change — this issue is about the missing log line,
not about making CSV-only mode write to the API.

## Out of scope

- Any change to `SAPPHIRE_API_AVAILABLE` detection or import behavior.
- The `SAPPHIRE_API_ENABLED=false`-with-client-installed case — already covered
  by ML-021's emptiness-first fix, not broken.
- Deciding whether "client library not installed" should itself be a supported
  production deployment shape (as opposed to a test/CI convenience) — an owner
  question, not resolved here.

## Acceptance criteria

- [ ] With `SAPPHIRE_API_AVAILABLE=False` and an empty forecast set, a WARNING
      (not INFO, per the root-logger cap at `scr/utils_ml_forecast.py:38`) is
      emitted identifying that nothing was forecast.
- [ ] A test pins this: `SAPPHIRE_API_AVAILABLE` patched `False`, empty
      `rivers_to_predict` (or equivalent empty input), asserts a WARNING-level
      log record is produced by `write_pentad_forecast`/`write_decad_forecast`.
- [ ] The existing `SAPPHIRE_API_ENABLED=false`-with-client-installed behavior
      (WARNING already emitted via `_write_ml_forecast_to_api`'s emptiness
      check) is unchanged — a regression test pins this too, so the fix for
      this issue cannot double-log or alter that path.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures,
      zero unexpected skips.

---

## Related

| ID | Relation |
|---|---|
| ML-021 | Source issue; this is "deliberately not addressed" defect 3 there. Its emptiness-first WARNING is the mechanism this issue extends to the client-absent path |
| ML-019 / ML-022 | Same silent-success family — a run reports success having done nothing observable |
