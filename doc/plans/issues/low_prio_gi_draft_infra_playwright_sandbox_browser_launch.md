# Playwright integration tests fail at browser launch under macOS sandbox

| Field | Value |
|---|---|
| Module | dev infra / test harness |
| Priority | Low |
| Status | Draft |
| Labels | `dev-infra`, `tests`, `playwright`, `macos` |

## Summary

The Playwright-based integration tests in
`apps/forecast_dashboard/tests/test_integration*.py` fail at browser launch with
macOS sandbox permission errors when run inside a constrained shell sandbox
(observed during PP-036 and PP-DASHBOARD-LONG-HORIZON red-phase verification,
2026-05-28 and 2026-05-31). The errors are infrastructure-level (browser
process cannot launch), not test assertions, so they surface as `errors`
rather than `failures` in the pytest summary.

## Symptoms

- ~6 errors in `apps/forecast_dashboard/tests/test_integration*.py` during
  `SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` runs from
  certain shell environments.
- Failure mode: Playwright unable to launch a Chromium/Firefox process; macOS
  raises sandbox-permission errors before any test code runs.
- The same tests run cleanly from an unrestricted terminal session.

## Why it matters

Two effects:

1. **Verification noise.** Anyone running the test suite from a sandboxed
   shell (e.g., during red-phase reproductions performed by an LLM agent) sees
   ~6 spurious errors and has to mentally exclude them when assessing
   whether the suite actually passed.
2. **Coverage gap.** If the Playwright tests are silently failing in CI or in
   any automated environment, the dashboard integration coverage they provide
   is not actually running. Verify this hasn't crept into a CI configuration.

## Suggested investigation

1. Confirm whether the failures occur only in shell-sandboxed sessions or also
   in `bash`/`zsh` directly. Document the threshold.
2. Decide whether Playwright tests should be gated on an environment variable
   (e.g., `SAPPHIRE_RUN_PLAYWRIGHT=1`) so they are explicitly opt-in for
   contexts that can launch a browser. The dashboard test suite already
   skips Playwright when the package is unavailable
   (`apps/forecast_dashboard/tests/conftest.py` — verify) but does not gate
   on launch capability.
3. If gating, update `bin/run_daily_maintenance.sh` and any CI workflow to
   set the gate explicitly where appropriate.

## Workaround

For now: ignore Playwright `errors` in red-phase verification runs as long as
they are clearly browser-launch failures, not assertion failures. Distinguish
in any verification report.

## Out of scope

- Fixing the underlying macOS sandbox permission model.
- Replacing Playwright with a non-browser test harness.

## Related

- PP-036 red-phase verification log (`doc/plans/issues/archive/high_prio_gi_draft_pp_ml_skill_horizon_archive_split.md`) noted the issue but did not file it.
- Dashboard long-horizon skill summary red-phase verification (2026-05-31) hit the same errors and prompted this issue.
