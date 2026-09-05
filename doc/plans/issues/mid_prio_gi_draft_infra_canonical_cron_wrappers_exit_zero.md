## The four canonical forecast cron wrappers exit 0 on failure (INFRA-047)

**Status**: Draft (2026-09-05)
**Module**: `bin/`
**Priority**: **Medium** — these are the *main* daily forecast schedules. Every one of them reports
success to cron whether or not the forecast ran.
**Labels**: `infra`, `cron`, `luigi`, `deployment`, `silent-success`
**Found**: 2026-09-05, by the two out-of-loop reviews of **INFRA-023**. Split out deliberately, on
owner decision, rather than widening that issue — one of these wrappers needs a different fix shape
(see the hazard below) and INFRA-023 was already scoped and staged.
**Related**: **INFRA-023** owns the same defect for seven other scripts and establishes the two fix
shapes; read it first. **P-007** (fixed) removed the layer below this one.

---

## Problem

Five wrappers submit work via `docker compose run` and then finish without propagating its status,
so cron records success no matter what happened:

| Script | Cron slot | Final statement |
|---|---|---|
| `bin/run_pentadal_forecasts.sh` | 04:00 daily | `echo` at `:74` (compose at `:66-71`) |
| `bin/run_decadal_forecasts.sh` | 05:00 daily | `echo` at `:74` (compose at `:66-71`) |
| `bin/run_long_term_forecasts.sh` | 06:00 daily | `echo` at `:125` (compose at `:118-122`) |
| `bin/run_daily_maintenance.sh` | 19:00 daily | `echo` at `:83` (compose at `:71-75`) |
| `bin/run_preprocessing_runoff.sh` | not a standalone cron row | `echo` at `:52` (compose at `:45-49`) |

These are Luigi-backed, so **two layers** are required — the same distinction INFRA-023 draws.
Capturing and propagating the compose status alone is not sufficient: Luigi defaults `task_failed`,
`missing_data`, `already_running`, `scheduling_error` and `not_run` to **0**, so the status
propagated would itself be 0 for an ordinary task failure. A `[retcode]` block plus
`LUIGI_CONFIG_PATH` is needed as well; `bin/run_periodic_maintenance.sh:139-152` is the working
pattern, currently applied to `lt_recovery` only.

## The hazard that makes this its own issue

**`run_daily_maintenance.sh` runs the frontend updater AFTER the Luigi submission.** An
immediate-exit-on-failure fix would suppress it — a "fix" that stops work which currently runs. It
must use a **sticky aggregate**: record the failure, let the remaining steps run, and exit non-zero
at the end. `bin/initialize_site_backfill.sh` (`main` does `exit "$overall_exit"` at `:608`) is the
pattern to copy. Do **not** use `set -e` and do **not** exit early.

Verify the same question for the other four before changing them: does anything run after the
compose call that must still happen?

## Precondition — do not skip

**Inspect the installed crontabs first**, for each of these wrapper names, looking specifically for
anything *after* the wrapper: a right-hand `&&`, a retry supervisor, or any other consumer of the
exit code. The repository's documented cron rows have nothing following `bash <wrapper>`, and no
systemd unit chains them — but installed crontabs have never been inspected. A consumer that stops
downstream work, or retries a mutating job, would turn this fix into an outage. INFRA-023's PR2
carries the same precondition; one survey can serve both.

## Expected effect, and it is the point

Four daily schedules on every deployment currently report success unconditionally. Afterwards they
report reality, so **expect newly-red cron where it was previously green** — that is the fix
working, not a regression. Note bare cron does not itself alert: these rows redirect output to log
files, so an operator sees the change only through an exit-aware monitor or by reading the log.
Several of these wrappers also print optimistic "submitted"/"completed" text that will need to stop
contradicting the exit status.

## Acceptance criteria

- Each of the five named scripts exits non-zero when its Luigi task fails, and 0 when it succeeds.
- `run_daily_maintenance.sh` **still runs the frontend updater** when the Luigi step fails, and
  exits non-zero afterwards. A test must prove the updater ran.
- For each Luigi-backed wrapper, the `[retcode]` block, `LUIGI_CONFIG_PATH`, compose-status capture
  and final propagation land **atomically** — any subset is a no-op or worse.
- A test proving the Luigi layer must run Luigi for real; a Docker stub proves shell propagation
  only, because stubbing Docker removes Luigi from the path. Follow
  `apps/pipeline/tests/test_lt_dated_recovery.py:299-429`.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh pipeline` green, zero skips.

## Contract not to break

- **Nothing that currently runs after a compose call may stop running.** This is the whole reason
  the issue exists separately; an early exit is the wrong fix shape here.
- Do not change what any wrapper *does* — only what it reports.
- Do not fold in INFRA-023's seven scripts; that issue is separately scoped and staged.
- Do not add a shared helper or framework for this. Five explicit local edits are the right size.
