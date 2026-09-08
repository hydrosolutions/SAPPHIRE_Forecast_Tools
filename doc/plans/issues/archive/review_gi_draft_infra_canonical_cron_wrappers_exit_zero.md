## The canonical forecast cron wrappers exit 0 on failure (INFRA-047)

**Status**: **Complete** — merged 2026-09-08 in **PR #499**. Filed 2026-09-05; rescoped
2026-09-05 against the installed-crontab survey (three of the five wrappers are installed on the
surveyed deployments, and **no cron line consumes an exit code today** — this is groundwork for
alerting, not a live-outage fix).
**Module**: `bin/`
**Priority**: **Medium** — these are the main scheduled forecast runs (three daily, plus long-term
on its configured issue days). Every one reports success to cron whether or not the forecast ran.
**Labels**: `infra`, `cron`, `luigi`, `deployment`, `silent-success`
**Found**: 2026-09-05, by the two out-of-loop reviews of **INFRA-023**. Split out deliberately, on
owner decision, rather than widening that issue — one of these wrappers needs a different fix shape
(see the hazard below) and INFRA-023 was already scoped and staged.
**Related**: **INFRA-023** — **merged in PR #494**, not pending. It fixed this same defect in two
scripts: `bin/run_periodic_maintenance.sh` and `bin/yearly_runoff_hydrograph_aggregation.sh`. Do not
revisit either; read it for the fix shape — and note `run_preprocessing_gateway.sh` moved from there to here on
2026-09-07, so the canonical scheduled wrappers are all in one place. **P-007** (fixed) removed the
layer below this one.

---

## Problem

These canonical scheduled wrappers submit work via `docker compose run` and then finish without
propagating its status, so cron records success no matter what happened:

| Script | Installed where (survey 2026-09-07) | Final statement |
|---|---|---|
| `bin/run_preprocessing_gateway.sh` | kghm, tjhm — **moved here from INFRA-023** | `echo` at `:76` |
| `bin/run_pentadal_forecasts.sh` | all three | `echo` at `:74` (compose at `:66-71`) |
| `bin/run_decadal_forecasts.sh` | all three | `echo` at `:74` (compose at `:66-71`) |
| `bin/run_long_term_forecasts.sh` | 06:00 on each deployment's **configured issue day(s)** — not daily | `echo` at `:125` (compose at `:118-122`) |
| `bin/run_daily_maintenance.sh` | 19:00 daily | `echo` at `:83` (compose at `:71-75`) |

**Cut: `bin/run_preprocessing_runoff.sh`.** The survey confirmed it has no cron row on any of the
three deployments, so fixing it is not worth a production diff. The recipe below applies if it is
ever scheduled.

These are Luigi-backed, so **two layers** are required — the same distinction INFRA-023 draws.
Capturing and propagating the compose status alone is not sufficient: Luigi defaults `task_failed`,
`missing_data`, `already_running`, `scheduling_error` and `not_run` to **0**, so the status
propagated would itself be 0 for an ordinary task failure. A `[retcode]` block plus
`LUIGI_CONFIG_PATH` is needed as well.

**The working pattern is `bin/run_periodic_maintenance.sh`, and it is FOUR parts, not just the
`[retcode]` block** — copying only the block leaves a no-op. INFRA-023 (PR #494) already applied it
there **unconditionally to all four task types**; an earlier revision of this issue said it was
`lt_recovery`-only and pointed at `:139-152`, which is now the daemon-wait loop. Current locations:

| Part | Line |
|---|---|
| `[retcode]` heredoc + `LUIGI_RETCODE_DOCKER_ARGS` | `:179-187` |
| the env/args forwarded to Compose | `:195-203` |
| immediate status capture | `:204` |
| final propagation | `:244` |

## The hazard that makes this its own issue

**`run_daily_maintenance.sh` runs the frontend updater AFTER the Luigi submission.** An
immediate-exit-on-failure fix would suppress it — a "fix" that stops work which currently runs. It
must use a **sticky aggregate**: record the failure, let the remaining steps run, and exit non-zero
at the end. `bin/initialize_site_backfill.sh` (`main` does `exit "$overall_exit"` at `:608`) is the
pattern to copy. Do **not** use `set -e` and do **not** exit early.

Verify the same question for the other four before changing them: does anything run after the
compose call that must still happen?

## Precondition — DONE (2026-09-07)

The installed-crontab survey has been run on all three deployments. **No exit-code consumers
exist**: every SAPPHIRE cron line is `cd /data/SAPPHIRE_Forecast_Tools && bash bin/<wrapper>.sh
<env> >> <log> 2>&1`, with the `&&` *before* the wrapper and nothing after it but a redirect. No
chained command, no retry supervisor, no systemd unit. The outage risk that gated this issue is
retired, and it is safe to implement.

**Per-deployment installation, so impact is not overstated:** kghm and tjhm run the gateway,
pentadal, decadal, long-term and daily maintenance. uzhm runs **linear regression only** — no
long-term, no machine learning — so it schedules pentadal, decadal and daily maintenance, and has
no gateway or long-term row at all. That is by design, not a gap.

## Known caller that changes behaviour

`dev_local_backfill.sh --run-pipeline` runs both `run_preprocessing_gateway.sh` and
`run_daily_maintenance.sh` from one `steps` array (`:534-537`) under the same failure guard
(`:553-555`, which calls `fail_or_warn` — it exits unless continuation is enabled, `:118-124`).
**The gateway runs first**, so its newly non-zero result stops `run_daily_maintenance.sh` running at
all, not merely phases 4-6, unless `--continue-on-error` is passed. That is
development behaviour, not a production cron chain — recorded so it is not mistaken for a regression
when it starts happening. Also recorded in INFRA-023.

## Expected effect, and it is the point

The survey established exactly which rows are installed, so the exposure can be stated per
deployment rather than hedged:

| Schedule | kghm | tjhm | uzhm |
|---|---|---|---|
| gateway 03:00 | yes | yes | — |
| pentadal 04:00 | yes | yes | yes |
| decadal 05:00 | yes | yes | yes |
| long-term 06:00, configured issue days | yes | yes | — |
| daily maintenance 19:00 | yes | yes | yes |
| `run_preprocessing_runoff.sh` | no cron row on any deployment | | |

uzhm runs linear regression only — no long-term, no ML — so its two absent rows are by design.

Every installed row above currently reports success whether or not the run worked. Afterwards they
return truthful statuses. **Nothing "turns red" by itself**: bare cron has no red/green state and
does not alert — these rows redirect output to log files, so the change is observable only to a
future exit-aware monitor, or to a direct caller such as `dev_local_backfill.sh`, or to someone
reading the log. Alerting is a separate, deliberately deferred question (owner decision
2026-09-07); this issue makes failure *detectable*, not *announced*.

Several of these wrappers also print optimistic "submitted"/"completed" text that must stop
contradicting the exit status.

**The way to get this wrong** — worth naming, because it silently removes production work: if an
implementer exits immediately on the Compose status, or adds `set -e`, then a Luigi failure in the
19:00 job skips the frontend updater on **all three** deployments. Always run it; aggregate, then
exit.

## Acceptance criteria

- The first four exit non-zero when their Luigi task fails, and 0 when it succeeds.
- `run_daily_maintenance.sh` exits 0 **only when both** Luigi and the frontend updater succeed —
  "0 when Luigi succeeds" is false for it, per the aggregate requirement below.
- `run_daily_maintenance.sh` **still runs the frontend updater** when the Luigi step fails, and
  exits non-zero afterwards. A test must prove the updater ran.
- **The aggregate includes the frontend updater's own status.** It is invoked at
  `run_daily_maintenance.sh:82` and can genuinely exit 1 during validation
  (`daily_update_sapphire_frontend.sh:55`), so "Luigi succeeded" alone is not sufficient for exit 0:
  a failed updater must also make the wrapper non-zero. Always run it; never let its failure skip
  the exit. This was ambiguous in the first draft — "sticky aggregate" implied both steps while the
  criterion above mentioned only Luigi.
- For each Luigi-backed wrapper, the `[retcode]` block, `LUIGI_CONFIG_PATH`, compose-status capture
  and final propagation land **atomically**. All four are required to cover *ordinary* Luigi task
  failures; propagating the compose status alone is not useless — it still surfaces compose-level
  failures and Luigi's `unhandled_exception=4` — but it misses the five zero-default categories,
  which is the case this issue exists for.
- A test proving the Luigi layer must run Luigi for real; a Docker stub proves shell propagation
  only, because stubbing Docker removes Luigi from the path. Follow
  `apps/pipeline/tests/test_lt_dated_recovery.py:299-429`.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh pipeline` green, zero skips.

## Contract not to break

- **Nothing that currently runs after a compose call may stop running.** This is the whole reason
  the issue exists separately; an early exit is the wrong fix shape here.
- Do not change what any wrapper *does* — only what it reports.
- Do not revisit `run_periodic_maintenance.sh` or `yearly_runoff_hydrograph_aggregation.sh` — INFRA-023 (PR #494) already fixed both.
- Do not add a shared helper or framework for this. A handful of explicit local edits is the right size.
