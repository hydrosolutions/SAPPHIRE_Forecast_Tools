# Uzbekistan long-term deployment preparation

Status: Implemented; isolated tests/review passed; live local validation found input blockers.
Base: `origin/maxat_sapphire_2` at
`164711fc`. Scope authorized 2026-11-20; no server deployment authorized here.

The deployment uses saved long-term model artifacts. Short-term ML remains
excluded; operational LR coefficient fitting remains allowed. The static
basin feature file is expected next week. Current long-term data loading
requires it even for LR-only runs, so live forecast completion is a separate
acceptance step after delivery.

## Phase 1 — Protect the saved models

- Goal: refuse Uzbekistan calibration and hyperparameter tuning before model,
  forecast configuration or artifact writes. Cover the shared Python entry
  point used by local and Docker maintenance commands and its direct helpers.
- Files: `apps/long_term_forecasting/calibrate_and_hindcast.py` and focused tests.
- Depends on: none. One implementation agent.
- Verification: synthetic configuration/model sentinels remain unchanged after
  rejected requests; selected Uzbek env files cannot be bypassed by an inherited
  organisation value; other organisations retain existing behavior. Audit the
  saved-model operational path separately for training fallbacks.

The deployment policy is `ieasyhydroforecast_lt_allow_calibration=False` in
both private Uzbek env files. Renaming the organisation must not enable
calibration or tuning. A restriction in either the inherited environment or
selected env file takes precedence over an allowance in the other. An absent
setting preserves existing behavior for other deployments; an explicitly set
value permits calibration only for `true`, `yes`, `1` or `on` (case and
surrounding whitespace ignored). The existing Uzbekistan organisation guard
also remains in place. Operational LR coefficient fitting is unaffected.

This is an application safeguard against accidental retraining, not access
control against an administrator who can change the policy or application.

## Phase 2 — Enable the runner and deployment image selection

- Goal: enable Uzbekistan gateway and long-term targets while retaining its
  short-term ML exclusion and existing demo behavior.
- Files: `apps/run_locally.sh`, `bin/utils/pull_docker_images.sh`, runner tests.
- Depends on: none; do not deploy before Phase 1. One implementation agent.
- Verification: fake-child operational/maintenance dispatch tests and captured
  image pulls; errors remain nonzero. No service/API changes.

### Gateway prerequisite — optional quantile mapping

The owner confirmed that Uzbekistan uses the Kyrgyz Data Gateway host/key but
does not apply quantile mapping. Both operational and historical meteo entry
points must treat an unset/blank parameter path as disabled mapping and keep
their raw forcing path. Missing optional name-twin configuration uses identity
mapping. Configured mapping and genuine download failures retain their behavior.
One additional agent implements these narrow gateway changes and main-entry
regressions, independently of Phases 1–3.

## Phase 3 — Separate long-term dashboard visibility

- Goal: `ieasyhydroforecast_display_long_term_forecasts` controls long horizons
  independently of short-term ML. When absent, preserve existing behavior.
- Files: dashboard config, widget manager, widget documentation and focused tests.
- Depends on: none. One implementation agent.
- Verification: env-to-widget tests cover both sidebar and publication selectors,
  explicit true/false and unset fallback, with the ML execution flag unchanged.

## Phase 4 — Private configuration and validation

- Goal: update both Uzbek env files for the supported six long-term modes,
  correct existing path conventions and independent dashboard visibility.
- Files: private `.env_uzhm` and `.env_develop_uzhm` outside Git; this record only
  contains nonsensitive settings and aggregate outcomes.
- Depends on: Phases 1–3. Orchestrator applies reviewed private configuration.
- Verification: preserve credentials, API destinations and unrelated settings;
  only copy the explicitly authorized Kyrgyz Data Gateway host/key; do not
  change the Uzbek SAPPHIRE API destination or invent missing input assets.
  Run full repository tests, application Docker smoke tests and isolated guard
  checks. The owner confirmed that the existing Uzbek API is cloud-hosted and
  needs no SSH tunnel.

## Remaining deployment prerequisites

- Confirm Data Gateway onboarding and ERA5 history availability with Sandro.
- Deliver the matching static features and verify model/scaler station coverage.
- Review the snow-group and elevation-band configuration with Sandro before
  clearing the backed-up elevation-band fields from the local test run.
- Verify daily forcing history and snow inputs. Gateway connection and paths
  are configured privately; control-member HRUs use the existing Uzbek monthly
  forcing setting, which matches all six model configurations. Quantile mapping
  is deliberately absent. Enabling the route does not create input history.
- Verify dependency hindcasts are imported from the training run; do not generate
  them by recalibrating the deployment's saved models.
- Confirm the station mapping/manual data feed and server cron/image versions.
- Perform operational and maintenance validation on the approved Uzbek test
  deployment after inputs are available. No successful live forecast run is
  implied by unit tests or Docker import checks.

## Validation record — 2026-11-20

The results below precede the organisation-independent policy follow-up;
its validation is recorded separately once complete.

- Final serial `SAPPHIRE_TEST_ENV=True bash run_tests.sh`: 9,752 passed across
  16 scopes, exit 0. The 12 owner-accepted skips and two expected failures match
  the baseline by test ID. An earlier attempt overlapped focused gateway tests
  with the full suite and failed its log-isolation checks; it is not counted as
  passing evidence.
- Affected scopes: pipeline 579 passed; gateway 560 passed; long-term 258 passed
  with three accepted skips; dashboard 1,102 passed with six accepted browser
  skips. No new skip was introduced.
- `run_docker_tests.sh --skip-ml`: eight builds and eight import smokes passed.
  Gateway was rebuilt and its smoke repeated successfully after the final
  optional-configuration fix.
- Four actual local calibration CLI probes and four actual Docker maintenance
  probes rejected Uzbekistan calibration/tuning with exit 1, visible guard
  messages and unchanged synthetic model/config files. Docker used
  `--network none` and `UV_NO_SYNC=1` to exercise the installed production
  environment: the existing default `uv run` otherwise attempts to download dev
  dependencies offline and fails before application startup.
- Both private env files are updated with backups. Non-gateway credentials and
  the Uzbek SAPPHIRE destinations are unchanged. Both configured cloud endpoints
  returned HTTP 200 for health and readiness.
- On isolated copies, all six mode configs loaded. November 10 has no active
  modes; November 25 has months 1–3 and quarterly. Original configuration hashes
  and model artifact metadata remained unchanged. The existing config loader
  synchronizes forecast settings by writing model JSON files, so this check was
  deliberately performed on copies; it is not evidence that operational config
  loading is read-only.
- `run_locally.sh all --dry-run` passed environment/venv validation in an isolated
  source copy. It does not execute pipeline stages. At this stage the full live
  pipeline had not been run because static features were unavailable. The
  subsequent real run is recorded below; successful completion remains a
  deployment prerequisite.
- Fresh independent adversarial review found no introduced defect. Live
  deployment/input readiness and offline `uv` dependency synchronization remain
  outside the successful checks above.

### Organisation-independent calibration policy follow-up

- Added 37 policy regression cases: renaming the organisation, conflicting
  inherited/file policies, direct helpers, CLI calibration/tuning, absent and
  explicit allowances for other deployments, and invalid values failing closed.
  The long-term wrapper passed 295 tests with three accepted baseline skips.
- Both private Uzbek env files explicitly disable calibration, with fresh
  backups; every other setting was preserved.
- Four actual local CLI probes and four actual Docker maintenance probes used
  a renamed organisation and disabled policy. All exited 1 with the policy
  refusal, including inherited `true` versus file `False`. The long-term image
  rebuild and import smoke passed. Docker probes again used `UV_NO_SYNC=1`
  and disabled networking.
- Mock assertions establish that rejected unit-test calls never reach
  calibration configuration/model boundaries. Synthetic files remained unchanged
  in the runtime probes; these checks do not constitute live trained-artifact
  validation or operational forecast completion.
- Independent review found no production-code defect. Its test-isolation
  finding was fixed with an autouse fixture clearing inherited policy before
  each guard test; the reviewer confirmed that correction.
- Final serial full suite on the corrected tests: 9,789 passed across 16
  scopes, exit 0, with zero failures/errors. The 12 accepted skips and two
  expected failures match the previous passing run by test ID. No new skip
  was introduced. This supersedes the earlier full-suite count above.

### Real local operational and maintenance run

Ran `bash apps/run_locally.sh daily --continue-on-error` with the private Uzbek
development env and configured cloud API. Relative deployment paths were
resolved against the original checkout because the candidate is an isolated
worktree. No forecast-date override or simulation was used. The real pipeline
ran for approximately four minutes and exited 1: nine module passes, seven
failures, two intentional ML skips, and failed API validation.

| Scope | Observed result |
| --- | --- |
| Runoff operational and maintenance | Exit 0, including the long-horizon maintenance step; current observation coverage still insufficient. |
| Gateway operational and maintenance | Both fail because an existing ERA5 reanalysis history file is missing. Quantile mapping correctly skips when unconfigured; operational meteo download and independent snow publication work. |
| LR operational and hindcast, PENTAD and DECAD | All four fail with no eligible forecast rows. Read-only API checks found no current-year daily observations later than April across the configured stations; one had no current-year rows. |
| Short-term postprocessing operational and maintenance, both horizons | All four exit 0; this is not evidence of complete forecasts. |
| Long-term operational | Scheduled months 1–3 and quarter attempted; all fail during static-feature path initialization because the setting/input is absent. Seasonal modes are outside the current schedule. |
| Long-term postprocessing operational, monthly/quarterly skills and maintenance | All exit 0; missing forecasts remain an input-readiness limitation. |
| API validation | Fails for absent current runoff/forecast data; also expects disabled ML products, so its aggregate failures are not all applicable to Uzbekistan. |

Both private snow-group lists were corrected, at the owner's instruction, to
retain only the selected Uzbek group. The first run had already processed the
previously configured additional `KGZ500m` group. A real gateway rerun with the
corrected list passed precipitation, temperature and all three primary snow
value read-back consistency checks. Its overall exit remains 1 because ERA5
history is still missing. Snow API keys do not contain the source HRU group;
the two groups overlapped. Existing elevation-band fields from the removed
group remain on the affected records. Exact records are backed up privately;
cleanup is deferred pending the owner's snow/elevation configuration discussion
with Sandro. No elevation-band cleanup was performed.

Also ran `bash apps/run_locally.sh calibrate_long_term` with the actual Uzbek
env. It exited 1 with the explicit disabled-calibration message, as intended.
Metadata for all 84 tracked non-JSON model files remained unchanged across the
daily run and follow-up probes. No model retraining occurred. Normal config
loading can still synchronize model JSON settings, as noted above.

Private reports and logs are retained outside Git under
`/tmp/sapphire-uzhm-*`, including `sapphire-uzhm-live-validation`. The real runs
published preprocessing data to the configured cloud API; no server deployment
or image publication was performed.

```json
{"phases":{"P1":{"depends_on":[],"parallel_agents":1},"P2":{"depends_on":[],"parallel_agents":1},"P3":{"depends_on":[],"parallel_agents":1},"gateway":{"depends_on":[],"parallel_agents":1},"P4":{"depends_on":["P1","P2","P3","gateway"],"parallel_agents":0}}}
```
