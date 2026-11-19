# Security dependency upgrade batches

Target: latest fetched `origin/maxat_sapphire_2`, commit
`77c0c071` (fetched 2026-11-18), plus reviewed recovery commits
`b263cd8c` and `263adb8f`. Work branch: `security/network-dependencies-latest`.
The original dependency worktree is preserved unchanged.

The earlier `main` assessment and worktree are superseded for this task.
No service source, service manifest, or service lockfile is changed.

## Sequence

| Batch | Packages | Scope and verification |
| --- | --- | --- |
| 1 | certifi, urllib3, tornado, anyio, aiohttp, multidict | Eleven app environments; shared HTTP/TLS compatibility, full tests, Docker smoke tests, operational and maintenance pipelines |
| 2 | Pillow >=13.0.0 | Dashboard, ML, long-term forecasting; report/image rendering |
| 3 | torch >=2.13.0, fsspec >=2026.6.0, filelock >=4.2.1, setuptools >=84.1.0, mako >=1.4.2 | ML and long-term environments; resolve CPU index constraints and validate saved-model inference |
| 4 | virtualenv >=21.14.9, filelock >=4.2.1 | Core development tooling; pre-commit environment creation |
| 5 | oauthlib >=4.0.1, pyasn1 >=0.6.5 | Runoff external-client dependencies; OAuth/client compatibility |
| Owner coordination | Service alerts | Separate owner-coordinated upgrades; python-jose alerts reported no fixed version in the retrieved alert data |

Each batch depends on the previous batch's validation and review. Versions for
pending batches are advisory minimums, not compatibility guarantees. Recheck
current advisories before starting each batch. No changes to application data
flow or API contracts are planned.

## Batch 1: verified and independently reviewed

Files: `apps/*/pyproject.toml` and corresponding `uv.lock` only where affected,
plus `apps/iEasyHydroForecast/tests/test_network_dependencies.py`.
`validate_pipeline` has no affected dependency and remains unchanged.

Locked versions: certifi 2026.10.23, urllib3 2.8.0, tornado 6.6.1,
anyio 4.14.2, aiohttp 3.14.4, multidict 6.9.1. The aiohttp upgrade requires
yarl >=1.25.1; both ML environments now lock yarl 1.25.1.

Per-project uv constraints retain minimum security versions on later resolution.
Long-term forecasting's SDK URL now explicitly names `@master`, matching the
core dependency and the already-locked SDK source. This resolves a conflicting
URL error without changing the SDK commit. No other package versions or Git
dependency revisions changed. Against the retrieved open-alert snapshot, the
new app locks are outside the vulnerable ranges for 122 alert instances across
34 distinct advisories in this batch; service alerts are excluded.

Offline compatibility tests exercise loading the Requests/certifi trust bundle
with certificate/hostname verification, incremental deflate response decoding,
and invalid compressed-response rejection.

## Earlier verification record (before target integration)

- All twelve app environments and five unchanged service environments installed
  with Python 3.12 from frozen lockfiles.
- All twelve app lock consistency checks passed.
- `git diff --check` passed.
- Full `SAPPHIRE_TEST_ENV=True bash run_tests.sh` completed. Initial pipeline
  failures were sandbox network restrictions and concurrent operational log
  writes. The final network-enabled, serialized pipeline rerun passed all
  458 tests with zero skips/errors. Do not run pipeline tests concurrently with
  run_locally.sh: their log-isolation fixture detects operator log writes.
- Using the latest result for each module: 8,869 passed, 12 skipped, two expected
  failures across apps and unchanged services. No assertion failures or errors
  remain after corrective reruns, but the zero-skips gate is not met.
- Core rerun with Docker access: 950 passed, one Bash-version skip, one existing
  expected failure. Dashboard rerun: 734 passed, six live-browser tests skipped.
- Remaining existing gaps: two runoff placeholder skips; three long-term tests
  requiring data configuration; one additional postprocessing expected failure.
  These are not accepted as a clean zero-skips validation gate.
- `bash apps/run_docker_tests.sh --skip-ml` passed: eight builds and eight
  import smoke tests; ML excluded by the prescribed flag (5m 5s).
- Operational `all` and then `maintenance` ran sequentially through
  `apps/run_locally.sh` with `SAPPHIRE_PREDICTION_MODE=BOTH`, using the existing
  documented private development config. Relative data paths were normalized for
  the isolated worktree; credentials and operational data are not stored here.
- Operational `all`: exit 1. Runoff completed; gateway meteo failed because
  upstream forecast files were unavailable. Long-term simulation completed,
  but long-term postprocessing failed because the development config lacks
  `ieasyhydroforecast_ml_long_term_supported_modes`. API validation also reported
  missing required records. This does not validate short-term operational
  forecasting after the failed gateway stage, or long-term operational inference
  (`all` invokes long-term simulation).
- Maintenance: exit 1 after completing runoff/hydrograph, ERA5 extension,
  ML maintenance (DECAD, as reported by the runner), and LR/postprocessing for
  both PENTAD and DECAD. Long-term maintenance postprocessing failed on the same
  missing required supported-modes setting. Inspection confirmed that all four
  required long-term settings are absent from this private config: configuration,
  output path, static-data path, and supported modes (see doc/configuration.md).
  No configuration values were guessed
  or written back to the private development config.
- Independent review, commit, push, merge, and deployment are pending.

Do not treat this batch as ready to merge while tests fail, unexpected skips
remain, or operational/maintenance and Docker validation are incomplete.

## Rerun with SSH tunnel open

On 2026-11-17, reran `run_locally.sh all` followed by `maintenance`, with
`SAPPHIRE_PREDICTION_MODE=BOTH` and the same development configuration.
Both exited 1. The operational weather gateway still reported unavailable
forecast files, and long-term operational/maintenance postprocessing still
failed on missing supported-mode configuration. Short-term maintenance stages
completed. The open tunnel did not resolve these separate data/configuration gaps.

Started the dashboard from this worktree on loopback port 5055 for manual visual
review. The local API gateway health and readiness endpoints returned HTTP 200.
Dashboard process details and private runtime logs are kept outside the repo.

## Integrated candidate — 2026-11-18

The current repository Dependabot API snapshot still reports 122 open app
alert instances across 34 advisories for this networking batch. The candidate
versions remain outside their reported vulnerable ranges; yarl is a required
aiohttp compatibility update, not a claimed direct alert fix. Service alerts
remain outside this patch.

The eleven existing app environments already contain these exact candidate
networking versions. The final recovery verification used those environments:
9,626 tests passed (12 owner-accepted existing skips and two existing expected
failures), and the ordinary local pipeline passed all eleven stages and three
validations. Separate live recovery verified raw and derived writes while
retaining failures for unresolved model inputs. This is supporting runtime
evidence, not a claim that those runs installed this candidate's lockfiles.
Candidate-specific full suite passed all 16 scopes: 9,629 passed, the same
12 owner-accepted existing skips, and two existing expected failures. All three
new network compatibility tests passed. All eleven offline lock consistency
checks passed. Candidate-lock Docker builds and smoke tests passed eight of
eight; ML was excluded by the prescribed `--skip-ml` command.

Every one of the 122 affected alert instances was checked against its candidate
lock version; none remains within its reported vulnerable range. Other app
security batches and colleague-owned service alerts remain open. A fresh
read-only independent review found no defects in the package scope, locks,
constraints, compatibility tests or verification claims. Ready for a local
commit; no push or deployment has occurred.

## Batch 2: Pillow — isolated candidate, 2026-11-18

Baseline: fetched `origin/maxat_sapphire_2` at `61d5db9f`, after merged
recovery PR #541 and networking PR #542. Branch: `security/pillow-update`.
The recovery execution notes remain in their separate worktree.

Current repository Dependabot data lists 129 Pillow alert instances across
43 advisories, affecting dashboard, machine-learning and long-term locks.
Every advisory names 13.0.0 as the first patched version. The three candidate
locks update Pillow 12.3.0 to 13.0.0 only; all other package versions and Git
source revisions are unchanged. Dashboard/ML direct requirements and the LT
transitive constraint enforce the security floor on future resolution.

Primary sources: [PyPI 13.0.0 metadata](https://pypi.org/pypi/Pillow/13.0.0/json)
and [Pillow 13.0.0 release notes](https://pillow.readthedocs.io/en/stable/releasenotes/13.0.0.html).
Python 3.10 support and several deprecated image APIs were removed. The three
apps require Python >=3.11; isolated validation uses 3.12. Source inspection
found no direct repository use of the removed APIs. Font-rendering changes
can affect glyph appearance, so this is not a claim of pixel-identical reports.

A focused regression exercises the existing skill-inspection plotting function
through Matplotlib PNG export and Pillow decode, checking dimensions, visible
content and chart labels. All three isolated environments installed from frozen
locks and report Pillow 13.0.0. Existing dashboard/runtime environments were
left unchanged. Offline lock checks and whitespace checks passed. The full
16-scope suite passed 9,630 tests, with the same 12 owner-accepted existing
skips and two existing expected failures; the new PNG report regression passed.
Candidate-lock Docker checks passed eight builds and eight smokes with the
prescribed --skip-ml exclusion. All 129 alert instances were checked against
the candidate locks; none remains in its reported vulnerable range. The required
local `all --continue-on-error` run completed in 27m 30s: eight execution
stages passed, three failed, and all three API validations passed (overall
exit 1). The three failures were LT operational postprocessing, LT skill
metrics and LT maintenance postprocessing; each raised FileNotFoundError
for the intentionally absent month_4 configuration. The supported-mode list
was left unchanged. Runoff, gateway, ML (DECAD), both LR/short-term PP passes
and LT simulation passed. This is not a green end-to-end result; no Pillow
compatibility failure was identified. The existing local ML horizon limitation
still applies. On 2026-11-18 the owner explicitly accepted these three known
configuration failures for this Pillow commit, with the supported-mode list
unchanged. This exception does not turn the pipeline result into a pass or
cover new failures. Fresh independent review
found only a minor overstatement in the test name/docstring; these now name
the encoding/decoding contract rather than curve preservation. Assertions
were unchanged, the full dashboard scope passed again, and the reviewer
confirmed the correction with no remaining material findings. Validation is
accepted for commit with the stated existing-skip and configuration-failure
exceptions. No Pillow PR or deployment has been performed.

## Batch 3: ML runtime and macOS runner — Review

Implemented, verified and independently reviewed; owner authorized commit and PR.
The final LTF-021 validation below supersedes the earlier feasibility-stage
limitations. Later dependency batches remain pending.

Baseline: fetched `origin/maxat_sapphire_2` at `49a5bb7b`, including merged
Pillow PR #543. Trial branch: `security/ml-runtime-updates`.

Both app locks resolve and install with torch 2.13.0, fsspec 2026.6.0,
filelock 4.2.1 and setuptools 84.1.0; the long-term lock also updates Mako
to 1.4.2. All unrelated package versions and Git revisions remain unchanged.
The explicit CPU index is preserved. Frozen isolated installs and lock checks
passed. Affected-scope tests via `run_tests.sh`: ML 530 passed with no skips;
long-term 239 passed with the same three owner-accepted configuration-gated
tests skipped. No assertion or collection failures remain.

Offline CPU checks loaded one configured saved model each for TFT, TiDE and
TSMixer using the existing Darts loaders and the application's exact six
safe globals. They compared deterministic synthetic-input likelihood outputs
under the previous and candidate frozen environments. A representative
long-term UncertaintyMixture model used the pinned package's actual state-dict
loader, synthetic forward inputs and analytical quantile calculation. All four
outputs were exactly equal between environments; selected artifact hashes
were unchanged. No training or API writes occurred. These checks cover sampled
existing artifacts, not every deployment, future training output, stochastic
sampling or end-to-end forecasts. Temporary probe evidence is kept outside Git.

### Colleague repository coordination

Read-only upstream inspection found:

| Repository | Inspected revision | Its own locked torch / fsspec / filelock / setuptools / Mako |
| --- | --- | --- |
| `hydrosolutions/st-forecast` | `b48ed458ba4d619913190a7d3ab6c8631c095405` | 2.8.0 / 2025.9.0 / 3.19.1 / 80.9.0 / 1.3.10 |
| `hydrosolutions/long-term-forecasting` | `e0df498ad58ef565358da05734a736cc004cef33` | 2.7.1 / 2025.7.0 / 3.18.0 / 80.9.0 / 1.3.10 |

SAPPHIRE does not consume these upstream lockfiles. Short-term inference uses
local wrappers and Darts directly; the pinned long-term package manifest
permits the candidate runtime. No upstream source change was needed for this
resolution or the sampled saved-model checks. The colleague should separately
refresh both repositories' locks for their own frozen training/development
environments, then validate newly produced checkpoints in SAPPHIRE. Existing
training/inference Lightning versions differ; the checks above do not prove
compatibility of future training outputs. No upstream files or messages changed.

Fresh independent diff review found no confirmed material defect at the
feasibility stage. Subsequent pipeline validation found the macOS native-runtime
regression described below. Full pre-commit testing and disposition of that
local-runtime limitation remained outstanding at that point; see the runner
follow-up below before assessing PR readiness.

### Candidate pipeline and Linux validation — 2026-11-19

All nine Docker builds and all nine import smoke tests passed, including ML
(no `--skip-ml`). Read-only, network-disabled candidate Linux containers also
loaded the four sampled saved-model families and produced finite synthetic
outputs matching the local baseline at `rtol=1e-4, atol=1e-5`; maximum absolute
difference was below 2.4e-7. Artifact and metadata hashes remained unchanged.

Using the existing Kyrgyz development configuration and candidate ML/LT
environments, executed `run_locally.sh all --continue-on-error`, then
`maintenance --continue-on-error`. The dispatcher runs DECAD ML. Explicit
PENTAD direct-script runs followed for all three models: operational
`recalculate_nan_forecasts.py`, `make_forecast.py`, `fill_ml_gaps.py`, then
maintenance `recalculate_nan_forecasts.py`, `fill_ml_gaps.py`,
`add_new_station.py`. Finally reran operational and maintenance short-term
postprocessing explicitly for PENTAD and DECAD.

- Initial `all`: all seven short-term execution stages and all three API
  validations passed. LT simulation crashed natively during GBT prediction;
  LT operational postprocessing, skill recalculation and maintenance
  postprocessing each failed on the intentionally absent `month_4` config.
  Overall exit 1, elapsed 18m 51s.
- `maintenance`: runoff/hydrograph, gateway, DECAD ML, and both horizons'
  LR and ensemble maintenance passed. LT maintenance postprocessing failed
  on the same absent configuration. Overall exit 1, elapsed 22m 33s.
- All 18 explicit PENTAD script invocations and all four final short-term
  postprocessing invocations exited zero.

The macOS crash report shows LightGBM prediction mixing Homebrew and Torch
OpenMP runtimes. An isolated test using the same saved LightGBM estimator
passes under baseline Torch 2.10, segfaults under candidate Torch 2.13, and
passes with a process-local `OMP_THREAD_LIMIT=1` set before startup. Predictions
with the cap are bitwise identical to baseline. `OMP_NUM_THREADS=1` alone and
a `threadpoolctl` context did not prevent the crash. The exact same artifact
passes prediction with estimator defaults in the Linux candidate image
(`OMP_NUM_THREADS=1` and one Torch thread, without `OMP_THREAD_LIMIT`). No
colleague model-code change was needed for these tested Linux paths.

Reran `run_locally.sh long-term --continue-on-error` with only the subprocess
environment addition `OMP_THREAD_LIMIT=1`. LT simulation completed in 11m 14s,
including GBT and MC_ALD. The three following postprocessing/skill stages
still failed on absent `month_4`; API validation passed. Overall exit 1,
elapsed 12m 28s. No source workaround or private-configuration change was
made during those runs, and the supported-mode list was preserved. At that
point the cap was only a local validation workaround. Prior acceptance of the
configuration failures for Pillow is not an acceptance of this new native
regression or an automatic waiver for this batch.

Private logs, diagnostic probes and aggregate results are retained under
`/tmp/sapphire-runtime-*` and `/tmp/sapphire-omp-*`, outside Git. These runs
exercise inference and existing maintenance; no model retraining was invoked.

### macOS local-runner follow-up

At the owner's request, `run_locally.sh::run_in_venv` now supplies
`OMP_THREAD_LIMIT=1` to long-term Python children on Darwin only, after other
per-call environment assignments. It does not mutate the parent environment,
Linux child settings or other modules. It applies to operational, simulation,
recovery and calibration scripts invoked through that helper. Direct Python
invocations on the affected Mac runtime still need the setting explicitly;
the runner's help documents this distinction.

The comment and help text say to remove the cap once newer runtimes pass the
saved-estimator and pipeline regression without it. No unsupported promise
about a future fixed package version is made. The separate, independently
reviewed [LTF-021 issue](issues/review_gi_draft_ltf_optional_mode_configs.md)
records the missing-month configuration fix, subsequently implemented and
verified in the follow-up below.

Final verification of the runner change: full 16-scope `run_tests.sh` passed
9,654 tests, with the same 12 owner-accepted skips and two existing expected
failures. The pipeline scope passed 566 tests, including all 24 new real-child
environment cases with zero skips. The first restricted-network pipeline
test attempt failed five DockerHub DNS checks; the final network-enabled
suite passed them. Independent read-only adversarial review found no defect
in the runner patch or its tests. The prescribed Docker rerun passed eight
builds and eight smokes with `--skip-ml`; the prior nine-image check and Linux
ML saved-model verification remain applicable to the unchanged package locks.

The final `all --continue-on-error` run explicitly removed `OMP_THREAD_LIMIT`
from the parent environment. After reopening the input tunnel and restarting
from runoff, eight execution stages and all three API validations passed.
Long-term simulation, including GBT and MC_ALD, completed in 10m 13s without
a native crash. Only LT operational postprocessing, skill recalculation and
maintenance postprocessing failed, each on absent `month_4`. Overall exit 1,
elapsed 27m 36s; this is not an unqualified green pipeline. Private evidence:
`/tmp/sapphire-runtime-final-full-tests.log`, `/tmp/sapphire-mac-omp-docker.log`
and `/tmp/sapphire-mac-omp-auto-pipeline*`. No commit or PR was created for
this batch during verification.

### LTF-021 follow-up: missing optional monthly configurations

The owner subsequently authorized implementation. Monthly batch discovery now
skips genuinely absent optional monthly JSON files with warnings, retaining
strict explicit lookups and errors for broken directories, broken links,
permissions, all-declared-monthly-files-missing and malformed existing configs.
Quarter/season processing, supported-mode declarations and private .env were
not changed. See the linked LTF-021 issue for contracts and regression evidence.

Full suite passed 9,677 tests with the same 12 accepted skips and two existing
expected failures. Docker passed eight builds and eight smokes with --skip-ml;
independent implementation review found no actionable defect. Final live all
pipeline passed all eleven execution stages and all three API validations
(exit 0, 38m 37s), including all three formerly failing LT postprocessing/skill
stages. The automatic macOS cap worked without a parent setting and the
development .env hash was unchanged. These results supersede the earlier
missing-month pipeline failures for this candidate. Owner authorized commit and PR after this validation.

## Batch 4: core development tooling — Review

Based on freshly fetched `origin/maxat_sapphire_2` at `8cc34e0a`, after merged
runtime PR #544. Only the core module's manifest and lockfile change; no
forecasting code, services or colleague repository changes are needed.

| Package | Previous | Candidate |
| --- | --- | --- |
| virtualenv | 20.36.1 | 21.14.10 |
| filelock | 3.20.3 | 4.2.1 |
| distlib | 0.4.0 | 0.4.3 |
| packaging | 25.0 | 26.3 |
| python-discovery | absent | 1.6.1 |

The manifest adds security floors for virtualenv and filelock. The other
three changes satisfy virtualenv's new requirements; packaging is also used
by pytest. All five belong to the optional development/test dependency graph.
The complete 22-package production dependency graph (including the core
package), all Git revisions, pre-commit and platformdirs remain unchanged.

The alert snapshot requires virtualenv 21.14.9, but that version has an
[additional advisory fixed in 21.14.10](https://github.com/advisories/GHSA-4f8h-vr72-p32c).
The selected releases were non-yanked and their official PyPI metadata listed
no vulnerabilities, but that check was insufficient: virtualenv 21.14.10 is
affected by [GHSA-cx6f-vjhg-g3cc](https://github.com/pypa/virtualenv/security/advisories/GHSA-cx6f-vjhg-g3cc),
published before this update. The initial audit missed this version-specific
regression. Batch 6 below corrects the selection using the GitHub advisory
catalog as well as release metadata.

Validation uses an isolated core environment with the same Python 3.12.10
as the previous batch. Frozen installation and fresh-cache pre-commit Python
hook environment creation/execution passed. Full `run_tests.sh`: 9,677 passed
across all 16 scopes, with exactly the same 12 owner-accepted skips and two
existing expected failures as the previous batch. The first restricted run
could not access Docker and skipped three additional container checks; those
checks passed in the final full run with Docker access. Fresh independent
adversarial review found no defects, and its documentation follow-up confirmed
the recorded scope. Evidence is retained outside Git under
`/tmp/sapphire-core-tooling-*` and `/tmp/sapphire-core-precommit-final.log`.

No live pipeline or Docker build rerun was performed for this development-only
update: production dependencies, entry points, Docker configuration and data
flow are unchanged. Those paths passed in merged PR #544; this batch adds
validation of the changed tooling itself. Platform-specific behavior outside
the tested Python 3.12 macOS environment was not independently exercised.

## Batch 5: runoff Google Sheets authentication dependencies — Review

Initially prepared from `origin/maxat_sapphire_2` at `8cc34e0a` while
core-tooling PR #545 awaited review/merge. Rebased onto `428127b4` after that
PR merged, preserving the agreed sequential PR order. Dependency changes are confined to `apps/preprocessing_runoff/pyproject.toml`
and its `uv.lock`; this document records their validation.
After rebasing, the full suite passed again with the merged core-tooling
environment: 9,677 passed across 16 scopes, the same 12 accepted skips and
two existing expected failures. The runoff manifest and lockfile remain
byte-identical to the candidate used for the live and Docker checks below.
Offline lock consistency and independent post-rebase review also passed.

- oauthlib 3.3.1 → 4.0.1, with a matching minimum-version constraint.
- pyasn1 0.6.4 → 0.6.5, with a matching minimum-version constraint.

These packages enter through the optional `google-sheets` extra, which the
runoff Docker image installs. Targeted resolution changes exactly these two
package records: parent libraries, other versions, SDK/API-client Git pins
and application code remain unchanged. Official PyPI version metadata lists
both candidates as non-yanked, compatible with the declared Python range and
without reported vulnerabilities at verification time. No shared-library
release or source update is required for this tested resolution.

Frozen installation uses an isolated Python 3.12.10 runoff environment.
A temporary synthetic compatibility probe exercises the actual Google Sheets
reader, real gspread service-account creation and verified JWT refresh,
requests-oauthlib authorization-code/token parsing, and ASN.1/RSA key parsing
and signature verification. Only HTTP transport is faked, with socket
connections blocked. Baseline and candidate results match exactly. The same
probe passes identically inside the rebuilt Linux image with networking
removed. Evidence records interpreter/package versions and the probe hash;
all generated credentials and synthetic data remain outside Git. The live
development configuration disables Google Sheets, so these probes establish
compatibility of the changed authentication stack, not live Google access.

Affected runoff tests: 639 passed with two existing accepted fixture skips.
The final isolated full suite passed all 16 scopes: 9,677 passed, the same
12 accepted skips and two existing expected failures as the previous batch.
An initial full-suite run overlapped the live pipeline in the same worktree:
all 566 pipeline test bodies passed, but 104 teardown checks correctly detected
the live run changing `apps/logs`. Rerunning the identical candidate in a
separate validation worktree eliminated those errors; no tests or code were
changed. Docker validation passed eight builds and eight smoke tests with the
prescribed `--skip-ml` option. Fresh independent adversarial review found no
defect; its request for explicit environment-version evidence was addressed
and confirmed.

Live `run_locally.sh all --continue-on-error` passed all eleven execution
stages and all three API validations (exit 0, about 39 minutes), including
LT simulation, operational postprocessing, skill recalculation and maintenance.
The `all` target exercises DECAD ML under its documented dispatcher behavior.
The subsequent explicit `maintenance:preprocessing_runoff` run also passed
(exit 0, 14m 20s), including its long-horizon hydrograph sync. The development
.env hash remained unchanged after both runs. Maintenance retained the same
warning messages as baseline for unavailable historical runoff and monthly
discharge norms; those data-availability gaps remain outside this dependency
patch. This batch follows merged core-tooling PR #545. Evidence is outside Git under
`/tmp/sapphire-runoff-auth-*`.

## Batch 6: virtualenv activation follow-up — Review

Initially prepared from `origin/maxat_sapphire_2` at `428127b4` after PR #545,
then rebased onto `4bf06c5c` after runoff PR #546 merged. This corrects the
missed advisory described in Batch 4. The only
package change is virtualenv 21.14.10 → 21.14.13, with the core manifest's
security floor raised to match. All transitive versions, Git revisions and
the complete 22-package production graph (including the core package) remain
unchanged. No application logic or service files change.

[GHSA-cx6f-vjhg-g3cc](https://github.com/pypa/virtualenv/security/advisories/GHSA-cx6f-vjhg-g3cc)
affects versions >=21.14.9,<21.14.13: a caller-supplied activation prompt can
execute command substitution when Bash prompt expansion is enabled later.
21.14.13 is the minimum forward fix. The paginated GitHub global catalog
returned 24 virtualenv advisories, the maintainer catalog returned 22, and
version-specific queries found none affecting the candidate or the five
retained transitives on 2026-11-19. Official release metadata declares the
same dependency requirements as 21.14.10. These are advisory-snapshot findings,
not a guarantee against future disclosures.

Validation uses an isolated Python 3.12.10 core environment. Frozen installation
and a fresh-cache local pre-commit Python hook passed. An offline, disposable
interactive Bash probe reproduces the advisory with 21.14.10 and verifies that
21.14.13 leaves the custom prompt literal without executing its harmless marker
command after prompt expansion is re-enabled. No user shell configuration,
credentials or deployment data were used. Evidence is outside Git under
`/tmp/sapphire-virtualenv-*`. Full `run_tests.sh` passed 9,677 tests across all
16 scopes, with exactly the same 12 owner-accepted skips and two existing
expected failures as the preceding batch. Three Docker checks skipped in the
initial restricted core run passed in the final full run with Docker access.
Fresh independent read-only adversarial review found no actionable defect.
After rebasing onto merged PR #546, the full suite passed again with the
updated runoff environment: the same 9,677 passes, 12 accepted skips and two
expected failures. The virtualenv manifest and lockfile are byte-identical
to the candidate used for the activation and pre-commit probes; both the core
and runoff lock consistency checks passed.

No additional live pipeline or Docker image build was run for this
optional development dependency: production dependencies, entry points,
Docker configuration and data flow remain unchanged. The preceding runtime
and runoff batches cover those paths; this follow-up exercises the affected
tooling directly. Other shells, platforms and Python versions were not tested.

## Batch 7: shared service dependency updates — Review

Based on `origin/maxat_sapphire_2` at `0d585a83`, after merged PR #547.
The owner authorized minor service dependency updates, with authentication
packages reserved for a separate batch. This batch changes only dependency
manifests, locks and requirements for the five services; no service logic,
API contracts, schemas or migrations change.

| Package | Previous lock versions | Candidate | Affected service locks |
| --- | --- | --- | --- |
| anyio | 4.12.1 / 4.13.0 | 4.14.2 | All five |
| certifi | 2026.1.4 / 2026.2.25 | 2026.10.18 | All five |
| Mako | 1.3.12 | 1.4.2 | Auth, user, preprocessing, postprocessing |
| urllib3 | 2.7.0 | 2.8.0 | Preprocessing, postprocessing |

These are minor updates, plus certifi's calendar-versioned certificate bundle.
They address 25 alerts in the current snapshot. Exact-version GitHub global
advisory queries found no matching advisories for the candidates on 2026-11-19.
The declared Python requirements permit the services' Docker Python 3.11.

Targeted resolution changes only the 16 intended package records across five
locks. All unrelated package records remain identical. Security floors in
`pyproject.toml` prevent resolution back to the vulnerable versions. Service
Dockerfiles install `requirements.txt`, so those files pin the same candidates
for runtime dependencies. Certifi is test-only in the user service and is not
added to that service's production requirements. Existing unconstrained pip
transitives remain unconstrained; Docker installations are checked separately
and must not be described as identical to the uv test environments.

Five isolated Python 3.12.10 frozen environments pass all 444 service tests,
with no skips or failures. All five actual service Docker images build and
pass isolated probes on Python 3.11.18: exact target versions and `pip check`,
AnyIO socket success/refusal, HTTPX where installed, requests/urllib3 in the
two processing services, certifi CA-bundle loading, and actual uvicorn
`/health` requests. Four Alembic/Mako images generate and compile a temporary
revision without connecting to a database. Probe containers use no external
network, deployment configuration or data, and are removed afterward.
Runtime inventories, image IDs and probe hashes are recorded with the evidence.
These checks do not exercise PostgreSQL migrations/readiness or real outbound
TLS trust compatibility. Fresh independent adversarial review found no
introduced defect. Full `run_tests.sh` passed 9,677 tests across all 16 scopes,
with exactly the same 12 owner-accepted skips and two expected failures as the
preceding batch. The prescribed application Docker run passed eight builds
and eight smoke tests with `--skip-ml`; these are separate from the five
service-image checks above. `run_locally.sh all --continue-on-error` completed
with exit 0: eleven execution stages and three API validations passed, zero
failed, in about 40 minutes. This includes DECAD ML, short-term regression and
postprocessing for both horizons, long-term simulation, and long-term
operational/maintenance postprocessing and skill recalculation. It does not
include explicit PENTAD ML. The private configuration hash remained unchanged;
the macOS thread cap was applied by the runner without a parent setting.
The pipeline uses existing remote development services; it does not exercise
the candidate service images or prove complete forecast coverage.
Private logs and synthetic probe artifacts are retained outside Git under
`/tmp/sapphire-service-shared-*`.
