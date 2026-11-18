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
