# Dependabot services/ handoff — 2026-06-17

These open Dependabot alerts live under `sapphire/services/*`, which is
colleague-managed (see CLAUDE.md → Ownership Boundaries). They are **not**
addressed by the apps-only PR (`infra_dependabot_safe_bumps_2`, bumps
cryptography + bleach in `apps/`). Listed here so the service owner can action
them. Each fix needs the service's `pyproject.toml`, `uv.lock`, and
`requirements.txt` updated together (the same package is pinned in all three).

## Recommended bumps (bump to the highest target to clear all stacked alerts)

| Package | Bump to | Severity | Affected services |
|---|---|---|---|
| **starlette** | **1.3.1** | high | api-gateway, auth, postprocessing, preprocessing, user |
| **python-multipart** | **0.0.31** | high | api-gateway, auth, user |
| **urllib3** | **2.7.0** | high | postprocessing, preprocessing |
| **Mako** | **1.3.12** | high | postprocessing, preprocessing |
| **cryptography** | **48.0.1** | high | auth |
| **idna** | **3.15** | medium | api-gateway, auth, postprocessing, preprocessing, user |
| **requests** | **2.33.0** | medium | postprocessing |
| **pytest** | **9.0.3** | medium | postprocessing, preprocessing |
| **python-dotenv** | **1.2.2** | medium | postprocessing |
| **Pygments** | **2.20.0** | low | postprocessing |
| **ecdsa** | **NO PATCH** | high | auth |

## Notes / risk

- **starlette 0.x → 1.x is a major jump** and is FastAPI-coupled — bump FastAPI
  in lockstep and run the full service test suite. This is the single biggest
  item (5 services).
- **ecdsa** has no patched release. Per prior analysis SAPPHIRE auth uses
  **HS256, not ECDSA**, so the Minerva timing attack is not exploitable here —
  likely dismissable as "not applicable" unless `JWT_ALGORITHM` changes.
- **cryptography → 48.0.1** in auth mirrors the apps fix (apps went to 49.0.0
  transitively); both are ABI-stable transitive bumps.
- **idna, urllib3, requests, Pygments, python-dotenv, Mako, pytest** are
  low-risk patch/minor bumps — same class as the apps bumps already shipped.

## Suggested method (per service)

```bash
cd sapphire/services/<service>
uv lock --upgrade-package <name>     # repeat per package, or --upgrade for all
uv sync --all-extras
bash run_tests.sh service:<service>  # from repo root, or the service's own runner
# regenerate requirements.txt from the lock if that's the service's workflow
```
