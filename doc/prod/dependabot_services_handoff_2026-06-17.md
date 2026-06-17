# Dependabot services/ handoff — 2026-06-17

These open Dependabot alerts live under `sapphire/services/*`, which is
colleague-managed (see CLAUDE.md → Ownership Boundaries). They are **not**
addressed by the apps-only PR (`infra_dependabot_safe_bumps_2`, bumps
cryptography + bleach in `apps/`). Listed here so the service owner can action
them. Each fix needs the service's `pyproject.toml`, `uv.lock`, and
`requirements.txt` updated together (the same package is pinned in all three).

## Already handled in this PR (services boundary crossed with user authorization)

These two were trivial transitive patch bumps (not pinned in pyproject.toml or
requirements.txt), so they were done in this branch with user sign-off. Service
tests pass (postprocessing 110 passed; preprocessing passed). Flagged for
colleague review before merge.

| Package | Bumped to | Severity | Services | Status |
|---|---|---|---|---|
| **urllib3** | 2.6.3 → **2.7.0** | high | postprocessing, preprocessing | ✅ done (PR #378) |
| **Mako** | 1.3.10/1.3.11 → **1.3.12** | high | postprocessing, preprocessing | ✅ done (PR #378) |
| **idna** | 3.11 → **3.18** | medium | api-gateway, postprocessing, preprocessing | ✅ done (tier-1 PR) |
| **python-dotenv** | 1.2.1 → **1.2.2** | medium | postprocessing | ✅ done (tier-1 PR) |
| **Pygments** | 2.19.2 → **2.20.0** | low | postprocessing | ✅ done (tier-1 PR) |
| **requests** | 2.32.5 → **2.34.2** | medium | postprocessing | ✅ done (tier-2 PR, lock+requirements) |

## ⚠️ auth + user — needs owner action (stale lock blocks a clean bump)

`auth` and `user` declare `alembic>=1.17.0` in `pyproject.toml`, but **alembic is
missing from their committed `uv.lock`** (the lock is stale). Any `uv lock` run
reconciles this and adds `alembic` + its sub-deps `mako`/`markupsafe`. Because
that expands the change beyond the security bump and touches dependency
resolution, the following were **deferred to you** rather than forced through:

- **auth**: cryptography 46.0.7 → 48.0.1 (high), idna 3.11 → 3.15+
- **user**: idna 3.11 → 3.15+

Recommend: first reconcile the lock (commit the alembic addition deliberately),
then the idna/cryptography bumps are clean lock-only changes.

## Remaining — recommended bumps (bump to the highest target to clear all stacked alerts)

| Package | Bump to | Severity | Affected services |
|---|---|---|---|
| **starlette** | **1.3.1** | high | api-gateway, auth, postprocessing, preprocessing, user |
| **python-multipart** | **0.0.31** | high | api-gateway, auth, user |
| **pytest** (requirements.txt `==8.4.2`) | **9.0.3** | medium | postprocessing, preprocessing |
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
