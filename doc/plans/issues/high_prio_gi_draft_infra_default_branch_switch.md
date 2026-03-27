# INFRA-016: Switch Default Branch from `main` to `maxat_sapphire_2`

**Status**: In Progress — Phases 1-3 complete (2026-03-27); Phases 4-5 pending branch merge + GitHub settings
**Module**: infra (cross-repo)
**Priority**: High
**Labels**: `infrastructure`, `ci-cd`, `git`, `breaking-change`

---

## Summary

Change the repository default branch from `main` to `maxat_sapphire_2`. This is a
clean cut: `maxat_sapphire_2` is the new architecture and is **not backward-compatible**
with the v0 line (`main`). No merge of `main` into `maxat_sapphire_2` is planned.

**Versioning scheme:**
- `main` is tagged `v0.3.0` (final v0 release, frozen/archived)
- `maxat_sapphire_2` will become `v1.0.0` when production-ready

## Context

The `maxat_sapphire_2` branch has become the active development branch with 30+
commits ahead of `main` (API integration, containerization, ML fixes, dashboard work).
Meanwhile `main` has 30+ commits of its own (uv migration, security patches, gateway
fixes). These two lines have diverged intentionally: v2 is a new architecture and does
not need v1 compatibility.

Commits on `main` that are **not** on `maxat_sapphire_2` include uv migration cleanup,
Pillow security fix, P-002 gateway double-run fix, protobuf update, and Dockerfile
renaming. Cherry-pick any that are still relevant individually — do not merge the whole
branch.

---

## Step-by-Step Procedure

### Phase 1: Preparation (before changing GitHub settings)

- [x] **1.1** Tag the current `main` as `v0.3.0` (via GitHub web UI):
  1. Go to the repository on GitHub → **Releases** → **Create a new release**
  2. Click **Choose a tag** → type `v0.3.0` → **Create new tag: v0.3.0 on publish**
  3. Set **Target** to `main`
  4. Title: `v0.3.0 — Final v0 release (archived)`
  5. Description: `Final state of the SAPPHIRE v0 codebase. The default branch is now maxat_sapphire_2, which will become v1.0.0 when production-ready. This branch (main) is frozen and will not receive further updates.`
  6. Check **Set as a pre-release** (so it doesn't show as the latest release)
  7. Click **Publish release**

- [x] **1.2** Check for open PRs targeting `main`: **None found.**
  ```bash
  gh pr list --base main --state open
  ```
  Retarget any open PRs to `maxat_sapphire_2`, or close them if they are v1-only.

- [x] **1.3** Review commits on `main` not on `maxat_sapphire_2` for cherry-pick candidates.
  Reviewed 2026-03-27. ~100 commits on `main` not on `maxat_sapphire_2`. Most are
  uv migration, CI updates, and planning docs — superseded by v2 work. **Deferred
  cherry-pick candidates** (security/dependency bumps and bug fixes to revisit later):
  - `eb904b2` Pillow >=12.1.1 security fix (ML, Dashboard)
  - `bbeefd1` Protobuf update + local ML script fix
  - `a6395e0` urllib3 bump to 2.6.3
  - `1a1bc8a` pandas groupby.apply() compat fix for pandas 2.2+
  - `bd70287`–`abad9d6` P-002: gateway double-run prevention (check if already addressed in v2)
  - `4ce1d37` P-001: root marker file cleanup (check if already addressed in v2)
  - `a52597d` Tier 1 postprocessing bug fixes (check if already addressed in v2)

### Phase 2: Update CI/CD workflows (on `maxat_sapphire_2`)

All edits below are on the `maxat_sapphire_2` branch.

- [x] **2.1** `.github/workflows/deploy_main.yml`
  - Renamed file to `deploy_production.yml`
  - Updated workflow name: `Test & push production branch to DockerHub`
  - Changed trigger branch: `branches: ["maxat_sapphire_2"]`

- [x] **2.2** `.github/workflows/build_test.yml`
  - `branches-ignore: [maxat_sapphire_2]`
  - `pull_request: branches: [maxat_sapphire_2]`
  - Updated comments referencing `deploy_production.yml`

- [x] **2.3** `.github/workflows/deploy_docs.yml`
  - Updated commented-out auto-deploy trigger to `maxat_sapphire_2`

- [x] **2.4** `.github/workflows/scheduled_security_rebuild.yml`
  - Cron/manual triggered, checks out default branch automatically — no branch reference to update
  - Updated comment referencing `deploy_production.yml`

### Phase 3: Update documentation and config (on `maxat_sapphire_2`)

- [x] **3.1** `CLAUDE.md` line ~259
  - Changed to `Target 'maxat_sapphire_2' for production-ready changes`
  - No other "Main branch" references found in CLAUDE.md

- [x] **3.2** `doc/plans/sapphire_v2_planning.md`
  - Replaced "Post-Merge Checklist" with "Branch Switch" section
  - Noted that `maxat_sapphire_2` is now the default branch; no merge planned

- [x] **3.3** `doc/plans/security_updates.md`
  - Updated to "Unblocked — verify on `maxat_sapphire_2` (SEC-005)"

- [x] **3.4** `.claude/skills/pre-deploy-validation/SKILL.md`
  - `origin/main` -> `origin/maxat_sapphire_2`

- [x] **3.5** `.claude/skills/requesting-code-review/SKILL.md`
  - `origin/main` -> `origin/maxat_sapphire_2`

### Phase 4: Change GitHub default branch

- [x] **4.1** Default branch changed to `maxat_sapphire_2` on GitHub (2026-03-27)

- [x] **4.2** Branch protection rules added to `maxat_sapphire_2`

- [x] **4.3** `main` branch locked (read-only)

### Phase 5: Post-switch verification

- [x] **5.1** Verified `remotes/origin/HEAD` points to `maxat_sapphire_2`

- [ ] **5.2** Push the CI/CD and doc changes from Phase 2-3:
  ```bash
  git push origin maxat_sapphire_2
  ```

- [ ] **5.3** Verify CI runs correctly:
  - Check that `deploy_production.yml` triggers on push to `maxat_sapphire_2`
  - Check that `build_test.yml` triggers on PRs targeting `maxat_sapphire_2`
  - Check that `:latest` Docker images are built from `maxat_sapphire_2`

- [ ] **5.4** Notify collaborators (especially Maxat) that:
  - Default branch is now `maxat_sapphire_2`
  - New PRs should target `maxat_sapphire_2`
  - `main` is frozen as v1 archive

- [ ] **5.5** Update local clones:
  ```bash
  git fetch origin
  git remote set-head origin --auto
  ```

---

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Lost fixes from `main` (uv cleanup, Pillow, P-002, protobuf) | Step 1.3: review and cherry-pick relevant ones before the switch |
| Collaborators push to wrong branch | Step 5.4: notify team; Step 4.3: protect `main` as read-only |
| CI breaks on first push | Step 5.3: monitor first CI run; rollback is easy (change default back) |
| Docker `:latest` tag changes meaning | Intentional: `:latest` should track v2 going forward |

## What Does NOT Need to Change

- **Deployment scripts in `bin/`**: Use environment variables for image tags, not branch names
- **Docker compose files**: No hardcoded branch references
- **Application code**: No branch name dependencies
- **Archived docs in `doc/plans/archive/`**: Historical, leave as-is
