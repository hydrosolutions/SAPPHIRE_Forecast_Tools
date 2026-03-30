# INFRA: Fix dependency vulnerabilities in sapphire/services/

**Priority:** High
**Owner:** Max
**Status:** Draft
**Created:** 2026-03-27

## Summary

Dependabot flagged several vulnerable dependencies in `sapphire/services/`.
These are all in `requirements.txt` files managed by the services developer.

## Vulnerabilities

### python-multipart — Arbitrary File Write (High)

- **CVE:** GHSA (Arbitrary File Write via Non-Default Configuration)
- **Severity:** High
- **Current versions:** 0.0.20 (`api-gateway`), 0.0.21 (`auth`, `user`)
- **Fix version:** >=0.0.22

**Affected files:**
- `sapphire/services/api-gateway/requirements.txt`
- `sapphire/services/auth/requirements.txt`
- `sapphire/services/user/requirements.txt`

### requests — Insecure Temp File Reuse (Moderate)

- **CVE:** CVE-2026-25645
- **Severity:** Moderate
- **Current version:** 2.32.5
- **Fix version:** >=2.33.0
- **Note:** Only affects code calling `extract_zipped_paths()` directly.
  Standard `requests.get()`/`requests.post()` usage is not affected.

**Affected files:**
- `sapphire/services/preprocessing/requirements.txt`
- `sapphire/services/postprocessing/requirements.txt`
- `sapphire/services/postprocessing/uv.lock`

## Steps

1. Bump `python-multipart` to `>=0.0.22` in api-gateway, auth, user
2. Bump `requests` to `>=2.33.0` in preprocessing, postprocessing
3. Regenerate lock files if applicable
4. Run service tests: `bash run_tests.sh service:postprocessing`
5. Verify services start correctly: `docker-compose up -d && curl localhost:8000/health`

## Acceptance Criteria

- [ ] All Dependabot alerts for python-multipart and requests in services are resolved
- [ ] Service tests pass
- [ ] Docker health checks pass
