# User Management Runbook Documentation Plan

## Goal

Add a documentation-only operator runbook for hydromet IT staff managing
SAPPHIRE dashboard user accounts on deployed servers. Wire it into MkDocs
navigation and deployment documentation. No service code changes.

## Edit Scope

Files to edit:

- `doc/operations/user_management.md` (new)
- `mkdocs.yml`
- `doc/deployment.md`

Read-only verification files:

- `sapphire/services/auth/app/main.py`
- `sapphire/services/auth/app/crud.py`
- `sapphire/services/user/app/main.py`
- `sapphire/services/user/app/schemas.py`
- `sapphire/services/user/app/crud.py`
- `sapphire/services/api-gateway/app/main.py`
- `sapphire/services/api-gateway/app/config.py`

Out of scope:

- No edits under `sapphire/services/`
- No `sapphire-api-client` user-management work
- No real credentials, station codes, or operational data in examples
- `bin/manage_sapphire_user.sh` wrapper is a separate follow-up
- Service-side admin password reset endpoint is a separate follow-up

## Settled API Facts To Encode

- `POST /api/auth/register` is unauthenticated and bypasses the gateway API key even when `API_KEY_ENABLED=true`.
- Register body: `email` required, `username` required min 3, `password` required min 8, `full_name` optional.
- Management endpoints:
  - `GET /api/user/users/`
  - `GET /api/user/users/{id}`
  - `PUT /api/user/users/{id}`
  - `DELETE /api/user/users/{id}`
- Deactivation is `PUT /api/user/users/{id}` with `{"is_active": false}`.
- All `/api/user/...` routes are gated only by gateway API key; no JWT/role authorization in user service routes.
- `X-API-Key` header value comes from env `API_KEY`; check is enabled by `API_KEY_ENABLED=true`.
- Gateway listens on `localhost:8000`.
- `POST /api/auth/login` is form-encoded via `OAuth2PasswordRequestForm`, not JSON. JSON body returns `422`.
- No admin password-reset endpoint exists. Only `POST /api/auth/change-password`, requiring the user's current password.

## Phase 0: Verification Gate

Files: read-only service files listed above.

Tasks:

- Verify whether `POST /api/auth/login` rejects users where `is_active=false` before issuing a token. Start at `auth/app/main.py` login path and follow `authenticate_user`.
- Verify whether a self-registered account via `/api/auth/register` alone can obtain elevated/admin privileges. Inspect register schema and CRUD role assignment behavior.
- Record the conclusions for use in Phase 1.

Acceptance criteria:

- Documentation does not claim deactivation blocks login unless verified.
- Security section accurately states whether self-registration creates ordinary accounts only or can grant elevated privileges.
- No settled API facts above are re-opened as questions.

## Phase 1: Create Runbook

File: `doc/operations/user_management.md`

Sections, in order:

```md
# User Management Runbook

## Audience
## Security Rules
## Preconditions
## Set Shell Variables
## Create a User
## Confirm the New Account Can Log In
## Verify a User Exists
## List Users
## Deactivate a User
## Delete a User
## Password Changes and Forgotten Passwords
## Staff Change Checklist
## Troubleshooting
## Related Documentation
```

Required content:

- Audience: hydromet IT staff managing dashboard accounts on deployed SAPPHIRE servers.
- Security Rules:
  - Gateway must stay localhost-only.
  - `/api/auth/register` is unauthenticated and must never be exposed through public reverse proxy.
  - `/api/user/...` routes have no authorization beyond the gateway API key.
  - `API_KEY` is an admin-level secret.
  - If `API_KEY_ENABLED=false`, `/api/user/...` destructive routes are unauthenticated behind only the localhost boundary.
- Preconditions:
  - SSH access to deployed server.
  - SAPPHIRE stack running.
  - Gateway reachable at `http://localhost:8000`.
  - Access to deployment env file or `API_KEY` for management operations.
- Set Shell Variables:
  - Use placeholder values only, e.g. `new.user@example.org`.
  - Use `read -s SAPPHIRE_NEW_USER_PASSWORD` to avoid shell history leakage.
- Create a User:
  - Local-only curl to `/api/auth/register`.
  - No API-key header.
  - Document username min 3 and password min 8.
- Confirm the New Account Can Log In:
  - Purpose: smoke test before handing credentials to staff.
  - Must use exactly this form, suppressing token output:

```bash
curl -s -o /dev/null -w "HTTP %{http_code}\n" \
  -X POST http://localhost:8000/api/auth/login \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=$USERNAME&password=$SAPPHIRE_NEW_USER_PASSWORD"
```

  - No API-key header.
  - Success is `HTTP 200`.
  - `401` means bad credentials or, if Phase 0 confirms it, inactive account.
  - `422` means wrong request shape, usually JSON instead of form-encoded.
  - Do not echo, copy, or document returned tokens.
- Verify/list/deactivate/delete:
  - Every `/api/user/...` curl includes `-H "X-API-Key: $SAPPHIRE_API_KEY"`.
  - State that `SAPPHIRE_API_KEY` is the deployment `API_KEY` value and is required when `API_KEY_ENABLED=true`.
- Deactivate a User:
  - Use `PUT /api/user/users/{id}` with `{"is_active": false}`.
  - If Phase 0 confirms inactive users cannot log in, add: repeat [Confirm the New Account Can Log In](#confirm-the-new-account-can-log-in) for the deactivated account and expect `HTTP 401`.
  - If not confirmed, state deactivation changes account state only and deletion may be required to prevent login.
- Delete a User:
  - Use `DELETE /api/user/users/{id}`.
  - Guidance: only when appropriate; prefer deactivate where audit/history matters.
- Password Changes and Forgotten Passwords:
  - No admin reset endpoint exists.
  - Change-password requires current password.
  - Forgotten-password options: delete-and-recreate, or maintainer-approved DB path if documented later.
  - Recommend a separate service issue for admin reset.
- Troubleshooting:
  - `422`: validation failure, min-length violation, or JSON sent to form-encoded login.
  - `401`: missing/wrong API key on `/api/user/...`, or bad login credentials.
  - Connection refused: stack down or gateway not on localhost.
  - Duplicate email/username.

Acceptance criteria:

- Runbook supports create, login-confirm, verify, list, deactivate, delete, and password-forgotten workflows.
- Register and login examples have no API-key header.
- Login example is form-encoded, suppresses response body, and defines success as `HTTP 200`.
- All `/api/user/...` examples include `X-API-Key`.
- No invented admin reset endpoint appears.
- No real credentials, real emails, station codes, or operational data appear.

## Phase 2: MkDocs Navigation

File: `mkdocs.yml`

Task:

Insert the new page into the existing Operations nav between Dashboard and Bulletin Templates:

```yaml
  - Operations:
      - Workflows: workflows.md
      - Dashboard: dashboard.md
      - User Management: operations/user_management.md
      - Bulletin Templates: bulletin_template_tags.md
      - Monitoring: monitoring/forecast_tools_monitoring.md
```

Acceptance criteria:

- Nav path is `operations/user_management.md`, relative to `docs_dir: doc`.
- Existing Operations ordering is preserved except for the new entry.

## Phase 3: Deployment Cross-Links

File: `doc/deployment.md`

Tasks:

- Near `### SAPPHIRE services (API stack)`, add a concise pointer to the runbook for recurring dashboard account administration.
- Near the Dashboards / Reverse proxy and HTTPS section, add a stronger security-framed pointer:
  - API gateway and `/api/auth/register` must not be exposed through the public reverse proxy.
  - Account management is done from the server over SSH.
  - Link to `operations/user_management.md`.

Acceptance criteria:

- Operators can discover account management from both API-stack context and dashboard/security context.
- Cross-links resolve correctly from `doc/deployment.md`.

## Phase 4: Documentation QA

Tasks:

- Search for discoverability terms:
  - `add user`
  - `new user`
  - `account`
  - `password`
  - `deactivate`
  - `staff`
  - `login`
- Check examples:
  - Placeholder emails only, e.g. `new.user@example.org`.
  - No real credentials.
  - No key-less `/api/user/...` examples.
  - No invented reset endpoint.
  - No login example that prints tokens.
- Optional: run `mkdocs build` if tooling is available.

Acceptance criteria:

- New page is discoverable through MkDocs nav and search terms.
- Markdown links resolve.
- Runbook can be followed without reading service source.
- QA confirms no security-sensitive example content.

## Follow-Up Issues

Out of scope but recommended separately:

- Add `bin/manage_sapphire_user.sh` wrapper for create/list/deactivate/delete.
- Add service-side admin password reset endpoint.

## Dependency Graph

```text
Phase 0: Verification gate
  -> Phase 1: Create user_management.md
      -> Phase 2: MkDocs nav
      -> Phase 3: Deployment cross-links
          -> Phase 4: Documentation QA

Out-of-scope follow-ups branching from Phase 1:
  - bin/manage_sapphire_user.sh wrapper
  - service-side admin password reset endpoint
```
