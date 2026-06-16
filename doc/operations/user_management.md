# User Management Runbook

This runbook explains how to create, verify, deactivate, and delete SAPPHIRE
dashboard user accounts on a deployed server. It is written so that it can be
followed without reading the service source code.

## Audience

Hydromet IT staff managing dashboard user accounts on deployed SAPPHIRE
servers. All operations are performed from the server itself over SSH, against
the API gateway on `localhost`.

## Security Rules

Read this section before running any command.

- **The API gateway must stay localhost-only.** It listens on
  `http://localhost:8000` and must never be exposed through the public reverse
  proxy. See the reverse-proxy guidance in
  [`../deployment.md`](../deployment.md#reverse-proxy-and-https).
- **`/api/auth/register` is unauthenticated.** It bypasses the gateway API key
  even when `API_KEY_ENABLED=true`. It must never be reachable from the public
  internet, or anyone could create accounts. This is another reason the gateway
  stays localhost-only.
- **Self-registration creates ordinary accounts only.** The register endpoint
  accepts only `email`, `username`, `password`, and `full_name`. It cannot set
  a role, `is_active`, or `is_superuser`. A self-registered account is always
  an ordinary, active user — registration can never grant elevated or admin
  privileges.
- **`/api/user/...` routes have no authorization beyond the gateway API key.**
  There is no JWT or role check on the user-management routes. Anyone who can
  reach the gateway and present the API key can list, modify, and delete users.
- **`API_KEY` is an admin-level secret.** Treat it like a root password. Do not
  paste it into shell history, commit it, or share it beyond the staff who run
  these operations.
- **If `API_KEY_ENABLED=false`,** the `/api/user/...` destructive routes
  (including delete) are unauthenticated and protected only by the localhost
  boundary. In that configuration, anyone with shell access to the server can
  delete users without a key. Keep `API_KEY_ENABLED=true` on real deployments.

## Preconditions

- SSH access to the deployed server.
- The SAPPHIRE service stack is running (see
  [`../deployment.md`](../deployment.md#sapphire-services-api-stack)).
- The gateway is reachable at `http://localhost:8000`. Confirm with:

  ```bash
  curl -sf http://localhost:8000/health && echo OK
  ```

- For management operations (list / verify / deactivate / delete) you need the
  deployment `API_KEY` value. It is set in the deployment env file as
  `API_KEY`.

## Set Shell Variables

Set the variables you will reuse below. Use placeholder values — never paste a
real password as a visible command argument, and never commit real emails or
credentials.

```bash
# The deployment API key (value of API_KEY in your env file).
read -rs -p "API key: " SAPPHIRE_API_KEY; echo
```

`read -rs` suppresses echo, so the key is not shown on screen and does not leak
into the terminal scrollback. The create-user command below prompts for the
remaining values (email, username, full name, password) interactively, so you
do not need to set them in advance.

## Create a User

Account creation uses `POST /api/auth/register`. This endpoint is
unauthenticated, so the command below has **no API-key header**. It creates an
ordinary, active account.

Validation rules enforced by the endpoint:

- `email` — required.
- `username` — required, minimum 3 characters.
- `password` — required, minimum 8 characters.
- `full_name` — optional.

The command prompts for each field, builds the JSON body with `python3` (which
safely escapes special characters in the values), and POSTs it. Only the HTTP
status code is printed.

```bash
read -r -p "Email: " EMAIL; read -r -p "Username: " USERNAME; read -r -p "Full name: " FULL_NAME; read -rs -p "Password: " PASSWORD; echo; python3 -c 'import json,sys; email,username,full_name,password=sys.argv[1:5]; print(json.dumps({"email":email,"username":username,"full_name":full_name,"password":password}))' "$EMAIL" "$USERNAME" "$FULL_NAME" "$PASSWORD" | curl -sS -o /dev/null -w "HTTP %{http_code}\n" -X POST http://localhost:8000/api/auth/register -H "Content-Type: application/json" --data-binary @-
```

Example values to use when testing: email `new.user@example.org`, username
`newuser`, password (8+ characters of your choosing).

Expected success is `HTTP 201`. If you see `422`, a validation rule failed
(usually a username shorter than 3 or a password shorter than 8). If you see a
duplicate-related error, the email or username already exists.

## Confirm the New Account Can Log In

Smoke-test the new account before handing credentials to staff. Login uses
`POST /api/auth/login`, which is **form-encoded** (OAuth2 password flow), not
JSON. The command below reuses the `$USERNAME` and `$PASSWORD` variables from
the create-user step, suppresses the response body (so the token is never
printed), and prints only the status code.

```bash
curl -s -o /dev/null -w "HTTP %{http_code}\n" \
  -X POST http://localhost:8000/api/auth/login \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=$USERNAME&password=$PASSWORD"
```

This endpoint is unauthenticated for the login itself, so there is **no
API-key header**. Interpret the status code as follows:

| Status | Meaning |
|--------|---------|
| `200` | Login OK — credentials are valid and the account is active. |
| `400` | Inactive / deactivated account (`detail: "Inactive user"`). The credentials are correct, but the account is deactivated, so no token is issued. |
| `401` | Bad credentials — wrong username or password. |
| `422` | Wrong request shape — usually JSON sent to this form-encoded endpoint. |

**Never echo, copy, log, or document the returned token.** The command above
discards the response body for exactly this reason.

## Verify a User Exists

List users and find the new account's `id`. This is a `/api/user/...` route, so
it **requires the API-key header**.

```bash
curl -s http://localhost:8000/api/user/users/ \
  -H "X-API-Key: $SAPPHIRE_API_KEY"
```

To fetch a single user by id (replace `<id>`):

```bash
curl -s http://localhost:8000/api/user/users/<id> \
  -H "X-API-Key: $SAPPHIRE_API_KEY"
```

`SAPPHIRE_API_KEY` is the deployment `API_KEY` value and is required whenever
`API_KEY_ENABLED=true`.

## List Users

`GET /api/user/users/` returns all users. Use it to audit accounts and to find
the `id` of the account you want to deactivate or delete.

```bash
curl -s http://localhost:8000/api/user/users/ \
  -H "X-API-Key: $SAPPHIRE_API_KEY"
```

Note each user's `id` and `is_active` flag from the response.

## Deactivate a User

Deactivation sets `is_active` to `false` via
`PUT /api/user/users/{id}`. Prefer deactivation over deletion when you want to
keep the account's history and audit trail. Replace `<id>` with the user's id.

```bash
curl -s -X PUT http://localhost:8000/api/user/users/<id> \
  -H "X-API-Key: $SAPPHIRE_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"is_active": false}'
```

**Deactivation blocks login.** To confirm, repeat
[Confirm the New Account Can Log In](#confirm-the-new-account-can-log-in) for
the deactivated account using its (still correct) credentials. It should now
return **`HTTP 400`** (`Inactive user`) instead of `200`. A `400` here means
deactivation worked — the credentials are still valid but the account can no
longer obtain a token. (`401` would mean the credentials themselves were wrong,
which is a different situation.)

## Delete a User

Deletion permanently removes the account via `DELETE /api/user/users/{id}`.
Replace `<id>` with the user's id.

```bash
curl -s -o /dev/null -w "HTTP %{http_code}\n" \
  -X DELETE http://localhost:8000/api/user/users/<id> \
  -H "X-API-Key: $SAPPHIRE_API_KEY"
```

Only delete when removal is genuinely appropriate (e.g., an account created in
error). Where audit history or accountability matters, **prefer deactivation**
so the record is retained.

## Password Changes and Forgotten Passwords

- **There is no admin password-reset endpoint.** An administrator cannot set a
  new password for another user.
- **Self-service change requires the current password.** Users change their own
  password via `POST /api/auth/change-password`, which requires their existing
  password. This is not an admin tool.
- **Forgotten passwords.** Without an admin reset endpoint, the practical
  options are:
  - Delete the account and recreate it (the user loses nothing but the old
    credentials), or
  - A maintainer-approved direct database path, if and when one is documented
    separately.
- A dedicated service-side admin password-reset endpoint is recommended as a
  separate follow-up issue.

## Staff Change Checklist

When a staff member joins or leaves:

- **New staff member:**
  1. [Create a User](#create-a-user).
  2. [Confirm the New Account Can Log In](#confirm-the-new-account-can-log-in)
     (expect `HTTP 200`).
  3. Hand over the credentials securely; ask the user to change their password
     via `POST /api/auth/change-password`.
- **Departing staff member:**
  1. [List Users](#list-users) to find the `id`.
  2. [Deactivate a User](#deactivate-a-user) to block login while keeping
     history.
  3. Confirm login now returns `HTTP 400` (`Inactive user`).
  4. [Delete a User](#delete-a-user) only if your policy requires full removal.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `422` on register | Validation failure — username shorter than 3 or password shorter than 8 characters. | Re-run with a valid username and password. |
| `422` on login | JSON body sent to the form-encoded login endpoint. | Use `Content-Type: application/x-www-form-urlencoded` and the `-d "username=...&password=..."` form, as shown above. |
| `400` on login | Account is deactivated (`Inactive user`). | Expected after deactivation. To restore, set `{"is_active": true}` via `PUT /api/user/users/{id}`. |
| `401` on login | Bad credentials — wrong username or password. | Verify the username and re-enter the password. |
| `401` on `/api/user/...` | Missing or wrong `X-API-Key` header. | Confirm `SAPPHIRE_API_KEY` matches the deployment `API_KEY` and that the header is present. |
| Duplicate email / username on register | An account with that email or username already exists. | Choose a different email/username, or manage the existing account. |
| `Connection refused` | The stack is down, or the gateway is not on localhost. | Check `docker ps --filter "name=sapphire"` and `curl -sf http://localhost:8000/health`. |

## Related Documentation

- [Deployment guide](../deployment.md) — server setup, the SAPPHIRE services
  (API stack), and reverse-proxy / HTTPS security.
- [`sapphire/README.md`](../../sapphire/README.md) — service stack
  configuration and startup.
