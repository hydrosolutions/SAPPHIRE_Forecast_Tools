# Runbook: machine_learning produces no forecasts

**Audience:** deployment operator (remote), running the pipeline by hand.
**Symptom this runbook is for:** operational data arrives normally, but no
`machine_learning` forecasts appear (dashboard empty for TFT/TIDE/TSMIXER, or no
ML rows in the postprocessing DB).
**Created:** 2026-08-20. Revised same day after out-of-loop review.

**Run every command from the repository root.** Set these up first — later steps
reuse them, and steps that need `apps/` use a `( … )` subshell so your working
directory never moves:

```bash
export REPO="$(git rev-parse --show-toplevel)"; cd "$REPO"
export ENVFILE=/path/to/your/.env
export SAPPHIRE_API_URL="$(grep -E '^SAPPHIRE_API_URL=' "$ENVFILE" | cut -d= -f2-)"
export RUN="$(ls -t apps/logs/run_locally_*.log | head -1)"
echo "api=$SAPPHIRE_API_URL  run=$RUN"
```

`run_locally.sh` never sources your `.env` — it only ever `grep`s specific
keys out of it (see `resolve_org`, `apps/run_locally.sh:562-581`, and the
explicit comment at `:1096`), so
`SAPPHIRE_API_URL` is not in your shell unless you export it as above — without
it, Step 9 silently checks `localhost` instead of your deployment.

Work through the steps **in order** and stop at the first one that fails. Each
step says what a healthy result looks like and what a bad one means. Steps 1–4
are cheap and discriminate hardest.

---

## Before you start — REDACT WHAT YOU SEND

Logs from this pipeline contain secrets and operationally sensitive values.
**Never paste a raw log into an issue, PR, chat or e-mail.**

- **`preprocessing_gateway` logs can contain the live Data Gateway API key.** The
  DG client embeds the key in the request URL, and that URL appears in the text of
  the `ValueError` it raises. Reachable leaks on the daily path include the
  **second-attempt** failure (today's data not published → retry yesterday → that
  also fails → exception text plus a full traceback) and its sibling
  unexpected-error branch. Line numbers are deliberately omitted: PREPG-015 is
  actively changing these call sites, so match the **statement**, not a number.
  **This is version-dependent** — hardening has landed progressively (some sites
  now log the exception *class* only, others use a redaction helper), and older
  deployments have none of it. Since your logs come from whatever version you
  deployed, **assume any gateway log contains a key.**
- **Station codes and discharge values are sensitive** and appear in plain text
  throughout the ML and runoff logs (`Rivers to predict: […]`,
  `Recalculating forecasts for codes […]`).

Redact before sending:

```bash
LOG=/path/to/logfile
sed -E -e 's/api_?key=[^& ]*/api_key=REDACTED/gI' \
       -e 's/([Aa]pi[-_ ]?[Kk]ey["'"'"':= ]+)[A-Za-z0-9_.:-]{8,}/\1REDACTED/g' \
       -e 's/([0-9]{1,3}\.){3}[0-9]{1,3}/IP-REDACTED/g' \
       "$LOG" > "$LOG.redacted"
```

**This `sed` does NOT redact station codes or discharge values** — it cannot tell
them from ordinary numbers. Read the redacted file before sending and strip those
by hand, or send only the counts each step asks for.

---

## Round 1 — run this one block and send the output

You are remote and each exchange is expensive, so this collects everything the
first four steps need in one paste. Run it after the preamble exports above, send
the output (redacted — see below), and only work through the numbered steps if
Round 1 does not already explain it.

```bash
{
echo "===== 0. CHECKOUT ====="
git rev-parse --short HEAD; git log -1 --date=short --format='%cd %s'
echo "envfile=$ENVFILE  api=$SAPPHIRE_API_URL"

echo "===== 1. ORG + ML AVAILABILITY (most decisive) ====="
grep -n "^ieasyhydroforecast_organization=" "$ENVFILE"
grep -n "^ieasyhydroforecast_available_ML_models=" "$ENVFILE"
grep -nE "^(SAPPHIRE_API_ENABLED|SAPPHIRE_API_URL)=" "$ENVFILE"
docker image ls 2>/dev/null | grep sapphire-ml || echo "no sapphire-ml image"
ls -d apps/machine_learning/.venv 2>/dev/null || echo "no ML venv"
apps/machine_learning/.venv/bin/python -c "import sapphire_api_client; print('api client OK')" 2>&1 | tail -1

echo "===== 2. RUN HEADER ====="
head -20 "$RUN"

echo "===== 3. DID ML RUN, AND DID IT WRITE ====="
grep -n "Skipping machine_learning\|Module: machine_learning" "$RUN"
grep -n "Running: machine_learning/\|SAPPHIRE API: Successfully wrote" "$RUN"

echo "===== 4. WHY NOT (counts only) ====="
for pat in "is not ready, skipping ML forecast write" \
           "Failed to write .* forecast to API" \
           "No meteo data" \
           "NaN present in ERA5 covariates" \
           "Error in forecast for code"; do
  printf "%-45s %s\n" "$pat" "$(grep -c "$pat" "$RUN")"
done

echo "===== 5. STATION LIST SIZES (counts, not codes) ====="
grep -o "Rivers to predict: .*" apps/machine_learning/logs/log | tail -1 \
  | tr ',' '\n' | wc -l
echo "(above = number of stations in the last 'Rivers to predict' list)"

echo "===== 6. RELATIVE PATH RESOLUTION (see note below) ====="
grep -E "^(ieasyforecast_intermediate_data_path|ieasyhydroforecast_models_and_scalers_path)=" "$ENVFILE"
( cd apps/machine_learning \
  && for rel in "$(grep -m1 '^ieasyforecast_intermediate_data_path=' "$ENVFILE" | cut -d= -f2-)" \
                "$(grep -m1 '^ieasyhydroforecast_models_and_scalers_path=' "$ENVFILE" | cut -d= -f2-)"; do
      if cd "$rel" 2>/dev/null; then echo "  RESOLVES -> $(pwd)"; cd - >/dev/null; \
      else echo "  *** DOES NOT RESOLVE: $rel ***"; fi
    done )

echo "===== 7. PREPROCESSING_RUNOFF FAILURE ====="
grep -n "preprocessing_runoff failed\|Traceback" "$RUN" | head -5

echo "===== 8. LONG-HORIZON HYDROGRAPH SYNC (INFRA-037, degraded not fatal) ====="
grep -n "SDK call failed for site\|long-horizon sync\|Long-horizon hydrograph sync had" "$RUN" | head -10
} 2>&1 | tee /tmp/ml_round1.txt
```

Then redact and send `/tmp/ml_round1.txt`:

```bash
sed -E -e 's/api_?key=[^& ]*/api_key=REDACTED/gI' \
       -e 's/([Aa]pi[-_ ]?[Kk]ey["'"'"':= ]+)[A-Za-z0-9_.:-]{8,}/\1REDACTED/g' \
       -e 's/([0-9]{1,3}\.){3}[0-9]{1,3}/IP-REDACTED/g' \
       /tmp/ml_round1.txt > /tmp/ml_round1.redacted.txt
```

Read `/tmp/ml_round1.redacted.txt` before sending — section 6 may contain station
codes, and the `sed` cannot remove those.

**Section 1 alone may end the investigation.** If your organisation is `demo` or
`uzhm`, ML is skipped by design; if it is anything other than `kghm` or `tjhm`
and you run via Docker, you have no ML image.

> **If you are on `kghm` (env file `.env_*_kghm`), the org is not your problem** —
> ML is expected to run. Start from section 1's
> `ieasyhydroforecast_available_ML_models` line and section 3, and read the two
> known-bug notes below before anything else: they used to fully explain this
> exact symptom with no deeper cause, and a fix for both has landed on this
> branch — INFRA-037 is **implemented and confirmed** (full apps test suite
> green — 16/16 modules and services, zero failures, no skips introduced by
> this branch; multiple rounds of out-of-loop adversarial review); confirm your
> checkout has both fixes before assuming they apply to you; see Step 0 for
> how to check the commit.
>
> - **INFRA-037 — `run_locally.sh daily` used to abort before ML was reached**
>   whenever the maintenance sub-step `sync_long_horizon_hydrograph.py` exited
>   4 (at least one station's iEasyHydro-HF monthly-norm SDK lookup raised) —
>   a condition the owner regards as expected/degraded, not fatal. **Fixed**:
>   that sub-step's exit 4 no longer aborts `daily`; the run continues into
>   ML and the other modules, but still exits non-zero overall, and records a
>   separate `preprocessing_runoff (long-horizon sync): FAIL` line in
>   `PIPELINE SUMMARY`. Look for a `WARNING` (not `DEBUG`) log line naming the
>   specific station code and the SDK exception —
>   `write_station_monthly_hydrograph: SDK call failed for site <code>,
>   continuing with a read-merge of any previously stored norm. Error: ...` —
>   that is the new visible signal for exactly this condition. The affected
>   station is **not** skipped: it still gets its full row set (12 monthly,
>   1 seasonal, 4 quarterly), just marked `SDK_FAILED`; only its monthly norm
>   is affected — the previously stored value is read-merged back in instead
>   of the fresh (failed) lookup. If your checkout predates the fix,
>   `--continue-on-error` is still the workaround: it continues to later
>   modules but still exits non-zero at the end, which is expected and not
>   a second failure. Other
>   `preprocessing_runoff`/`sync_long_horizon_hydrograph.py` failures (exits
>   1, 3, 5, or any of the four reachable `preprocessing_runoff.py`
>   `sys.exit(1)` sites) are still fatal to `daily` — this fix narrowly
>   targets the one degraded-but-partial-success case.
> - **ML-016 — `run_locally.sh machine_learning` on its own used to crash**
>   with `ValueError: Prediction mode %s is not supported` unless you exported
>   `SAPPHIRE_PREDICTION_MODE` yourself. **Fixed**: the bare target now
>   resolves its own mode (from `ML_MODE` if `SAPPHIRE_PREDICTION_MODE` is
>   unset, with a `WARN`) instead of forwarding an empty value, and the `%s`
>   in the error message now shows the actual offending value if it is ever
>   hit. See Step 2a — the manual export is no longer required, though it
>   still works if you want to force a specific mode.
>
> Between them these two used to fully account for "the daily run does not
> produce ML" and "running ML by hand does not work either" with no further
> investigation needed. If both are fixed on your checkout and you still see
> the symptom, treat it as a fresh investigation rather than assuming these
> two apply.
>
> **And check section 6 — relative paths.** Your env file gives the module paths
> *relative to the module's working directory*, and `run_in_venv` runs each module
> from `apps/<module>/`. So `../../../kyg_data_forecast_tools/...` resolves to
> **`<parent-of-your-repo>/kyg_data_forecast_tools`**, not to your `DATA_PATH`.
>
> Those two are the same directory on some machines and different on others, and
> the env file looks *identical* either way — a diff will never show it. If your
> repo is not a sibling of your `kyg_data_forecast_tools` directory, then
> `make_forecast.py` cannot find the scaler directory, raises
> `FileNotFoundError: Directory … not found` at `:565`, and `break 2` stops ML for
> **all three models**. The same mismatch also breaks the intermediate-data path,
> which can produce a `preprocessing_runoff` failure at the same time.
>
> **Healthy:** both lines under section 6 print `RESOLVES -> …` and the directory
> shown is the one that actually holds your models and intermediate data.

---

## Step 0 — Identify the checkout

```bash
git rev-parse --short HEAD && git log -1 --date=short --format='%cd %s'
echo "env file: ${ENVFILE:-<NOT SET>}"
```

**Healthy:** a commit hash, a date, and a path to an existing `.env` file.

**Capture:** all three lines — the commit decides which known bugs apply to you.

> **PREPG-010 (gateway dies on one transient network fault):** fixed on
> `maxat_sapphire_2` on **2026-08-20 at 13:06 CEST** (PR #459, merge commit
> `e1b9ccd7`). **Images built before that commit lack the fix** — verify the
> commit your image was built from rather than guessing from its tag. See Step 7.

---

## Step 1 — Read the run header

`run_locally.sh` prints its three decisions before anything runs.

```bash
head -20 "$(ls -t apps/logs/run_locally_*.log | head -1)"
```

**Healthy** (lines carry a timestamp; the sample below trims it for width):

```
[2026-08-20 09:14:02] [INFO] Target: daily
[2026-08-20 09:14:02] [INFO] Organization: <your org>   ← or "<not set, running all modules>"
[2026-08-20 09:14:02] [INFO] Continue on error: false
[2026-08-20 09:14:02] [INFO] Dry run: false
[2026-08-20 09:14:02] [INFO] ML mode: DECAD
[2026-08-20 09:14:02] [INFO] Log file: .../run_locally_20260820_091402.log
```

1. **`Organization:` — check this first, it is the single most decisive value.**
   Machine learning is only ever *expected* to run for two organisations:

   | `ieasyhydroforecast_organization` | `run_locally.sh` runs ML? | ML Docker image pulled? |
   |---|---|---|
   | `kghm` | yes | **yes** |
   | `tjhm` | yes | **yes** |
   | `demo` | **no** — skipped by design (`should_skip_module`, `:482-494`) | no |
   | `uzhm` | **no** — skipped by design (`should_skip_module`, `:482-494`) | no |
   | unset / misspelled / anything else | **yes** (`resolve_org` falls back to "run all", `:562-581`) | **no** |

   - **If it is `demo` or `uzhm`**, ML is skipped on purpose — along with
     `preprocessing_gateway` and `long_term_forecasting`. That alone explains the
     symptom and nothing below applies. The script prints positive confirmation
     (`:2078-2082`): `Demo org: skipping modules — …` / `Uzhm org: skipping
     modules — …`. Look for that line rather than inferring it.
   - **If it is unset or misspelled, you are in the worst cell of the table.**
     `run_locally.sh` will happily *try* to run ML, but
     `bin/utils/pull_docker_images.sh` only pulls `mabesa/sapphire-ml` for `kghm`
     and `tjhm` — every other value falls into an `else` that prints
     "No further images to pull for this organization". So a Docker-based
     deployment has **no ML image at all**, while a venv-based one may still work.
     The two disagree, silently.

   ```bash
   grep -n "^ieasyhydroforecast_organization=" "$ENVFILE"
   docker image ls | grep sapphire-ml || echo "no sapphire-ml image present"
   ls -d apps/machine_learning/.venv 2>/dev/null || echo "no ML venv present"
   ```

   **Healthy:** the org is `kghm` or `tjhm`, spelled exactly, **and** you have
   either the ML image (Docker) or the ML venv (local) depending on how you run
   the pipeline.
   *`<not set, running all modules>` is fine* — `resolve_org` falls back to empty
   and runs everything (`run_locally.sh:562-581`; the fallback `ORG=""` is at
   `:580`). A **misspelled** organisation also runs everything, so a typo will not
   cause a skip here — but fix it anyway: the modules themselves branch on
   `ieasyhydroforecast_organization` (`iEasyHydroForecast/forecast_library.py`
   tests for `kghm`/`tjhm`), and a typo silently takes neither branch.

2. **`ML mode:`** — defaults to `DECAD` (`run_locally.sh:194`). ML runs for DECAD
   only; PENTAD uses linear regression. Intended, not a bug.

3. **`Target:`** —
   - `daily` runs **both** PENTAD and DECAD explicitly (`run_daily_pipeline`,
     `:1552`), so an unset `SAPPHIRE_PREDICTION_MODE` does not stop ML here.
   - `short-term`, `all`, `maintenance` default to **PENTAD** when the mode is
     unset (`:1366`/`:1498`, with a WARN) — and PENTAD + `ML_MODE=DECAD` skips ML entirely.

---

## Step 2 — Was machine_learning skipped, or did it run?

```bash
grep -n "Skipping machine_learning\|Module: machine_learning\|SAPPHIRE_PREDICTION_MODE not set" \
  "$(ls -t apps/logs/run_locally_*.log | head -1)"
```

**Healthy depends on your target — read carefully:**

- **On `daily`:** **two** skip lines plus a `Module: machine_learning` banner is
  the **normal, healthy** result — `daily` loops PENTAD/DECAD in both the
  operational and the maintenance phase, so you get
  `Skipping machine_learning for PENTAD (ML_MODE=DECAD)` **and**
  `Skipping machine_learning maintenance for PENTAD (ML_MODE=DECAD)`. Skip lines
  alone are not a problem; a missing banner is.
- **On `short-term`/`all`/`maintenance`:** a skip line with **no banner anywhere**
  means ML never ran. Re-run with the mode set (Step 2a).

**If there is no banner and no skip line at all:** ML was skipped by organisation
(Step 1.1), or the run aborted earlier (Step 7). On `daily` the org-skip branch is
a bare `:` (`run_daily_pipeline`, `run_locally.sh:1556`) that logs *nothing*, so silence is the
expected signature of an org skip there.

> **The summary does not report skips.** A skipped module records no result, so
> `PIPELINE SUMMARY` can read "2 passed, 0 failed" for a run where ML never
> executed (INFRA-030). Trust the banner, not the counts.

### Step 2a — running ML by hand

**On a fixed checkout (implemented and confirmed — ML-016):** the bare
`machine_learning` target now resolves its own prediction mode via
`resolve_ml_bare_target_modes` instead of crashing on an empty mode. You no
longer need to export `SAPPHIRE_PREDICTION_MODE` by hand — leaving it unset
derives the mode from `ML_MODE` (default `DECAD`) with a `WARN`, and
`ML_MODE=BOTH` now runs both PENTAD and DECAD in one invocation instead of
being silently ignored. **`SAPPHIRE_PREDICTION_MODE=BOTH` by itself does
NOT** — it still loops PENTAD and DECAD internally, but each pass is
filtered against `ML_MODE` (default `DECAD`), so with `ML_MODE` unset only
DECAD actually runs. To run both horizons, set `ML_MODE=BOTH` (with or
without `SAPPHIRE_PREDICTION_MODE=BOTH` — `ML_MODE=BOTH` alone is
sufficient). You can still force a specific mode explicitly:

```bash
cd apps
SAPPHIRE_PREDICTION_MODE=DECAD ieasyhydroforecast_env_file_path="$ENVFILE" \
  bash run_locally.sh machine_learning 2>&1 | tee /tmp/ml_run.log
```

**On an unfixed checkout**, the old behaviour still applies: the bare target
does not resolve a prediction mode and crashes with `ValueError: Prediction
mode %s is not supported` (the literal un-substituted `%s` is also fixed on
this branch — an unfixed checkout still prints it) unless you export the mode
as shown above, and it silently ignores `ML_MODE`. Check Step 0's commit
against this fix's branch before assuming which behaviour you have.

---

## Step 3 — Did ML write anything? (highest-discrimination check in this runbook)

The API writer emits a plain `print()` on success, which `run_locally.sh` captures.
This single command separates "wrote nothing" from "wrote fine" faster than
anything else.

**Why this is first, and why you should trust it over any log line:** it is a
`print()`, not a log record. No logger configuration can suppress it. Everything
else in this runbook depends on which logger emitted a message and whether that
logger set its own level — and this repo gets that wrong in both directions
(Step 4 has two causes that are invisible in every log for exactly this reason).
A `print` is immune to the entire class of problem.

**Do not count these lines on their own.** Three different scripts emit the
*identical* string — `recalculate_nan_forecasts.py:434`, `make_forecast.py` (via
`:171`/`:222`) and `fill_ml_gaps.py:381` — with nothing in the message to tell
them apart. A healthy single-mode run produces about **nine** such lines (3
writers × 3 models), and three lines emitted by the repair scripts alone would
look "healthy" while the operational writer wrote nothing. Interleave them with
the script markers instead:

```bash
grep -n "Running: machine_learning/\|SAPPHIRE API: Successfully wrote" "$RUN"
```

**Healthy:** after each `Running: machine_learning/make_forecast.py` marker and
before the next `Running:` marker, there is **one** write line — for each of the
three models.

**If a `make_forecast.py` marker is followed straight by the next `Running:`
marker with no write line between them:** that model computed and wrote nothing.
Go to Step 4, then Step 5. This is the case the whole runbook exists for.

**Two caveats on the numbers**, so you do not over-read them:
- The count is **station-days**, not stations — do not compare it to your station
  count.
- The helper prints success and returns `True` even when the API reports **zero**
  records written (`utils_ml_forecast.py:805-813`), so `Successfully wrote 0` is
  possible. Treat a zero in the message as a failure.

The authoritative confirmation is row counts in the database (Step 9); this step
is the fast triage that tells you *where* to look.

---

## Step 4 — Why was the write skipped?

**Exit 0 proves nothing.** `make_forecast.py` exits 0 even when every write failed.
There are **four** places where the API write is skipped or fails and `False` is
returned to a caller that ignores it, plus a `try/except` that only logs
(`make_forecast.py:173-175`, `:224-226`).

**Grep the run log, not the module log.** Only `Failed to write … to API` can
appear in `apps/machine_learning/logs/log`; the other messages come from a
different logger that never reaches that file. And `logs/log` is **cumulative
across runs**, so a match there may be from a previous day.

```bash
grep -n "is not ready, skipping ML forecast write\|Failed to write .* forecast to API" "$RUN"
```

**Healthy:** no matches.

| Message | Meaning | Fix |
|---|---|---|
| `SAPPHIRE API at <url> is not ready, skipping ML forecast write` | Postprocessing API failed its readiness check. ML computed forecasts, wrote a CSV, wrote **nothing** to the DB, exited 0. | Step 9; check `SAPPHIRE_API_URL`. |
| `Failed to write pentad/decad forecast to API: …` | Write attempted and rejected. | Send the message (redacted). |

**Three causes produce no log line at all**, so "no matches" does not rule them
out — you must check each directly:

1. **`SAPPHIRE_API_ENABLED` is not `true`** — writes disabled by configuration.
2. **The record set was empty** — ML produced no rows. This is Step 5, and it is
   the most common real cause.
3. **`sapphire-api-client` is not installed** — every caller checks this *before*
   calling the writer, so the "not installed" message is unreachable. A missing
   client is **totally silent**.

Causes 1 and 2 log at `INFO`, suppressed because `scr/utils_ml_forecast.py:38`
caps the root logger at import. Check all three by hand:

```bash
grep -n "SAPPHIRE_API_ENABLED\|SAPPHIRE_API_URL" "$ENVFILE"
apps/machine_learning/.venv/bin/python -c "import sapphire_api_client; print('client OK')"
```

**Healthy:** `SAPPHIRE_API_ENABLED` absent (defaults true) or `true`,
`SAPPHIRE_API_URL` points at your gateway, and `client OK` prints. An
`ImportError` means `cd apps/machine_learning && uv sync --all-extras`.

---

## Step 5 — Does ML have any stations left to forecast?

**This is the most likely cause if `preprocessing_runoff`'s operational run
(Phase 1, `preprocessing_runoff.py`) actually failed** — the discharge data ML
reads. Note this is distinct from INFRA-037, which (as corrected — see the
`kghm` note above) is about a **maintenance** sub-step
(`sync_long_horizon_hydrograph.py`, Phase 2) that runs only after Phase 1
already succeeded and writes month-horizon hydrograph rows that neither
`machine_learning` nor `linear_regression` reads. If you are running modules
by hand because `daily` aborted, check which phase actually failed before
assuming this step's cause applies — an INFRA-037-only abort on an unfixed
checkout leaves Phase 1's discharge data intact.

**There are two independent station lists, and only one of them can cause a
zero write.** An earlier draft conflated them:

- **`rivers_to_predict`** (`get_rivers_to_predict`, `:480-492`; loop domain at
  `:749`) — the station-selection union intersected with the per-model `== True`
  column. **Empty ⇒ zero records written ⇒ silent exit 0.** This is the
  zero-write cause.
- **`codes_to_use`** (`:613`) — the intersection of past discharge
  (`preprocessing_runoff`, `:600`), forcing meteo (`preprocessing_gateway`,
  `:604`) and static features (`:610`). It drives only the PET/daylight
  enrichment loop at `:634` and never filters the forecast loop. **Short ⇒
  degraded rows** (`flag=1`/`flag=2`, possibly NaN) that **are still written**.

So: empty `rivers_to_predict` explains "no forecasts"; short `codes_to_use`
explains "forecasts present but empty/NaN on the dashboard".

Both lists are in the ML log at DEBUG:

```bash
grep -o "Rivers to predict: .*" apps/machine_learning/logs/log | tail -3
grep -o "codes_to_use.*" apps/machine_learning/logs/log | tail -3
```

**Healthy:** both lists are non-empty and roughly your station count.

- **`Rivers to predict` empty** — nothing was forecast. If empty for one model
  only, that model is disabled in the hydropost configuration — a very plausible
  cause of "TFT works, TIDE doesn't".
- **`codes_to_use` empty or much shorter than `Rivers to predict`** — one of the
  three inputs is missing stations. Check discharge first: if
  `preprocessing_runoff` failed, this is your answer and the fix is upstream.
  Expect NaN/flagged rows rather than absent ones.

Two more signals worth grepping here, both of which produce *written but useless*
forecasts rather than missing ones:

```bash
grep -c "NaN present in ERA5 covariates" "$RUN"
grep -c "Error in forecast for code" "$RUN"
```

**Healthy:** both zero. Non-zero means forcing gaps survived the bounded gap-fill
(`utils_ml_forecast.py:377-432`) and the model may have emitted all-NaN rows,
which are written with null values and counted as a successful write.

**Capture:** the **lengths** of these lists, not the lists themselves — they are
station codes.

---

## Step 6 — Is the forcing data current?

**Check `SAPPHIRE_API_ENABLED` first (Step 4).** ML reads meteo through
`read_meteo_data_combined`, which is **API-first**. The switch itself is in
`forecast_library.py:2780-2794` (the wrapper at `utils_ml_forecast.py:91-118` is
just signature and docstring): it reads from the preprocessing API and falls back
to CSV only when `SAPPHIRE_API_ENABLED=false`.

- **If API-enabled (the normal case):** the `*_control_member.csv` files on disk
  are **not** what ML consumed — their timestamps prove nothing. Check the
  preprocessing API's meteo coverage instead (latest date per code).

  An **unavailable** preprocessing API makes this read **raise** — verified by
  running ML's read path against a stopped stack, which produced
  `SapphireAPIError: SAPPHIRE API at <url> is not ready` after four retries,
  *before* any forecast or write. So ML crashes loudly in that case.

  > **This splits the diagnosis cleanly, and it is worth checking early.**
  > ML **reads** from preprocessing and **writes** to postprocessing.
  > - Preprocessing down (or both) → ML **crashes** with `SapphireAPIError`.
  >   You will see it; this is not the silent case.
  > - **Preprocessing up, postprocessing down** → ML reads fine, computes fine,
  >   then silently skips the write and exits 0. **That is the silent failure
  >   this runbook exists for.**
  >
  > The messages are confusingly similar. The *read* failure says `is not ready`;
  > the *write* skip says `is not ready, skipping ML forecast write`. Grep for the
  > longer string. But an API that is
  **up and holding no meteo rows** does not: it logs a warning and returns an
  empty frame (`:2705-2707`), which empties `codes_to_use` and produces NaN or
  zero rows with exit 0. That is the expected state right after a
  `preprocessing_gateway` failure — i.e. the scenario this runbook exists for:

  ```bash
  grep -n "No meteo data" "$RUN"
  ```

  **Healthy:** no matches.
- **Only if `SAPPHIRE_API_ENABLED=false`,** the CSVs are the real input:

```bash
grep -n "ieasyforecast_intermediate_data_path\|ieasyhydroforecast_PATH_TO_QMAPPED_ERA5\|ieasyhydroforecast_HRU_CONTROL_MEMBER" "$ENVFILE"
# then, substituting those two values:
QM="<intermediate_data_path>/<PATH_TO_QMAPPED_ERA5>"
ls -la "$QM"/*_control_member.csv
for f in "$QM"/*_control_member.csv; do
  [ -e "$f" ] || { echo "no control member files found"; break; }
  echo "$(basename "$f"): $(tail -1 "$f" | cut -d, -f1-2)"
done
```

**Healthy:** a `<HRU>_P_control_member.csv` and `<HRU>_T_control_member.csv` per
HRU, mtime today, last date at or beyond today.

**Missing or stale:** `Quantile_Mapping_OP.py` did not complete — Step 7.

---

## Step 7 — Did preprocessing_gateway die on its first failure?

`run_preprocessing_gateway` runs three scripts and **breaks on the first failure**
(`run_preprocessing_gateway`, `run_locally.sh:681-682`):

```
Quantile_Mapping_OP.py  →  extend_era5_reanalysis.py  →  snow_data_operational.py
```

`Quantile_Mapping_OP.py` is first and produces the meteo ML needs. Before PR #459
a single transient TLS/connection reset in the ~50-member ECMWF ensemble download
aborted the whole run, taking the ERA5 extension and snow with it (PREPG-010).

```bash
grep -n "ERROR\|Traceback\|ConnectionError\|ChunkedEncodingError\|SSLError\|Max retries" \
  "$(ls -t apps/logs/run_locally_*.log | head -1)" | head -40
```

**A transport error on its own is not a failure.** Since PR #459 the retry helper
logs the exception class and retries, so a *recovered* healthy run also matches
this grep. Only treat it as PREPG-010 if the gateway actually **failed** — look
for `preprocessing_gateway failed` or an exhausted-retry traceback, not merely
`Transport fault … Retrying` followed by completion.

**If the gateway genuinely failed mid-download:** re-run it — the fault is
transient and a second attempt usually succeeds:

```bash
cd apps && ieasyhydroforecast_env_file_path="$ENVFILE" \
  bash run_locally.sh preprocessing_gateway 2>&1 | tee /tmp/prepg.log
```

> Still open: a **partial** ensemble is accepted as success (PREPG-016), so a
> gateway run reporting success may have downloaded fewer members than it should.

**Capture:** this grep output — **redacted**, this is the log most likely to carry
the API key.

---

## Step 8 — Model artefacts, and which script stopped the run

`run_machine_learning` (`run_locally.sh:717-743`) loops models × scripts and uses
`break 2` on the first failure. The scripts run in this order:

| Script | Role |
|---|---|
| `recalculate_nan_forecasts.py` | repair |
| **`make_forecast.py`** | **the operational writer** |
| `fill_ml_gaps.py` | repair |

So a `recalculate_nan_forecasts.py` failure for **TFT** stops the operational
writer for **all three** models. Find where it stopped — the ML log interleaves
seven scripts with no script name in its format, so use the run log:

```bash
grep -n "Running: machine_learning/" "$(ls -t apps/logs/run_locally_*.log | head -1)" | tail -20
```

The last entry is where it stopped — **valid only when the run actually aborted**
(check Step 2's banner and the module's FAIL line first). On a run that completed,
the last entry is simply the last script, and on `daily` the maintenance phase
adds further invocations after the operational ones.

Artefacts — each model needs a scaler directory containing a `.pt`, with `_Decad`
appended in DECAD mode (`make_forecast.py:557-570`):

```bash
grep -n "ieasyhydroforecast_models_and_scalers_path\|PATH_TO_SCALER_" "$ENVFILE"
ls -la "<models_and_scalers_path>"/
```

**Missing directory** → `FileNotFoundError: Directory … not found`.
**Directory with no `.pt`** → `IndexError` from the glob (`:570`).

### Check `ieasyhydroforecast_available_ML_models` — a known per-user drift

`get_predictor_class` (`make_forecast.py:454-474`) is called at the very top of
the forecast run (`:519`) and validates `MODEL_TO_USE` against this env variable.
It is checked **before** any model file is touched, so a mismatch aborts the first
model and `break 2` takes all three — indistinguishable from a total ML outage.

**This variable is known to differ between users of the same organisation.** Two
`kghm` env files in the shared config directory disagree today: one lists
`TFT,TIDE,TSMIXER`, the other `TFT,TIDE,TSMIXER,LSTM`. It is exactly the kind of
value that drifts when env files are maintained per person.

```bash
grep -n "ieasyhydroforecast_available_ML_models" "$ENVFILE"
```

**Healthy:** contains at least `TFT,TIDE,TSMIXER` — comma-separated, **no spaces**.

Four ways this bites, all producing "no ML forecasts":

| Your value | What happens |
|---|---|
| missing one of TFT/TIDE/TSMIXER | `ValueError: Model %s is not supported` (literal `%s`), `break 2`, no models run |
| `TFT, TIDE, TSMIXER` (spaces) | bare `split(",")` yields `" TIDE"`, which matches nothing — same crash |
| variable **unset** | `os.getenv` returns `None`, `.split` raises `AttributeError: 'NoneType' object has no attribute 'split'` — a confusing crash with an unrelated message |
| lists a model with no branch, e.g. `LSTM` | passes validation, then `predictor_class` is never assigned → `UnboundLocalError` if that model is ever selected |

The last row is latent for `run_locally.sh`, which only iterates
`ML_MODELS=(TFT TIDE TSMIXER)` (`:173`) — but it will fire for anyone selecting
the model by hand.

---

## Step 9 — Confirm against the data, not the exit code

`run_locally.sh` normalises a failed module's exit code to 1 and `print_summary`
can force 1, so per-module exit codes are effectively log-only. Read the log.

```bash
echo "checking: $SAPPHIRE_API_URL"
curl -s "$SAPPHIRE_API_URL/health/ready"
```

**Healthy:** a ready response — and confirm the URL echoed is your deployment,
not `localhost`. A failure here alone explains Step 4's "API is not ready,
skipping". (If `SAPPHIRE_API_URL` is empty, go back to the preamble — the
`.env` is never sourced by `run_locally.sh`.)

Then confirm today's rows exist — easiest via the dashboard (look for today's
traces). If you query the API directly, note the stored model names **differ from
the internal ones**: the API values are `TFT`, `TiDE`, `TSMixer`
(`utils_ml_forecast.py:540`). Querying `model_type=TIDE` returns empty on a
perfectly healthy system.

**Capture:** row counts only — never the response body.

---

## What to send back

1. Step 0 output.
2. Step 1 output (first 20 lines of the newest run log).
3. Step 2 grep output.
4. **Step 3's two commands** — the single most useful thing you can send.
5. Whichever later step you stopped at, **redacted**.

If every step looks healthy and there are still no forecasts, say so and include
Step 3 and Step 5 output — that combination is not a known failure mode.

---

## Related known issues

| ID | Relevance |
|---|---|
| ML-016 | Bare `run_locally.sh machine_learning` used to crash on empty `SAPPHIRE_PREDICTION_MODE` and ignore `ML_MODE` — fixed (implemented and confirmed): now resolves its own mode — Step 2a |
| ML-017 | A single missing ERA5 day cascades to NaN across all short-term ML — Step 6 |
| ML-021 | `make_forecast.py` exits 0 after writing no forecasts — the defect Steps 3–4 detect |
| PREPG-010 | Transient transport fault killed the whole gateway run; fixed on trunk 2026-08-20 13:06 CEST, too recent for any deployed image — Step 7 |
| PREPG-016 | A partial ensemble is accepted as success — Step 7 |
| PREPG-015 | Data Gateway API key reachable in logs — the redaction rules above |
| INFRA-029 | Root logger capped at WARNING; why two Step 4 causes are invisible |
| INFRA-030 | Skipped modules leave no summary line — Step 2 |
| INFRA-037 | A `sync_long_horizon_hydrograph.py` exit-4 (SDK norm lookup) failure in the Phase 2 maintenance sub-step used to abort the whole `daily` run before ML ran — fixed (implemented and confirmed: full apps test suite green — 16/16 modules and services, zero failures, no skips introduced by this branch; multiple rounds of out-of-loop review): exit 4 now continues but still exits non-zero and records a separate FAIL row; exits 1/3/5 remain fatal — why you are here, and see Step 5 |
