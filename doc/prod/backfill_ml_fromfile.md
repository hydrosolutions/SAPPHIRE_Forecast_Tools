# ML Forecast From-File Backfill

Draft procedure for backfilling short-term ML forecasts and long-term forecast hindcasts from CSV files already present on a deployment server, without recomputing hindcasts.

Use Kyrgyz first, then repeat the same procedure on Tajik by changing only the env file, data root, image tag, and station/model scope.

## 1. Scope And Current Decision

Use the existing migration wrappers as the backbone:

- Short-term ML: generate an export-format CSV plus `.manifest` from the raw operational CSV files, then run `bin/initialize_ml_forecast_history.sh --from-export`.
- Long-term: run `bin/initialize_long_forecast_history.sh` directly, one configured mode at a time, for month, quarter, and season modes.

Do not pass raw short-term ML files directly to `initialize_ml_forecast_history.sh`; they are not in the required schema.

Current blocker for full long-term coverage: the existing long-term importer accepts only `horizon_type=month` configs, its per-mode cutoff query is hard-coded to `MONTH`, and it does not map generic GBT-family ensemble columns named `Q_xgb`, `Q_lgbm`, and `Q_catboost`. Do not run long-term writes until the importer and wrapper have been updated for all configured horizon types and generic ensemble columns. The rejected month-only fallback is superseded by `doc/plans/working/ml_fromfile_backfill_plan.md`.

## 2. Prerequisites

- Deployment stack is healthy and the postprocessing API is reachable.
- `maxat_sapphire_2` checkout is present on the server.
- Docker is running.
- `mabesa/sapphire-prepgateway:<tag>` is available or pullable.
- The deployment env file is known:

```bash
export ENV_FILE=<deployment-env-file>
export IMAGE=mabesa/sapphire-prepgateway:<pinned-tag>
```

Use a pinned image tag, not `latest`, for the real run.

Set common shell state:

```bash
cd "$REPO"
set -euo pipefail
umask 077

source bin/utils/common_functions.sh
read_configuration "$ENV_FILE"

export IMPORT_DIR="${ieasyhydroforecast_data_root_dir}/imports/ml_fromfile"
mkdir -p "$IMPORT_DIR"

# Set ML_CANARY_STATION to your deployment canary station at runtime; never commit a real code.
export ML_CANARY_STATION=<your-canary-station-code>
```

## 3. Read-Only Source Inventory

Short-term ML raw files:

```bash
python3 - <<'PY'
import csv, os
from pathlib import Path

base = Path(os.environ["ieasyhydroforecast_data_ref_dir"]) / "intermediate_data" / os.environ.get("ieasyhydroforecast_OUTPUT_PATH_DISCHARGE", "predictions")
models = {"TFT", "TIDE", "TSMIXER"}

for p in sorted(base.glob("*/*_forecast.csv")):
    if p.parent.name not in models:
        continue
    with p.open(newline="") as f:
        r = csv.DictReader(f)
        rows = 0
        codes = set()
        spans = {}
        for row in r:
            rows += 1
            code = (row.get("code") or "").strip()
            if code:
                codes.add(code)
            for col in ("forecast_date", "date"):
                v = (row.get(col) or "")[:10]
                if v:
                    spans.setdefault(col, [v, v])
                    spans[col][0] = min(spans[col][0], v)
                    spans[col][1] = max(spans[col][1], v)
        print(f"{p}:")
        print(f"  header={r.fieldnames}")
        print(f"  rows={rows} stations={len(codes)} spans={spans}")
PY
```

Expected short-term raw schema:

```text
Q5,Q25,Q50,Q75,Q95,date,code,forecast_date,flag
```

or the wider pentad variant with `Q10`..`Q90`. In these raw files, `forecast_date` is the issue date and `date` is the target date.

Long-term source inventory:

```bash
python3 - <<'PY'
import csv, json, os
from pathlib import Path

root = Path(os.environ["ieasyhydroforecast_data_ref_dir"])
config = root / "config" / "long_term_configs"
data = root / "intermediate_data" / "long_term_predictions"

print("Configured modes:")
for cfg in sorted(config.glob("*.json")):
    raw = json.load(open(cfg))
    print(cfg.stem, "horizon_type=", raw.get("horizon_type", "month"), "lead=", raw.get("operational_month_lead_time"))

print("Hindcast CSVs:")
for p in sorted(data.glob("*/*/*_hindcast.csv")):
    with p.open(newline="") as f:
        r = csv.DictReader(f)
        rows = sum(1 for _ in r)
    print(p.relative_to(data), "rows=", rows, "header=", r.fieldnames)
PY
```

Stop if a required configured mode has no CSV, or if the importer has not been updated to accept every configured `horizon_type` required for this deployment.

## 4. Read-Only Target And Model Pre-Flights

Run these before backup and before any write on each deployment. They are required on both Kyrgyz and Tajik; do not reuse results across servers.

Probe the running postprocessing API's OpenAPI enum. This mirrors the PREPQ-008 OpenAPI-enum probe, but targets postprocessing on port 8003:

```bash
docker exec -i sapphire-postprocessing-api python3 - <<'PY'
import json
import sys
import urllib.request

required = {"month", "quarter", "season"}
schemas = json.load(
    urllib.request.urlopen("http://localhost:8003/openapi.json")
)["components"]["schemas"]

matches = []
for name, schema in schemas.items():
    enum = schema.get("enum")
    if enum and {"month", "pentad"}.issubset(set(enum)):
        matches.append((name, set(enum)))

if not matches:
    raise SystemExit("no HorizonType-like enum found in postprocessing OpenAPI")

ok = False
for name, enum in matches:
    print(name, "=", sorted(enum), "required_present=", required.issubset(enum))
    ok = ok or required.issubset(enum)

if not ok:
    raise SystemExit("postprocessing OpenAPI does not advertise month+quarter+season")
PY
```

Confirm the deployed postprocessing DB enum and ensemble columns:

```bash
docker exec -i sapphire-postprocessing-db \
  psql -U postgres -d postprocessing_db -P pager=off <<'SQL'
SELECT enumlabel
FROM pg_enum
WHERE enumtypid='horizontype'::regtype
ORDER BY enumsortorder;

SELECT column_name
FROM information_schema.columns
WHERE table_name = 'long_forecasts'
  AND column_name IN ('q_xgb', 'q_lgbm', 'q_catboost', 'q_loc')
ORDER BY column_name;
SQL
```

Expected: enum labels include `MONTH`, `QUARTER`, and `SEASON`; columns include all of `q_xgb`, `q_lgbm`, `q_catboost`, and `q_loc`. Stop if any are missing.

Cross-check configured long-term model names before any long-term write. This mirrors the current operational model-name map and fails closed on names such as `GBT_Base`, which are not valid `ModelType` values in the postprocessing API.

```bash
python3 - <<'PY'
import json
import os
from pathlib import Path

config = Path(os.environ["ieasyhydroforecast_data_ref_dir"]) / "config" / "long_term_configs"

model_map = {
    "LR_Base": "LR_Base",
    "LR_SM": "LR_SM",
    "LR_SM_DT": "LR_SM_DT",
    "LR_SM_ROF": "LR_SM_ROF",
    "SM_GBT": "SM_GBT",
    "SM_GBT_LR": "SM_GBT_LR",
    "SM_GBT_Norm": "SM_GBT_Norm",
    "MC_ALD": "MC_ALD",
    "GBT": "GBT",
}
valid_model_types = {
    "TSMixer", "TiDE", "TFT", "EM", "NE", "RRAM", "LR", "GBT",
    "LR_Base", "LR_SM", "LR_SM_DT", "LR_SM_ROF", "MC_ALD",
    "SM_GBT", "SM_GBT_LR", "SM_GBT_Norm", "Skilled Mean", "Naive Mean",
}

bad = []
for cfg in sorted(config.glob("*.json")):
    if cfg.stem == "monthly":
        continue
    raw = json.load(open(cfg))
    models_to_use = raw.get("models_to_use", {})
    for family, models in models_to_use.items():
        for model in models:
            mapped = model_map.get(model)
            if mapped is None or mapped not in valid_model_types:
                bad.append((cfg.name, family, model, mapped))

if bad:
    for cfg_name, family, model, mapped in bad:
        print(
            f"INVALID_MODEL config={cfg_name} family={family} "
            f"model={model} mapped={mapped}"
        )
    raise SystemExit("unmappable or non-enum long-term model names found")

print("long-term model-name pre-flight OK")
PY
```

Dry-runs do not prove deployed `model_type` validity. The long-term station canary write below is the true gate for API acceptance.

## 5. Backup And Quiet Window

Take a database backup before any write and pause scheduled jobs using the deployment team's standard backup/cron procedure. Do not start a multi-hour write near the daily backup or maintenance windows.

## 6. Short-Term ML Export-Format Generation

Generate one same-host export-format CSV from the raw operational files. This does not write to the database.

```bash
export ML_EXPORT="${IMPORT_DIR}/ml_forecast_from_raw_$(date -u +%Y%m%dT%H%M%SZ).csv"
export ML_RAW_DIR="${ieasyhydroforecast_data_ref_dir}/intermediate_data/${ieasyhydroforecast_OUTPUT_PATH_DISCHARGE:-predictions}"

PYTHONPATH=bin/utils python3 -m migration_py.ml_raw_to_export \
  --data-ref "$ML_RAW_DIR" \
  --out "$ML_EXPORT" \
  --dry-run

PYTHONPATH=bin/utils python3 -m migration_py.ml_raw_to_export \
  --data-ref "$ML_RAW_DIR" \
  --out "$ML_EXPORT"
```

Inspect the generated header:

```bash
head -2 "$ML_EXPORT"
cat "${ML_EXPORT}.manifest"
```

## 7. Short-Term ML Dry-Run And Canary

Dry-run all rows:

```bash
bash bin/initialize_ml_forecast_history.sh "$ENV_FILE" \
  --image "$IMAGE" \
  --from-export "$ML_EXPORT" \
  --dry-run
```

Canary dry-run:

```bash
bash bin/initialize_ml_forecast_history.sh "$ENV_FILE" \
  --image "$IMAGE" \
  --from-export "$ML_EXPORT" \
  --station-filter "$ML_CANARY_STATION" \
  --dry-run
```

Use your deployment canary station code (set ML_CANARY_STATION at runtime; never commit a real code).

Canary write:

```bash
bash bin/initialize_ml_forecast_history.sh "$ENV_FILE" \
  --image "$IMAGE" \
  --from-export "$ML_EXPORT" \
  --station-filter "$ML_CANARY_STATION"
```

Verify canary:

```bash
docker exec -i sapphire-postprocessing-db \
  psql -U postgres -d postprocessing_db -P pager=off <<'SQL'
SELECT horizon_type, model_type, code,
       COUNT(*) AS rows,
       MIN(date) AS first_issue_date,
       MAX(date) AS last_issue_date,
       MIN(target) AS first_target,
       MAX(target) AS last_target
FROM forecasts
WHERE model_type::text IN ('TFT','TIDE','TSMIXER')
GROUP BY horizon_type, model_type, code
ORDER BY model_type, code, horizon_type;
SQL
```

Use `GROUP BY` on enum columns and compare text values through result rows; do not write mixed-case enum literals in SQL predicates.

## 8. Short-Term ML Full Run

After the dry-run and canary are accepted:

```bash
bash bin/initialize_ml_forecast_history.sh "$ENV_FILE" \
  --image "$IMAGE" \
  --from-export "$ML_EXPORT"
```

Verification:

```bash
docker exec -i sapphire-postprocessing-db \
  psql -U postgres -d postprocessing_db -P pager=off <<'SQL'
SELECT horizon_type, model_type,
       COUNT(*) AS rows,
       COUNT(DISTINCT code) AS stations,
       MIN(date) AS first_issue_date,
       MAX(date) AS last_issue_date,
       MIN(target) AS first_target,
       MAX(target) AS last_target
FROM forecasts
WHERE model_type::text IN ('TFT','TIDE','TSMIXER')
GROUP BY horizon_type, model_type
ORDER BY model_type, horizon_type;
SQL
```

Expected default result: `horizon_type` is `DAY` in the DB enum display, and model labels display as `TFT`, `TIDE`, `TSMIXER`.

## 9. Long-Term All-Horizon Dry-Runs

Run this section only after the long-term importer accepts `horizon_type=month`, `horizon_type=quarter`, and `horizon_type=season`, maps generic `Q_xgb` / `Q_lgbm` / `Q_catboost` columns, and the wrapper's per-mode cutoff query derives the SQL enum label from each mode config.

Generate the configured operational mode list from the deployment config. The `monthly` mode is excluded because the wrapper always treats it as non-operational.

```bash
python3 - <<'PY' > "$IMPORT_DIR/long_modes.txt"
import json, os
from pathlib import Path

config = Path(os.environ["ieasyhydroforecast_data_ref_dir"]) / "config" / "long_term_configs"
for cfg in sorted(config.glob("*.json")):
    if cfg.stem == "monthly":
        continue
    raw = json.load(open(cfg))
    horizon_type = str(raw.get("horizon_type", "month")).lower()
    if horizon_type not in {"month", "quarter", "season"}:
        raise SystemExit(f"{cfg.name}: unsupported horizon_type={horizon_type!r}")
    print(cfg.stem)
PY

cat "$IMPORT_DIR/long_modes.txt"
```

Run per mode so target cutoffs are scoped correctly:

```bash
while read -r MODE; do
  bash bin/initialize_long_forecast_history.sh "$ENV_FILE" \
    --image "$IMAGE" \
    --mode "$MODE" \
    --dry-run
done < "$IMPORT_DIR/long_modes.txt"
```

Canary one mode/model/station dry-run:

```bash
CANARY_MODE="$(head -n 1 "$IMPORT_DIR/long_modes.txt")"

bash bin/initialize_long_forecast_history.sh "$ENV_FILE" \
  --image "$IMAGE" \
  --mode "$CANARY_MODE" \
  --model LR_Base \
  --station-filter "$ML_CANARY_STATION" \
  --dry-run
```

Use your deployment canary station code (set ML_CANARY_STATION at runtime; never commit a real code).

Canary write. This is the gate that proves the deployed API accepts the mapped `model_type` and `horizon_type`; a dry-run alone cannot prove that.

```bash
bash bin/initialize_long_forecast_history.sh "$ENV_FILE" \
  --image "$IMAGE" \
  --mode "$CANARY_MODE" \
  --model LR_Base \
  --station-filter "$ML_CANARY_STATION"
```

## 10. Long-Term All-Horizon Writes

After dry-runs and the canary write are accepted:

```bash
while read -r MODE; do
  bash bin/initialize_long_forecast_history.sh "$ENV_FILE" \
    --image "$IMAGE" \
    --mode "$MODE"
done < "$IMPORT_DIR/long_modes.txt"
```

Verification:

```bash
docker exec -i sapphire-postprocessing-db \
  psql -U postgres -d postprocessing_db -P pager=off <<'SQL'
SELECT horizon_type, horizon_value, model_type,
       COUNT(*) AS rows,
       COUNT(DISTINCT code) AS stations,
       COUNT(q) AS q_rows,
       COUNT(q_xgb) AS q_xgb_rows,
       COUNT(q_lgbm) AS q_lgbm_rows,
       COUNT(q_catboost) AS q_catboost_rows,
       COUNT(q_loc) AS q_loc_rows,
       COUNT(q05) AS q05_rows,
       COUNT(q25) AS q25_rows,
       COUNT(q50) AS q50_rows,
       COUNT(q75) AS q75_rows,
       COUNT(q95) AS q95_rows,
       MIN(date) AS first_issue_date,
       MAX(date) AS last_issue_date,
       MIN(valid_from) AS first_valid_from,
       MAX(valid_to) AS last_valid_to
FROM long_forecasts
GROUP BY horizon_type, horizon_value, model_type
ORDER BY horizon_type, horizon_value, model_type;
SQL
```

If GBT-family `q_xgb_rows`, `q_lgbm_rows`, or `q_catboost_rows` are zero while the source files contain `Q_xgb`, `Q_lgbm`, and `Q_catboost`, stop and treat it as an importer mapping regression.

## 11. Stop Conditions

Stop before any full write if:

- Short-term generated manifest row count does not match the generated CSV.
- Short-term dry-run reports unexpected `SKIPPED_UNKNOWN_MODEL`.
- Short-term dry-run date spans do not match the source inventory.
- The postprocessing OpenAPI enum probe does not show `month`, `quarter`, and `season`.
- The postprocessing DB enum does not include `MONTH`, `QUARTER`, and `SEASON`.
- The deployed `long_forecasts` table lacks `q_xgb`, `q_lgbm`, `q_catboost`, or `q_loc`.
- The configured long-term model-name pre-flight reports an unmappable or non-enum name such as `GBT_Base`.
- Long-term dry-run reports missing configured hindcast files.
- Long-term importer still rejects any configured `horizon_type` required for the deployment.
- Long-term wrapper target-state SQL is still hard-coded to `horizon_type::text='MONTH'` for non-month modes.
- Long-term canary write fails with a model-type or horizon-type 422.
- GBT-family source files contain generic ensemble columns but verification shows zero imported ensemble rows.
- The resolved image is unpinned or unexpected.

## 12. Repeat On Tajik

Repeat with the Tajik env file:

```bash
export ENV_FILE=<tajik-deployment-env-file>
export IMAGE=mabesa/sapphire-prepgateway:<pinned-tag>
```

Then rerun sections 2 through 11 unchanged.

Before the Tajik write, re-check the source-branch caveat:

- Short-term raw files may have been produced by a different branch than the import wrappers. Always run the source inventory and export-format generation checks.
- Long-term files should be generated from the same branch as the importer, but still verify config horizon types and GBT-family column names.
- Target capability and model-name pre-flights must be repeated on Tajik. Do not reuse Kyrgyz results.

Parameterize in operator notes:

- `ENV_FILE`
- `IMAGE`
- Deployment data root derived by `read_configuration`
- Station canary code
- Configured ML models
- Long-term modes included in this run

> **Before the Tajik run, read Section 13** — the kyg run hit several issues that are
> deployment-agnostic and will recur unless anticipated.

---

## 13. Troubleshooting & Learnings (kyg run, 2026-06)

The kyg backfill did **not** go smoothly. These are the issues hit, as symptom → cause → fix,
for fast lookup during the Tajik run. (More may be added as we learn.) All examples use the
sentinel code `19999`; never paste real station codes into committed files.

### 13.1 Shell / config load
- **`<token>: unbound variable`, then the SSH session closes**, during `read_configuration`.
  Cause: a value in the `.env` (typically a password) contains a literal `$…`, and
  `read_configuration` does `set -a; source "$env_file"` (`bin/utils/common_functions.sh:69-71`),
  which **expands** it. Under your `set -euo pipefail`, that's an unbound-variable error and
  `set -e` exits your **login shell** → logout.
  - Fix: **single-quote** any secret containing `$` in the `.env` (`KEY='…$x…'`). **Do not run
    `set -euo pipefail` in the interactive login shell** — run backfill blocks as a script
    (`bash steps.sh`). Note: even without `set -u`, the `source` silently truncates the secret at
    `$` — so quoting matters regardless.
- **Multi-line / heredoc paste mangles in the terminal** → use **single-line** commands
  (`psql -c "…"`, not `<<'SQL'`).

### 13.2 Short-term ML reshape (`ml_raw_to_export.py`)
- **`ERROR: unknown ML model directory: ARIMA`** — the reshape **hard-fails** on a
  deprecated/unknown model dir (the `skipped_unknown_model` counter exists but isn't wired to
  skip). Workaround: move the dir aside
  (`mv …/predictions/ARIMA …/predictions/ARIMA.excluded`). Proper fix: skip-with-warning.
- **It writes `horizon=day` rows only** → that feeds the DAY archive, **NOT** the dashboard's
  pentad/decade ML (see 13.4). The short-term-ML-from-file path is largely a detour for the
  operational dashboard.

### 13.3 `initialize_ml_forecast_history.sh` pre-cutoff trap
- **Import exits 0 with `SKIPPED_CUTOFF=<all>` / "No records to POST"** — the wrapper's MODE
  detection counts **all** ML horizons (`:289`, no horizon filter). With a DAY archive (esp.
  TIDE) back to 2009, it derives `cutoff=2009-…` and `pre-cutoff` skips every source row. There
  is **no `--full-import` override** — it can only backfill *older-than-existing*, not gap-fill a
  populated target.

### 13.4 The dashboard's pentad/decade ML comes from the COMBINED forecasts
- Migrate them with the postprocessing **service** migrator (not the toolkit), inside the container:
  ```
  docker exec -it sapphire-postprocessing-api python app/data_migrator.py --type combinedforecast
  ```
  Reads `combined_forecasts_pentad.csv` / `combined_forecasts_decad.csv` from `settings.csv_folder`
  → `/forecast/` (all models, incl. ML + EM/NE). **No `--dry-run`** → back up first; upserts are
  idempotent; verify after. This — not the day-ML toolkit path — is what puts ML in the dashboard.

### 13.5 Combined-migration `500` (duplicate key)
- **`500: Failed to create or update forecasts in bulk` / `psycopg2.errors.UniqueViolation …
  uq_forecasts_horizon_code_model_date_target`** — the combined CSV has **duplicate
  `(horizon,code,model,date,target)` rows**; the API's bulk write isn't dedup-safe → the **whole
  batch rolls back** → 0 written (this is why kyg decade stayed empty).
  - See the traceback: `docker logs sapphire-postprocessing-api 2>&1 | grep -vi Fetched | grep -iE "UniqueViolation|500: Failed" | tail`.
  - Fix now: dedupe the CSV (exact dup rows: `awk 'NR==1||!s[$0]++'`; same-key/different-values:
    last-wins on the key columns). Proper fix: API `ON CONFLICT`/dedup-within-batch
    (colleague-managed, INFRA-013 class).
- **Permissions:** combined CSVs are **root-owned** (migrator runs as root in-container) → your
  shell can't read/edit them. Operate **inside the container** (`docker exec … sh -lc '…'`) or `sudo`.

### 13.6 Verification queries (what actually surfaced the problems)
- ML coverage by horizon/model (single line):
  ```
  docker exec sapphire-postprocessing-db psql -U postgres -d postprocessing_db -tAc "SELECT horizon_type::text,model_type::text,count(*),min(date),max(date) FROM forecasts WHERE model_type::text IN ('TFT','TIDE','TSMIXER','EM','NE') GROUP BY 1,2 ORDER BY 1,2"
  ```
- API errors during a migration: `docker logs sapphire-postprocessing-api 2>&1 | grep -vi Fetched | tail`.

### 13.7 Model-name enum gaps
- A model in a CSV that isn't a valid postprocessing `ModelType` → 422/500 on write. Known:
  **`GBT_Base`** (long-term, tracked as **LTF-006**). Run the §4 model-name pre-flight before
  long-term writes; exclude/skip unmappable names.

### Open hardening items (deployment-agnostic, not Tajik-blocking)
- API bulk forecast write should be dedup-safe (INFRA-013 class).
- `ml_raw_to_export.py` should skip unknown/deprecated models, not raise.
- `initialize_ml_forecast_history.sh` needs a `--full-import` override + horizon-scoped MODE detection.
- `GBT_Base` `ModelType` resolution (LTF-006).
- (Operational, separate from backfill) `MLMaintenance` 900s timeout not honoring `config/timeout_config` — under investigation.
