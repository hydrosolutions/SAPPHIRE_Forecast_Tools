# INFRA-013: Postprocessing API container crashes on bulk forecast writes

**Status**: Complete — Client-side workaround (batch_size=1) verified 2026-03-17. 892,381 ML gap-fill records written successfully. Server-side root cause documented for colleague.
**Module**: infra (sapphire/services/postprocessing)
**Priority**: Critical
**Labels**: `bug`, `api-integration`, `infrastructure`, `data-loss`

---

## Summary

The postprocessing API container (`sapphire-postprocessing-api`) crashes when
receiving bulk forecast POST requests. The uvicorn worker disconnects without
sending a response, causing the API gateway to return 500 to clients. The
container auto-restarts (118 restarts observed) but the write is lost.

This is the **actual server-side root cause** of ML-004's persistent gap-fill
failures. Even after Bugs A-D were fixed in the client code, the API write
still fails because the server crashes on bulk upserts.

## Evidence

### Pipeline log (2026-03-16)

Every ML gap-fill API write failed with 500 — all models, all horizons:

| Time | Script | Model | Horizon | Error |
|------|--------|-------|---------|-------|
| 11:32:52 | fill_ml_gaps.py | TFT | PENTAD | 500 Internal Server Error |
| 12:09:38 | fill_ml_gaps.py | TFT | DECAD | 500 Internal Server Error |
| 12:17:37 | recalculate_nan | TIDE | DECAD | 500 Internal Server Error |
| 12:28:38 | fill_ml_gaps.py | TIDE | DECAD | 500 Internal Server Error |
| 12:36:40 | recalculate_nan | TSMIXER | DECAD | 500 Internal Server Error |
| 12:42:55 | fill_ml_gaps.py | TSMIXER | DECAD | 500 Internal Server Error |

### API gateway log

```
Error proxying request to http://postprocessing-api:8003/forecast/:
  Server disconnected without sending a response.
Request completed in 18.73s with status: 500
```

### Container restarts

```bash
docker logs sapphire-postprocessing-api 2>&1 | grep -c "Application startup complete"
# Result: 118
```

Not OOM: `docker inspect` shows `OOMKilled=false`, memory limit = 0 (unlimited).

### Reproduction

A single POST of 100 valid forecast records to `/api/postprocessing/forecast/`
causes the server to disconnect after ~14-19 seconds. Single-record POSTs
succeed (201 Created). The postprocessing module's own writes (from
`postprocessing_maintenance.py` at 11:49) succeed because they use the
`sapphire-api-client` which goes through the gateway with smaller batches.

```bash
# Single record: works
curl -X POST http://localhost:8000/api/postprocessing/forecast/ \
  -H "Content-Type: application/json" \
  -d '{"data": [{"horizon_type": "day", "code": "15013", ...}]}'
# Returns 201

# 100 records: server crashes
curl -X POST http://localhost:8000/api/postprocessing/forecast/ \
  -H "Content-Type: application/json" \
  -d '{"data": [<100 records>]}'
# Returns 500 after ~18s — "Server disconnected without sending a response"
```

## Root Cause (confirmed)

In `sapphire/services/postprocessing/app/crud.py`, the `_bulk_upsert()` function
has two code paths for re-querying after INSERT...ON CONFLICT DO UPDATE:

- **Single-record path (lines 65-68)**: Builds a precise filter using ALL unique
  keys → returns exactly 1 row. Fast and safe.
- **Multi-record path (lines 70-84)**: Filters on ONLY the first unique key
  (`horizon_type`) using `IN-list` → loads ALL rows matching that horizon type.

For ML writes with `horizon_type = 'day'`, this loads **~8.5M rows** (TFT 2.9M +
TIDE 3.0M + TSMIXER 2.7M) into Python memory, crashing the uvicorn worker.

PP maintenance writes succeed because they write `horizon_type = 'PENTAD'` or
`'DECADE'`, which have ~166K and ~84K rows respectively — small enough to fit.

```python
# crud.py lines 70-76 — the problematic code path:
first_key = unique_keys[0]  # = "horizon_type"
first_key_values = list({r[first_key] for r in records})  # = ["day"]
candidates = (
    db.query(model).filter(getattr(model, first_key).in_(first_key_values)).all()
)
# ^^^ This loads ALL 8.5M DAY rows into memory → OOM → worker crash
```

**Fix**: The re-query should filter on ALL unique keys (or at minimum, use a
composite filter on horizon_type + code + model_type), not just the first key.
This is in `sapphire/services/` (colleague's domain).

## Relationship to ML-004

ML-004 documented Bugs A-D (client-side code issues) and Bug E (pagination).
This is effectively **Bug F** — the server crashes on bulk writes, making all
client-side fixes irrelevant until this is resolved.

```
ML gap-fill pipeline:
  1. fill_ml_gaps.py detects gaps → runs hindcast → produces filled data ✓
  2. _write_ml_forecast_to_api() sends 1000 records to API
  3. API gateway proxies to postprocessing-api:8003
  4. Postprocessing API worker crashes during bulk upsert ← THIS BUG
  5. Gateway returns 500 ("Server disconnected")
  6. Client falls back to CSV
  7. Next run reads from API → gaps still there → cycle repeats
```

## Impact

- All ML gap-fill data from 2024-2026 exists only in CSV, not in the DB
- Every pipeline run wastes ~30-60 minutes re-running hindcasts that already
  succeeded, only to have the API write fail again
- The postprocessing container has restarted 118 times, which could cause
  intermittent failures for other API consumers

---

## Investigation Plan

### Phase 1: Reproduce and diagnose (requires colleague coordination)

Since `sapphire/services/` is managed by the colleague, the investigation
must be coordinated. Provide the colleague with:

1. This issue file with reproduction steps
2. The API gateway log showing "Server disconnected without sending a response"
3. The container restart count (118)

Ask the colleague to:
- Check uvicorn worker configuration (timeout, workers, keepalive)
- Check if the bulk upsert endpoint has a request body size limit
- Add error logging in the POST handler's except block (currently swallows errors)
- Check if `ON CONFLICT DO UPDATE` with 1000 rows on a 5M-row table has
  known performance issues with the current PostgreSQL config

### Phase 2: Interim client-side mitigation

While waiting for the server fix, reduce batch size in the ML gap-fill write
path to avoid crashing the server:

**File**: `apps/machine_learning/scr/utils_ml_forecast.py`

Option A: Reduce `SapphirePostprocessingClient` batch size for ML writes:
```python
client = SapphirePostprocessingClient(base_url=api_url, batch_size=50)
```

Option B: Add retry with exponential backoff per batch in `_write_ml_forecast_to_api`.

### Phase 3: Verify fix

After either the server or client fix, run:
```bash
ieasyhydroforecast_env_file_path=~/Documents/GitHub/kyg_data_forecast_tools/config/.env_develop_kghm \
  bash run_locally.sh --continue-on-error daily
```

Then verify:
```sql
-- Gap-fill data should now be in the DB
SELECT model_type, horizon_type, COUNT(*) as total, MAX(date) as max_date
FROM forecasts
WHERE flag IN (3, 4)  -- hindcast/backfill flags
GROUP BY model_type, horizon_type
ORDER BY model_type, horizon_type;
```

---

## Dependency Graph

```json
{
  "phases": {
    "1": {
      "title": "Reproduce and diagnose with colleague",
      "owner": "colleague (sapphire/services/)",
      "depends_on": [],
      "files": ["sapphire/services/postprocessing/app/"]
    },
    "2": {
      "title": "Interim client-side batch size reduction",
      "owner": "us (apps/)",
      "depends_on": [],
      "files": ["apps/machine_learning/scr/utils_ml_forecast.py"],
      "parallel_with": ["1"]
    },
    "3": {
      "title": "Verify fix with full pipeline run",
      "depends_on": ["1 OR 2"],
      "files": []
    }
  },
  "execution_groups": [
    {
      "group": 1,
      "parallel": true,
      "agents": [
        {
          "id": "agent_batch_size",
          "phases": ["2"],
          "reason": "Can be done immediately without colleague"
        }
      ]
    },
    {
      "group": 2,
      "parallel": false,
      "agents": [
        {
          "id": "agent_verify",
          "phases": ["3"],
          "reason": "Needs server or client fix to be in place"
        }
      ]
    }
  ]
}
```
