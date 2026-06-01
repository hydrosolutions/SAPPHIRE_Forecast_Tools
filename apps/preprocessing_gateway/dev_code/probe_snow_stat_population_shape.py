import os
import sys

_probe_code = os.environ.get("PROBE_CODE", "")
_allow_real = os.environ.get("ALLOW_REAL_CODE")
if not (_probe_code.startswith("199") or _allow_real):
    sys.stderr.write(
        "Refusing to run with a real station code. "
        "Set PROBE_CODE to a placeholder starting with '199' "
        "(e.g. PROBE_CODE=19999), or set ALLOW_REAL_CODE=1 to opt in "
        "explicitly when probing real operational data.\n"
    )
    sys.exit(2)

"""Phase 0 spike for snow-stat population. Read-only. Reads `PROBE_CODE` and `SNOW_PROBE_API_BASE` from env. See `doc/plans/working/snow_stat_population_decisions.md` for the companion artifact."""

import pandas as pd
import requests


def _redact(code: str) -> str:
    """Mask all but the first character of a station code in printed output."""
    if not code:
        return "<empty>"
    if len(code) <= 1:
        return "<redacted>"
    return code[0] + "*" * (len(code) - 1)


STAT_KEYS = [
    "mean",
    "min",
    "max",
    "q05",
    "q25",
    "q50",
    "q75",
    "q95",
    "previous",
    "current",
    "count",
    "std",
]
SNOW_TYPES = ["HS", "ROF", "SWE"]


def _fetch_rows(api_base: str, snow_type: str, code: str) -> list[dict]:
    response = requests.get(
        f"{api_base}/preprocessing/snow/",
        params={
            "snow_type": snow_type,
            "code": code,
            "start_date": "2024-01-01",
            "end_date": "2025-12-31",
            "limit": "10000",
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("results", "data", "items"):
            rows = payload.get(key)
            if isinstance(rows, list):
                return rows
    return []


def _columns_for_long_shape(rows: list[dict]) -> list[str]:
    frame = pd.DataFrame(rows)
    if "date" in frame.columns:
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame["dayofyear"] = frame["date"].dt.dayofyear
    else:
        frame["dayofyear"] = pd.Series(dtype="Int64")
    return list(frame.columns)


def main() -> int:
    api_base = os.environ.get("SNOW_PROBE_API_BASE", "http://localhost:8000/api").rstrip("/")
    code = _probe_code
    rows_by_type: dict[str, list[dict]] = {}
    present_by_type: dict[str, set[str]] = {}

    for snow_type in SNOW_TYPES:
        try:
            rows = _fetch_rows(api_base, snow_type, code)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            print(f"{snow_type}: HTTP error {status} for code {_redact(code)}")
            rows_by_type[snow_type] = []
            continue
        except requests.RequestException as exc:
            print(f"{snow_type}: request error for code {_redact(code)}: {exc.__class__.__name__}")
            rows_by_type[snow_type] = []
            continue

        rows_by_type[snow_type] = rows
        print(f"{snow_type}: row count={len(rows)} for code {_redact(code)}")
        if rows:
            present = [key for key in STAT_KEYS if key in rows[0]]
            present_by_type[snow_type] = set(present)
            print(f"{snow_type}: stat keys present in first row={present}")
        else:
            present_by_type[snow_type] = set()
            print(f"{snow_type}: stat keys present in first row=[]")

    columns = _columns_for_long_shape(rows_by_type.get("HS", []))
    print(f"HS long-format columns={columns}")

    present_across_first_rows = [
        key
        for key in STAT_KEYS
        if all(key in present_by_type[snow_type] for snow_type in SNOW_TYPES)
    ]
    print(
        "Spike OK — stat keys present: "
        f"{len(present_across_first_rows)}/{len(STAT_KEYS)} across HS/ROF/SWE first rows; "
        f"long-format shape: columns={columns}"
    )
    return 0


if __name__ == "__main__":
    # Example: PROBE_CODE=19999 python apps/preprocessing_gateway/dev_code/probe_snow_stat_population_shape.py
    raise SystemExit(main())
