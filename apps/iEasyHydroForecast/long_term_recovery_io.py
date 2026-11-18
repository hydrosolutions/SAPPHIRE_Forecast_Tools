"""Strict, API-only I/O shared by the two latest-LT recovery entry points."""

import datetime as dt
import hashlib
import math
import os

KEY_FIELDS = (
    "horizon_type",
    "horizon_value",
    "code",
    "date",
    "model_type",
    "valid_from",
    "valid_to",
)
FORECAST_FIELDS = (
    "q",
    "q_xgb",
    "q_lgbm",
    "q_catboost",
    "q_loc",
    "q05",
    "q10",
    "q25",
    "q50",
    "q75",
    "q90",
    "q95",
)
VALUE_FIELDS = (*FORECAST_FIELDS, "q_obs", "composition", "flag")


def destination_hash():
    return hashlib.sha256(
        os.getenv("SAPPHIRE_API_URL", "http://localhost:8000").rstrip("/").encode()
    ).hexdigest()


def iso_date(value):
    if isinstance(value, dt.datetime):
        if value.time() != dt.time():
            raise ValueError("Non-midnight forecast identity")
        value = value.date()
    if isinstance(value, dt.date):
        return value.isoformat()
    return dt.date.fromisoformat(str(value)).isoformat()


def identity(record):
    key = {field: record[field] for field in KEY_FIELDS}
    if key["horizon_type"] not in {"month", "quarter", "season"}:
        raise ValueError("Unsupported recovery horizon")
    lead = key["horizon_value"]
    if isinstance(lead, bool) or int(lead) != lead or lead < 0:
        raise ValueError("Invalid recovery lead")
    key["horizon_value"] = int(lead)
    if not isinstance(key["code"], str) or not key["code"]:
        raise ValueError("Invalid recovery station identity")
    if not isinstance(key["model_type"], str) or not key["model_type"]:
        raise ValueError("Invalid recovery model identity")
    key["model_type"] = {
        "LR_BASE": "LR_Base",
        "SM_GBT_NORM": "SM_GBT_Norm",
        "NAIVE_MEAN": "Naive Mean",
        "SKILLED_MEAN": "Skilled Mean",
    }.get(key["model_type"].upper().replace(" ", "_"), key["model_type"])
    for field in ("date", "valid_from", "valid_to"):
        key[field] = iso_date(key[field])
    if key["valid_from"] > key["valid_to"]:
        raise ValueError("Reversed validity interval")
    return key


def key_tuple(record):
    key = identity(record)
    return tuple(key[field] for field in KEY_FIELDS)


def number(value):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("Malformed recovery numeric value")
    if math.isnan(value):
        return None  # SDK DataFrames represent JSON nulls as NaN.
    if not math.isfinite(value):
        raise ValueError("Nonfinite recovery numeric value")
    return float(value)


def healthy(record):
    if record is None:
        return False
    flag = record.get("flag")
    if isinstance(flag, bool) or flag not in (0, 1, 2):
        raise ValueError("Unknown recovery flag")
    values = [number(record.get(field)) for field in FORECAST_FIELDS]
    return any(value is not None for value in values)


def eligible(record):
    return record is None or (not healthy(record) and record.get("flag") == 2)


def read_exact(client, expected):
    """Read all pages, refusing unexpected or duplicate identities."""
    expected = identity(expected)
    rows = []
    for offset in range(0, 10000, 100):
        frame = client.read_long_term_forecasts(
            horizon_type=expected["horizon_type"],
            horizon_value=expected["horizon_value"],
            code=expected["code"],
            model=expected["model_type"],
            start_date=expected["date"],
            end_date=expected["date"],
            valid_from=expected["valid_from"],
            valid_to=expected["valid_to"],
            skip=offset,
            limit=100,
        )
        if frame is None:
            raise ValueError("Missing recovery API response")
        page = frame.to_dict("records") if hasattr(frame, "to_dict") else frame
        if not isinstance(page, list):
            raise ValueError("Malformed recovery API response")
        for row in page:
            if identity(row) != expected:
                raise ValueError("Recovery API identity mismatch")
            healthy(row)  # Validate flags and all numeric fields, even protected rows.
            rows.append(row)
        if len(page) < 100:
            break
    else:
        raise ValueError("Recovery pagination limit exceeded")
    if len(rows) > 1:
        raise ValueError("Duplicate recovery API identity")
    return rows[0] if rows else None


def equivalent(actual, expected):
    if actual is None or identity(actual) != identity(expected):
        return False
    for field in VALUE_FIELDS:
        a, b = actual.get(field), expected.get(field)
        if field in (*FORECAST_FIELDS, "q_obs"):
            a, b = number(a), number(b)
            if a is not None and b is not None:
                if not math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-9):
                    return False
            elif a != b:
                return False
        elif a != b:
            return False
    return True


def preserve_metadata(record, existing):
    result = dict(record)
    if existing:
        for field in ("q_obs", "composition"):
            if field not in result:
                value = existing.get(field)
                result[field] = number(value) if field == "q_obs" else value
    return result


def write_verified(client, record):
    """A positive write count is never evidence of complete delivery."""
    client.write_long_forecasts([record])
    if not equivalent(read_exact(client, record), record):
        raise ValueError("Recovery write read-back mismatch")


def in_window(issue, reference):
    issue, reference = (
        dt.date.fromisoformat(iso_date(issue)),
        dt.date.fromisoformat(iso_date(reference)),
    )
    previous = reference.replace(day=1) - dt.timedelta(days=1)
    return issue <= reference and (issue.year, issue.month) in {
        (reference.year, reference.month),
        (previous.year, previous.month),
    }
