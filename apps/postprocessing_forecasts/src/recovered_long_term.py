"""Exact-scope LT rebuilds from a fresh producer handoff, without LT model imports."""

import datetime as dt
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / "iEasyHydroForecast"))

import pandas as pd
from long_term_recovery_io import (
    FORECAST_FIELDS,
    destination_hash,
    eligible,
    equivalent,
    healthy,
    identity,
    in_window,
    iso_date,
    key_tuple,
    number,
    preserve_metadata,
    read_exact,
    write_verified,
)
from src import data_reader, ensemble_calculator
from src.aggregation import derive_quarterly_from_monthly_same_issue, get_season_year
from src.model_names import (
    QUARTER_NATIVE_RAW_MODELS,
    QUARTERLY_DERIVED_MODELS,
    canonical_model_short,
)

ENSEMBLES = {"Naive Mean": "NAIVE_MEAN", "Skilled Mean": "SKILLED_MEAN"}


def _skills(client, scope):
    pages = []
    for skip in range(0, 100000, 1000):
        page = client.read_skill_metrics(
            horizon=scope["horizon_type"], code=scope["code"], skip=skip, limit=1000
        )
        if page is None:
            raise ValueError("Missing skill API response")
        if not page.empty:
            if not (page["code"].astype(str) == scope["code"]).all():
                raise ValueError("Unexpected skill station")
            pages.append(page)
        if len(page) < 1000:
            break
    else:
        raise ValueError("Skill pagination limit exceeded")
    if not pages:
        raise ValueError("No stored skills for recovered scope")
    frame = pd.concat(pages, ignore_index=True)
    if scope["horizon_type"] == "month":
        frame = data_reader._normalize_api_monthly_skill_metrics(frame)
    else:
        frame = data_reader._normalize_horizon_skill_metrics(frame, scope["horizon_type"])
    return data_reader._drop_tombstone_rows(frame)


def _normalized(rows, scope):
    frame = pd.DataFrame(rows).rename(columns={"model_type": "model_short"})
    frame["forecasted_discharge"] = [
        number(r.get("q")) if number(r.get("q")) is not None else number(r.get("q50")) for r in rows
    ]
    for field in FORECAST_FIELDS:
        if field not in frame:
            frame[field] = float("nan")
        else:
            # API null-only columns otherwise retain object dtype (None),
            # which cannot participate in the ensemble helper's weighted mean.
            frame[field] = pd.Series([number(value) for value in frame[field]], dtype=float)
    for field in ("date", "valid_from", "valid_to"):
        frame[field] = pd.to_datetime(frame[field])
    start = dt.date.fromisoformat(scope["valid_from"])
    frame["year"] = start.year
    frame["month"] = frame["month_in_year"] = start.month
    frame["quarter_in_year"] = (start.month - 1) // 3 + 1
    frame["season_year"] = start.year
    frame["season_in_year"] = 1
    if scope["horizon_type"] == "season":
        frame["season_year"] = get_season_year(start.year, start.month)
        frame["season_in_year"] = scope["horizon_value"]
    return frame


def _members(client, scope):
    sources = {key_tuple(key): identity(key) for key in scope["sources"]}
    if len(sources) != len(scope["sources"]):
        raise ValueError("Duplicate authorized source")
    trigger_keys = {key_tuple(key) for key in scope["triggers"]}
    if not trigger_keys or not trigger_keys.issubset(sources):
        raise ValueError("Missing authorized trigger")
    rows, active_triggers = [], set()
    for token, key in sources.items():
        if key["code"] != scope["code"] or key["date"] != scope["date"]:
            raise ValueError("Source escaped issue/station scope")
        is_month = scope["horizon_type"] == "quarter" and key["horizon_type"] == "month"
        if is_month:
            if not (
                scope["valid_from"] <= key["valid_from"] <= key["valid_to"] <= scope["valid_to"]
                and scope["horizon_value"] <= key["horizon_value"] <= scope["horizon_value"] + 2
            ):
                raise ValueError("Monthly source escaped quarter scope")
        elif any(
            key[field] != scope[field]
            for field in ("horizon_type", "horizon_value", "valid_from", "valid_to")
        ):
            raise ValueError("Source escaped forecast scope")
        row = read_exact(client, key)
        if token in trigger_keys and (not healthy(row) or row["flag"] != 1):
            raise ValueError("Recovery trigger is no longer healthy flag 1")
        if healthy(row) and (
            number(row.get("q")) is not None or number(row.get("q50")) is not None
        ):
            rows.append(row)
            if token in trigger_keys:
                active_triggers.add(token)
    if not rows:
        return pd.DataFrame(), pd.DataFrame(), set()
    direct = [r for r in rows if r["horizon_type"] == scope["horizon_type"]]
    derived = pd.DataFrame()
    contributors = {key_tuple(r) for r in direct}
    if scope["horizon_type"] == "season":
        direct = [
            r for r in direct if canonical_model_short(r["model_type"]) in QUARTER_NATIVE_RAW_MODELS
        ]
        contributors = {key_tuple(r) for r in direct}
    if scope["horizon_type"] == "quarter":
        direct = [
            r for r in direct if canonical_model_short(r["model_type"]) in QUARTER_NATIVE_RAW_MODELS
        ]
        contributors = {key_tuple(r) for r in direct}
        monthly = [r for r in rows if r["horizon_type"] == "month"]
        if monthly:
            renamed = pd.DataFrame(monthly).rename(columns={"model_type": "model_short"})
            derived, _ = derive_quarterly_from_monthly_same_issue(
                renamed,
                scope["horizon_value"],
                dt.date.fromisoformat(scope["date"]).day,
                QUARTERLY_DERIVED_MODELS | QUARTER_NATIVE_RAW_MODELS,
            )
            if not derived.empty:
                native = {canonical_model_short(r["model_type"]) for r in direct}
                derived = derived[
                    ~derived.model_short.map(canonical_model_short).isin(native)
                ].copy()
                derived_models = set(derived.model_short.map(canonical_model_short))
                contributors.update(
                    key_tuple(r)
                    for r in monthly
                    if canonical_model_short(r["model_type"]) in derived_models
                )
    frame = _normalized(direct, scope) if direct else pd.DataFrame()
    if not derived.empty:
        # Computation helper owns values; authorization owns the exact destination.
        derived["horizon_type"] = "quarter"
        derived["horizon_value"] = scope["horizon_value"]
        for field in ("date", "valid_from", "valid_to"):
            if not (pd.to_datetime(derived[field]) == pd.Timestamp(scope[field])).all():
                raise ValueError("Quarter derivation escaped authorized identity")
        frame = pd.concat([frame, derived], ignore_index=True)
    return frame, derived, active_triggers & contributors


def _payload(row, scope, model):
    for field in ("code", "date", "valid_from", "valid_to"):
        value = row.get(field)
        if value is not None and not pd.isna(value):
            normalized = str(value) if field == "code" else iso_date(value)
            if normalized != scope[field]:
                raise ValueError("Derived output escaped authorized identity")
    record = identity(dict(scope, model_type=model))
    record.update(flag=1, q=number(row.get("forecasted_discharge")))
    if record["q"] is None:
        raise ValueError("Derived output has no usable point forecast")
    for field in FORECAST_FIELDS:
        if field != "q":
            record[field] = number(row.get(field))
    composition = row.get("composition")
    if isinstance(composition, str):
        record["composition"] = composition
    return record


def _composition(value):
    return (
        {m.strip().upper().replace(" ", "_") for m in value.split(",") if m.strip()}
        if isinstance(value, str)
        else set()
    )


def consume_handoff(path, reference, client, *, skills_reader=None):
    reference = dt.date.fromisoformat(str(reference))
    document = json.loads(Path(path).read_text())
    if (
        document.get("schema_version") != 1
        or document.get("destination_hash") != destination_hash()
        or document.get("forecast_date") != reference.isoformat()
        or not isinstance(document.get("scopes"), list)
    ):
        raise ValueError("Invalid or stale LT recovery handoff")
    summary = dict(written=0, unchanged=0, blocked=0, errors=[])
    seen = set()
    for scope in document["scopes"]:
        try:
            canonical = identity(dict(scope, model_type="NAIVE_MEAN"))
            token = key_tuple(canonical)
            if token in seen:
                raise ValueError("Duplicate consumer scope")
            seen.add(token)
            if not in_window(scope["date"], reference):
                raise ValueError("Consumer issue outside recovery window")
            frame, derived, triggers = _members(client, scope)
            if not triggers:
                summary["blocked"] += 1
                continue
            skills = (skills_reader or _skills)(client, scope)
            if skills is None or skills.empty:
                raise ValueError("No applicable stored skills")
            function = {
                "month": ensemble_calculator.create_monthly_ensemble_forecasts,
                "quarter": ensemble_calculator.create_quarterly_ensemble_forecasts,
                "season": ensemble_calculator.create_seasonal_ensemble_forecasts,
            }[scope["horizon_type"]]
            joint = function(frame, skills)
            aggregate = joint[joint.model_short.isin(ENSEMBLES)].copy()
            outputs = [
                (row, ENSEMBLES[row["model_short"]], True) for row in aggregate.to_dict("records")
            ]
            outputs.extend((row, row["model_short"], False) for row in derived.to_dict("records"))
            trigger_models = {key[4].upper().replace(" ", "_") for key in triggers}
            output_keys = set()
            for row, model, is_ensemble in outputs:
                contributing_models = (
                    _composition(row.get("composition"))
                    if is_ensemble
                    else {canonical_model_short(model)}
                )
                if not trigger_models.intersection(contributing_models):
                    continue
                record = _payload(row, scope, model)
                key = key_tuple(record)
                if key in output_keys:
                    raise ValueError("Duplicate derived output")
                output_keys.add(key)
                existing = read_exact(client, record)
                if not is_ensemble and healthy(existing):
                    # Direct/native quarter and monthly-derived rows share a key.
                    # Never replace a healthy member merely because the reader
                    # intentionally selects monthly-derived values in memory.
                    summary["unchanged"] += 1
                    continue
                if is_ensemble and healthy(existing):
                    before, after = (
                        _composition(existing.get("composition")),
                        _composition(record.get("composition")),
                    )
                    if not before or not before.issubset(after):
                        raise ValueError("Existing ensemble membership would be lost")
                    if any(
                        number(existing.get(f)) is not None and number(record.get(f)) is None
                        for f in FORECAST_FIELDS
                    ):
                        raise ValueError("Existing ensemble forecast fields would be lost")
                elif not eligible(existing):
                    raise ValueError("Derived destination is not recoverable")
                record = preserve_metadata(record, existing)
                if equivalent(existing, record):
                    summary["unchanged"] += 1
                    continue
                write_verified(client, record)
                summary["written"] += 1
        except Exception as error:
            summary["errors"].append(f"{type(error).__name__}: {error}")
    return summary
