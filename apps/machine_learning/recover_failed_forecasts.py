"""Opt-in CLI execution and bounded maintenance recovery of wholly failed ML issues.

Locks coordinate this application only. The API has no compare-and-swap: a foreign
writer can still race the final eligibility read. No service contract is changed.
Audit artifacts contain private forecast data and MUST live outside the repository.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import tempfile
import time
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


class ComputeUnavailable(RuntimeError):
    """The existing compute subprocess could not produce a complete result."""


class Backend:
    def __init__(self, module=None):
        if module is None:
            import recalculate_nan_forecasts as module
        self.module = module
        self.allow_absent = False

    def preview(self, dates, models, codes):
        options = {"allow_absent": True} if self.allow_absent else {}
        return self.module.preview_recovery_candidates(dates, models, codes, **options)

    def compute(self, candidate, timeout_seconds):
        return self.module.compute_recovery_unit(candidate, timeout_seconds)

    def write(self, frame, model):
        return self.module._write_ml_forecast_to_api(frame, "day", model)

    def read(self, candidate):
        client = self.module.SapphirePostprocessingClient(
            base_url=os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
        )
        return self.module.read_unit_for_recovery_preview(
            client,
            self.module.ML_MODEL_TYPE_MAP[candidate["model"]],
            candidate["issue_date"],
            candidate["code"],
        )


def audit_directory(value=None):
    configured = value or os.getenv("ML_FAILED_ISSUE_RECOVERY_AUDIT_DIR")
    if not configured:
        intermediate = os.getenv("ieasyforecast_intermediate_data_path")
        if not intermediate:
            raise ValueError("Recovery requires a private intermediate data directory")
        configured = Path(intermediate) / "ml_failed_issue_recovery"
    path = Path(configured).expanduser().resolve()
    if any((parent / ".git").exists() for parent in (path, *path.parents)):
        raise ValueError("Recovery audit directory must be outside a Git repository")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    return path


def unit_id(candidate):
    identity = [
        os.getenv("SAPPHIRE_API_URL", "http://localhost:8000"),
        candidate["issue_date"],
        candidate["model"],
        candidate["code"],
    ]
    return hashlib.sha256(json.dumps(identity).encode()).hexdigest()


def atomic_json(path, value):
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".recovery-")
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


@contextmanager
def unit_lock(path):
    with open(path, "a") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def _keys(candidate):
    return (
        sorted(map(tuple, candidate["replace_keys"])),
        sorted(map(tuple, candidate["insert_keys"])),
    )


def validate_computed(frame, candidate):
    if not isinstance(frame, pd.DataFrame) or len(frame) != 11:
        raise ValueError("Compute must return exactly eleven targets")
    targets = pd.date_range(pd.Timestamp(candidate["issue_date"]) + timedelta(days=1), periods=11)
    if (
        frame["date"].duplicated().any()
        or set(pd.to_datetime(frame["date"])) != set(targets)
        or not (
            pd.to_datetime(frame["forecast_date"]) == pd.Timestamp(candidate["issue_date"])
        ).all()
        or not (frame["code"].astype(str) == candidate["code"]).all()
        or not (frame["flag"] == 4).all()
    ):
        raise ValueError("Compute identity, target dates or flags do not match")
    for column in [f"Q{q}" for q in range(5, 100, 5)]:
        values = frame[column]
        if (
            any(isinstance(v, (bool, str)) for v in values)
            or not np.isfinite(values.astype(float)).all()
        ):
            raise ValueError("Compute contains invalid or nonfinite quantiles")
    return frame.sort_values("date").copy()


def verify_readback(rows, frame, candidate, backend):
    # The preview classifier strictly validates raw API identities and field shape.
    expected_model = {"TFT": "TFT", "TIDE": "TiDE", "TSMIXER": "TSMixer"}[candidate["model"]]
    expected_targets = {v.date().isoformat() for v in pd.to_datetime(frame["date"])}
    if len(rows) != 11 or rows["target"].astype(str).duplicated().any():
        raise ValueError("Read-back is incomplete or duplicated")
    for field, expected in [
        ("horizon_type", "day"),
        ("code", candidate["code"]),
        ("model_type", expected_model),
        ("date", candidate["issue_date"]),
    ]:
        if not (rows[field].astype(str) == expected).all():
            raise ValueError("Read-back identity mismatch")
    if set(rows["target"].astype(str)) != expected_targets or not (rows["flag"] == 4).all():
        raise ValueError("Read-back targets or flags mismatch")
    actual = rows.sort_values("target")
    for api, column in [
        ("q05", "Q5"),
        ("q25", "Q25"),
        ("forecasted_discharge", "Q50"),
        ("q75", "Q75"),
        ("q95", "Q95"),
    ]:
        values = actual[api]
        if any(isinstance(v, (bool, str)) for v in values):
            raise ValueError("Read-back values have invalid types")
        if not np.isfinite(values.astype(float)).all() or not np.allclose(
            values.astype(float), frame[column].astype(float), rtol=1e-7, atol=1e-8
        ):
            raise ValueError("Read-back values mismatch")


def remaining_owned_targets(rows, frame, saved):
    """Validate an interrupted own write; return only still-failed approved targets."""
    if saved.get("write_started") is not True:
        raise ValueError("No durable evidence this recovery started a write")
    model = {"TFT": "TFT", "TIDE": "TiDE", "TSMIXER": "TSMixer"}[saved["model"]]
    expected = {v.date().isoformat() for v in pd.to_datetime(frame["date"])}
    approved = {tuple(key) for key in saved["replace_keys"] + saved["insert_keys"]}
    expected_keys = {
        ("day", saved["code"], model, saved["issue_date"], target) for target in expected
    }
    if approved != expected_keys:
        raise ValueError("Saved approval does not cover precisely the computed target keys")
    values = [
        ("q05", "Q5"),
        ("q25", "Q25"),
        ("forecasted_discharge", "Q50"),
        ("q75", "Q75"),
        ("q95", "Q95"),
    ]
    healthy = set()
    seen = set()
    computed = frame.set_index(frame["date"].dt.strftime("%Y-%m-%d"))
    for _, row in rows.iterrows():
        key = tuple(row[name] for name in ("horizon_type", "code", "model_type", "date", "target"))
        if key not in approved or key in seen:
            raise ValueError("Unknown, duplicate or changed persisted key during resume")
        seen.add(key)
        flag = row["flag"]
        if isinstance(flag, (bool, str)) or pd.isna(flag) or flag not in (3, 4):
            raise ValueError("Protected or unknown flag during resume")
        if flag == 4:
            for api, column in values:
                value = row[api]
                if (
                    isinstance(value, (bool, str))
                    or pd.isna(value)
                    or not math.isfinite(float(value))
                    or float(value) != float(computed.loc[row["target"], column])
                ):
                    raise ValueError("Healthy row differs from the saved recovery artifact")
            healthy.add(row["target"])
        elif any(not pd.isna(row[api]) for api, _ in values):
            raise ValueError("Remaining flag-3 row is no longer wholly null")
    return expected - healthy


def run_recovery(
    issue_dates,
    models,
    station_codes,
    *,
    execute=False,
    audit_dir=None,
    compute_fn=None,
    max_units=None,
    budget_seconds=300,
    timeout_seconds=120,
    cooldown_seconds=0,
    backend=None,
    clock=time.monotonic,
):
    """Return explicit preview/results/errors; compute_fn may supply validated artifacts."""
    backend = backend or Backend()
    summary = {"status": "empty", "results": [], "errors": []}
    if os.getenv("SAPPHIRE_TEST_ENV", "").lower() == "true" and execute:
        return {
            **summary,
            "status": "blocked",
            "reason": "SAPPHIRE_TEST_ENV forbids recovery writes",
        }
    if any(not math.isfinite(v) or v <= 0 for v in (budget_seconds, timeout_seconds)):
        raise ValueError("Recovery time limits must be positive and finite")
    start = clock()
    preview = backend.preview(issue_dates, models, station_codes)
    summary["preview"] = preview
    if preview["gate_blocked"]:
        return {**summary, "status": "blocked", "reason": preview["gate_blocked"]}
    summary["errors"].extend(preview["blocked"])
    if not execute:
        summary["status"] = "error" if summary["errors"] else "preview"
        return summary
    directory = audit_directory(audit_dir)
    # Resume only interrupted writes owned by this exact audit/artifact. Healthy
    # targets must match exactly, and are never included in a resumed write.
    attempted = 0
    reconciled = set()
    for path in directory.glob("*.json"):
        if path.name.endswith(".computed.json") or path.name.startswith("scan-"):
            continue
        saved = json.loads(path.read_text())
        if (
            saved.get("issue_date") not in issue_dates
            or saved.get("model") not in models
            or saved.get("code") not in station_codes
            or saved.get("status") == "verified"
            or not saved.get("computed_artifact")
            or saved.get("write_started") is not True
            or path.stem != unit_id(saved)
        ):
            continue
        if clock() - start >= budget_seconds or (max_units is not None and attempted >= max_units):
            summary["deferred"] = True
            break
        with unit_lock(path.with_suffix(".lock")) as acquired:
            if not acquired:
                continue
            saved = json.loads(path.read_text())
            if saved.get("status") == "verified":
                continue
            reconciled.add(path.stem)
            try:
                frame = validate_computed(
                    pd.read_json(saved["computed_artifact"], orient="table", precise_float=True),
                    saved,
                )
                remaining = remaining_owned_targets(backend.read(saved), frame, saved)
                if remaining:
                    age = (
                        datetime.now(UTC) - datetime.fromisoformat(saved["attempted_at"])
                    ).total_seconds()
                    if age < cooldown_seconds:
                        summary["results"].append({**saved, "status": "cooldown"})
                        continue
                    attempted += 1
                    # Recheck the entire unit immediately before selecting the subset.
                    remaining = remaining_owned_targets(backend.read(saved), frame, saved)
                    if clock() - start >= budget_seconds:
                        raise TimeoutError("Recovery budget exhausted before resumed write")
                    if remaining:
                        subset = frame[frame["date"].dt.strftime("%Y-%m-%d").isin(remaining)].copy()
                        saved.update(status="writing", attempted_at=datetime.now(UTC).isoformat())
                        atomic_json(path, saved)
                        if backend.write(subset, saved["model"]) is not True:
                            raise ValueError("Resumed API writer did not confirm success")
                persisted = backend.read(saved)
                if remaining_owned_targets(persisted, frame, saved):
                    raise ValueError("Resumed write did not persist every approved target")
                verify_readback(persisted, frame, saved, backend)
                saved.update(
                    status="verified",
                    after={"rows": 11, "flag": 4, "values_verified": True},
                    period_rebuild_pending=True,
                    pending_horizons=["pentad", "decade"],
                )
                # Ordinary preview rejects mixed flags; this owned, verified resume
                # is the sole exception and has now completed all eleven targets.
                summary["errors"] = [
                    e
                    for e in summary["errors"]
                    if not all(e.get(k) == saved[k] for k in ("issue_date", "model", "code"))
                ]
            except Exception as error:
                saved.update(
                    status="error", error=f"Unresolved previous write verification: {error}"
                )
                summary["errors"].append(
                    {
                        "issue_date": saved["issue_date"],
                        "model": saved["model"],
                        "code": saved["code"],
                        "error": saved["error"],
                    }
                )
            atomic_json(path, saved)
            summary["results"].append(saved)
    candidates = [c for c in preview["candidates"] if unit_id(c) not in reconciled]

    # Failed attempts go to the back of the queue, including after cooldown expiry.
    def previous(candidate):
        path = directory / (unit_id(candidate) + ".json")
        return json.loads(path.read_text()) if path.exists() else {}

    candidates.sort(key=lambda c: previous(c).get("attempted_at", ""))
    for candidate in candidates:
        remaining = budget_seconds - (clock() - start)
        if remaining <= 0 or (max_units is not None and attempted >= max_units):
            summary["deferred"] = True
            break
        stem = unit_id(candidate)
        state_path = directory / (stem + ".json")
        with unit_lock(directory / (stem + ".lock")) as acquired:
            if not acquired:
                summary["results"].append({**candidate, "status": "locked"})
                continue
            old = previous(candidate)
            if old.get("attempted_at") and old.get("status") != "verified":
                age = (
                    datetime.now(UTC) - datetime.fromisoformat(old["attempted_at"])
                ).total_seconds()
                if age < cooldown_seconds:
                    summary["results"].append({**candidate, "status": "cooldown"})
                    continue
            attempted += 1
            state = {
                **candidate,
                "status": "computing",
                "attempted_at": datetime.now(UTC).isoformat(),
                "before": candidate,
                "period_rebuild_pending": False,
            }
            atomic_json(state_path, state)
            try:
                unit_start = clock()
                limit = min(timeout_seconds, remaining)
                computed = (compute_fn or backend.compute)(candidate, limit)
                if computed is None:
                    raise ComputeUnavailable(
                        "Compute returned no result; inspect ML subprocess logs for missing inputs or model failures"
                    )
                frame = validate_computed(computed, candidate)
                if clock() - unit_start >= limit or clock() - start >= budget_seconds:
                    raise TimeoutError("Recovery compute exceeded its time budget")
                artifact = directory / (stem + ".computed.json")
                payload = json.loads(frame.to_json(orient="table", date_format="iso"))
                # Preserve the exact Python float representation used by the writer.
                for record, (_, row) in zip(payload["data"], frame.iterrows(), strict=True):
                    for q in range(5, 100, 5):
                        record[f"Q{q}"] = float(row[f"Q{q}"])
                atomic_json(artifact, payload)
                state["computed_artifact"] = str(artifact)
                fresh = backend.preview(
                    [candidate["issue_date"]], [candidate["model"]], [candidate["code"]]
                )
                if fresh["gate_blocked"] or fresh["blocked"]:
                    raise ValueError("Fresh eligibility read blocked")
                if not fresh["candidates"]:
                    state["status"] = "protected"
                    state["after"] = fresh
                elif len(fresh["candidates"]) != 1 or _keys(fresh["candidates"][0]) != _keys(
                    candidate
                ):
                    raise ValueError("Eligibility keys changed before write")
                else:
                    if clock() - start >= budget_seconds:
                        raise TimeoutError("Recovery budget exhausted before write")
                    state["status"] = "writing"
                    state["write_started"] = True
                    atomic_json(state_path, state)
                    if backend.write(frame, candidate["model"]) is not True:
                        raise ValueError("API writer did not confirm success")
                    verify_readback(backend.read(candidate), frame, candidate, backend)
                    state.update(
                        status="verified",
                        after={"rows": 11, "flag": 4, "values_verified": True},
                        period_rebuild_pending=True,
                        pending_horizons=["pentad", "decade"],
                    )
            except ComputeUnavailable as error:
                state.update(status="unrecoverable", error=str(error))
            except Exception as error:
                state.update(status="error", error=f"{type(error).__name__}: {error}")
                summary["errors"].append(
                    {
                        "issue_date": candidate["issue_date"],
                        "model": candidate["model"],
                        "code": candidate["code"],
                        "error": state["error"],
                    }
                )
            atomic_json(state_path, state)
            summary["results"].append(state)
    summary["counts"] = {
        status: sum(r["status"] == status for r in summary["results"])
        for status in {"verified", "unrecoverable", "error", "protected", "cooldown", "locked"}
    }
    summary["status"] = (
        "error"
        if summary["errors"]
        else ("complete" if attempted or summary["results"] else "empty")
    )
    return summary


def latest_issue_dates(forecast_date):
    """Latest due boundaries, independently for each consumer horizon."""
    result = {}
    for horizon, days in (("pentad", {5, 10, 15, 20, 25}), ("decade", {10, 20})):
        issue = forecast_date
        while issue.day not in days and (issue + timedelta(days=1)).month == issue.month:
            issue -= timedelta(days=1)
        result[horizon] = issue.isoformat()
    return result


def configured_latest_units(forecast_date):
    """Union shared DAY units without granting stations another horizon's dates."""
    mode = os.getenv("SAPPHIRE_PREDICTION_MODE", "") or "BOTH"
    if mode not in {"PENTAD", "DECAD", "BOTH"}:
        raise ValueError("Invalid SAPPHIRE_PREDICTION_MODE")
    dates = latest_issue_dates(forecast_date)
    units = set()
    for horizon, name, env in (
        ("pentad", "PENTAD", "ieasyforecast_config_file_station_selection"),
        ("decade", "DECAD", "ieasyforecast_config_file_station_selection_decad"),
    ):
        if mode not in (name, "BOTH"):
            continue
        filename = os.getenv(env)
        if not filename:
            raise ValueError(f"Missing station configuration: {env}")
        path = Path(os.getenv("ieasyforecast_configuration_path", "")) / filename
        config = json.loads(path.read_text())
        if not isinstance(config.get("stationsID"), list):
            raise ValueError(f"Invalid station configuration: {env}")
        units.update((dates[horizon], str(code)) for code in config["stationsID"])
    return sorted(units, reverse=True)


def run_maintenance_recovery(
    forecast_date=None,
    *,
    backend=None,
    models=None,
    execute=True,
    audit_dir=None,
    budget_seconds=300,
):
    """Bounded latest-only recovery with a durable fair date/station cursor."""
    if (
        os.getenv("SAPPHIRE_TEST_ENV", "").lower() == "true"
        or os.getenv("ML_FAILED_ISSUE_RECOVERY_ENABLED", "true").lower() == "false"
        or os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "false"
        or os.getenv("ieasyhydroforecast_run_ML_models", "").lower() in {"false", "0", "no"}
    ):
        return {"status": "disabled", "results": [], "errors": []}
    if not math.isfinite(budget_seconds) or budget_seconds <= 0:
        raise ValueError("Recovery budget must be positive and finite")
    backend = backend or Backend()
    # Keep the automatic opt-in local; a caller may reuse its backend manually.
    if type(backend) is Backend:
        backend = Backend(backend.module)
    backend.allow_absent = True
    models = models or [os.getenv("SAPPHIRE_MODEL_TO_USE", "").upper()]
    today = forecast_date or date.today()
    # Preserve readiness/configuration gates before opening durable state.
    codes = sorted(backend.module.get_permitted_station_codes() or [])
    gate = backend.preview([latest_issue_dates(today)["pentad"]], models, codes[:1])
    if gate["gate_blocked"]:
        reason = gate["gate_blocked"]
        configuration_off = reason.startswith(
            (
                "SAPPHIRE_API_ENABLED=false",
                "ieasyhydroforecast_run_ML_models is disabled",
                "unsupported model(s):",
                "requested model(s)",
            )
        )
        return {
            "status": "disabled" if configuration_off else "blocked",
            "reason": reason,
            "results": [],
            "errors": [],
        }
    try:
        units = [(d, code, model) for d, code in configured_latest_units(today) for model in models]
    except (OSError, ValueError, TypeError) as error:
        return {"status": "blocked", "reason": str(error), "results": [], "errors": []}
    combined = {
        "status": "empty",
        "results": [],
        "errors": [],
        "skipped": [],
        "previews": [],
        "latest_issue_dates": latest_issue_dates(today),
        "limitations": [
            "Mixed/unsupported partial units remain protected; confirmed wholly absent units are eligible."
        ],
    }
    if not units:
        return combined
    directory = audit_directory(audit_dir) if execute else None
    cursor_path = directory / ("scan-latest-" + "-".join(models) + ".json") if execute else None
    saved = json.loads(cursor_path.read_text()) if cursor_path and cursor_path.exists() else {}
    next_unit = (saved.get("next_issue_date"), saved.get("next_code"), saved.get("next_model"))
    cursor = units.index(next_unit) if next_unit in units else 0
    start = time.monotonic()
    attempts = 0
    for offset in range(len(units)):
        remaining = min(budget_seconds, 300) - (time.monotonic() - start)
        if remaining <= 0 or attempts >= 3:
            combined["deferred"] = True
            break
        index = (cursor + offset) % len(units)
        issue, code, model = units[index]
        result = run_recovery(
            [issue],
            [model],
            [code],
            execute=execute,
            backend=backend,
            max_units=1,
            budget_seconds=remaining,
            timeout_seconds=120,
            cooldown_seconds=86400,
            audit_dir=directory,
        )
        if "preview" in result:
            combined["previews"].append(result["preview"])
        combined["results"].extend(result["results"])
        combined["errors"].extend(result["errors"])
        combined["skipped"].extend(result.get("preview", {}).get("skipped", []))
        attempts += sum(
            r["status"] in {"verified", "error", "protected", "unrecoverable"}
            for r in result["results"]
        )
        if execute:
            following = units[(index + 1) % len(units)]
            atomic_json(
                cursor_path,
                {
                    "next_issue_date": following[0],
                    "next_code": following[1],
                    "next_model": following[2],
                },
            )
        if result["status"] == "blocked":
            combined["errors"].append({"error": result.get("reason", "Recovery blocked")})
    combined["counts"] = {
        status: sum(r["status"] == status for r in combined["results"])
        for status in {"verified", "unrecoverable", "error", "protected", "cooldown", "locked"}
    }
    combined["attempted"] = attempts
    combined["status"] = (
        "error"
        if combined["errors"] or combined["counts"]["unrecoverable"]
        else ("preview" if not execute else ("complete" if attempts else "empty"))
    )
    return combined


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    dates = parser.add_mutually_exclusive_group(required=True)
    dates.add_argument("--issue-dates", nargs="+")
    dates.add_argument("--latest", action="store_true")
    parser.add_argument("--forecast-date", type=date.fromisoformat)
    parser.add_argument("--models", nargs="+", required=True)
    selector = parser.add_mutually_exclusive_group(required=True)
    selector.add_argument("--stations", nargs="+")
    selector.add_argument("--configured-stations", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--audit-dir")
    parser.add_argument("--budget-seconds", type=float, default=300)
    args = parser.parse_args()
    if args.latest and (not args.forecast_date or not args.configured_stations):
        parser.error("--latest requires --forecast-date and --configured-stations")
    backend = Backend()
    backend.module.sl.load_environment()
    if args.latest:
        result = run_maintenance_recovery(
            args.forecast_date,
            models=args.models,
            execute=args.execute,
            audit_dir=args.audit_dir,
            budget_seconds=args.budget_seconds,
            backend=backend,
        )
    else:
        codes = args.stations or sorted(backend.module.get_permitted_station_codes() or [])
        result = run_recovery(
            args.issue_dates,
            args.models,
            codes,
            execute=args.execute,
            audit_dir=args.audit_dir,
            budget_seconds=args.budget_seconds,
            backend=backend,
        )
    print(json.dumps(result, default=str))
    return 1 if result["status"] in {"error", "blocked"} else 0


if __name__ == "__main__":
    raise SystemExit(main())
