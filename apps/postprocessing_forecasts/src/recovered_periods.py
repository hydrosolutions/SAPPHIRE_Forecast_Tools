"""Bounded, private-audit-driven period repair after verified DAY recovery.

Only recovered issue dates that are actual period boundaries are eligible.
Individual period records with finite medians are protected; affected ensembles
are recomputed with their new members. An audit is acknowledged only after
strict API read-back. Local locks do not provide server-side compare-and-swap.
"""

import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import math
import os
import tempfile
import time
from pathlib import Path

import pandas as pd
from src import api_writer, data_reader, ensemble_calculator

VALUES = ("forecasted_discharge", "q05", "q25", "q75", "q95")
MODELS = ("TFT", "TIDE", "TSMIXER")


def _finite(row):
    return all(
        not isinstance(row.get(k), bool) and pd.notna(row.get(k)) and math.isfinite(float(row[k]))
        for k in VALUES
    )


def _members(value):
    if not isinstance(value, str) or not value.strip():
        return set()
    return {part.strip().upper() for part in value.split(",") if part.strip()}


def _protect_members(existing, replacement):
    if existing.empty:
        return
    before = _members(existing.iloc[0].get("composition"))
    after = _members(replacement.iloc[0].get("composition"))
    if not before or not before.issubset(after):
        raise RuntimeError("Existing ensemble membership cannot be safely preserved")


def _identity(state):
    identity = [
        os.getenv("SAPPHIRE_API_URL", "http://localhost:8000"),
        state["issue_date"],
        state["model"],
        state["code"],
    ]
    return hashlib.sha256(json.dumps(identity).encode()).hexdigest()


def _ack(path, state, horizon, reason):
    state["pending_horizons"] = [h for h in state["pending_horizons"] if h != horizon]
    state["period_rebuild_pending"] = bool(state["pending_horizons"])
    state.setdefault("period_rebuild_results", {})[horizon] = reason
    _save_state(path, state)


def _save_state(path, state):
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".period-recovery-")
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(state, stream, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _check_deadline(deadline):
    if time.monotonic() >= deadline:
        raise RuntimeError("Period recovery deadline exceeded")


def _read(client, config, issue, code, model, forecast_date, deadline):
    _check_deadline(deadline)
    status, frame, reason = data_reader.read_combined_period_forecasts_for_recovery(
        client, [code], model, config.name, issue, issue, forecast_date, total_deadline=deadline
    )
    if status not in (data_reader.RECOVERY_STATUS_COMPLETE, data_reader.RECOVERY_STATUS_EMPTY):
        raise RuntimeError(f"Period read blocked: {reason}")
    if frame is None or len(frame) > 1:
        raise RuntimeError("Ambiguous period read")
    if not frame.empty:
        row = frame.iloc[0]
        target = issue + dt.timedelta(days=1)
        if (
            pd.Timestamp(row["date"]) != pd.Timestamp(issue)
            or pd.Timestamp(row["target"]) != pd.Timestamp(target)
            or row["code"] != code
            or str(row["model_type"]).upper() != model.upper()
            or row["horizon_type"] != ("decade" if config.name == "decad" else "pentad")
            or row["horizon_value"] != data_reader._period_in_month_for_date(target, config.name)
            or row["horizon_in_year"] != data_reader._period_in_year_for_date(target, config.name)
        ):
            raise RuntimeError("Period response identity does not match recovered boundary")
    return frame


def _repair(config, issue, code, models, forecast_date, client, deadline):
    # Strict reads distinguish confirmed absence from an unavailable API.
    existing = {
        m: _read(client, config, issue, code, m, forecast_date, deadline)
        for m in (*MODELS, "NE", "EM")
    }
    # Use bounded strict snapshots exclusively: the ordinary reader loads a
    # whole year and tolerates outages, neither suitable for recovery writes.
    strict_members = []
    for model in MODELS:
        _check_deadline(deadline)
        status, daily, reason = data_reader.read_day_forecasts_for_recovery(
            client, [code], model, issue, issue, forecast_date, total_deadline=deadline
        )
        if status not in (data_reader.RECOVERY_STATUS_COMPLETE, data_reader.RECOVERY_STATUS_EMPTY):
            raise RuntimeError(f"DAY source read blocked: {reason}")
        if daily is None:
            raise RuntimeError("DAY source response missing")
        if not daily.empty:
            strict_members.append(data_reader._normalize_ml_forecasts(daily, model, config.name))
    _check_deadline(deadline)
    status, lr, reason = data_reader.read_lr_forecasts_for_recovery(
        client, [code], config.name, issue, issue, forecast_date, total_deadline=deadline
    )
    if status not in (data_reader.RECOVERY_STATUS_COMPLETE, data_reader.RECOVERY_STATUS_EMPTY):
        raise RuntimeError(f"LR source read blocked: {reason}")
    if lr is None:
        raise RuntimeError("LR source response missing")
    if not lr.empty:
        strict_members.append(data_reader._normalize_lr_forecasts(lr, config.name)[0])
    if not strict_members:
        raise RuntimeError("No strictly verified period members")
    members = pd.concat(strict_members, ignore_index=True)
    if (
        members.empty
        or not (pd.to_datetime(members["date"]) == pd.Timestamp(issue)).all()
        or not (members["code"] == code).all()
    ):
        raise RuntimeError("Strict period sources escaped recovered date/station scope")
    members["model_short"] = members["model_short"].str.upper()
    members = members[members.model_short.isin((*MODELS, "LR"))].copy()
    if members.duplicated(["date", "code", "model_short"]).any():
        raise RuntimeError("Duplicate period members")
    writes = []
    for model in MODELS:
        raw = existing[model]
        selected = members[members.model_short == model]
        # Protect all existing finite individual medians, including legacy rows
        # without quantiles. Use the persisted values as ensemble members.
        protected = not raw.empty and pd.notna(raw.iloc[0]["forecasted_discharge"])
        if protected:
            if selected.empty:
                row = raw.iloc[0].to_dict()
                row.update(model_short=model, date=pd.Timestamp(issue))
                row[config.period_col] = row["horizon_in_year"]
                row[config.period_in_month_col] = row["horizon_value"]
                members = pd.concat([members, pd.DataFrame([row])], ignore_index=True)
            else:
                for column in VALUES:
                    members.loc[selected.index, column] = raw.iloc[0].get(column)
        elif model in models:
            if len(selected) != 1 or not _finite(selected.iloc[0]):
                raise RuntimeError("Recovered individual period forecast incomplete")
            writes.append(selected)
    members = config.neural_ensemble_func(members)
    ne = members[members.model_short == "NE"]
    if len(ne) != 1 or not _finite(ne.iloc[0]):
        raise RuntimeError("Recovered neural ensemble incomplete")
    _protect_members(existing["NE"], ne)
    writes.append(ne)
    _check_deadline(deadline)
    skills = data_reader._read_skill_metrics_api(config.name, codes=[code], strict=True)
    if skills is None:
        raise RuntimeError("Skill read failed")
    if not skills.empty:
        # The API preserves TiDE/TSMixer spelling; period member identities are
        # uppercase. Normalize both sides of the case-sensitive skill merge.
        skills = skills.copy()
        skills["model_short"] = skills["model_short"].str.upper()
        output, _ = ensemble_calculator.create_ensemble_forecasts(
            forecasts=members,
            skill_stats=skills,
            period_col=config.period_col,
            period_in_month_col=config.period_in_month_col,
            get_period_in_month_func=config.get_period_func,
        )
        em = output[output.model_short == "EM"]
        if not em.empty:
            if len(em) != 1 or not _finite(em.iloc[0]):
                raise RuntimeError("Recovered skilled ensemble incomplete")
            _protect_members(existing["EM"], em)
            writes.append(em)
        elif not existing["EM"].empty:
            raise RuntimeError("Existing EM cannot be safely rebuilt")
    elif not existing["EM"].empty:
        raise RuntimeError("Existing EM cannot be rebuilt without skills")
    joint = pd.concat(writes, ignore_index=True)
    if (
        not (pd.to_datetime(joint.date) == pd.Timestamp(issue)).all()
        or not (joint.code == code).all()
        or not joint.apply(_finite, axis=1).all()
    ):
        raise RuntimeError("Invalid recovery write scope or values")
    if time.monotonic() >= deadline:
        raise RuntimeError("Period recovery deadline exceeded")
    if api_writer._write_combined_forecast_to_api(joint, config.name) is not True:
        raise RuntimeError("Period API writer did not confirm success")
    for _, row in joint.iterrows():
        persisted = _read(client, config, issue, code, row.model_short, forecast_date, deadline)
        if len(persisted) != 1 or any(
            pd.isna(persisted.iloc[0].get(k))
            or not math.isclose(
                float(persisted.iloc[0][k]), float(row[k]), rel_tol=1e-9, abs_tol=1e-9
            )
            for k in VALUES
        ):
            raise RuntimeError("Period read-back did not verify every value")
        if row.model_short in ("NE", "EM") and (
            persisted.iloc[0].get("composition") != row.get("composition")
        ):
            raise RuntimeError("Ensemble composition read-back mismatch")


def consume_pending_recoveries(
    config,
    forecast_date,
    *,
    codes=None,
    audit_dir=None,
    client=None,
    max_dates=3,
    budget_seconds=300,
    latest_only=False,
):
    """Consume at most three issue dates; failures leave their audit pending.

    Test mode and the shared recovery opt-out prevent any filesystem/API work.
    Non-boundary horizons are acknowledged as inapplicable, never shifted to a
    different issue date. Audits are bound to the current API destination hash.
    latest_only leaves superseded audits untouched for explicit manual recovery;
    each horizon independently selects its latest boundary at forecast_date.
    """
    result = {"completed": 0, "errors": []}
    if (
        os.getenv("SAPPHIRE_TEST_ENV", "").lower() == "true"
        or os.getenv("ML_FAILED_ISSUE_RECOVERY_ENABLED", "true").lower() == "false"
    ):
        return result
    if config.name not in ("pentad", "decad"):
        raise ValueError("Unsupported recovery horizon")
    directory = audit_dir or os.getenv("ML_FAILED_ISSUE_RECOVERY_AUDIT_DIR")
    if not directory:
        intermediate = os.getenv("ieasyforecast_intermediate_data_path")
        if not intermediate:
            return result
        directory = Path(intermediate) / "ml_failed_issue_recovery"
    directory = Path(directory).expanduser().resolve()
    if any((parent / ".git").exists() for parent in (directory, *directory.parents)):
        raise ValueError("Recovery audit directory must be outside a Git repository")
    horizon = "decade" if config.name == "decad" else "pentad"
    deadline = time.monotonic() + min(float(budget_seconds), 300)
    latest_issue = forecast_date
    if latest_only:
        boundary_check = (
            data_reader._is_pentad_boundary
            if config.name == "pentad"
            else data_reader._is_decad_boundary
        )
        while not boundary_check(latest_issue):
            latest_issue -= dt.timedelta(days=1)
    pending = []
    date_last_attempt = {}
    unit_last_attempt = {}
    for path in directory.glob("*.json"):
        if len(path.stem) != 64:
            continue
        state = json.loads(path.read_text())
        if (
            state.get("status") == "verified"
            and state.get("period_rebuild_pending") is True
            and horizon in state.get("pending_horizons", [])
            and state.get("model") in MODELS
            and _identity(state) == path.stem
            and (codes is None or state["code"] in codes)
        ):
            issue = dt.date.fromisoformat(state["issue_date"])
            if issue <= forecast_date and (not latest_only or issue == latest_issue):
                pending.append((issue, state["code"], path))
                attempted = state.get("period_rebuild_attempted_at", {}).get(horizon, 0)
                date_last_attempt[issue] = min(date_last_attempt.get(issue, attempted), attempted)
                unit = (issue, state["code"])
                unit_last_attempt[unit] = min(unit_last_attempt.get(unit, attempted), attempted)
    # Never-attempted dates first, then least-recently-attempted dates. Attempt
    # timestamps live in each locked audit, so blocked old dates cannot starve
    # new repairs and a process restart does not reset the rotation.
    selected_dates = sorted(date_last_attempt, key=lambda d: (date_last_attempt[d], d))[
        : min(int(max_dates), 3)
    ]
    date_priority = {issue: rank for rank, issue in enumerate(selected_dates)}
    scheduled_units = sorted(
        (unit for unit in unit_last_attempt if unit[0] in date_priority),
        key=lambda unit: (date_priority[unit[0]], unit_last_attempt[unit], unit[1]),
    )
    for issue, code in scheduled_units:
        if time.monotonic() >= deadline:
            break
        try:
            with contextlib.ExitStack() as stack:
                states = []
                for d, c, path in sorted(pending):
                    if (d, c) != (issue, code):
                        continue
                    lock = stack.enter_context(open(path.with_suffix(".lock"), "a"))
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    state = json.loads(path.read_text())
                    if (
                        state.get("status") == "verified"
                        and state.get("period_rebuild_pending")
                        and horizon in state.get("pending_horizons", [])
                        and _identity(state) == path.stem
                    ):
                        states.append((path, state))
                if not states:
                    continue
                for path, state in states:
                    state.setdefault("period_rebuild_attempted_at", {})[horizon] = time.time()
                    _save_state(path, state)
                boundary = (
                    data_reader._is_pentad_boundary(issue)
                    if config.name == "pentad"
                    else data_reader._is_decad_boundary(issue)
                )
                if boundary:
                    if client is None:
                        client = api_writer._get_postprocessing_client()
                    _repair(
                        config,
                        issue,
                        code,
                        {s["model"] for _, s in states},
                        forecast_date,
                        client,
                        deadline,
                    )
                for path, state in states:
                    _ack(
                        path,
                        state,
                        horizon,
                        "verified" if boundary else "not_applicable_non_boundary",
                    )
                    result["completed"] += 1
        except Exception as error:
            result["errors"].append(
                f"{issue.isoformat()} {config.name}: {type(error).__name__}: {error}"
            )
    return result
