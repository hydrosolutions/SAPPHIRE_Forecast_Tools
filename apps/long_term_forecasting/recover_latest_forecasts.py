"""Retry only latest configured LT issues; preserve healthy raw forecasts.

The API is restart state. A fresh handoff authorizes exact derived rebuilds;
no historical maintenance, model training or skill recalculation is invoked.
"""

import argparse
import calendar
import datetime as dt
import json
import os
import sys
import tempfile
from pathlib import Path

# Keep the application __init__.py ahead of the shared library.
sys.path.append(str(Path(__file__).resolve().parents[1] / "iEasyHydroForecast"))

import pandas as pd
from long_term_recovery_io import (
    destination_hash,
    eligible,
    healthy,
    identity,
    key_tuple,
    preserve_metadata,
    read_exact,
    write_verified,
)


def latest_issue(reference, issue_day, months):
    reference = dt.date.fromisoformat(str(reference))
    if isinstance(issue_day, bool) or not 1 <= int(issue_day) <= 31:
        raise ValueError("Invalid configured issue day")
    if not months or any(isinstance(m, bool) or int(m) != m or not 1 <= m <= 12 for m in months):
        raise ValueError("Invalid configured forecast months")
    for back in range(13):
        year, month0 = divmod(reference.year * 12 + reference.month - 1 - back, 12)
        month = month0 + 1
        if month in months:
            candidate = dt.date(
                year, month, min(int(issue_day), calendar.monthrange(year, month)[1])
            )
            if candidate <= reference:
                return candidate
    raise ValueError("No due issue in configured calendar")


def expected_key(config, model, issue, code):
    from lt_utils import map_model_name_to_model_type
    from post_process_lt_forecast import adjust_forecast_dates_dynamic, adjust_forecast_dates_only

    # Date-only helpers use a sentinel, never actual model output, as authority.
    sentinel = pd.DataFrame(
        [
            dict(
                date=pd.Timestamp(issue),
                code=code,
                flag=1,
                valid_from=pd.Timestamp(issue),
                valid_to=pd.Timestamp(issue),
            )
        ]
    )
    lead = config.get_operational_month_lead_time()
    if config.get_calendar_month_adjustment():
        start = pd.Timestamp(issue).replace(day=1) + pd.DateOffset(months=lead)
        end = start + pd.offsets.MonthEnd(0)
    elif config.get_target_start_month() is not None:
        adjusted = adjust_forecast_dates_only(
            sentinel, config.get_target_start_month(), config.get_target_end_month()
        )
        start, end = adjusted.iloc[0][["valid_from", "valid_to"]]
    else:
        adjusted = adjust_forecast_dates_dynamic(
            sentinel, lead, config.get_forecast_horizon_months()
        )
        start, end = adjusted.iloc[0][["valid_from", "valid_to"]]
    return identity(
        dict(
            horizon_type=config.get_horizon_type(),
            horizon_value=lead,
            code=code,
            model_type=map_model_name_to_model_type(model),
            date=issue,
            valid_from=start,
            valid_to=end,
        )
    )


def _compute(config, model, issue, codes, writer, cache):
    from __init__ import initialize_today
    from data_interface import DataInterfaceDB
    from run_forecast import run_single_model

    initialize_today(issue.isoformat())
    cache_key = (issue, tuple(codes), config.get_forcing_HRU())
    if cache_key not in cache:
        interface = DataInterfaceDB(station_codes=codes)
        cache[cache_key] = interface, interface.get_base_data(forcing_HRU=config.get_forcing_HRU())
    interface, data = cache[cache_key]
    return run_single_model(
        interface,
        config,
        model,
        data["temporal_data"].copy(),
        data["static_data"],
        data["offset_date_base"],
        data["offset_date_discharge"],
        station_codes=codes,
        recovery_flag=1,
        forecast_writer=writer,
    )


def _handoff(path, reference, keys, triggers):
    # The producer alone resolves scheduling. PP receives only current scopes.
    grouped = {}
    for key in keys.values():
        scope = {k: v for k, v in key.items() if k != "model_type"}
        group = tuple(scope.values())
        grouped.setdefault(group, dict(scope, sources=[], triggers=[]))["sources"].append(key)
    for scope in grouped.values():
        if scope["horizon_type"] == "quarter":
            # Monthly triplets may contribute only to an independently current quarter.
            for key in keys.values():
                if (
                    key["horizon_type"] == "month"
                    and key["code"] == scope["code"]
                    and key["date"] == scope["date"]
                    and scope["valid_from"]
                    <= key["valid_from"]
                    <= key["valid_to"]
                    <= scope["valid_to"]
                    and scope["horizon_value"] <= key["horizon_value"] <= scope["horizon_value"] + 2
                ):
                    scope["sources"].append(key)
        scope["triggers"] = [key for key in scope["sources"] if key_tuple(key) in triggers]
    document = dict(
        schema_version=1,
        destination_hash=destination_hash(),
        forecast_date=reference.isoformat(),
        scopes=[scope for scope in grouped.values() if scope["triggers"]],
    )
    path = Path(path)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".lt-handoff-")
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(document, stream, allow_nan=False)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return document


def run_latest(reference, modes, codes, client, handoff_path, *, config_factory=None, compute=None):
    from lt_recovery import (
        RecoveryRefused,
        _default_config_factory,
        check_recovery_window,
        check_station_codes,
        member_model_names,
    )
    from lt_schedule_query import NON_OPERATIONAL_MODES
    from lt_utils import prepare_long_forecast_records

    reference = dt.date.fromisoformat(str(reference))
    config_factory = config_factory or _default_config_factory
    summary = dict(recovered=0, protected=0, blocked=0, errors=[])
    keys, triggers = {}, set()
    try:
        codes = check_station_codes(codes)
        for mode in dict.fromkeys(modes):
            if mode in NON_OPERATIONAL_MODES:
                continue
            try:
                config = config_factory(mode)
                members = member_model_names(config)
                if not members:
                    raise ValueError("No configured raw members")
                ordered = [m for m in config.get_model_execution_order() if m in members]
                if set(ordered) != set(members):
                    raise ValueError("Incomplete model execution order")
                plan, mode_keys = [], {}
                for model in ordered:
                    issue = latest_issue(
                        reference,
                        config.get_operational_issue_day(),
                        config.get_forecast_months(model),
                    )
                    try:
                        check_recovery_window(pd.Timestamp(issue), pd.Timestamp(reference))
                    except RecoveryRefused:
                        summary["blocked"] += 1
                        continue
                    expected = [expected_key(config, model, issue, code) for code in codes]
                    for key in expected:
                        token = key_tuple(key)
                        if token in mode_keys or token in keys:
                            raise ValueError("Duplicate configured raw identities")
                        mode_keys[token] = key
                    plan.append((model, issue, expected))
                # Publish the complete configured source set, never a prefix of
                # whichever API reads happened to succeed before an exception.
                keys.update(mode_keys)
                cache = {}
                for model, issue, expected in plan:
                    try:
                        selected = {}
                        for key in expected:
                            token = key_tuple(key)
                            current = read_exact(client, key)
                            if healthy(current):
                                summary["protected"] += 1
                                if current["flag"] == 1:
                                    triggers.add(token)
                            elif eligible(current):
                                selected[token] = key
                            else:
                                raise ValueError("Unresolved non-recoverable raw result")
                        if not selected:
                            continue
                        ready = {}
                        for token, key in selected.items():
                            try:
                                for dependency in config.get_model_dependencies().get(model, []):
                                    if not healthy(
                                        read_exact(
                                            client,
                                            expected_key(config, dependency, issue, key["code"]),
                                        )
                                    ):
                                        raise ValueError(
                                            "Required dependency is not healthy at this issue"
                                        )
                                ready[token] = key
                            except Exception as error:
                                summary["errors"].append(
                                    f"{mode}/{model}: {type(error).__name__}: {error}"
                                )
                        selected = ready
                        # Empty station lists disable database scoping; never compute them.
                        if not selected:
                            continue
                        selected_codes = [k["code"] for k in selected.values()]
                        delivered = set()

                        def writer(
                            *,
                            forecast_df,
                            model_name,
                            horizon_type,
                            horizon_value,
                            selected=selected,
                            delivered=delivered,
                            mode=mode,
                            model=model,
                            **unused,
                        ):
                            records = prepare_long_forecast_records(
                                forecast_df, model_name, horizon_type, horizon_value
                            )
                            candidates = {}
                            for record in records:
                                token = key_tuple(record)
                                if token not in selected or token in candidates:
                                    raise ValueError(
                                        "Computed output escaped selected raw identities"
                                    )
                                candidates[token] = record
                            ok = True
                            for token, key in selected.items():
                                try:
                                    record = candidates.get(token)
                                    if record is None or not healthy(record):
                                        raise ValueError(
                                            "Selected model produced no usable forecast"
                                        )
                                    current = read_exact(client, key)
                                    if healthy(current):
                                        delivered.add(token)
                                        if current["flag"] == 1:
                                            triggers.add(token)
                                        continue
                                    if not eligible(current):
                                        raise ValueError("Raw eligibility changed before write")
                                    record = preserve_metadata(dict(record, flag=1), current)
                                    write_verified(client, record)
                                    delivered.add(token)
                                    triggers.add(token)
                                    summary["recovered"] += 1
                                except Exception as error:
                                    ok = False
                                    summary["errors"].append(
                                        f"{mode}/{model}: {type(error).__name__}: {error}"
                                    )
                            return ok

                        success = (
                            compute(config, model, issue, selected_codes, writer)
                            if compute
                            else _compute(config, model, issue, selected_codes, writer, cache)
                        )
                        if not success or delivered != set(selected):
                            raise ValueError("Raw recovery incomplete")
                    except Exception as error:
                        summary["errors"].append(f"{mode}/{model}: {type(error).__name__}: {error}")
            except Exception as error:
                summary["errors"].append(f"{mode}: {type(error).__name__}: {error}")
    except Exception as error:
        summary["errors"].append(f"{type(error).__name__}: {error}")
    finally:
        _handoff(handoff_path, reference, keys, triggers)
    return summary


def main():
    import setup_library as sl
    from lt_recovery import build_postprocessing_client

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--forecast-date", required=True, type=dt.date.fromisoformat)
    parser.add_argument("--handoff", required=True)
    args = parser.parse_args()
    sl.load_environment()
    if os.getenv("SAPPHIRE_API_ENABLED", "true").lower() != "true":
        parser.error("Latest LT recovery requires the API")
    modes = [
        m.strip()
        for m in os.getenv("ieasyhydroforecast_ml_long_term_supported_modes", "").split(",")
        if m.strip()
    ]
    if not modes:
        parser.error("No configured long-term modes")
    station_path = (
        Path(os.environ["ieasyforecast_configuration_path"])
        / os.environ["ieasyforecast_config_file_station_selection"]
    )
    codes = [str(code) for code in json.loads(station_path.read_text()).get("stationsID", [])]
    result = run_latest(
        args.forecast_date,
        modes,
        codes,
        build_postprocessing_client(),
        args.handoff,
    )
    print(json.dumps(result))
    return int(bool(result["errors"]))


if __name__ == "__main__":
    raise SystemExit(main())
