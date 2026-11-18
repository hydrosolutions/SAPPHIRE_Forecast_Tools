"""Rebuild only current periods from verified recovery audits; no historical scan."""

import argparse
import datetime as dt
import json
import os

from postprocessing_maintenance import DECAD, PENTAD, _read_station_codes, sl
from src.recovered_periods import consume_pending_recoveries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--forecast-date", required=True, type=dt.date.fromisoformat)
    args = parser.parse_args()
    sl.load_environment()
    mode = os.getenv("SAPPHIRE_PREDICTION_MODE", "") or "BOTH"
    if mode not in {"PENTAD", "DECAD", "BOTH"}:
        parser.error("Invalid SAPPHIRE_PREDICTION_MODE")
    summary = {"completed": 0, "errors": []}
    for config in (PENTAD, DECAD):
        if mode not in (config.name.upper(), "BOTH"):
            continue
        try:
            result = consume_pending_recoveries(
                config,
                args.forecast_date,
                codes=_read_station_codes(config),
                latest_only=True,
            )
            summary["completed"] += result["completed"]
            summary["errors"].extend(result["errors"])
        except Exception as error:
            summary["errors"].append(f"{config.name}: {type(error).__name__}: {error}")
    print(json.dumps(summary))
    return 1 if summary["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
