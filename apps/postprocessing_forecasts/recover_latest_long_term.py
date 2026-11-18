"""Rebuild only producer-authorized latest LT scopes; no ordinary maintenance."""

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "iEasyHydroForecast"))

import setup_library as sl
from sapphire_api_client import SapphirePostprocessingClient
from src.recovered_long_term import consume_handoff


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--forecast-date", required=True, type=dt.date.fromisoformat)
    parser.add_argument("--handoff", required=True)
    args = parser.parse_args()
    sl.load_environment()
    if os.getenv("SAPPHIRE_API_ENABLED", "true").lower() != "true":
        parser.error("Latest LT recovery requires the API")
    client = SapphirePostprocessingClient(
        base_url=os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
    )
    result = consume_handoff(args.handoff, args.forecast_date, client)
    print(json.dumps(result))
    return int(bool(result["errors"]))


if __name__ == "__main__":
    raise SystemExit(main())
