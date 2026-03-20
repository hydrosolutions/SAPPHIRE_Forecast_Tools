#!/usr/bin/env python3
"""Generate deterministic fixture JSON files for forecast_dashboard tests.

All values are hand-calculable so tests can assert on exact numbers.

Stations:
    99001 — TFT best, LR available
    99002 — TiDE best, LR available
    99003 — LR only (no ML models)

Run:
    cd apps/forecast_dashboard/tests
    python generate_dashboard_test_data.py
"""

import json
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent / "fixtures"

STATIONS = ["99001", "99002", "99003"]
MODELS = ["TFT", "TiDE", "TSMixer"]
MODEL_DESCRIPTIONS = {
    "TFT": "Temporal Fusion Transformer (TFT)",
    "TiDE": "Temporal Improved Diverse Ensemble (TiDE)",
    "TSMixer": "TSMixer",
}

# Base forecasted discharge values per station per model (hand-calculable)
FC_BASE = {
    "99001": {"TFT": 10.0, "TiDE": 11.0, "TSMixer": 12.0},
    "99002": {"TFT": 20.0, "TiDE": 22.0, "TSMixer": 18.0},
    # 99003 has no ML models
}

# Quantile offsets (symmetric around E[Q])
Q_OFFSETS = {"q05": -3.0, "q25": -1.5, "q75": 1.5, "q95": 3.0}


def _ml_row(code, model, forecast_date, target_date, pentad_in_year):
    base = FC_BASE[code][model]
    return {
        "id": None,
        "horizon_type": "pentad",
        "horizon_value": (pentad_in_year - 1) % 6 + 1,
        "horizon_in_year": pentad_in_year,
        "code": code,
        "model_type": model,
        "model_type_description": MODEL_DESCRIPTIONS[model],
        "date": forecast_date,
        "target": target_date,
        "forecasted_discharge": base,
        "q05": base + Q_OFFSETS["q05"],
        "q25": base + Q_OFFSETS["q25"],
        "q50": base,
        "q75": base + Q_OFFSETS["q75"],
        "q95": base + Q_OFFSETS["q95"],
        "flag": 0,
        "composition": None,
    }


def generate_forecast_response():
    """ML forecast API response (/postprocessing/forecast/)."""
    rows = []
    forecast_date = "2026-03-01"
    target_date = "2026-03-06"
    pentad_in_year = 13

    for code in ["99001", "99002"]:
        for model in MODELS:
            rows.append(_ml_row(code, model, forecast_date, target_date, pentad_in_year))
    return rows


def generate_lr_forecast_response():
    """LR forecast API response (/postprocessing/lr-forecast/)."""
    rows = []
    forecast_date = "2026-03-01"
    pentad_in_year = 13

    lr_base = {"99001": 9.5, "99002": 19.0, "99003": 15.0}
    for code in STATIONS:
        rows.append(
            {
                "id": None,
                "horizon_type": "pentad",
                "horizon_value": (pentad_in_year - 1) % 6 + 1,
                "horizon_in_year": pentad_in_year,
                "code": code,
                "date": forecast_date,
                "discharge_avg": lr_base[code] - 0.5,
                "predictor": lr_base[code] - 1.0,
                "slope": 1.1,
                "intercept": 0.5,
                "forecasted_discharge": lr_base[code],
                "q_mean": lr_base[code] - 0.2,
                "q_std_sigma": 1.5,
                "delta": 1.0,
                "rsquared": 0.85,
            }
        )
    return rows


def generate_skill_metric_response():
    """Skill metric API response (/postprocessing/skill-metric/)."""
    rows = []
    pentad_in_year = 13
    date_str = "2026-03-01"

    # accuracy values: 99001 TFT best (90), 99002 TiDE best (92)
    skill_data = {
        "99001": {
            "TFT": {"acc": 90.0, "sdiv": 0.50, "nse": 0.80, "mae": 1.0},
            "TiDE": {"acc": 85.0, "sdiv": 0.60, "nse": 0.75, "mae": 1.2},
            "TSMixer": {"acc": 80.0, "sdiv": 0.70, "nse": 0.70, "mae": 1.5},
            "NE": {"acc": 88.0, "sdiv": 0.55, "nse": 0.78, "mae": 1.1},
        },
        "99002": {
            "TFT": {"acc": 82.0, "sdiv": 0.65, "nse": 0.72, "mae": 1.4},
            "TiDE": {"acc": 92.0, "sdiv": 0.45, "nse": 0.85, "mae": 0.8},
            "TSMixer": {"acc": 78.0, "sdiv": 0.72, "nse": 0.68, "mae": 1.6},
            "NE": {"acc": 86.0, "sdiv": 0.52, "nse": 0.80, "mae": 1.0},
        },
    }
    model_descriptions = {
        **MODEL_DESCRIPTIONS,
        "NE": "Neural Ensemble (NE)",
    }

    for code, models in skill_data.items():
        for model, metrics in models.items():
            rows.append(
                {
                    "id": None,
                    "horizon_type": "pentad",
                    "horizon_in_year": pentad_in_year,
                    "code": code,
                    "model_type": model,
                    "model_type_description": model_descriptions[model],
                    "date": date_str,
                    "sdivsigma": metrics["sdiv"],
                    "nse": metrics["nse"],
                    "delta": 1.0,
                    "accuracy": metrics["acc"],
                    "mae": metrics["mae"],
                    "n_pairs": 12,
                    "crps": None,
                    "pbias": None,
                    "kgelf": None,
                    "nse_log": None,
                    "fhv": None,
                    "flv": None,
                }
            )
    return rows


def main():
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

    fixtures = {
        "forecast_response.json": generate_forecast_response(),
        "lr_forecast_response.json": generate_lr_forecast_response(),
        "skill_metric_response.json": generate_skill_metric_response(),
    }

    for filename, data in fixtures.items():
        path = FIXTURES_DIR / filename
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        print(f"  wrote {path} ({len(data)} rows)")


if __name__ == "__main__":
    main()
