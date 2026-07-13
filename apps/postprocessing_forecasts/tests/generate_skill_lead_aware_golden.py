"""One-off generator for the M1 P0 flag-OFF golden baseline.

Run manually (never picked up by pytest — filename doesn't match
``test_*.py``) to (re)write
``golden/skill_lead_aware_flag_off_baseline.json`` from the current
behavior of ``skill_metrics.py`` / ``ensemble_calculator.py`` on the
synthetic fixtures in ``_skill_lead_aware_golden_fixtures.py``.

Only re-run this deliberately, when a later phase intentionally changes
flag-OFF output (which should never happen — flag-OFF must stay
byte-identical to trunk). Regenerating silently defeats the point of the
snapshot; if ``test_skill_lead_aware_golden_baseline.py`` fails, treat it
as a regression signal first and only regenerate after confirming the
change is intentional and reviewed.

Usage:
    cd apps/postprocessing_forecasts
    SAPPHIRE_TEST_ENV=True python tests/generate_skill_lead_aware_golden.py
"""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

import _skill_lead_aware_golden_fixtures as fx  # noqa: E402

GOLDEN_PATH = os.path.join(
    os.path.dirname(__file__), "golden", "skill_lead_aware_flag_off_baseline.json"
)


def _compute_all_outputs() -> dict:
    from src.ensemble_calculator import (
        create_monthly_ensemble_forecasts,
        create_quarterly_ensemble_forecasts,
        create_seasonal_ensemble_forecasts,
    )
    from src.skill_metrics import (
        calculate_monthly_skill_metrics,
        calculate_quarterly_skill_metrics,
        calculate_seasonal_skill_metrics,
    )

    month_obs = fx.build_monthly_observations()
    month_fc = fx.build_monthly_forecasts()
    month_skill, month_joint, _ = calculate_monthly_skill_metrics(month_obs, month_fc)
    month_ensembles = create_monthly_ensemble_forecasts(month_fc, month_skill)

    quarter_obs = fx.build_quarterly_observations()
    quarter_fc = fx.build_quarterly_forecasts()
    quarter_skill, quarter_joint, _ = calculate_quarterly_skill_metrics(quarter_obs, quarter_fc)
    quarter_ensembles = create_quarterly_ensemble_forecasts(quarter_fc, quarter_skill)

    season_obs = fx.build_seasonal_observations()
    season_fc = fx.build_seasonal_forecasts()
    season_skill, season_joint, _ = calculate_seasonal_skill_metrics(season_obs, season_fc)
    season_ensembles = create_seasonal_ensemble_forecasts(season_fc, season_skill)

    return {
        "month_skill": month_skill,
        "month_joint": month_joint,
        "month_ensembles": month_ensembles,
        "quarter_skill": quarter_skill,
        "quarter_joint": quarter_joint,
        "quarter_ensembles": quarter_ensembles,
        "season_skill": season_skill,
        "season_joint": season_joint,
        "season_ensembles": season_ensembles,
    }


def main() -> None:
    for key, value in fx.THRESHOLD_ENV.items():
        os.environ[key] = value

    outputs = _compute_all_outputs()
    snapshot = {key: fx.canonicalize(df) for key, df in outputs.items()}

    os.makedirs(os.path.dirname(GOLDEN_PATH), exist_ok=True)
    with open(GOLDEN_PATH, "w") as handle:
        json.dump(snapshot, handle, indent=2, sort_keys=True)
        handle.write("\n")

    print(f"Wrote golden baseline to {GOLDEN_PATH}")
    for key, records in snapshot.items():
        print(f"  {key}: {len(records)} rows")


if __name__ == "__main__":
    main()
