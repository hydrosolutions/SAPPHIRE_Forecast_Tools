"""M1 P0 flag-OFF golden-baseline regression test.

Pins the CURRENT trunk output (as of `develop_pp_lead_aware_skill` base
9a1d4b17, which already ships PP-038 monthly per-lead skill/ensemble
stratification) of the key skill/ensemble entry points on a synthetic
fixture, so P1+ can assert flag-OFF reproduces this byte-for-byte while
building the new per-lead aggregated (quarter/season) behavior behind
``SAPPHIRE_SKILL_LEAD_AWARE``.

No behavior is gated on the flag yet (P0 is audit + scaffold only) — this
test currently just documents "this is what trunk does today" and will
start actually discriminating flag-OFF once P1 wires the flag into these
call sites.

If this test fails on a branch that is NOT intentionally changing
flag-OFF behavior, treat it as a regression. Regenerate the snapshot only
via ``generate_skill_lead_aware_golden.py`` after confirming the change
is deliberate and reviewed.
"""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

import _skill_lead_aware_golden_fixtures as fx
import pytest
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

GOLDEN_PATH = os.path.join(
    os.path.dirname(__file__), "golden", "skill_lead_aware_flag_off_baseline.json"
)


@pytest.fixture(autouse=True)
def _pin_threshold_env(monkeypatch):
    for key, value in fx.THRESHOLD_ENV.items():
        monkeypatch.setenv(key, value)


@pytest.fixture(scope="module")
def golden() -> dict:
    with open(GOLDEN_PATH) as handle:
        return json.load(handle)


def test_golden_file_exists():
    assert os.path.exists(GOLDEN_PATH), (
        f"Golden baseline missing at {GOLDEN_PATH}; regenerate with "
        f"tests/generate_skill_lead_aware_golden.py"
    )


def test_golden_baseline_is_non_trivial(golden):
    """Sanity check: the fixture must actually exercise EM/Naive/Skilled Mean.

    An all-empty golden file would make every snapshot assertion below
    vacuously true and defeat the purpose of the regression pin.
    """
    non_empty_keys = [key for key, records in golden.items() if records]
    assert len(non_empty_keys) >= 6, (
        f"Expected most of the 9 snapshot keys to be non-empty, got only "
        f"{non_empty_keys}. The synthetic fixture may no longer clear the "
        f"min-n_pairs / skill thresholds."
    )


@pytest.mark.parametrize(
    "key",
    [
        "month_skill",
        "month_joint",
        "month_ensembles",
        "quarter_skill",
        "quarter_joint",
        "quarter_ensembles",
        "season_skill",
        "season_joint",
        "season_ensembles",
    ],
)
def test_flag_off_matches_golden_baseline(golden, key):
    outputs = _compute_all_outputs()
    actual = fx.canonicalize(outputs[key])
    expected = golden[key]
    assert actual == expected, (
        f"{key} snapshot diverged from the flag-OFF golden baseline. "
        f"If this is an intentional flag-OFF behavior change, that is "
        f"itself a bug per the M1 plan (flag-OFF must stay byte-identical "
        f"to trunk) — stop and escalate rather than regenerating."
    )


def _compute_all_outputs() -> dict:
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
