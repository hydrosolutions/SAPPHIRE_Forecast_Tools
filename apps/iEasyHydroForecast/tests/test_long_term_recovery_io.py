"""Strict LT I/O contracts. All station identities and values are synthetic."""

import copy

import pytest
from long_term_recovery_io import (
    eligible,
    equivalent,
    healthy,
    preserve_metadata,
    read_exact,
    write_verified,
)

KEY = dict(
    horizon_type="month",
    horizon_value=0,
    code="90001",
    date="2026-11-10",
    model_type="GBT",
    valid_from="2026-11-01",
    valid_to="2026-11-30",
)


class Client:
    def __init__(self, rows):
        self.rows = rows
        self.pages = []

    def read_long_term_forecasts(self, **query):
        self.pages.append(query["skip"])
        return copy.deepcopy(self.rows[query["skip"] : query["skip"] + query["limit"]])

    def write_long_forecasts(self, records):
        return len(records)  # Deliberately lies about delivery.


def test_quantile_only_is_protected_and_observation_is_not_forecast():
    assert healthy(dict(KEY, flag=0, q75=3.0))
    assert not eligible(dict(KEY, flag=2, q75=3.0))
    assert eligible(dict(KEY, flag=2, q_obs=3.0))
    assert eligible(None)
    assert not eligible(dict(KEY, flag=0))


@pytest.mark.parametrize("patch", [{"flag": 7}, {"q": "3"}, {"q": True}, {"q": float("inf")}])
def test_uncertain_values_fail_closed(patch):
    with pytest.raises(ValueError):
        healthy(dict(KEY, flag=0, **patch) if "flag" not in patch else dict(KEY, **patch))


@pytest.mark.parametrize(
    "field,value",
    [
        ("date", "2026-10-10"),
        ("valid_to", "2026-11-29"),
        ("horizon_value", 1),
        ("model_type", "LR_BASE"),
        ("code", "90002"),
    ],
)
def test_query_filters_are_not_trusted(field, value):
    wrong = dict(KEY, flag=0, q=3.0)
    wrong[field] = value
    with pytest.raises(ValueError, match="identity mismatch"):
        read_exact(Client([wrong]), KEY)


def test_duplicate_and_pagination_are_checked():
    client = Client([dict(KEY, flag=0, q=3.0)] * 101)
    with pytest.raises(ValueError, match="Duplicate"):
        read_exact(client, KEY)
    assert client.pages == [0, 100]


def test_positive_count_does_not_prove_delivery():
    with pytest.raises(ValueError, match="read-back"):
        write_verified(Client([]), dict(KEY, flag=1, q=3.0))


def test_metadata_and_omitted_fields_are_verified():
    record = preserve_metadata(dict(KEY, flag=1, q=3.0), dict(KEY, q_obs=2.0, composition="old"))
    assert record["q_obs"] == 2.0 and record["composition"] == "old"
    assert equivalent(dict(record, q05=None), record)
    assert not equivalent(dict(record, q_obs=None), record)
    assert not equivalent(dict(record, q75=9.0), record)
