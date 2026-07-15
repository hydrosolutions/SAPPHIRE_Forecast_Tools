"""
Tests for ModelType.coerce and the schema-level model_type coercion.

Background: the DB column stores the enum NAME (e.g. "TIDE") while the
API schema validates the enum VALUE (e.g. "TiDE"). The data migrator posts
model_short (an upper-case NAME, e.g. "TIDE") straight through, which used
to 422 against the ModelType schema field. ModelType.coerce() plus a
field_validator(mode="before") on the affected base schemas resolves the
value-form, the name-form, and any case-insensitive variant of either,
while still rejecting genuinely unknown models.
"""

import pytest
from app.models import ModelType
from app.schemas import (
    BulletinCreate,
    ForecastCreate,
    LongForecastCreate,
    SkillMetricCreate,
)
from pydantic import ValidationError

STATION_CODE = "19999"


# ---------------------------------------------------------------------------
# ModelType.coerce() — unit tests
# ---------------------------------------------------------------------------


class TestCoerceFromValue:
    """The happy path (exact enum VALUE match) must be untouched."""

    @pytest.mark.parametrize("member", list(ModelType))
    def test_value_round_trips(self, member):
        assert ModelType.coerce(member.value) is member


class TestCoerceFromName:
    """Exact enum NAME match (how the DB / model_short spells it)."""

    @pytest.mark.parametrize("member", list(ModelType))
    def test_name_resolves(self, member):
        assert ModelType.coerce(member.name) is member


class TestCoerceCaseInsensitive:
    """Case-insensitive match against either value or name."""

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("tide", ModelType.TIDE),
            ("Tide", ModelType.TIDE),
            ("TIDE", ModelType.TIDE),
            ("tsmixer", ModelType.TSMIXER),
            ("TsMixer", ModelType.TSMIXER),
            ("lr_base", ModelType.LR_BASE),
            ("Lr_Base", ModelType.LR_BASE),
            ("sm_gbt_norm", ModelType.SM_GBT_NORM),
            ("SM_GBT_NORM", ModelType.SM_GBT_NORM),
            ("em", ModelType.ENSEMBLE_MEAN),
            ("ne", ModelType.NEURAL_ENSEMBLE),
        ],
    )
    def test_case_insensitive_resolves(self, raw, expected):
        assert ModelType.coerce(raw) is expected


class TestCoerceMultiWordMembers:
    """Multi-word values ("Skilled Mean", "Naive Mean") must not break."""

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("Skilled Mean", ModelType.SKILLED_MEAN),
            ("skilled mean", ModelType.SKILLED_MEAN),
            ("SKILLED MEAN", ModelType.SKILLED_MEAN),
            ("SKILLED_MEAN", ModelType.SKILLED_MEAN),  # NAME form
            ("skilled_mean", ModelType.SKILLED_MEAN),  # NAME form, lowered
            ("Naive Mean", ModelType.NAIVE_MEAN),
            ("naive mean", ModelType.NAIVE_MEAN),
            ("NAIVE_MEAN", ModelType.NAIVE_MEAN),
        ],
    )
    def test_multiword_resolves(self, raw, expected):
        assert ModelType.coerce(raw) is expected


class TestCoerceUnknown:
    """Anything unresolvable must NOT be silently mapped to a default."""

    def test_unknown_model_returned_unchanged(self):
        # coerce() hands back the raw value so Pydantic's normal enum
        # validation still rejects it -- it must not raise or substitute
        # a default member itself.
        assert ModelType.coerce("NOT_A_MODEL") == "NOT_A_MODEL"

    def test_already_a_member_passes_through(self):
        assert ModelType.coerce(ModelType.TIDE) is ModelType.TIDE

    def test_none_returned_unchanged(self):
        # Non-string, non-member input must be passed through untouched
        # so downstream validation (not coerce) handles the error.
        assert ModelType.coerce(None) is None


# ---------------------------------------------------------------------------
# Schema-level tests: raw name-form input must validate where it used to 422
# ---------------------------------------------------------------------------


class TestForecastSchemaCoercion:
    def _payload(self, **overrides):
        from datetime import date

        defaults = {
            "horizon_type": "pentad",
            "code": STATION_CODE,
            "model_type": "TIDE",  # NAME form -- previously 422
            "date": date(2024, 6, 15),
            "target": date(2024, 6, 20),
            "horizon_value": 3,
            "horizon_in_year": 33,
        }
        defaults.update(overrides)
        return defaults

    def test_name_form_validates(self):
        obj = ForecastCreate(**self._payload())
        assert obj.model_type is ModelType.TIDE

    def test_value_form_still_validates(self):
        obj = ForecastCreate(**self._payload(model_type="TiDE"))
        assert obj.model_type is ModelType.TIDE

    def test_unknown_model_still_rejected(self):
        with pytest.raises(ValidationError):
            ForecastCreate(**self._payload(model_type="NOT_A_MODEL"))


class TestLongForecastSchemaCoercion:
    def _payload(self, **overrides):
        from datetime import date

        defaults = {
            "horizon_type": "month",
            "horizon_value": 1,
            "code": STATION_CODE,
            "date": date(2024, 6, 15),
            "model_type": "TSMIXER",  # NAME form
            "valid_from": date(2024, 7, 1),
            "valid_to": date(2024, 7, 31),
        }
        defaults.update(overrides)
        return defaults

    def test_name_form_validates(self):
        obj = LongForecastCreate(**self._payload())
        assert obj.model_type is ModelType.TSMIXER

    def test_unknown_model_still_rejected(self):
        with pytest.raises(ValidationError):
            LongForecastCreate(**self._payload(model_type="NOT_A_MODEL"))


class TestSkillMetricSchemaCoercion:
    def _payload(self, **overrides):
        from datetime import date

        defaults = {
            "horizon_type": "pentad",
            "code": STATION_CODE,
            "model_type": "SM_GBT_NORM",  # NAME form
            "date": date(2024, 6, 15),
            "horizon_in_year": 33,
        }
        defaults.update(overrides)
        return defaults

    def test_name_form_validates(self):
        obj = SkillMetricCreate(**self._payload())
        assert obj.model_type is ModelType.SM_GBT_NORM

    def test_unknown_model_still_rejected(self):
        with pytest.raises(ValidationError):
            SkillMetricCreate(**self._payload(model_type="NOT_A_MODEL"))


class TestBulletinSchemaCoercion:
    def _payload(self, **overrides):
        defaults = {
            "horizon_type": "pentad",
            "year": 2024,
            "horizon_value": 3,
            "code": STATION_CODE,
            "model_type": "SKILLED_MEAN",  # NAME form
        }
        defaults.update(overrides)
        return defaults

    def test_name_form_validates(self):
        obj = BulletinCreate(**self._payload())
        assert obj.model_type is ModelType.SKILLED_MEAN

    def test_unknown_model_still_rejected(self):
        with pytest.raises(ValidationError):
            BulletinCreate(**self._payload(model_type="NOT_A_MODEL"))


# ---------------------------------------------------------------------------
# model_type_description must still resolve correctly after coercion
# ---------------------------------------------------------------------------


class TestModelTypeDescriptionAfterCoercion:
    def test_description_after_name_form_coercion(self):
        from datetime import date

        obj = ForecastCreate(
            horizon_type="pentad",
            code=STATION_CODE,
            model_type="TIDE",  # NAME form
            date=date(2024, 6, 15),
            horizon_value=3,
            horizon_in_year=33,
        )
        assert obj.model_type.description == "Time-Series Dense Encoder (TIDE)"


# ---------------------------------------------------------------------------
# End-to-end HTTP tests: raw NAME-form model_type must validate through the
# full request -> Pydantic -> CRUD -> response path, where it used to 422.
# ---------------------------------------------------------------------------


class TestForecastEndpointCoercion:
    def test_post_with_name_form_model_type_succeeds(self, client):
        payload = {
            "data": [
                {
                    "horizon_type": "pentad",
                    "code": STATION_CODE,
                    "model_type": "TIDE",  # NAME form -- previously 422
                    "date": "2024-06-15",
                    "target": "2024-06-20",
                    "horizon_value": 3,
                    "horizon_in_year": 33,
                    "forecasted_discharge": 100.0,
                }
            ]
        }
        resp = client.post("/forecast/", json=payload)
        assert resp.status_code == 201
        body = resp.json()
        assert body[0]["model_type"] == "TiDE"

    def test_post_with_unknown_model_type_still_422(self, client):
        payload = {
            "data": [
                {
                    "horizon_type": "pentad",
                    "code": STATION_CODE,
                    "model_type": "NOT_A_MODEL",
                    "date": "2024-06-15",
                    "target": "2024-06-20",
                    "horizon_value": 3,
                    "horizon_in_year": 33,
                    "forecasted_discharge": 100.0,
                }
            ]
        }
        resp = client.post("/forecast/", json=payload)
        assert resp.status_code == 422


class TestLongForecastEndpointCoercion:
    def test_post_with_name_form_model_type_succeeds(self, client):
        payload = {
            "data": [
                {
                    "horizon_type": "month",
                    "horizon_value": 1,
                    "code": STATION_CODE,
                    "date": "2024-06-15",
                    "model_type": "TSMIXER",  # NAME form
                    "valid_from": "2024-07-01",
                    "valid_to": "2024-07-31",
                }
            ]
        }
        resp = client.post("/long-forecast/", json=payload)
        assert resp.status_code == 201
        body = resp.json()
        assert body[0]["model_type"] == "TSMixer"


class TestSkillMetricEndpointCoercion:
    def test_post_with_name_form_model_type_succeeds(self, client):
        payload = {
            "data": [
                {
                    "horizon_type": "pentad",
                    "code": STATION_CODE,
                    "model_type": "SM_GBT_NORM",  # NAME form
                    "date": "2024-06-15",
                    "horizon_in_year": 33,
                }
            ]
        }
        resp = client.post("/skill-metric/", json=payload)
        assert resp.status_code == 201
        body = resp.json()
        assert body[0]["model_type"] == "SM_GBT_Norm"


class TestBulletinEndpointCoercion:
    def test_post_with_name_form_model_type_succeeds(self, client):
        payload = {
            "data": [
                {
                    "horizon_type": "pentad",
                    "year": 2024,
                    "horizon_value": 3,
                    "code": STATION_CODE,
                    "model_type": "SKILLED_MEAN",  # NAME form
                }
            ]
        }
        resp = client.post("/bulletin/", json=payload)
        assert resp.status_code == 201
        body = resp.json()
        assert body[0]["model_type"] == "Skilled Mean"
