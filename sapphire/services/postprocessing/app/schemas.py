import math
from datetime import date as DateType
from datetime import datetime

from pydantic import BaseModel, computed_field, field_validator, model_validator

from app.models import HorizonType, ModelType


class ForecastBase(BaseModel):
    horizon_type: HorizonType
    code: str
    model_type: ModelType
    date: DateType
    target: DateType | None = None
    flag: int | None = None
    horizon_value: int
    horizon_in_year: int
    composition: str | None = None

    q05: float | None = None
    q25: float | None = None
    # q50: Optional[float] = None
    q75: float | None = None
    q95: float | None = None

    forecasted_discharge: float | None = None

    @field_validator("model_type", mode="before")
    @classmethod
    def _coerce_model_type(cls, value):
        return ModelType.coerce(value)


class ForecastCreate(ForecastBase):
    pass


class ForecastBulkCreate(BaseModel):
    data: list[ForecastCreate]


class ForecastResponse(ForecastBase):
    id: int

    @computed_field
    @property
    def model_type_description(self) -> str:
        return self.model_type.description

    class Config:
        from_attributes = True


class LongForecastBase(BaseModel):
    horizon_type: HorizonType
    horizon_value: int
    code: str
    date: DateType
    model_type: ModelType
    valid_from: DateType
    valid_to: DateType
    flag: int | None = None
    composition: str | None = None

    q: float | None = None
    q_obs: float | None = None
    q_xgb: float | None = None
    q_lgbm: float | None = None
    q_catboost: float | None = None
    q_loc: float | None = None
    q05: float | None = None
    q10: float | None = None
    q25: float | None = None
    q50: float | None = None
    q75: float | None = None
    q90: float | None = None
    q95: float | None = None

    @field_validator("model_type", mode="before")
    @classmethod
    def _coerce_model_type(cls, value):
        return ModelType.coerce(value)

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "horizon_type": "month",
                    "horizon_value": 1,
                    "code": "15013",
                    "date": "2026-01-22",
                    "model_type": "GBT",
                    "valid_from": "2026-02-01",
                    "valid_to": "2026-02-28",
                    "flag": 0,
                    "composition": "",
                    "q": 123.45,
                    "q_obs": 120.0,
                    "q_xgb": 125.0,
                    "q_lgbm": 124.0,
                    "q_catboost": 123.0,
                    "q_loc": 122.0,
                    "q05": 100.0,
                    "q10": 110.0,
                    "q25": 115.0,
                    "q50": 123.0,
                    "q75": 130.0,
                    "q90": 135.0,
                    "q95": 140.0,
                }
            ]
        }
    }


class LongForecastCreate(LongForecastBase):
    pass


class LongForecastBulkCreate(BaseModel):
    data: list[LongForecastCreate]


class LongForecastResponse(LongForecastBase):
    id: int

    @computed_field
    @property
    def model_type_description(self) -> str:
        return self.model_type.description

    class Config:
        from_attributes = True


class LRForecastBase(BaseModel):
    horizon_type: HorizonType
    code: str
    date: DateType
    horizon_value: int
    horizon_in_year: int

    discharge_avg: float | None = None
    predictor: float | None = None

    slope: float | None = None
    intercept: float | None = None

    forecasted_discharge: float | None = None

    q_mean: float | None = None
    q_std_sigma: float | None = None
    delta: float | None = None
    rsquared: float | None = None


class LRForecastCreate(LRForecastBase):
    pass


class LRForecastBulkCreate(BaseModel):
    data: list[LRForecastCreate]


class LRForecastResponse(LRForecastBase):
    id: int

    class Config:
        from_attributes = True


class SkillMetricBase(BaseModel):
    horizon_type: HorizonType
    code: str
    model_type: ModelType
    date: DateType
    horizon_in_year: int
    horizon_value: int | None = None  # PP-038: lead for month skill; sentinel 0 for all other horizons
    composition: str | None = None

    sdivsigma: float | None = None
    nse: float | None = None
    delta: float | None = None
    accuracy: float | None = None
    mae: float | None = None
    n_pairs: float | None = None
    crps: float | None = None
    pbias: float | None = None
    kgelf: float | None = None
    nse_log: float | None = None
    fhv: float | None = None
    flv: float | None = None

    @field_validator("model_type", mode="before")
    @classmethod
    def _coerce_model_type(cls, value):
        return ModelType.coerce(value)


class SkillMetricCreate(SkillMetricBase):
    pass


class SkillMetricBulkCreate(BaseModel):
    data: list[SkillMetricCreate]


class SkillMetricResponse(SkillMetricBase):
    id: int

    @computed_field
    @property
    def model_type_description(self) -> str:
        return self.model_type.description

    class Config:
        from_attributes = True


class QuarterlySnapshot(BaseModel):
    """Frozen quarterly forecast copy stored on a monthly bulletin row."""

    model_type: ModelType
    valid_from: DateType
    valid_to: DateType
    forecasted_discharge: float | None
    fc_lower: float | None
    fc_upper: float | None

    @field_validator("model_type", mode="before")
    @classmethod
    def _coerce_model_type(cls, value):
        return ModelType.coerce(value)

    @field_validator("forecasted_discharge", "fc_lower", "fc_upper", mode="after")
    @classmethod
    def _reject_nonfinite(cls, v):
        if v is not None and not math.isfinite(v):
            raise ValueError("numeric bound must be finite")
        return v

    @model_validator(mode="after")
    def _check_quarter_period_and_bounds(self) -> "QuarterlySnapshot":
        Q_STARTS = {(1, 1), (4, 1), (7, 1), (10, 1)}
        Q_ENDS = {(3, 31), (6, 30), (9, 30), (12, 31)}
        Q_NUM = {1: 1, 2: 1, 3: 1, 4: 2, 5: 2, 6: 2, 7: 3, 8: 3, 9: 3, 10: 4, 11: 4, 12: 4}

        vf, vt = self.valid_from, self.valid_to
        if (vf.month, vf.day) not in Q_STARTS:
            raise ValueError(
                "valid_from must be the first day of a calendar quarter (Jan 1, Apr 1, Jul 1, Oct 1)"
            )
        if (vt.month, vt.day) not in Q_ENDS:
            raise ValueError(
                "valid_to must be the last day of a calendar quarter (Mar 31, Jun 30, Sep 30, Dec 31)"
            )
        if Q_NUM[vf.month] != Q_NUM[vt.month] or vf.year != vt.year:
            raise ValueError("valid_from and valid_to must span the same calendar quarter")

        lo, hi = self.fc_lower, self.fc_upper
        if (lo is None) != (hi is None):
            raise ValueError("fc_lower and fc_upper must both be null or both be provided")
        if lo is not None and hi is not None and lo > hi:
            raise ValueError("fc_lower must be <= fc_upper")

        return self

    model_config = {"from_attributes": True}


class BulletinBase(BaseModel):
    horizon_type: HorizonType
    year: int
    horizon_value: int
    code: str
    model_type: ModelType

    basin_name: str | None = None
    station_label: str | None = None
    forecasted_discharge: float | None = None
    fc_lower: float | None = None
    fc_upper: float | None = None
    delta: float | None = None
    sdivsigma: float | None = None
    mae: float | None = None
    accuracy: float | None = None
    quarterly_snapshot: QuarterlySnapshot | None = None

    @field_validator("model_type", mode="before")
    @classmethod
    def _coerce_model_type(cls, value):
        return ModelType.coerce(value)

    @model_validator(mode="after")
    def _snapshot_only_on_month(self) -> "BulletinBase":
        if self.quarterly_snapshot is not None and self.horizon_type != HorizonType.MONTH:
            raise ValueError("quarterly_snapshot is only valid on month-horizon bulletin rows")
        return self


class BulletinCreate(BulletinBase):
    pass


class BulletinBulkCreate(BaseModel):
    data: list[BulletinCreate]


class BulletinResponse(BulletinBase):
    id: int

    class Config:
        from_attributes = True


class BulletinShareCreate(BaseModel):
    horizon: HorizonType
    year: int
    horizon_value: int
    expires_at: datetime
    payload: dict
    station_codes: list[str] | None = None


class BulletinShareCreateResponse(BaseModel):
    token: str
    url: str
    expires_at: datetime


class BulletinSharePublicResponse(BaseModel):
    # returned verbatim to third parties
    payload: dict
    expires_at: datetime


class LRVisibilityBase(BaseModel):
    horizon_type: HorizonType
    code: str
    month: int
    horizon_value: int
    year: int
    visible: bool


class LRVisibilityCreate(LRVisibilityBase):
    pass


class LRVisibilityBulkCreate(BaseModel):
    data: list[LRVisibilityCreate]


class LRVisibilityResponse(LRVisibilityBase):
    id: int

    class Config:
        from_attributes = True
