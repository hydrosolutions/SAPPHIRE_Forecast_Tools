from datetime import date as DateType

from pydantic import BaseModel, ConfigDict, computed_field

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


class ForecastCreate(ForecastBase):
    pass


class ForecastBulkCreate(BaseModel):
    data: list[ForecastCreate]


class ForecastResponse(ForecastBase):
    model_config = ConfigDict(from_attributes=True)

    id: int

    @computed_field
    @property
    def model_type_description(self) -> str:
        return self.model_type.description


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
    model_config = ConfigDict(from_attributes=True)

    id: int

    @computed_field
    @property
    def model_type_description(self) -> str:
        return self.model_type.description


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
    model_config = ConfigDict(from_attributes=True)

    id: int


class SkillMetricBase(BaseModel):
    horizon_type: HorizonType
    code: str
    model_type: ModelType
    date: DateType
    horizon_in_year: int
    composition: str | None = None

    sdivsigma: float | None = None
    nse: float | None = None
    delta: float | None = None
    accuracy: float | None = None
    mae: float | None = None
    n_pairs: int | None = None
    crps: float | None = None
    pbias: float | None = None
    kgelf: float | None = None
    nse_log: float | None = None
    fhv: float | None = None
    flv: float | None = None


class SkillMetricCreate(SkillMetricBase):
    pass


class SkillMetricBulkCreate(BaseModel):
    data: list[SkillMetricCreate]


class SkillMetricResponse(SkillMetricBase):
    model_config = ConfigDict(from_attributes=True)

    id: int

    @computed_field
    @property
    def model_type_description(self) -> str:
        return self.model_type.description
