from pydantic import BaseModel, computed_field
from datetime import date as DateType
from typing import Optional, List
from app.models import HorizonType, ModelType


class ForecastBase(BaseModel):
    horizon_type: HorizonType
    code: str
    model_type: ModelType
    date: DateType
    target: Optional[DateType] = None
    flag: Optional[int] = None
    horizon_value: int
    horizon_in_year: int
    composition: Optional[str] = None

    q05: Optional[float] = None
    q25: Optional[float] = None
    q50: Optional[float] = None
    q75: Optional[float] = None
    q95: Optional[float] = None

    forecasted_discharge: Optional[float] = None


class ForecastCreate(ForecastBase):
    pass


class ForecastBulkCreate(BaseModel):
    data: List[ForecastCreate]


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
    flag: Optional[int] = None
    composition: Optional[str] = None

    q: Optional[float] = None
    q_obs: Optional[float] = None
    q_xgb: Optional[float] = None
    q_lgbm: Optional[float] = None
    q_catboost: Optional[float] = None
    q_loc: Optional[float] = None
    q05: Optional[float] = None
    q10: Optional[float] = None
    q25: Optional[float] = None
    q50: Optional[float] = None
    q75: Optional[float] = None
    q90: Optional[float] = None
    q95: Optional[float] = None

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
                    "q95": 140.0
                }
            ]
        }
    }


class LongForecastCreate(LongForecastBase):
    pass


class LongForecastBulkCreate(BaseModel):
    data: List[LongForecastCreate]


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

    discharge_avg: Optional[float] = None
    predictor: Optional[float] = None

    slope: Optional[float] = None
    intercept: Optional[float] = None

    forecasted_discharge: Optional[float] = None

    q_mean: Optional[float] = None
    q_std_sigma: Optional[float] = None
    delta: Optional[float] = None
    rsquared: Optional[float] = None


class LRForecastCreate(LRForecastBase):
    pass


class LRForecastBulkCreate(BaseModel):
    data: List[LRForecastCreate]


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
    composition: Optional[str] = None  # Tracks which models compose ensemble

    sdivsigma: Optional[float] = None
    nse: Optional[float] = None
    delta: Optional[float] = None
    accuracy: Optional[float] = None
    mae: Optional[float] = None
    n_pairs: Optional[float] = None


class SkillMetricCreate(SkillMetricBase):
    pass


class SkillMetricBulkCreate(BaseModel):
    data: List[SkillMetricCreate]


class SkillMetricResponse(SkillMetricBase):
    id: int

    @computed_field
    @property
    def model_type_description(self) -> str:
        return self.model_type.description

    class Config:
        from_attributes = True
