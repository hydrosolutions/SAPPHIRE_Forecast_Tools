from pydantic import BaseModel
from datetime import date as DateType
from typing import Optional, List
from app.models import HorizonType, ModelType


class ForecastBase(BaseModel):
    horizon_type: HorizonType
    code: str
    model_type: ModelType
    date: DateType
    target: Optional[DateType]
    flag: Optional[int]
    horizon_value: int
    horizon_in_year: int

    q05: Optional[float]
    q25: Optional[float]
    q50: Optional[float]
    q75: Optional[float]
    q95: Optional[float]

    forecasted_discharge: Optional[float]


class ForecastCreate(ForecastBase):
    pass


class ForecastBulkCreate(BaseModel):
    data: List[ForecastCreate]


class ForecastResponse(ForecastBase):
    id: int

    class Config:
        from_attributes = True


class LRForecastBase(BaseModel):
    horizon_type: HorizonType
    code: str
    date: DateType

    discharge: Optional[float]
    predictor: Optional[float]
    horizon_value: int
    horizon_in_year: int

    slope: Optional[float]
    intercept: Optional[float]

    forecasted_discharge: Optional[float]

    q_mean: Optional[float]
    q_std_sigma: Optional[float]
    delta: Optional[float]
    rsquared: Optional[float]


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

    sdivsigma: Optional[float]
    nse: Optional[float]
    delta: Optional[float]
    accuracy: Optional[float]
    mae: Optional[float]
    n_pairs: Optional[float]


class SkillMetricCreate(SkillMetricBase):
    pass


class SkillMetricBulkCreate(BaseModel):
    data: List[SkillMetricCreate]


class SkillMetricResponse(SkillMetricBase):
    id: int

    class Config:
        from_attributes = True
