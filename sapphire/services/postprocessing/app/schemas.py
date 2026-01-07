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
