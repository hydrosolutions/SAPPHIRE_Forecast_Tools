from pydantic import BaseModel, Field
from datetime import date as DateType
from typing import List, Optional
from app.models import HorizonType


class RunoffBase(BaseModel):
    code: str = Field(..., max_length=10, example="12345", description="Unique station code")
    date: DateType = Field(..., example="2000-01-01")
    discharge: float = Field(..., ge=0, description="Discharge value (must be >= 0)")
    predictor: Optional[float] = Field(None, ge=0)
    horizon_type: HorizonType
    horizon_value: int = Field(..., ge=1)
    horizon_in_year: int = Field(..., ge=1, le=366)


class RunoffCreate(RunoffBase):
    pass


class RunoffBulkCreate(BaseModel):
    data: List[RunoffCreate] = Field(..., description="List of runoff records to create")


class RunoffUpdate(BaseModel):
    code: Optional[str] = None
    date: Optional[DateType] = None
    discharge: Optional[float] = None
    predictor: Optional[float] = None
    horizon_type: Optional[HorizonType] = None
    horizon_value: Optional[int] = None
    horizon_in_year: Optional[int] = None


class RunoffResponse(RunoffBase):
    id: int

    class Config:
        from_attributes = True


class HydrographBase(BaseModel):
    code: str = Field(..., max_length=10)
    date: DateType
    horizon_type: HorizonType
    horizon_value: int = Field(..., ge=1)
    horizon_in_year: int = Field(..., ge=1, le=366)
    day_of_year: int = Field(..., ge=1, le=366)
    count: Optional[int] = None
    mean: float
    std: float
    min: float
    max: float
    q05: float
    q25: float
    q50: float
    q75: float
    q95: float
    norm: Optional[float] = None
    previous: Optional[float] = None
    current: Optional[float] = None


class HydrographCreate(HydrographBase):
    pass


class HydrographResponse(HydrographBase):
    id: int

    class Config:
        from_attributes = True


class HydrographBulkCreate(BaseModel):
    data: List[HydrographCreate]
