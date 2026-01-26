from sqlalchemy import Column, Integer, String, Date, Float, Enum as SQLEnum, Index, UniqueConstraint
from app.database import Base
from enum import Enum


class HorizonType(str, Enum):
    """Enumeration of supported horizon types"""
    DAY = "day"
    PENTAD = "pentad"
    DECADE = "decade"
    MONTH = "month"
    SEASON = "season"
    YEAR = "year"


class Runoff(Base):
    __tablename__ = "runoffs"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)

    # Metadata fields
    horizon_type = Column(SQLEnum(HorizonType), nullable=False)
    code = Column(String(10), nullable=False)
    date = Column(Date, nullable=False)

    # Runoff values
    discharge = Column(Float)
    predictor = Column(Float)

    # Horizon values
    horizon_value = Column(Integer, nullable=False)
    horizon_in_year = Column(Integer, nullable=False)

    # Composite index for filtering and ordering, plust unique constraint
    __table_args__ = (
        Index('ix_runoffs_horizon_code_date', 'horizon_type', 'code', 'date'),
        UniqueConstraint('horizon_type', 'code', 'date', name='uq_runoffs_horizon_code_date')
    )


class Hydrograph(Base):
    __tablename__ = "hydrographs"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)

    # Metadata fields
    horizon_type = Column(SQLEnum(HorizonType), nullable=False)
    code = Column(String(10), nullable=False)
    date = Column(Date, nullable=False)

    # Horizon values
    horizon_value = Column(Integer, nullable=False)
    horizon_in_year = Column(Integer, nullable=False)
    day_of_year = Column(Integer, nullable=False)

    # Statistical measures
    count = Column(Integer)
    mean = Column(Float)
    std = Column(Float)
    min = Column(Float)
    max = Column(Float)

    # Percentiles
    q05 = Column(Float)
    q25 = Column(Float)
    q50 = Column(Float)
    q75 = Column(Float)
    q95 = Column(Float)

    # Norm and comparison values
    norm = Column(Float)
    previous = Column(Float)
    current = Column(Float)

    # Composite index for filtering and ordering, plus unique constraint
    __table_args__ = (
        Index('ix_hydrographs_horizon_code_date', 'horizon_type', 'code', 'date'),
        UniqueConstraint('horizon_type', 'code', 'date', name='uq_hydrographs_horizon_code_date')
    )


class MeteoType(str, Enum):
    """Enumeration of meteorological variable types"""
    T = "T"  # Temperature
    P = "P"  # Precipitation


class Meteo(Base):
    __tablename__ = "meteo"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)

    # Metadata fields
    meteo_type = Column(SQLEnum(MeteoType), nullable=False)
    code = Column(String(10), nullable=False)
    date = Column(Date, nullable=False)

    value = Column(Float)
    norm = Column(Float)
    day_of_year = Column(Integer, nullable=False)


    # Composite index for filtering and ordering, plus unique constraint
    __table_args__ = (
        Index('ix_meteo_type_code_date', 'meteo_type', 'code', 'date'),
        UniqueConstraint('meteo_type', 'code', 'date', name='uq_meteo_type_code_date')
    )


class SnowType(str, Enum):
    """Enumeration of snow variable types"""
    HS = "HS"  # Snow height
    ROF = "ROF"  # Snow melt plus rainfall runoff
    SWE = "SWE"  # Snow water equivalent


class Snow(Base):
    __tablename__ = "snow"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)

    # Metadata fields
    snow_type = Column(SQLEnum(SnowType), nullable=False)
    code = Column(String(10), nullable=False)
    date = Column(Date, nullable=False)

    value = Column(Float)
    norm = Column(Float)
    value1 = Column(Float)
    value2 = Column(Float)
    value3 = Column(Float)
    value4 = Column(Float)
    value5 = Column(Float)
    value6 = Column(Float)
    value7 = Column(Float)
    value8 = Column(Float)
    value9 = Column(Float)
    value10 = Column(Float)
    value11 = Column(Float)
    value12 = Column(Float)
    value13 = Column(Float)
    value14 = Column(Float)

    # Composite index for filtering and ordering, plus unique constraint
    __table_args__ = (
        Index('ix_snow_type_code_date', 'snow_type', 'code', 'date'),
        UniqueConstraint('snow_type', 'code', 'date', name='uq_snow_type_code_date')
    )
