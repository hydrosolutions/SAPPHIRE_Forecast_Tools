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
    code = Column(String(10), nullable=False)
    date = Column(Date, nullable=False)
    discharge = Column(Float, nullable=False)
    predictor = Column(Float)

    # Horizon type and values
    horizon_type = Column(SQLEnum(HorizonType), nullable=False)
    horizon_value = Column(Integer, nullable=False)
    horizon_in_year = Column(Integer, nullable=False)

    # Composite index for filtering and ordering, plust unique constraint
    __table_args__ = (
        Index('ix_horizon_code_date', 'horizon_type', 'code', 'date'),
        UniqueConstraint('horizon_type', 'code', 'date', name='uq_horizon_code_date')
    )
