from sqlalchemy import Column, Integer, String, Date, Float, Enum as SQLEnum, Index, UniqueConstraint
from app.database import Base
from enum import Enum


class HorizonType(str, Enum):
    """Enumeration of supported horizon types"""
    DAY = "day"
    PENTAD = "pentad"
    DECADE = "decade"
    MONTH = "month"
    QUARTER = "quarter"
    SEASON = "season"


class ModelType(str, Enum):
    """Enum for different model types"""
    TSMIXER = "TSMixer"
    TIDE = "TIDE"
    TFT = "TFT"
    ENSEMBLE_MEAN = "EM"
    NEURAL_ENSEMBLE = "NE"
    RRAM = "RRAM"
    LINEAR_REGRESSION = "LR"

    @property
    def description(self) -> str:
        """Get the long description for the model type"""
        descriptions = {
            "TSMixer": "Time-Series Mixer (TSMixer)",
            "TIDE": "Time-Series Dense Encoder (TIDE)",
            "TFT": "Temporal Fusion Transformer (TFT)",
            "EM": "Ens. Mean with LR, TFT, TIDE (EM)",
            "NE": "Neural Ensemble with TIDE, TFT, TSMixer (NE)",
            "RRAM": "Rainfall runoff assimilation model",
            "LR": "Linear Regression"
        }
        return descriptions.get(self.value, self.value)


class Forecast(Base):
    __tablename__ = "forecasts"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)

    # Metadata fields
    horizon_type = Column(SQLEnum(HorizonType), nullable=False)
    code = Column(String(10), nullable=False)
    model_type = Column(SQLEnum(ModelType), nullable=False)
    date = Column(Date, nullable=False)  # when the forecast is made
    target = Column(Date)  # forecast target date
    flag = Column(Integer)
    horizon_value = Column(Integer, nullable=False)
    horizon_in_year = Column(Integer, nullable=False)

    # Quantile predictions (Q5 to Q95)
    q05 = Column(Float)
    q25 = Column(Float)
    q50 = Column(Float)
    q75 = Column(Float)
    q95 = Column(Float)

    # Results
    forecasted_discharge = Column(Float)

    # Composite index for filtering and ordering, plus unique constraint
    __table_args__ = (
        Index('ix_forecasts_horizon_code_model_date_target', 'horizon_type', 'code', 'model_type', 'date', 'target'),
        UniqueConstraint('horizon_type', 'code', 'model_type', 'date', 'target', name='uq_forecasts_horizon_code_model_date_target')
    )


class LRForecast(Base):
    __tablename__ = "lr_forecasts"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)

    # Metadata fields
    horizon_type = Column(SQLEnum(HorizonType), nullable=False)
    code = Column(String(10), nullable=False)
    date = Column(Date, nullable=False)

    # Regression parameters
    discharge = Column(Float)
    predictor = Column(Float)
    horizon_value = Column(Integer, nullable=False)
    horizon_in_year = Column(Integer, nullable=False)

    # Model coefficients
    slope = Column(Float)
    intercept = Column(Float)

    # Results
    forecasted_discharge = Column(Float)

    # Statistical measures
    q_mean = Column(Float)
    q_std_sigma = Column(Float)
    delta = Column(Float)
    rsquared = Column(Float)

    # Composite index for filtering and ordering, plus unique constraint
    __table_args__ = (
        Index('ix_lr_forecasts_horizon_code_date', 'horizon_type', 'code', 'date'),
        UniqueConstraint('horizon_type', 'code', 'date', name='uq_lr_forecasts_horizon_code_date')
    )


class SkillMetric(Base):
    __tablename__ = "skill_metrics"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)

    # Metadata fields
    horizon_type = Column(SQLEnum(HorizonType), nullable=False)
    code = Column(String(10), nullable=False)
    model_type = Column(SQLEnum(ModelType), nullable=False)
    date = Column(Date, nullable=False)
    horizon_in_year = Column(Integer, nullable=False)

    # Skill metrics
    sdivsigma = Column(Float)
    nse = Column(Float)
    delta = Column(Float)
    accuracy = Column(Float)
    mae = Column(Float)
    n_pairs = Column(Integer)

    # Composite index for filtering and ordering, plus unique constraint
    __table_args__ = (
        Index('ix_skill_metrics_horizon_code_model_date', 'horizon_type', 'code', 'model_type', 'date'),
        UniqueConstraint('horizon_type', 'code', 'model_type', 'date', name='uq_skill_metrics_horizon_code_model_date')
    )
