from __future__ import annotations

import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))


@dataclass
class FakeSapphireClient:
    """In-memory client that mimics paginated SAPPHIRE reads."""

    forecasts_rows: list[dict[str, Any]] = field(default_factory=list)
    lr_forecasts_rows: list[dict[str, Any]] = field(default_factory=list)
    long_forecasts_rows: list[dict[str, Any]] = field(default_factory=list)
    hydrograph_rows: list[dict[str, Any]] = field(default_factory=list)
    runoff_rows: list[dict[str, Any]] = field(default_factory=list)
    calls: list[tuple[str, dict[str, Any]]] = field(default_factory=list)

    def read_short_term_forecasts(
        self,
        horizon: str,
        code: str | None,
        model: str | None,
        target: str | None,
        start_target: str | None,
        end_target: str | None,
        skip: int,
        limit: int,
    ) -> pd.DataFrame:
        kwargs = {
            "horizon": horizon,
            "code": code,
            "model": model,
            "target": target,
            "start_target": start_target,
            "end_target": end_target,
            "skip": skip,
            "limit": limit,
        }
        self.calls.append(("read_short_term_forecasts", kwargs))
        return self._page(self._filter(self.forecasts_rows, kwargs), skip, limit)

    def read_lr_forecasts(
        self,
        horizon: str,
        code: str | None,
        start_date: str | None,
        end_date: str | None,
        skip: int,
        limit: int,
    ) -> pd.DataFrame:
        kwargs = {
            "horizon": horizon,
            "code": code,
            "start_date": start_date,
            "end_date": end_date,
            "skip": skip,
            "limit": limit,
        }
        self.calls.append(("read_lr_forecasts", kwargs))
        return self._page(self._filter(self.lr_forecasts_rows, kwargs), skip, limit)

    def read_long_term_forecasts(
        self,
        horizon_type: str,
        code: str | None,
        model: str | None,
        horizon_value: int | None,
        valid_from: str | None,
        valid_to: str | None,
        skip: int,
        limit: int,
    ) -> pd.DataFrame:
        kwargs = {
            "horizon_type": horizon_type,
            "code": code,
            "model": model,
            "horizon_value": horizon_value,
            "valid_from": valid_from,
            "valid_to": valid_to,
            "skip": skip,
            "limit": limit,
        }
        self.calls.append(("read_long_term_forecasts", kwargs))
        return self._page(self._filter(self.long_forecasts_rows, kwargs), skip, limit)

    def read_hydrograph(
        self,
        horizon: str,
        code: str | None,
        start_date: str | None,
        end_date: str | None,
        skip: int,
        limit: int,
    ) -> pd.DataFrame:
        kwargs = {
            "horizon": horizon,
            "code": code,
            "start_date": start_date,
            "end_date": end_date,
            "skip": skip,
            "limit": limit,
        }
        self.calls.append(("read_hydrograph", kwargs))
        return self._page(self._filter(self.hydrograph_rows, kwargs), skip, limit)

    def read_runoff(
        self,
        horizon: str,
        code: str | None,
        start_date: str | None,
        end_date: str | None,
        skip: int,
        limit: int,
    ) -> pd.DataFrame:
        kwargs = {
            "horizon": horizon,
            "code": code,
            "start_date": start_date,
            "end_date": end_date,
            "skip": skip,
            "limit": limit,
        }
        self.calls.append(("read_runoff", kwargs))
        return self._page(self._filter(self.runoff_rows, kwargs), skip, limit)

    def read_forecasts(self, *_args: Any, **_kwargs: Any) -> pd.DataFrame:
        raise AssertionError("deprecated read_forecasts must not be called")

    def read_long_forecasts(self, *_args: Any, **_kwargs: Any) -> pd.DataFrame:
        raise AssertionError("deprecated read_long_forecasts must not be called")

    @staticmethod
    def _page(rows: list[dict[str, Any]], skip: int, limit: int) -> pd.DataFrame:
        return pd.DataFrame(rows[skip : skip + limit])

    @staticmethod
    def _filter(
        rows: list[dict[str, Any]],
        kwargs: dict[str, Any],
    ) -> list[dict[str, Any]]:
        filtered = rows
        for key in ("horizon", "code", "model", "target", "horizon_value"):
            value = kwargs.get(key)
            if value is None:
                continue
            filtered = [row for row in filtered if key not in row or row[key] == value]
        horizon_type = kwargs.get("horizon_type")
        if horizon_type is not None:
            filtered = [
                row
                for row in filtered
                if (
                    ("horizon_type" not in row and "horizon" not in row)
                    or row.get("horizon_type") == horizon_type
                    or row.get("horizon") == horizon_type
                )
            ]
        return filtered


@pytest.fixture
def fake_client_factory() -> Callable[..., FakeSapphireClient]:
    return FakeSapphireClient
