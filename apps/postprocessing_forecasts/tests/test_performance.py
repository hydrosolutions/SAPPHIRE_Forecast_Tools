"""Performance benchmarks for postprocessing_forecasts optimizations.

Skipped by default (marked @pytest.mark.benchmark). Run explicitly:
    cd apps && SAPPHIRE_TEST_ENV=True pytest postprocessing_forecasts/tests/test_performance.py -v -k bench

Benchmarks use synthetic data at realistic scale:
  72 pentads × 50 stations × 4 models = 14,400 groups, ~10 data points each
"""

import os
import sys
import time

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast')
)

import forecast_library as fl
from src.ensemble_calculator import _calculate_ensemble_skill


# ---------------------------------------------------------------------------
# Synthetic data generators
# ---------------------------------------------------------------------------

def _make_skill_metrics_df(
    n_pentads: int = 72,
    n_stations: int = 50,
    n_models: int = 4,
    n_points_per_group: int = 10,
) -> pd.DataFrame:
    """Generate synthetic skill metrics data at realistic scale.

    Returns a DataFrame with columns matching forecast_library expectations:
    [pentad_in_year, code, model_long, model_short,
     discharge_avg, forecasted_discharge, delta, date]
    """
    rng = np.random.default_rng(42)
    models = [
        ('Linear regression (LR)', 'LR'),
        ('Temporal Fusion Transformer (TFT)', 'TFT'),
        ('Time-series Dense Encoder (TiDE)', 'TiDE'),
        ('TSMixer (TSMIXER)', 'TSMIXER'),
    ][:n_models]

    rows = []
    base_date = pd.Timestamp('2020-01-01')
    for pentad in range(1, n_pentads + 1):
        for stn_idx in range(n_stations):
            code = f'{10000 + stn_idx}'
            for model_long, model_short in models:
                for pt in range(n_points_per_group):
                    obs = rng.normal(50, 10)
                    sim = obs + rng.normal(0, 5)
                    rows.append({
                        'pentad_in_year': pentad,
                        'code': code,
                        'model_long': model_long,
                        'model_short': model_short,
                        'discharge_avg': obs,
                        'forecasted_discharge': sim,
                        'delta': 5.0,
                        'date': base_date + pd.Timedelta(days=pt),
                    })

    return pd.DataFrame(rows)


def _make_iterrows_df(n_rows: int = 22_000) -> pd.DataFrame:
    """Generate synthetic data for iterrows benchmark."""
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        'code': [f'{10000 + i % 50}' for i in range(n_rows)],
        'date': pd.date_range('2020-01-01', periods=n_rows, freq='D'),
        'pentad_in_month': rng.integers(1, 7, n_rows),
        'pentad_in_year': rng.integers(1, 73, n_rows),
        'model_short': rng.choice(['LR', 'TFT', 'TiDE', 'TSMIXER'], n_rows),
        'model_long': rng.choice([
            'Linear regression (LR)',
            'Temporal Fusion Transformer (TFT)',
        ], n_rows),
        'forecasted_discharge': rng.normal(50, 10, n_rows),
        'discharge_avg': rng.normal(50, 10, n_rows),
        'sdivsigma': rng.uniform(0.1, 1.0, n_rows),
        'nse': rng.uniform(0.5, 1.0, n_rows),
        'delta': np.full(n_rows, 5.0),
        'accuracy': rng.uniform(0.5, 1.0, n_rows),
        'mae': rng.uniform(1, 10, n_rows),
        'n_pairs': rng.integers(5, 20, n_rows),
    })


# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

@pytest.mark.benchmark
class TestTripleGroupbyVsSinglePass:
    """Benchmark triple-groupby + merge vs single-pass calculate_all_skill_metrics."""

    @pytest.fixture(autouse=True)
    def setup_data(self):
        self.df = _make_skill_metrics_df(
            n_pentads=72, n_stations=50, n_models=4, n_points_per_group=10
        )
        self.group_keys = ['pentad_in_year', 'code', 'model_long', 'model_short']

    def test_bench_triple_groupby(self):
        """Baseline: 3 separate groupby + 2 merges."""
        t0 = time.perf_counter()

        skill_stats = self.df.groupby(self.group_keys)[
            ['discharge_avg', 'forecasted_discharge']
        ].apply(
            fl.sdivsigma_nse,
            observed_col='discharge_avg',
            simulated_col='forecasted_discharge',
        ).reset_index()

        mae_stats = self.df.groupby(self.group_keys)[
            ['discharge_avg', 'forecasted_discharge']
        ].apply(
            fl.mae,
            observed_col='discharge_avg',
            simulated_col='forecasted_discharge',
        ).reset_index()

        accuracy_stats = self.df.groupby(self.group_keys)[
            ['discharge_avg', 'forecasted_discharge', 'delta']
        ].apply(
            fl.forecast_accuracy_hydromet,
            observed_col='discharge_avg',
            simulated_col='forecasted_discharge',
            delta_col='delta',
        ).reset_index()

        result = pd.merge(skill_stats, accuracy_stats, on=self.group_keys)
        result = pd.merge(result, mae_stats, on=self.group_keys)

        elapsed = time.perf_counter() - t0
        print(f"\n  Triple groupby: {elapsed:.3f}s ({len(result)} groups)")
        assert len(result) == 14400, (
            f"Expected 14400 groups (72 pentads x 50 stations x 4 models), "
            f"got {len(result)}"
        )

    def test_bench_single_pass(self):
        """After optimization: single groupby with calculate_all_skill_metrics.

        Updated automatically once the function is implemented (Step 2).
        Until then, runs the triple-groupby baseline as a placeholder.
        """
        if not hasattr(fl, 'calculate_all_skill_metrics'):
            # Placeholder: run triple-groupby baseline until Step 2
            t0 = time.perf_counter()
            skill_stats = self.df.groupby(self.group_keys)[
                ['discharge_avg', 'forecasted_discharge']
            ].apply(
                fl.sdivsigma_nse,
                observed_col='discharge_avg',
                simulated_col='forecasted_discharge',
            ).reset_index()
            elapsed = time.perf_counter() - t0
            print(f"\n  Single pass (placeholder): {elapsed:.3f}s ({len(skill_stats)} groups)")
            assert len(skill_stats) == 14400, (
                f"Expected 14400 groups, got {len(skill_stats)}"
            )
            return

        t0 = time.perf_counter()

        result = self.df.groupby(self.group_keys)[
            ['discharge_avg', 'forecasted_discharge', 'delta']
        ].apply(
            fl.calculate_all_skill_metrics,
            observed_col='discharge_avg',
            simulated_col='forecasted_discharge',
            delta_col='delta',
        ).reset_index()

        elapsed = time.perf_counter() - t0
        print(f"\n  Single pass: {elapsed:.3f}s ({len(result)} groups)")
        assert len(result) == 14400, (
            f"Expected 14400 groups, got {len(result)}"
        )


@pytest.mark.benchmark
class TestIsinVsMerge:
    """Benchmark .isin() filter vs inner merge."""

    @pytest.fixture(autouse=True)
    def setup_data(self):
        self.skill_metrics_df = _make_skill_metrics_df(
            n_pentads=72, n_stations=50, n_models=4, n_points_per_group=10
        )
        # Simulate ensemble subset (50% of groups)
        rng = np.random.default_rng(42)
        keys = self.skill_metrics_df[
            ['pentad_in_year', 'code', 'model_long', 'model_short']
        ].drop_duplicates()
        self.ensemble_keys = keys.sample(frac=0.5, random_state=42)

    def test_bench_isin_filter(self):
        """Baseline: 4 sequential .isin() calls."""
        t0 = time.perf_counter()

        result = self.skill_metrics_df[
            self.skill_metrics_df['pentad_in_year'].isin(self.ensemble_keys['pentad_in_year']) &
            self.skill_metrics_df['code'].isin(self.ensemble_keys['code']) &
            self.skill_metrics_df['model_long'].isin(self.ensemble_keys['model_long']) &
            self.skill_metrics_df['model_short'].isin(self.ensemble_keys['model_short'])
        ].copy()

        elapsed = time.perf_counter() - t0
        print(f"\n  .isin() filter: {elapsed:.3f}s ({len(result)} rows)")
        # isin is column-independent (checks each column separately), so it
        # matches at least as many rows as the composite-key merge.
        assert len(result) >= 72000, (
            f"isin should match at least 72000 rows, got {len(result)}"
        )

    def test_bench_merge_filter(self):
        """Optimized: inner merge on composite key."""
        merge_keys = ['pentad_in_year', 'code', 'model_long', 'model_short']

        t0 = time.perf_counter()

        result = self.skill_metrics_df.merge(
            self.ensemble_keys[merge_keys].drop_duplicates(),
            on=merge_keys,
            how='inner',
        )

        elapsed = time.perf_counter() - t0
        print(f"\n  Merge filter: {elapsed:.3f}s ({len(result)} rows)")
        # 50% of 14,400 groups = 7,200 keys, each matching 10 rows = 72,000.
        assert len(result) == 72000, (
            f"Expected 72000 rows from inner merge, got {len(result)}"
        )


@pytest.mark.benchmark
class TestIterrowsVsVectorized:
    """Benchmark iterrows record building vs vectorized approach."""

    @pytest.fixture(autouse=True)
    def setup_data(self):
        self.df = _make_iterrows_df(n_rows=22_000)

    def test_bench_iterrows(self):
        """Baseline: iterrows loop to build record dicts."""
        import re

        model_type_map = {
            "LR": "LR", "TFT": "TFT", "TIDE": "TiDE",
            "TSMIXER": "TSMixer", "EM": "EM", "NE": "NE",
        }

        t0 = time.perf_counter()

        records = []
        for _, row in self.df.iterrows():
            model_short = str(row.get('model_short', ''))
            api_model_type = model_type_map.get(model_short.upper(), model_short)
            composition = None
            if model_short.upper() in ('EM', 'NE') and pd.notna(row.get('model_long')):
                match = re.search(r'with\s+(.+?)\s+\([EN][ME]\)', str(row['model_long']))
                if match:
                    composition = match.group(1).strip()

            record = {
                "horizon_type": "pentad",
                "code": str(row['code']),
                "model_type": api_model_type,
                "date": pd.to_datetime(row['date']).strftime('%Y-%m-%d'),
                "horizon_value": int(row['pentad_in_month']),
                "horizon_in_year": int(row['pentad_in_year']),
                "composition": composition,
                "forecasted_discharge": float(row['forecasted_discharge']),
            }
            records.append(record)

        elapsed = time.perf_counter() - t0
        print(f"\n  iterrows: {elapsed:.3f}s ({len(records)} records)")
        assert len(records) == 22_000

    def test_bench_vectorized(self):
        """After optimization: vectorized record building.

        This test will be updated in Step 5 once the vectorized code exists.
        For now, demonstrate the vectorized approach.
        """
        model_type_map = {
            "LR": "LR", "TFT": "TFT", "TIDE": "TiDE",
            "TSMIXER": "TSMixer", "EM": "EM", "NE": "NE",
        }

        t0 = time.perf_counter()

        df = self.df.copy()
        df['model_type'] = df['model_short'].str.upper().map(model_type_map).fillna(df['model_short'])
        df['date_str'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

        # Vectorize composition extraction
        is_ensemble = df['model_short'].str.upper().isin(['EM', 'NE'])
        df['composition'] = None
        if is_ensemble.any():
            df.loc[is_ensemble, 'composition'] = (
                df.loc[is_ensemble, 'model_long']
                .str.extract(r'with\s+(.+?)\s+\([EN][ME]\)', expand=False)
                .str.strip()
            )

        result = df[['code', 'model_type', 'date_str', 'pentad_in_month',
                      'pentad_in_year', 'composition', 'forecasted_discharge']].copy()
        result.columns = ['code', 'model_type', 'date', 'horizon_value',
                          'horizon_in_year', 'composition', 'forecasted_discharge']
        result['horizon_type'] = 'pentad'
        result['horizon_value'] = result['horizon_value'].astype(int)
        result['horizon_in_year'] = result['horizon_in_year'].astype(int)
        records = result.to_dict('records')

        elapsed = time.perf_counter() - t0
        print(f"\n  Vectorized: {elapsed:.3f}s ({len(records)} records)")
        assert len(records) == 22_000
