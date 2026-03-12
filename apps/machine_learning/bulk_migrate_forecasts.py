"""Bulk-migrate historical ML forecast CSVs into PostgreSQL via docker exec.

Reads old-format CSV files (Q5..Q95, date, code, forecast_date, flag),
transforms to match the forecasts table schema, writes a temp CSV,
and pipes it into PostgreSQL via `docker exec psql COPY`.

Usage:
    python bulk_migrate_forecasts.py

Requires: pandas (already in ML venv). No DB driver needed.
"""

import os
import subprocess
import sys
import tempfile

import pandas as pd

CONTAINER = "sapphire-postprocessing-db"
DB = "postprocessing_db"
DB_USER = "postgres"

MODEL_FILES = {
    "TFT": (
        "/Users/bea/Documents/GitHub/kyg_data_forecast_tools/"
        "intermediate_data/predictions/TFT/copies_WK2025/"
        "decad_TFT_forecast copy.csv"
    ),
    "TIDE": (
        "/Users/bea/Documents/GitHub/kyg_data_forecast_tools/"
        "intermediate_data/predictions/TIDE/copy_workshop_2025/"
        "decad_TIDE_forecast copy.csv"
    ),
    "TSMIXER": (
        "/Users/bea/Documents/GitHub/kyg_data_forecast_tools/"
        "intermediate_data/predictions/TSMIXER/copy_workshop_2025/"
        "decad_TSMIXER_forecast copy.csv"
    ),
}


def run_psql(sql: str):
    """Run SQL via docker exec psql."""
    result = subprocess.run(
        ["docker", "exec", CONTAINER, "psql", "-U", DB_USER, "-d", DB, "-c", sql],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"  SQL ERROR: {result.stderr.strip()}")
    else:
        print(result.stdout.strip())
    return result


def copy_csv_to_db(csv_path: str):
    """COPY a CSV file into _tmp_forecasts via docker exec."""
    # Copy file into container
    container_path = "/tmp/bulk_forecasts.csv"
    subprocess.run(
        ["docker", "cp", csv_path, f"{CONTAINER}:{container_path}"],
        check=True,
    )

    # COPY into temp table
    result = subprocess.run(
        [
            "docker",
            "exec",
            CONTAINER,
            "psql",
            "-U",
            DB_USER,
            "-d",
            DB,
            "-c",
            f"\\COPY _tmp_forecasts FROM '{container_path}' WITH (FORMAT csv, NULL '', HEADER false)",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"  COPY ERROR: {result.stderr.strip()}")
        return False
    print(f"  {result.stdout.strip()}")
    return True


def transform_and_load(model_type: str, filepath: str):
    """Transform CSV, load to temp table, upsert into forecasts."""
    print(f"\nReading {filepath} ...")
    df = pd.read_csv(filepath, low_memory=False)
    print(f"  Loaded {len(df):,} rows")

    # Drop null Q50
    before = len(df)
    df = df.dropna(subset=["Q50"])
    dropped = before - len(df)
    if dropped > 0:
        print(f"  Dropped {dropped:,} null-Q50 rows")

    # Transform
    out = pd.DataFrame(index=df.index)
    out["horizon_type"] = "DAY"
    out["code"] = df["code"].astype(int).astype(str)
    out["model_type"] = model_type
    out["date"] = pd.to_datetime(df["forecast_date"]).dt.strftime("%Y-%m-%d")
    out["target"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    out["flag"] = df["flag"].where(df["flag"].notna()).astype("Int64")
    out["horizon_value"] = 0
    out["horizon_in_year"] = 0
    out["q05"] = df["Q5"]
    out["q25"] = df["Q25"]
    out["q75"] = df["Q75"]
    out["q95"] = df["Q95"]
    out["forecasted_discharge"] = df["Q50"]
    print(f"  Transformed {len(out):,} rows")

    # Write to temp CSV
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, prefix=f"forecast_{model_type}_"
    ) as f:
        tmp_path = f.name
        # Write without header, NaN as empty string
        out.to_csv(f, index=False, header=False, na_rep="")

    print(f"  Wrote temp CSV: {tmp_path}")

    # Create temp table
    run_psql("""
        DROP TABLE IF EXISTS _tmp_forecasts;
        CREATE TABLE _tmp_forecasts (
            horizon_type text,
            code text,
            model_type text,
            date date,
            target date,
            flag integer,
            horizon_value integer,
            horizon_in_year integer,
            q05 double precision,
            q25 double precision,
            q75 double precision,
            q95 double precision,
            forecasted_discharge double precision
        );
    """)

    # COPY into temp table
    if not copy_csv_to_db(tmp_path):
        os.unlink(tmp_path)
        return False

    # Upsert
    print("  Upserting into forecasts...")
    run_psql("""
        INSERT INTO forecasts (
            horizon_type, code, model_type, date, target,
            flag, horizon_value, horizon_in_year,
            q05, q25, q75, q95, forecasted_discharge
        )
        SELECT
            horizon_type::horizontype,
            code,
            model_type::modeltype,
            date, target, flag,
            horizon_value, horizon_in_year,
            q05, q25, q75, q95, forecasted_discharge
        FROM _tmp_forecasts
        ON CONFLICT (horizon_type, code, model_type, date, target)
        DO UPDATE SET
            flag = EXCLUDED.flag,
            q05 = EXCLUDED.q05,
            q25 = EXCLUDED.q25,
            q75 = EXCLUDED.q75,
            q95 = EXCLUDED.q95,
            forecasted_discharge = EXCLUDED.forecasted_discharge;
    """)

    # Cleanup
    run_psql("DROP TABLE IF EXISTS _tmp_forecasts;")
    os.unlink(tmp_path)
    return True


def main():
    for model_type, filepath in MODEL_FILES.items():
        print(f"\n{'=' * 60}")
        print(f"  {model_type}")
        print(f"{'=' * 60}")
        if not transform_and_load(model_type, filepath):
            print(f"  FAILED for {model_type}")
            sys.exit(1)
        print(f"  {model_type} complete")

    # Verify
    print(f"\n{'=' * 60}")
    print("  VERIFICATION")
    print(f"{'=' * 60}")
    run_psql("""
        SELECT model_type, COUNT(*) as total, MIN(date) as min_date, MAX(date) as max_date
        FROM forecasts WHERE horizon_type = 'DAY'
        GROUP BY model_type ORDER BY model_type;
    """)


if __name__ == "__main__":
    main()
