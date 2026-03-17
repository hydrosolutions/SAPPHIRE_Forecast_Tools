"""Re-aggregate DAY forecasts into PENTAD and DECADE records with quantiles.

Reads DAY-level ML forecast records from PostgreSQL, filters for
pentad/decad boundary dates, aggregates (mean discharge + quantiles),
creates NEURAL_ENSEMBLE rows, and upserts into the forecasts table.

This replaces the old combined_forecasts CSV migration records that
lacked quantile columns (q05, q25, q75, q95).

Usage:
    python reaggregate_day_to_periods.py [--dry-run] [--pentad-only] [--decad-only]

Requires: pandas. No DB driver needed (uses docker exec psql).
"""

import argparse
import calendar
import os
import subprocess
import tempfile

import pandas as pd

CONTAINER = "sapphire-postprocessing-db"
DB = "postprocessing_db"
DB_USER = "postgres"

MODELS = ["TFT", "TIDE", "TSMIXER"]


def run_psql(sql: str, quiet: bool = False) -> subprocess.CompletedProcess:
    """Run SQL via docker exec psql."""
    result = subprocess.run(
        ["docker", "exec", CONTAINER, "psql", "-U", DB_USER, "-d", DB, "-c", sql],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"  SQL ERROR: {result.stderr.strip()}")
    elif not quiet:
        print(result.stdout.strip())
    return result


def run_psql_csv(sql: str) -> pd.DataFrame:
    """Run SQL via docker exec psql, return result as DataFrame."""
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
            "--csv",
            "-c",
            sql,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"  SQL ERROR: {result.stderr.strip()}")
        return pd.DataFrame()
    if not result.stdout.strip():
        return pd.DataFrame()
    from io import StringIO

    return pd.read_csv(StringIO(result.stdout))


def copy_csv_to_db(csv_path: str, table: str) -> bool:
    """COPY a CSV file into a table via docker exec."""
    container_path = "/tmp/reagg_bulk.csv"
    subprocess.run(
        ["docker", "cp", csv_path, f"{CONTAINER}:{container_path}"],
        check=True,
    )
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
            f"\\COPY {table} FROM '{container_path}' WITH (FORMAT csv, NULL '', HEADER true)",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"  COPY ERROR: {result.stderr.strip()}")
        return False
    print(f"  {result.stdout.strip()}")
    return True


def is_pentad_boundary(d) -> bool:
    last_day = calendar.monthrange(d.year, d.month)[1]
    return d.day in (5, 10, 15, 20, 25, last_day)


def is_decad_boundary(d) -> bool:
    last_day = calendar.monthrange(d.year, d.month)[1]
    return d.day in (10, 20, last_day)


def aggregate_for_horizon(horizon_type: str, dry_run: bool = False):
    """Aggregate DAY records to PENTAD or DECADE level."""
    is_boundary = is_pentad_boundary if horizon_type == "PENTAD" else is_decad_boundary
    label = horizon_type

    print(f"\n{'=' * 60}")
    print(f"  {label}: Reading DAY records from DB")
    print(f"{'=' * 60}")

    # Read all distinct (date, code, model_type) groups with aggregated values
    # Do the aggregation in SQL for speed — 8.5M rows is too much for pandas
    sql = """
        SELECT
            date,
            code,
            model_type,
            AVG(forecasted_discharge) AS forecasted_discharge,
            AVG(q05) AS q05,
            AVG(q25) AS q25,
            AVG(q75) AS q75,
            AVG(q95) AS q95,
            MAX(flag) AS flag
        FROM forecasts
        WHERE horizon_type = 'DAY'
        GROUP BY date, code, model_type
        ORDER BY date, code, model_type;
    """
    print("  Running SQL aggregation (this may take a minute)...")
    df = run_psql_csv(sql)

    if df.empty:
        print(f"  No DAY records found. Skipping {label}.")
        return

    print(f"  Got {len(df):,} (date, code, model) groups from DAY records")

    # Parse dates and filter for boundary dates
    df["date"] = pd.to_datetime(df["date"])
    df["is_boundary"] = df["date"].apply(is_boundary)
    df = df[df["is_boundary"]].drop(columns=["is_boundary"]).copy()
    print(f"  After filtering for {label.lower()} boundary dates: {len(df):,} rows")

    if df.empty:
        print(f"  No boundary-date records. Skipping {label}.")
        return

    # Compute target = date + 1 day (first day of forecast period)
    df["target"] = (df["date"] + pd.Timedelta(days=1)).dt.strftime("%Y-%m-%d")
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    # Set horizon columns (will be 0 — same as bulk_migrate_forecasts.py)
    df["horizon_type"] = label
    df["horizon_value"] = 0
    df["horizon_in_year"] = 0

    # Round to 3 decimal places (matches operational pipeline)
    for col in ["forecasted_discharge", "q05", "q25", "q75", "q95"]:
        if col in df.columns:
            df[col] = df[col].round(3)

    # Ensure flag is nullable integer (MAX of integer flags can become float via pandas)
    if "flag" in df.columns:
        df["flag"] = df["flag"].astype("Int64")

    # --- Create NEURAL_ENSEMBLE rows ---
    print("  Creating NEURAL_ENSEMBLE rows...")
    ml_only = df[df["model_type"].isin(MODELS)].copy()
    ne_agg = (
        ml_only.groupby(
            ["date", "target", "code", "horizon_type", "horizon_value", "horizon_in_year"]
        )
        .agg(
            {
                "forecasted_discharge": "mean",
                "q05": "mean",
                "q25": "mean",
                "q75": "mean",
                "q95": "mean",
                "flag": "max",
            }
        )
        .reset_index()
    )
    ne_agg["model_type"] = "NEURAL_ENSEMBLE"

    # Round NE quantiles and fix flag type
    for col in ["forecasted_discharge", "q05", "q25", "q75", "q95"]:
        ne_agg[col] = ne_agg[col].round(3)
    if "flag" in ne_agg.columns:
        ne_agg["flag"] = ne_agg["flag"].astype("Int64")

    print(f"  Created {len(ne_agg):,} NEURAL_ENSEMBLE rows")

    # Combine individual models + NE
    all_records = pd.concat([df, ne_agg], ignore_index=True)

    # Select output columns in the right order
    out_cols = [
        "horizon_type",
        "code",
        "model_type",
        "date",
        "target",
        "flag",
        "horizon_value",
        "horizon_in_year",
        "q05",
        "q25",
        "q75",
        "q95",
        "forecasted_discharge",
    ]
    all_records = all_records[out_cols]

    print(f"  Total records to upsert: {len(all_records):,}")
    print(f"    Per model: {all_records.groupby('model_type').size().to_dict()}")

    if dry_run:
        print("  DRY RUN — skipping DB write")
        print(f"  Sample records:\n{all_records.head(10).to_string()}")
        return

    # Write to temp CSV
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".csv",
        delete=False,
        prefix=f"reagg_{label}_",
    ) as f:
        tmp_path = f.name
        all_records.to_csv(f, index=False, na_rep="")
    print(f"  Wrote temp CSV: {tmp_path}")

    # Create temp table
    run_psql(
        """
        DROP TABLE IF EXISTS _tmp_reagg;
        CREATE TABLE _tmp_reagg (
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
    """,
        quiet=True,
    )

    # COPY into temp table
    if not copy_csv_to_db(tmp_path, "_tmp_reagg"):
        os.unlink(tmp_path)
        return

    # Upsert into forecasts
    print(f"  Upserting {label} records into forecasts table...")
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
        FROM _tmp_reagg
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
    run_psql("DROP TABLE IF EXISTS _tmp_reagg;", quiet=True)
    os.unlink(tmp_path)
    print(f"  {label} upsert complete.")


def verify():
    """Show summary of PENTAD/DECADE records after re-aggregation."""
    print(f"\n{'=' * 60}")
    print("  VERIFICATION")
    print(f"{'=' * 60}")
    run_psql("""
        SELECT horizon_type, model_type,
               COUNT(*) as total,
               SUM(CASE WHEN q05 IS NULL THEN 1 ELSE 0 END) as no_quantiles,
               SUM(CASE WHEN q05 IS NOT NULL THEN 1 ELSE 0 END) as has_quantiles,
               MIN(date) as min_date, MAX(date) as max_date
        FROM forecasts
        WHERE horizon_type IN ('PENTAD', 'DECADE')
        GROUP BY horizon_type, model_type
        ORDER BY horizon_type, model_type;
    """)


def main():
    parser = argparse.ArgumentParser(
        description="Re-aggregate DAY forecasts into PENTAD/DECADE records"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without writing to DB",
    )
    parser.add_argument(
        "--pentad-only",
        action="store_true",
        help="Only re-aggregate PENTAD records",
    )
    parser.add_argument(
        "--decad-only",
        action="store_true",
        help="Only re-aggregate DECADE records",
    )
    args = parser.parse_args()

    do_pentad = not args.decad_only
    do_decad = not args.pentad_only

    if do_pentad:
        aggregate_for_horizon("PENTAD", dry_run=args.dry_run)
    if do_decad:
        aggregate_for_horizon("DECADE", dry_run=args.dry_run)

    if not args.dry_run:
        verify()


if __name__ == "__main__":
    main()
