"""Read discharge data from a private Google Sheet via service account."""

import logging
import os
import re

import pandas as pd

logger = logging.getLogger(__name__)

try:
    import gspread
except ImportError:
    gspread = None

# Validation thresholds — overridable per-deployment via env vars.
# Chosen to be operationally sane: real glacial rivers can reach ~5000 m³/s;
# 50000 is a conservative upper cap. Row cap prevents memory exhaustion.
_DEFAULT_MAX_ROWS_PER_SITE = 10000
_DEFAULT_MAX_DISCHARGE_M3_S = 50000.0
_DEFAULT_MIN_DATE = "1900-01-01"
_DEFAULT_MAX_FUTURE_DAYS = 365


def is_google_sheets_enabled() -> bool:
    """Check if Google Sheets ingestion is configured and enabled."""
    return os.getenv("GOOGLE_SHEETS_ENABLED", "").lower() == "true"


def get_google_sheets_site_codes() -> list[str]:
    """Parse and validate site codes from GOOGLE_SHEETS_SITE_CODES env var.

    Returns:
        List of validated numeric site code strings.
    """
    raw = os.getenv("GOOGLE_SHEETS_SITE_CODES", "")
    if not raw.strip():
        return []

    codes = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        if not re.fullmatch(r"\d+", token):
            logger.error(
                f"Invalid site code '{token}' in GOOGLE_SHEETS_SITE_CODES — "
                f"must be digits only. Skipping."
            )
            continue
        codes.append(token)
    return codes


def _validate_credentials_path(path: str) -> bool:
    """Validate that the credentials path is a regular file."""
    if not path:
        logger.error("GOOGLE_SHEETS_CREDENTIALS_PATH is empty.")
        return False
    path = os.path.expanduser(path)
    if not os.path.exists(path):
        logger.error(f"Credentials file not found: {path}")
        return False
    if not os.path.isfile(path):
        logger.error(f"Credentials path is not a regular file: {path}")
        return False
    if not path.endswith(".json"):
        logger.error(f"Credentials file does not end in .json: {path}")
        return False
    return True


def read_discharge_from_google_sheet(
    sheet_id: str,
    site_codes: list[str],
    credentials_path: str,
) -> pd.DataFrame:
    """Fetch daily average discharge for manual sites from a Google Sheet.

    Each site_code corresponds to a worksheet (tab) in the spreadsheet.
    Expected columns: date (DD.MM.YYYY or YYYY-MM-DD), discharge (float or '-').

    Args:
        sheet_id: Google Sheets spreadsheet ID.
        site_codes: List of site codes; each must match a tab name.
        credentials_path: Path to Google service account JSON file.

    Returns:
        DataFrame with columns: code, date, discharge.
        Empty DataFrame if fetch fails (logged, not raised).
    """
    empty = pd.DataFrame(columns=["code", "date", "discharge"])

    # Defensive arg check — must come before gspread path so opt-in safety
    # (env vars unset → caller passes None/"") never touches gspread.
    if not sheet_id:
        logger.info("Google Sheets: sheet_id is None or empty — skipping.")
        return empty
    if credentials_path is None or (isinstance(credentials_path, str) and not credentials_path):
        logger.info("Google Sheets: credentials_path is None or empty — skipping.")
        return empty

    # Read env-override thresholds once (not per-row). Fall back to defaults
    # on malformed values so a typo in an env var never breaks the pipeline.
    def _read_int_env(var: str, default: int) -> int:
        raw = os.getenv(var, "")
        if raw.strip():
            try:
                return int(raw.strip())
            except ValueError:
                logger.warning(
                    f"Google Sheets: {var}='{raw}' is not a valid integer — "
                    f"using default {default}."
                )
        return default

    def _read_float_env(var: str, default: float) -> float:
        raw = os.getenv(var, "")
        if raw.strip():
            try:
                return float(raw.strip())
            except ValueError:
                logger.warning(
                    f"Google Sheets: {var}='{raw}' is not a valid float — using default {default}."
                )
        return default

    def _read_date_env(var: str, default: str) -> pd.Timestamp:
        raw = os.getenv(var, "")
        if raw.strip():
            try:
                return pd.Timestamp(raw.strip())
            except Exception:
                logger.warning(
                    f"Google Sheets: {var}='{raw}' is not a valid date — using default {default}."
                )
        return pd.Timestamp(default)

    max_rows = _read_int_env("GOOGLE_SHEETS_MAX_ROWS_PER_SITE", _DEFAULT_MAX_ROWS_PER_SITE)
    max_discharge = _read_float_env("GOOGLE_SHEETS_MAX_DISCHARGE_M3_S", _DEFAULT_MAX_DISCHARGE_M3_S)
    min_date_ts = _read_date_env("GOOGLE_SHEETS_MIN_DATE", _DEFAULT_MIN_DATE)
    max_future_days_val = _read_int_env("GOOGLE_SHEETS_MAX_FUTURE_DAYS", _DEFAULT_MAX_FUTURE_DAYS)
    now_ts = pd.Timestamp.now().normalize()
    max_date_ts = now_ts + pd.Timedelta(days=max_future_days_val)

    if gspread is None:
        logger.error(
            "gspread is not installed. Install it to enable Google Sheets "
            "ingestion: pip install 'gspread>=6.0,<7'"
        )
        return empty

    if not site_codes:
        logger.info("No site codes for Google Sheets fetch — skipping.")
        return empty

    credentials_path = os.path.expanduser(credentials_path)
    if not _validate_credentials_path(credentials_path):
        return empty

    try:
        gc = gspread.service_account(filename=credentials_path)
    except Exception as e:
        # Catch auth errors specifically
        err_msg = str(e)
        if "401" in err_msg or "403" in err_msg or "auth" in err_msg.lower():
            logger.error(
                f"Google Sheets auth failed — check credentials at {credentials_path}: {e}"
            )
        else:
            logger.error(f"Failed to authenticate with Google Sheets: {e}")
        return empty

    try:
        spreadsheet = gc.open_by_key(sheet_id)
    except Exception as e:
        logger.error(f"Failed to open Google Sheet {sheet_id}: {e}")
        return empty

    all_rows = []
    n_skipped = 0
    for code in site_codes:
        try:
            worksheet = spreadsheet.worksheet(code)
            records = worksheet.get_all_values()

            # Row-count cap: header + max_rows. Truncate silently after warning.
            if len(records) > max_rows + 1:
                logger.warning(
                    f"Google Sheets: site {code} has {len(records) - 1} rows, "
                    f"exceeding cap {max_rows} — truncating to first {max_rows} rows."
                )
                records = records[: max_rows + 1]

            if len(records) <= 1:
                logger.info(f"Google Sheets: site {code} — no data rows.")
                continue

            # Skip header row
            for row in records[1:]:
                if len(row) < 2:
                    continue
                date_str, discharge_str = row[0], row[1]

                # Parse date — accept DD.MM.YYYY or YYYY-MM-DD
                date_val = None
                for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
                    try:
                        date_val = pd.to_datetime(date_str, format=fmt)
                        break
                    except (ValueError, TypeError):
                        continue
                if date_val is None:
                    logger.warning(
                        f"Google Sheets site {code}: invalid date '{date_str}' — skipping row."
                    )
                    n_skipped += 1
                    continue

                # Date range check
                if date_val < min_date_ts or date_val > max_date_ts:
                    logger.warning(
                        f"Google Sheets site {code}: date {date_val.date()} out of "
                        f"plausible range [{min_date_ts.date()}, {max_date_ts.date()}] "
                        f"— skipping row."
                    )
                    n_skipped += 1
                    continue

                # Parse discharge
                if discharge_str.strip() in ("-", "", "—"):
                    discharge_val = float("nan")
                else:
                    try:
                        discharge_val = float(discharge_str)
                    except (ValueError, TypeError):
                        logger.warning(
                            f"Google Sheets site {code}: non-numeric discharge "
                            f"'{discharge_str}' on {date_str} — skipping row."
                        )
                        n_skipped += 1
                        continue

                    # Negative discharge: reject (not warn-and-include)
                    if discharge_val < 0:
                        logger.warning(
                            f"Google Sheets site {code}: negative discharge "
                            f"{discharge_val} on {date_val.date()} — likely typo; "
                            f"skipping row."
                        )
                        n_skipped += 1
                        continue

                    # Upper-bound discharge check
                    if discharge_val > max_discharge:
                        logger.warning(
                            f"Google Sheets site {code}: discharge {discharge_val} "
                            f"exceeds cap {max_discharge} m³/s on {date_val.date()} "
                            f"— likely sensor/entry error; skipping row."
                        )
                        n_skipped += 1
                        continue

                all_rows.append(
                    {
                        "code": str(code),
                        "date": date_val,
                        "discharge": discharge_val,
                    }
                )

        except gspread.exceptions.WorksheetNotFound:
            logger.warning(f"Google Sheets: no tab named '{code}' in spreadsheet — skipping site.")
        except Exception as e:
            logger.error(f"Google Sheets: error reading site {code}: {e}")

    if not all_rows:
        n_valid = 0
        n_sites = 0
        logger.info(
            f"Google Sheets: read {n_valid} valid rows across {n_sites} sites "
            f"(skipped {n_skipped} rows due to validation)"
        )
        return empty

    df = pd.DataFrame(all_rows)

    # Log summary per site
    for code in df["code"].unique():
        site_df = df[df["code"] == code].dropna(subset=["discharge"])
        if not site_df.empty:
            logger.info(
                f"Google Sheets: site {code} — {len(site_df)} rows "
                f"({site_df['date'].min().date()} to "
                f"{site_df['date'].max().date()})"
            )

    # Top-level summary for operator confirmation
    n_valid = len(df.dropna(subset=["discharge"]))
    n_sites = df["code"].nunique()
    logger.info(
        f"Google Sheets: read {n_valid} valid rows across {n_sites} sites "
        f"(skipped {n_skipped} rows due to validation)"
    )

    return df
