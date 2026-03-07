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

    if gspread is None:
        logger.error(
            "gspread is not installed. Install it to enable Google Sheets "
            "ingestion: pip install 'gspread>=6.0,<7'"
        )
        return empty

    if not site_codes:
        logger.info("No site codes for Google Sheets fetch — skipping.")
        return empty

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
    for code in site_codes:
        try:
            worksheet = spreadsheet.worksheet(code)
            records = worksheet.get_all_values()

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

    # Validate discharge values
    valid_discharge = df.dropna(subset=["discharge"])
    neg_mask = valid_discharge["discharge"] < 0
    if neg_mask.any():
        neg_rows = valid_discharge[neg_mask]
        for _, row in neg_rows.iterrows():
            logger.warning(
                f"Google Sheets site {row['code']}: negative discharge "
                f"{row['discharge']} on {row['date'].date()} — likely typo."
            )

    return df
