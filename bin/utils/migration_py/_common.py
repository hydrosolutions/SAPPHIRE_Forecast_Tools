"""Shared primitives for the update-time migration toolkit.

Public-API summary:
    - resolve_image: Docker image string resolution (CLI / configured / fallback)
    - detect_mode: pick "full-import" vs "pre-cutoff" based on target table state
    - validate_manifest: parse and verify the `<csv>.manifest` sidecar file
    - acquire_temp_workspace: create a mode-0o700 temp dir under data_root/logs/
    - log_redacted_station_count: log only count, never the actual codes
    - ManifestError + typed subclasses for manifest validation failures
    - ALLOWED_EXPORT_TYPES, FALLBACK_IMAGE

Rule: stdlib-only. Any third-party import here would justify a new Docker image
variant — explicitly out of scope (see Stage B v2 architecture §Q1). The
`_audit.py` module + companion pytest test verify this rule for every `*.py`
under this package.

v2 revision R1: `urllib.error` / `urllib.request` are NOT imported here. No P0
function makes HTTP calls; ruff F401 would fail acceptance. The POST helper that
needs urllib lands in P1 wrapper modules, not in `_common.py`.
"""

from __future__ import annotations

import csv
import datetime
import logging
import os
import pathlib
import re

__all__ = [
    "ALLOWED_EXPORT_TYPES",
    "FALLBACK_IMAGE",
    "ManifestDateRangeMismatchError",
    "ManifestError",
    "ManifestExportTypeMismatchError",
    "ManifestMissingError",
    "ManifestRowCountMismatchError",
    "ManifestStationCountMismatchError",
    "acquire_temp_workspace",
    "detect_mode",
    "log_redacted_station_count",
    "resolve_image",
    "validate_manifest",
]


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ALLOWED_EXPORT_TYPES: frozenset[str] = frozenset(
    {
        "runoff_period",
        "hydrograph_period",
        "lr_forecast",
        "ml_forecast",
        "long_forecast",
    }
)

FALLBACK_IMAGE: str = "mabesa/sapphire-prepgateway:latest"


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class ManifestError(Exception):
    """Raised when a migration export manifest fails validation."""


class ManifestMissingError(ManifestError):
    """Raised when the manifest file does not exist beside the export CSV."""


class ManifestRowCountMismatchError(ManifestError):
    """Raised when manifest row_count does not equal the parsed CSV row count."""


class ManifestExportTypeMismatchError(ManifestError):
    """Raised when manifest export_type does not match the wrapper's expected type."""


class ManifestStationCountMismatchError(ManifestError):
    """v2 R6: manifest station_count != count of distinct ``code`` values in CSV.

    Catches unfiltered / cross-org exports — a security-relevant leak of another
    deployment's station codes.
    """


class ManifestDateRangeMismatchError(ManifestError):
    """v2 R6: manifest date_min/date_max do not match CSV min/max of ``date`` column.

    Catches stale-file errors (operator grabbed last week's export and forgot to
    refresh it).
    """


# ---------------------------------------------------------------------------
# Image resolution
# ---------------------------------------------------------------------------


_UNPINNED_TAG_SUFFIXES: tuple[str, ...] = (":local", ":latest")


def _warn_if_unpinned_tag(image: str) -> None:
    """Log a stdlib warning if the resolved image uses an unpinned tag.

    The deployment-server detection (running ``sapphire-*-db`` containers) is
    done in the SHELL helper via ``docker ps``, NOT here, to keep this module
    import-light and subprocess-free.
    """
    if image.endswith(_UNPINNED_TAG_SUFFIXES):
        logger = logging.getLogger(__name__)
        logger.warning(
            "Resolved Docker image '%s' uses an unpinned tag. "
            "Operational deployments must pin to the release tag "
            "(e.g. mabesa/sapphire-prepgateway:v1.0.0).",
            image,
        )


def resolve_image(
    cli_override: str | None,
    configured_tag: str | None,
    *,
    warn_on_unpinned: bool = True,
) -> tuple[str, str]:
    """Resolve the Docker image string.

    Order: CLI override, then configured tag turned into
    ``mabesa/sapphire-prepgateway:<tag>``, then ``FALLBACK_IMAGE``.

    Args:
        cli_override: image string from ``--image`` flag, or None / empty
            string if the operator did not pass one.
        configured_tag: short tag string from the env file
            (``ieasyhydroforecast_backend_docker_image_tag``), or None / empty
            string if the env file did not set one.
        warn_on_unpinned: when True (default), log a WARNING if the resolved
            image ends with ``:local`` or ``:latest``.

    Returns:
        Tuple ``(image_string, source)`` where ``source`` is one of
        ``"cli"``, ``"configured"``, or ``"fallback"``.
    """
    # Empty-string treated as None (operators pass "" for unset CLI flags).
    cli = cli_override if cli_override else None
    cfg = configured_tag if configured_tag else None

    if cli is not None:
        image = cli
        source = "cli"
    elif cfg is not None:
        image = f"mabesa/sapphire-prepgateway:{cfg}"
        source = "configured"
    else:
        image = FALLBACK_IMAGE
        source = "fallback"

    if warn_on_unpinned:
        _warn_if_unpinned_tag(image)
    return image, source


# ---------------------------------------------------------------------------
# Mode detection
# ---------------------------------------------------------------------------


def detect_mode(
    *,
    target_count: int,
    target_min_date: str | None,
    cutoff_fallback: str | None = None,
) -> tuple[str, str | None]:
    """Decide migration MODE from target table state.

    Args:
        target_count: integer row count from ``SELECT COUNT(*) FROM <table>``.
        target_min_date: ISO date string from ``SELECT MIN(date) FROM <table>``,
            or None if NULL / no rows.
        cutoff_fallback: optional override cutoff date (operator-supplied);
            when provided AND target is non-empty, used instead of
            ``target_min_date``.

    Returns:
        Tuple ``(mode, cutoff)`` where:
            - mode is ``"full-import"`` or ``"pre-cutoff"``
            - cutoff is None for full-import; ISO date string for pre-cutoff

    Rules:
        - ``target_count == 0`` OR ``target_min_date is None`` -> ``("full-import", None)``
        - otherwise -> ``("pre-cutoff", cutoff_fallback or target_min_date)``

    Never produces a SQL ``WHERE date < NULL`` condition.
    """
    if target_count == 0 or target_min_date is None:
        return ("full-import", None)
    cutoff = cutoff_fallback if cutoff_fallback else target_min_date
    return ("pre-cutoff", cutoff)


# ---------------------------------------------------------------------------
# Manifest validation
# ---------------------------------------------------------------------------


_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _parse_manifest_text(text: str) -> dict[str, str]:
    """Parse manifest text into a key=value dict.

    Skips blank lines and ``#``-prefixed comments. Trailing whitespace on
    values is stripped. First ``=`` separates key from value (so values may
    contain ``=``).
    """
    out: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            # Tolerate stray non-key lines (logged but not fatal).
            continue
        key, _, value = line.partition("=")
        out[key.strip()] = value.strip()
    return out


def _csv_row_summary(csv_path: pathlib.Path) -> tuple[int, set[str], str | None, str | None]:
    """Return (row_count_excl_header, distinct_codes, date_min, date_max)."""
    row_count = 0
    codes: set[str] = set()
    date_min: str | None = None
    date_max: str | None = None
    with csv_path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_count += 1
            code = (row.get("code") or "").strip()
            if code:
                codes.add(code)
            date_val = (row.get("date") or "").strip()
            if date_val:
                # Use lexicographic min/max for ISO YYYY-MM-DD strings.
                if date_min is None or date_val < date_min:
                    date_min = date_val
                if date_max is None or date_val > date_max:
                    date_max = date_val
    return row_count, codes, date_min, date_max


def validate_manifest(
    export_csv_path: str | os.PathLike,
    expected_export_type: str,
) -> dict[str, str]:
    """Parse and validate the manifest beside an export CSV.

    Args:
        export_csv_path: path to the CSV file. The manifest is expected at
            ``<export_csv_path>.manifest`` (sibling of the CSV).
        expected_export_type: one of ALLOWED_EXPORT_TYPES; the wrapper's
            declared type. Mismatch raises ManifestExportTypeMismatchError.

    Returns:
        Parsed manifest as ``dict[str, str]`` with all ``key=value`` lines.

    Raises:
        ManifestMissingError: manifest file not found.
        ManifestExportTypeMismatchError: manifest export_type unknown or does
            not match expected_export_type.
        ManifestRowCountMismatchError: manifest row_count is missing, not an
            integer, or does not equal the parsed CSV row count
            (counted with csv.reader, excluding header).
        ManifestStationCountMismatchError: manifest station_count does not
            equal the count of distinct ``code`` values in the CSV.
        ManifestDateRangeMismatchError: manifest date_min or date_max does not
            match the min/max of the CSV's ``date`` column.

    Notes:
        Manifest format: one ``key=value`` per line, blank lines and ``#``
        comments allowed. Required keys (v2 R6 expanded):
        ``export_type``, ``row_count``, ``station_count``, ``date_min``,
        ``date_max``. Other keys logged but not validated.
    """
    csv_path = pathlib.Path(export_csv_path)
    manifest_path = csv_path.with_name(csv_path.name + ".manifest")

    if not manifest_path.is_file():
        raise ManifestMissingError(f"manifest file not found beside export CSV: {manifest_path}")

    manifest = _parse_manifest_text(manifest_path.read_text(encoding="utf-8"))

    # 1. export_type
    export_type = manifest.get("export_type", "")
    if export_type not in ALLOWED_EXPORT_TYPES:
        raise ManifestExportTypeMismatchError(
            f"manifest export_type {export_type!r} is not in ALLOWED_EXPORT_TYPES "
            f"({sorted(ALLOWED_EXPORT_TYPES)})"
        )
    if export_type != expected_export_type:
        raise ManifestExportTypeMismatchError(
            f"manifest export_type {export_type!r} does not match wrapper's "
            f"expected type {expected_export_type!r}"
        )

    # 2. row_count
    row_count_raw = manifest.get("row_count", "").strip()
    try:
        manifest_row_count = int(row_count_raw)
    except ValueError as exc:
        raise ManifestRowCountMismatchError(
            f"manifest row_count is missing or not an integer: {row_count_raw!r}"
        ) from exc

    csv_row_count, csv_codes, csv_date_min, csv_date_max = _csv_row_summary(csv_path)
    if manifest_row_count != csv_row_count:
        raise ManifestRowCountMismatchError(
            f"manifest row_count={manifest_row_count} does not equal csv "
            f"row_count={csv_row_count} for {csv_path.name}"
        )

    # 3. station_count (v2 R6)
    station_count_raw = manifest.get("station_count", "").strip()
    try:
        manifest_station_count = int(station_count_raw)
    except ValueError as exc:
        raise ManifestStationCountMismatchError(
            f"manifest station_count is missing or not an integer: {station_count_raw!r}"
        ) from exc
    csv_station_count = len(csv_codes)
    if manifest_station_count != csv_station_count:
        raise ManifestStationCountMismatchError(
            f"manifest station_count={manifest_station_count} does not equal "
            f"csv distinct station count={csv_station_count} for {csv_path.name}"
        )

    # 4 + 5. date_min / date_max (v2 R6)
    manifest_date_min = manifest.get("date_min", "").strip()
    manifest_date_max = manifest.get("date_max", "").strip()
    if not _ISO_DATE_RE.match(manifest_date_min):
        raise ManifestDateRangeMismatchError(
            f"manifest date_min {manifest_date_min!r} is missing or not ISO date "
            f"(YYYY-MM-DD) for {csv_path.name}"
        )
    if not _ISO_DATE_RE.match(manifest_date_max):
        raise ManifestDateRangeMismatchError(
            f"manifest date_max {manifest_date_max!r} is missing or not ISO date "
            f"(YYYY-MM-DD) for {csv_path.name}"
        )
    if manifest_date_min != csv_date_min:
        raise ManifestDateRangeMismatchError(
            f"manifest date_min={manifest_date_min!r} does not equal csv "
            f"date_min={csv_date_min!r} for {csv_path.name}"
        )
    if manifest_date_max != csv_date_max:
        raise ManifestDateRangeMismatchError(
            f"manifest date_max={manifest_date_max!r} does not equal csv "
            f"date_max={csv_date_max!r} for {csv_path.name}"
        )

    return manifest


# ---------------------------------------------------------------------------
# Temp workspace acquisition
# ---------------------------------------------------------------------------


def acquire_temp_workspace(
    data_root_dir: str | os.PathLike,
    wrapper_short_name: str,
    *,
    timestamp: str | None = None,
) -> pathlib.Path:
    """Create a temp dir under the deployment data root with strict permissions.

    Path layout::

        <data_root_dir>/logs/<wrapper_short_name>_tmp/<timestamp>

    where ``timestamp`` defaults to ``datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")``.

    Creates the directory with mode ``0o700`` (via ``os.makedirs`` + an explicit
    ``os.chmod`` because the mode passed to ``mkdir`` is masked by the current
    umask). Sets the process umask to ``0o077`` as a side effect so files
    written into the directory inherit restrictive permissions.

    Does NOT register a cleanup trap — the caller (shell wrapper) owns the
    ``EXIT INT TERM`` trap (v2 R3 widened signal set).

    v2 S2 NOTE on umask scope: the umask change inside this Python function is
    PROCESS-LOCAL — it dies with the inline python3 child and cannot leak up to
    the parent shell. The shell helper ``umh_acquire_temp_workspace`` separately
    applies ``umask 077`` to the shell process, which persists for the rest of
    the wrapper run (any subsequent file writes / scp inherit restrictive
    perms). If a later wrapper step must emit a shareable artifact, the wrapper
    must save and restore the umask around that step.

    Args:
        data_root_dir: deployment data root
            (``$ieasyhydroforecast_data_root_dir``).
        wrapper_short_name: short identifier, e.g. ``"runoff_day"``.
        timestamp: optional override (mainly for tests); defaults to current
            UTC time as ``YYYYMMDDTHHMMSSZ``.

    Returns:
        Path to the created directory.

    Raises:
        FileExistsError: if the directory already exists (caller must use a
            new timestamp; this avoids reusing a workspace with mixed
            permissions).
        PermissionError: if creation fails due to permissions.
    """
    if timestamp is None:
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    root = pathlib.Path(data_root_dir)
    target = root / "logs" / f"{wrapper_short_name}_tmp" / timestamp

    # Apply process-local umask (will outlive this function in CPython but not
    # outlive the inline python3 subprocess in the wrapper). See docstring.
    os.umask(0o077)

    # Parents are created lazily; the leaf must not pre-exist.
    if target.exists():
        raise FileExistsError(f"temp workspace already exists (pick a new timestamp): {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.mkdir(mode=0o700)
    # Re-chmod defensively in case the mode= argument was masked by umask
    # (POSIX allows this on some platforms).
    os.chmod(target, 0o700)
    return target


# ---------------------------------------------------------------------------
# Redacted logging
# ---------------------------------------------------------------------------


_SENTINEL_PREFIX = "19999"
_SENTINEL_HRU_CODES: frozenset[str] = frozenset({f"0000{i}" for i in range(10)})


def _is_sentinel_code(code: str) -> bool:
    """Return True iff `code` is a recognized sentinel (safe to leave un-redacted)."""
    code = (code or "").strip()
    if code.startswith(_SENTINEL_PREFIX):
        return True
    return code in _SENTINEL_HRU_CODES


def log_redacted_station_count(
    logger: logging.Logger,
    station_codes: list[str] | tuple[str, ...] | set[str],
    *,
    message_prefix: str = "stations",
) -> None:
    """Log only the count and a redacted summary of station codes.

    Never logs the actual codes. The 19999-class and 00000..00009 sentinels
    are exempted from redaction (they may appear in dry-run output for
    fixture-driven tests) but are still NOT printed individually — only
    annotated as ``sentinel-only``.

    Args:
        logger: stdlib logger to emit to.
        station_codes: iterable of station code strings.
        message_prefix: optional log message prefix.

    Behavior:
        Empty iterable -> ``"<prefix>: count=0"``
        All sentinels -> ``"<prefix>: count=N (sentinel-only: 19999-class)"``
        Otherwise     -> ``"<prefix>: count=N (all redacted)"``
    """
    codes = list(station_codes)
    n = len(codes)
    if n == 0:
        logger.info("%s: count=0", message_prefix)
        return
    if all(_is_sentinel_code(c) for c in codes):
        logger.info("%s: count=%d (sentinel-only: 19999-class)", message_prefix, n)
        return
    logger.info("%s: count=%d (all redacted)", message_prefix, n)
