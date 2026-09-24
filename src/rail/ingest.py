"""Validation of the Infrabel D-1 export before it is landed in Bronze."""

import csv
import datetime as dt
import io

DATE_COLUMN = "DATDEP"

# Columns the Silver crosswalk and downstream classification work depend on.
REQUIRED_OPERATIONAL_POINT_COLUMNS = [
    "ptcarid",
    "longnamedutch",
    "shortnamedutch",
    "commerciallongnamedutch",
    "symbolicname",
    "class_en",
    "geo_point_2d",
]


class StaleExportError(RuntimeError):
    """The upstream export does not yet contain the expected service date."""


def export_service_dates(content: bytes, sep: str) -> set[dt.date]:
    """Return the distinct service dates present in a D-1 CSV payload."""
    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")), delimiter=sep)
    if not reader.fieldnames or DATE_COLUMN not in reader.fieldnames:
        raise ValueError(f"D-1 export has no {DATE_COLUMN} column: {reader.fieldnames}")
    return {
        dt.date.fromisoformat(row[DATE_COLUMN][:10])
        for row in reader
        if row.get(DATE_COLUMN)
    }


def validate_d1_export(content: bytes, sep: str, expected: dt.date) -> dt.date:
    """Check that the payload covers the expected service date and return it.

    A fetch that runs before the upstream refresh returns the previous service
    date. Raising lets the task retry policy wait for the refresh instead of
    landing a stale file.
    """
    dates = export_service_dates(content, sep)
    if not dates:
        raise StaleExportError("D-1 export contains no rows")
    latest = max(dates)
    if latest != expected:
        raise StaleExportError(f"D-1 export holds service date {latest}; expected {expected}")
    return latest


def validate_operational_point_export(content: bytes, sep: str) -> int:
    """Check the operational point export is non-empty and has the required columns.

    Every column is kept as published in Bronze, with no declared DDL to enforce
    a schema, so a silently renamed upstream column would otherwise land as a
    table with missing fields instead of failing the run.
    """
    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")), delimiter=sep)
    missing = set(REQUIRED_OPERATIONAL_POINT_COLUMNS) - set(reader.fieldnames or [])
    if missing:
        raise ValueError(f"operational point export missing required columns: {sorted(missing)}")
    row_count = sum(1 for _ in reader)
    if row_count == 0:
        raise ValueError("operational point export contains no rows")
    return row_count
