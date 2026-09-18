"""Validation of the Infrabel D-1 export before it is landed in Bronze."""

import csv
import datetime as dt
import io

DATE_COLUMN = "DATDEP"


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
