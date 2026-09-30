"""Validation of the Infrabel D-1 export before it is landed in Bronze."""

import csv
import datetime as dt
import io

DATE_COLUMN = "DATDEP"

# Every D-1 field the Silver transform consumes. PTCAR_NO is absent: the D-1
# feed does not carry it.
REQUIRED_D1_COLUMNS = [
    "DATDEP", "TRAIN_NO", "RELATION", "RELATION_DIRECTION", "TRAIN_SERV",
    "PTCAR_LG_NM_NL", "LINE_NO_DEP", "LINE_NO_ARR",
    "PLANNED_DATE_ARR", "PLANNED_TIME_ARR",
    "PLANNED_DATE_DEP", "PLANNED_TIME_DEP",
    "REAL_DATE_ARR", "REAL_TIME_ARR",
    "REAL_DATE_DEP", "REAL_TIME_DEP",
    "DELAY_ARR", "DELAY_DEP",
]

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

# Columns the crosswalk to operational_point.taftapcode and downstream
# is_passenger classification depend on.
REQUIRED_IRAIL_STATION_COLUMNS = [
    "taf-tap-code",
    "country-code",
    "name",
]


class StaleExportError(RuntimeError):
    """The upstream export does not yet contain the expected service date."""


def export_service_dates(content: bytes, sep: str) -> set[dt.date]:
    """Return the distinct service dates present in a D-1 CSV payload."""
    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")), delimiter=sep)
    # Upstream header casing is not stable; row keys follow the file, so
    # resolve each canonical name to the key as received.
    header = {name.upper(): name for name in reader.fieldnames or []}
    missing = [col for col in REQUIRED_D1_COLUMNS if col not in header]
    if missing:
        raise ValueError(
            f"D-1 export missing required columns {missing}: {reader.fieldnames}"
        )
    date_key = header[DATE_COLUMN]
    return {
        dt.date.fromisoformat(row[date_key][:10])
        for row in reader
        if row.get(date_key)
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


def validate_irail_station_export(content: bytes) -> int:
    """Check the iRail station list is non-empty and has the required columns.

    Every column is kept as published in Bronze, with no declared DDL to enforce
    a schema, so a silently renamed upstream column would otherwise land as a
    table with missing fields instead of failing the run.
    """
    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
    missing = set(REQUIRED_IRAIL_STATION_COLUMNS) - set(reader.fieldnames or [])
    if missing:
        raise ValueError(f"iRail station export missing required columns: {sorted(missing)}")
    row_count = sum(1 for _ in reader)
    if row_count == 0:
        raise ValueError("iRail station export contains no rows")
    return row_count
