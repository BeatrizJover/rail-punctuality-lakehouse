import datetime as dt

import pytest

from src.rail.ingest import (
    REQUIRED_D1_COLUMNS,
    REQUIRED_OPERATIONAL_POINT_COLUMNS,
    StaleExportError,
    validate_d1_export,
    validate_operational_point_export,
)

HEADER = ";".join(REQUIRED_D1_COLUMNS)
OP_HEADER = ";".join(REQUIRED_OPERATIONAL_POINT_COLUMNS + ["classification", "class_fr"])


def payload(rows, bom=False):
    text = "\n".join([HEADER, *rows])
    content = text.encode("utf-8")
    return (b"\xef\xbb\xbf" + content) if bom else content


def test_current_date_export_returns_expected_date():
    expected = dt.date(2026, 9, 18)
    content = payload([f"{expected:%Y-%m-%d};1234"], bom=True)
    assert validate_d1_export(content, ";", expected) == expected


def test_previous_date_export_raises_stale_export_error():
    expected = dt.date(2026, 9, 18)
    previous = expected - dt.timedelta(days=1)
    content = payload([f"{previous:%Y-%m-%d};1234"])
    with pytest.raises(StaleExportError):
        validate_d1_export(content, ";", expected)


def test_header_only_export_raises_stale_export_error():
    expected = dt.date(2026, 9, 18)
    content = payload([])
    with pytest.raises(StaleExportError):
        validate_d1_export(content, ";", expected)


def test_missing_date_column_raises_value_error():
    expected = dt.date(2026, 9, 18)
    content = ("TRAIN_NO\n1234").encode("utf-8")
    with pytest.raises(ValueError):
        validate_d1_export(content, ";", expected)


def test_lowercase_header_returns_expected_date():
    expected = dt.date(2026, 9, 18)
    text = "\n".join([HEADER.lower(), f"{expected:%Y-%m-%d};1234"])
    assert validate_d1_export(text.encode("utf-8"), ";", expected) == expected


def test_missing_required_column_raises_value_error_naming_it():
    expected = dt.date(2026, 9, 18)
    header = ";".join(c for c in REQUIRED_D1_COLUMNS if c != "DELAY_ARR")
    content = "\n".join([header, f"{expected:%Y-%m-%d};1234"]).encode("utf-8")
    with pytest.raises(ValueError, match="DELAY_ARR"):
        validate_d1_export(content, ";", expected)


def op_payload(rows):
    text = "\n".join([OP_HEADER, *rows])
    return text.encode("utf-8")


def test_operational_point_export_returns_row_count():
    row = "7;AALST-OOST;AALST-OOST;Aalst-Oost;FLSO;Station;50.93, 4.05;Station;Gare"
    content = op_payload([row, row])
    assert validate_operational_point_export(content, ";") == 2


def test_operational_point_empty_export_raises_value_error():
    content = op_payload([])
    with pytest.raises(ValueError):
        validate_operational_point_export(content, ";")


def test_operational_point_missing_required_column_raises_value_error():
    content = ("ptcarid;longnamedutch\n7;AALST-OOST").encode("utf-8")
    with pytest.raises(ValueError):
        validate_operational_point_export(content, ";")
