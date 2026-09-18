import datetime as dt

import pytest

from src.rail.ingest import StaleExportError, validate_d1_export

HEADER = "DATDEP;TRAIN_NO"


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
