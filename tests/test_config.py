import importlib
import sys

import pytest

from src.rail import config


@pytest.fixture
def reload_config(monkeypatch):
    def _reload(argv):
        monkeypatch.setattr(sys, "argv", argv)
        return importlib.reload(config)

    yield _reload
    monkeypatch.undo()
    importlib.reload(config)


def test_default_resolution_matches_prod_literals(reload_config):
    cfg = reload_config(["pytest", "-q", "tests/"])

    assert cfg.CATALOG == "rail_punctuality"
    assert cfg.BRONZE == "rail_punctuality.bronze"
    assert cfg.SILVER == "rail_punctuality.silver"
    assert cfg.GOLD == "rail_punctuality.gold"
    assert cfg.OPS == "rail_punctuality.ops"
    assert cfg.LANDING == "/Volumes/rail_punctuality/bronze/landing"
    assert cfg.CHECKPOINTS == "/Volumes/rail_punctuality/bronze/checkpoints"
    assert cfg.MONTHLY_LANDING == "/Volumes/rail_punctuality/bronze/landing/monthly"
    assert cfg.OPERATIONAL_POINT_LANDING == "/Volumes/rail_punctuality/bronze/landing/reference"
    assert cfg.BRONZE_RAW == "rail_punctuality.bronze.punctuality_raw"
    assert cfg.BRONZE_STATION_REF == "rail_punctuality.bronze.station_ref"
    assert cfg.BRONZE_RAW_MONTHLY == "rail_punctuality.bronze.punctuality_raw_monthly"
    assert cfg.BRONZE_OPERATIONAL_POINT == "rail_punctuality.bronze.operational_point"
    assert cfg.BRONZE_IRAIL_STATION == "rail_punctuality.bronze.irail_station"
    assert cfg.SILVER_STOP == "rail_punctuality.silver.stop_event"
    assert cfg.DQ_RESULTS == "rail_punctuality.ops.dq_results"
    assert cfg.GOLD_DIM_STATION == "rail_punctuality.gold.dim_station"


@pytest.mark.parametrize("flag", [["--catalog", "rail_punctuality_dev"], ["--catalog=rail_punctuality_dev"]])
def test_catalog_override_propagates_to_derived_constants(reload_config, flag):
    cfg = reload_config(["job.py", "--unrelated", "x", *flag, "-v"])

    assert cfg.CATALOG == "rail_punctuality_dev"
    assert cfg.SILVER_STOP == "rail_punctuality_dev.silver.stop_event"
    assert cfg.GOLD_DIM_STATION == "rail_punctuality_dev.gold.dim_station"
    assert cfg.LANDING == "/Volumes/rail_punctuality_dev/bronze/landing"
    assert cfg.CHECKPOINTS == "/Volumes/rail_punctuality_dev/bronze/checkpoints"


def test_abbreviated_flag_is_not_picked_up(reload_config):
    assert reload_config(["job.py", "--cat", "other"]).CATALOG == "rail_punctuality"
