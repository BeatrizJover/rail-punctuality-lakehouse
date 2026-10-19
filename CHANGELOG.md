# Changelog

All notable changes to this project are documented here. Versions follow [Semantic Versioning](https://semver.org/).

## [2.0.0] — 2026-10-19

Stations are now keyed on Infrabel's official `PTCAR_NO`, the Gold export moves to contract v2, and data quality now enforces its rules instead of only recording them. The pipeline also gains the monthly feed, history backfill, station classification, CI and per-target environments.

### ⚠️ Breaking changes

- **Station identity.** `gold.dim_station.station_key` is now an `INT` (`PTCAR_NO`), resolved through `silver.station_alias`, instead of an MD5 string of the station name. Unresolved stop points map to `-1`. `station_ref` is retired.
- **Fact grain.** `gold.fact_stop_event` is keyed on `(date_key, stop_point_key, train_no)`; `station_key` is a foreign key.
- **Gold export contract v2.** Published under a new `gold_v2/` prefix with `contract_version: 2` in the manifest and a 9-column `dim_station` (adds `ptcar_no`, `station_type`, `is_passenger`, `latitude`, `longitude`). The v1 `gold/` prefix is left untouched. Consumers must re-read from `gold_v2/`; mixing v1 and v2 partitions breaks PyArrow schema inference.
- **Data quality semantics in `ops.dq_results`.** Metric rules (e.g. `ptcar_no_coverage`) write `passed = NULL`. An assertion evaluated over zero rows now fails. Failed blocking Silver assertions fail the `data_quality` task, so the job reports the run as failed.

### Added

- **Monthly feed:** monthly export ingest with a declared column contract and date normalisation (`01c`).
- **Backfill:** year-parameterised Silver/Gold backfill (`91`).
- **Reference data:** Infrabel operational points (`01d`) and the iRail NMBS/SNCB station list (`01e`).
- **Station enrichment:** `dim_station` gains `station_type`, `is_passenger` and coordinates.
- **Export:** Gold published as date-partitioned Parquet with a coverage manifest. `92b` writes to a UC Volume; `92` writes to Azure Blob and is not configured yet. An optional `end_date` widget bounds the export window. 100-row fixtures are extracted for the RAG.
- **CI:** ruff and pytest run on GitHub Actions.

### Changed

- **Incremental Gold:** `fact_stop_event` and `dim_relation` are maintained by `MERGE` scoped to the run's `service_date`, and `dim_station` is rebuilt from the aliases. No step scans the full Silver table.
- **Bounded Silver read:** the daily Silver run reads only the last 5 days of Bronze, drops blank station names, and fails on an empty window or a processing gap.
- **Bounded drift check:** the Bronze schema-drift check is bounded to the same 5-day window.
- **Environment isolation:** each bundle target resolves its own catalog (`rail_punctuality_dev` / `rail_punctuality`). The dev schedule is paused.
- **Schedule:** the daily job runs at 10:00 Europe/Brussels.

### Fixed

- **D-1 export:** stale or empty exports are rejected before landing, and landed files are named by fetch timestamp.
- **D-1 header:** the export is read and validated case-insensitively after an upstream column rename.

**Full changelog:** https://github.com/BeatrizJover/rail-punctuality-lakehouse/compare/v1.0.0...v2.0.0

## [1.0.0] — 2026-08-19

First release: daily D-1 ingest from the Infrabel API, Silver stop events, Gold star schema on an MD5 station key, data quality rules recorded in `ops.dq_results`, and the scheduled job deployed with Databricks Asset Bundles.
