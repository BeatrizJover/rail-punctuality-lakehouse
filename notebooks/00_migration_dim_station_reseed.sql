-- ONE-SHOT MIGRATION — run once, manually, before deploying the incremental
-- gold script. Not part of the scheduled job.
--
-- Reseeds dim_station without observed_stop_events (a fact-derived count that
-- cannot be maintained idempotently and is better expressed as a DAX measure
-- over fact_stop_event). This is the last full scan of Silver this table needs;
-- from here on 03_gold_star_schema.sql maintains it by MERGE.

CREATE OR REPLACE TABLE rail_punctuality.gold.dim_station AS
SELECT
    stop_point_key       AS station_key,
    max(stop_point_name) AS station_name,
    max(ptcar_no)        AS ptcar_no,
    min(service_date)    AS first_seen,
    max(service_date)    AS last_seen
FROM rail_punctuality.silver.stop_event
GROUP BY stop_point_key;

-- Expected: one row per distinct stop_point_key, ptcar_no populated for every
-- station covered by the monthly feed.
SELECT
    count(*)                                        AS stations,
    count(ptcar_no)                                 AS with_ptcar_no,
    min(first_seen)                                 AS earliest,
    max(last_seen)                                  AS latest
FROM rail_punctuality.gold.dim_station;
