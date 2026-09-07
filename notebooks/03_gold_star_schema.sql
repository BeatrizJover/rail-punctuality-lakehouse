-- Gold layer dimensional model for reporting and BI analytics.
-- Dimensions and fact are maintained incrementally for a single service_date,
-- supplied by the upstream silver_transform task value. No statement in this
-- file scans Silver in full, so daily runtime stays flat as history grows.

DECLARE OR REPLACE VARIABLE run_service_date DATE;
SET VARIABLE run_service_date = CAST(:service_date AS DATE);

-- dim_date -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rail_punctuality.gold.dim_date AS
SELECT
    d                        AS date_key,
    year(d)                  AS year,
    quarter(d)               AS quarter,
    month(d)                 AS month,
    date_format(d, 'MMMM')   AS month_name,
    weekofyear(d)            AS week_of_year,
    dayofweek(d)             AS day_of_week,
    date_format(d, 'EEEE')   AS day_name,
    dayofweek(d) IN (1, 7)   AS is_weekend
FROM (SELECT explode(sequence(DATE'2014-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS d);

-- dim_station ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rail_punctuality.gold.dim_station (
    station_key  STRING,
    station_name STRING,
    ptcar_no     INT,
    first_seen   DATE,
    last_seen    DATE
)
USING DELTA;

COMMENT ON TABLE rail_punctuality.gold.dim_station IS
  'One row per measuring point. first_seen and last_seen are idempotent accumulators, so reruns and out-of-order historical backfills converge to the same result.';

MERGE INTO rail_punctuality.gold.dim_station t
USING (
    SELECT
        stop_point_key       AS station_key,
        max(stop_point_name) AS station_name,
        max(ptcar_no)        AS ptcar_no,
        min(service_date)    AS first_seen,
        max(service_date)    AS last_seen
    FROM rail_punctuality.silver.stop_event
    WHERE service_date = run_service_date
    GROUP BY stop_point_key
) s
ON t.station_key = s.station_key
WHEN MATCHED THEN UPDATE SET
    t.station_name = coalesce(s.station_name, t.station_name),
    t.ptcar_no     = coalesce(s.ptcar_no, t.ptcar_no),
    t.first_seen   = least(t.first_seen, s.first_seen),
    t.last_seen    = greatest(t.last_seen, s.last_seen)
WHEN NOT MATCHED THEN INSERT *;

-- dim_relation ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rail_punctuality.gold.dim_relation (
    relation_key       STRING,
    relation           STRING,
    relation_direction STRING,
    operator           STRING
)
USING DELTA;

-- relation_key is the MD5 of the three attributes the row carries, so a matched row can never differ from its source. 
MERGE INTO rail_punctuality.gold.dim_relation t
USING (
    SELECT DISTINCT
        md5(concat_ws('|', relation, relation_direction, operator)) AS relation_key,
        relation,
        relation_direction,
        operator
    FROM rail_punctuality.silver.stop_event
    WHERE service_date = run_service_date
) s
ON t.relation_key = s.relation_key
WHEN NOT MATCHED THEN INSERT *;

-- fact_stop_event ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rail_punctuality.gold.fact_stop_event (
    date_key          DATE,
    station_key       STRING,
    relation_key      STRING,
    train_no          INT,
    planned_hour      INT,
    delay_arr_s       INT,
    delay_dep_s       INT,
    dwell_delta_s     INT,
    punctual_arrivals INT,
    stop_events       INT
)
USING DELTA
CLUSTER BY (date_key, station_key);

COMMENT ON TABLE rail_punctuality.gold.fact_stop_event IS
  'Grain: one train passing one measuring point on one service date. Additive measures: stop_events, punctual_arrivals.';

MERGE INTO rail_punctuality.gold.fact_stop_event t
USING (
    SELECT
        s.service_date   AS date_key,
        s.stop_point_key AS station_key,
        md5(concat_ws('|', s.relation, s.relation_direction, s.operator)) AS relation_key,
        s.train_no,
        s.planned_hour,
        s.delay_arr_s,
        s.delay_dep_s,
        s.dwell_delta_s,
        CAST(s.is_punctual_arr AS INT) AS punctual_arrivals,
        1                              AS stop_events
    FROM rail_punctuality.silver.stop_event s
    WHERE s.service_date = run_service_date
) s
ON  t.date_key    = run_service_date
AND t.date_key    = s.date_key
AND t.station_key = s.station_key
AND t.train_no    = s.train_no
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
