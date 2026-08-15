-- Gold layer dimensional model for reporting and BI analytics

-- dim_date
CREATE OR REPLACE TABLE rail_punctuality.gold.dim_date AS
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

-- dim_station
CREATE OR REPLACE TABLE rail_punctuality.gold.dim_station AS
SELECT
    stop_point_key         AS station_key,
    max(stop_point_name)   AS station_name,
    max(ptcar_no)          AS ptcar_no,
    count(*)                AS observed_stop_events,
    min(service_date)        AS first_seen,
    max(service_date)         AS last_seen
FROM rail_punctuality.silver.stop_event
GROUP BY stop_point_key;

-- dim_relation
CREATE OR REPLACE TABLE rail_punctuality.gold.dim_relation AS
SELECT
    md5(concat_ws('|', relation, relation_direction, operator)) AS relation_key,
    relation,
    relation_direction,
    operator
FROM rail_punctuality.silver.stop_event
GROUP BY relation, relation_direction, operator;

-- fact_stop_event
CREATE OR REPLACE TABLE rail_punctuality.gold.fact_stop_event
CLUSTER BY (date_key, station_key) AS
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
FROM rail_punctuality.silver.stop_event s;

COMMENT ON TABLE rail_punctuality.gold.fact_stop_event IS
  'Grain: one train passing one measuring point on one service date. Additive measures: stop_events, punctual_arrivals.';