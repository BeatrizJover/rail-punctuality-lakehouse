-- Initializes Unity Catalog objects: catalogs, schemas, and the landing volume
CREATE CATALOG IF NOT EXISTS rail_punctuality;

CREATE SCHEMA IF NOT EXISTS rail_punctuality.bronze;
CREATE SCHEMA IF NOT EXISTS rail_punctuality.silver;
CREATE SCHEMA IF NOT EXISTS rail_punctuality.gold;
CREATE SCHEMA IF NOT EXISTS rail_punctuality.ops;

CREATE VOLUME IF NOT EXISTS rail_punctuality.bronze.landing;

CREATE VOLUME IF NOT EXISTS rail_punctuality.bronze.checkpoints;