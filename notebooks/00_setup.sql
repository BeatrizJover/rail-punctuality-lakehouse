-- Initializes Unity Catalog objects: catalogs, schemas, and the landing volume
CREATE CATALOG IF NOT EXISTS IDENTIFIER(:catalog);

USE CATALOG IDENTIFIER(:catalog);

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS ops;

CREATE VOLUME IF NOT EXISTS bronze.landing;

CREATE VOLUME IF NOT EXISTS bronze.checkpoints;