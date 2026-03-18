--==============================
-- Database and SCHEMAs
--==============================
USE ROLE SYSADMIN;
 
CREATE DATABASE IF NOT EXISTS CITYRIDE_DB
  COMMENT = 'CityRide unified bike-rental data platform';
 
-- Layer schemas
CREATE SCHEMA IF NOT EXISTS CITYRIDE_DB.RAW
  COMMENT = 'Staging layer — exact source shape, all varchar, append-only';
 
CREATE SCHEMA IF NOT EXISTS CITYRIDE_DB.VALIDATED
  COMMENT = 'Type-cast, deduped, DQ-checked, referential-integrity verified';
 
CREATE SCHEMA IF NOT EXISTS CITYRIDE_DB.CURATED
  COMMENT = 'Star schema — SCD2 dimensions + fact_rental';
 
CREATE SCHEMA IF NOT EXISTS CITYRIDE_DB.ANALYTICS
  COMMENT = 'KPI snapshot tables, audit log, BI-ready';
 
-- Governance schema (masking policies, row-access policies live here)
CREATE SCHEMA IF NOT EXISTS CITYRIDE_DB.GOVERNANCE
  COMMENT = 'Data masking and row-access policy objects';

  SHOW SCHEMAS;

--==============================
-- Roles and Priviliges
--==============================
  USE ROLE SECURITYADMIN;
 
-- ── functional roles ─────────────────────────────────────────
CREATE ROLE IF NOT EXISTS CITYRIDE_SYSADMIN
  COMMENT = 'Full platform DDL + DML ownership';
 
CREATE ROLE IF NOT EXISTS CITYRIDE_PIPELINE
  COMMENT = 'Service account role — runs ingestion and SP chain';
 
CREATE ROLE IF NOT EXISTS CITYRIDE_ANALYST
  COMMENT = 'Read VALIDATED + CURATED + ANALYTICS; PII masked';
 
CREATE ROLE IF NOT EXISTS CITYRIDE_OPS_NORTH
  COMMENT = 'Ops team — North region only, row-level filtered';
 
CREATE ROLE IF NOT EXISTS CITYRIDE_OPS_SOUTH
  COMMENT = 'Ops team — South region only, row-level filtered';
 
CREATE ROLE IF NOT EXISTS CITYRIDE_OPS_EAST
  COMMENT = 'Ops team — East region only, row-level filtered';
 
CREATE ROLE IF NOT EXISTS CITYRIDE_GOVERNANCE
  COMMENT = 'Applies and manages masking + row-access policies';
 
-- ── role hierarchy ────────────────────────────────────────────
GRANT ROLE CITYRIDE_SYSADMIN   TO ROLE SYSADMIN;
GRANT ROLE CITYRIDE_PIPELINE   TO ROLE CITYRIDE_SYSADMIN;
GRANT ROLE CITYRIDE_ANALYST    TO ROLE CITYRIDE_SYSADMIN;
GRANT ROLE CITYRIDE_OPS_NORTH  TO ROLE CITYRIDE_SYSADMIN;
GRANT ROLE CITYRIDE_OPS_SOUTH  TO ROLE CITYRIDE_SYSADMIN;
GRANT ROLE CITYRIDE_OPS_EAST   TO ROLE CITYRIDE_SYSADMIN;
GRANT ROLE CITYRIDE_GOVERNANCE TO ROLE CITYRIDE_SYSADMIN;
 
-- ── warehouse grants ──────────────────────────────────────────

GRANT USAGE ON WAREHOUSE COMPUTE_WH      TO ROLE CITYRIDE_ANALYST;
GRANT USAGE ON WAREHOUSE COMPUTE_WH      TO ROLE CITYRIDE_OPS_NORTH;
GRANT USAGE ON WAREHOUSE COMPUTE_WH      TO ROLE CITYRIDE_OPS_SOUTH;
GRANT USAGE ON WAREHOUSE COMPUTE_WH      TO ROLE CITYRIDE_OPS_EAST;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE CITYRIDE_PIPELINE;
 
-- ── database + schema privileges ─────────────────────────────
USE ROLE SYSADMIN;
 
-- PIPELINE role — full access to RAW + ANALYTICS (for audit writes),
--                read on VALIDATED so it can verify post-truncation
GRANT USAGE  ON DATABASE CITYRIDE_DB TO ROLE CITYRIDE_PIPELINE;
GRANT USAGE  ON SCHEMA CITYRIDE_DB.RAW        TO ROLE CITYRIDE_PIPELINE;
GRANT USAGE  ON SCHEMA CITYRIDE_DB.VALIDATED  TO ROLE CITYRIDE_PIPELINE;
GRANT USAGE  ON SCHEMA CITYRIDE_DB.CURATED    TO ROLE CITYRIDE_PIPELINE;
GRANT USAGE  ON SCHEMA CITYRIDE_DB.ANALYTICS  TO ROLE CITYRIDE_PIPELINE;
GRANT USAGE  ON SCHEMA CITYRIDE_DB.GOVERNANCE TO ROLE CITYRIDE_PIPELINE;


USE ROLE SYSADMIN;

-- ============================================================
-- RAW  (all VARCHAR — exact source shape, append-only)
-- ============================================================

USE SCHEMA CITYRIDE_DB.RAW;

CREATE TABLE IF NOT EXISTS STATIONS (
  _load_id     VARCHAR   NOT NULL COMMENT 'Batch UUID',
  _loaded_at   TIMESTAMP NOT NULL COMMENT 'UTC load time',
  _source_file VARCHAR   NOT NULL COMMENT 'S3 file path (lineage)',
  station_id   VARCHAR,
  station_name VARCHAR,
  latitude     VARCHAR,
  longitude    VARCHAR,
  capacity     VARCHAR,
  neighborhood VARCHAR,
  city_zone    VARCHAR,
  install_date VARCHAR,
  status       VARCHAR
);

CREATE TABLE IF NOT EXISTS BIKES (
  _load_id          VARCHAR   NOT NULL,
  _loaded_at        TIMESTAMP NOT NULL,
  _source_file      VARCHAR   NOT NULL,
  bike_id           VARCHAR,
  bike_type         VARCHAR,
  status            VARCHAR,
  purchase_date     VARCHAR,
  last_service_date VARCHAR,
  odometer_km       VARCHAR,
  battery_level     VARCHAR,
  firmware_version  VARCHAR
);

CREATE TABLE IF NOT EXISTS RENTALS (
  _load_id         VARCHAR   NOT NULL,
  _loaded_at       TIMESTAMP NOT NULL,
  _source_file     VARCHAR   NOT NULL,
  rental_id        VARCHAR,
  user_id          VARCHAR,
  bike_id          VARCHAR,
  start_station_id VARCHAR,
  end_station_id   VARCHAR,
  start_time       VARCHAR,
  end_time         VARCHAR,
  duration_sec     VARCHAR,
  distance_km      VARCHAR,
  price            VARCHAR,
  plan_type        VARCHAR,
  channel          VARCHAR,
  device_info      VARCHAR,
  start_gps        VARCHAR,
  end_gps          VARCHAR,
  is_flagged       VARCHAR
);

CREATE TABLE IF NOT EXISTS USERS (
  _load_id          VARCHAR   NOT NULL,
  _loaded_at        TIMESTAMP NOT NULL,
  _source_file      VARCHAR   NOT NULL,
  user_id           VARCHAR,
  customer_name     VARCHAR,
  dob               VARCHAR,
  gender            VARCHAR,
  email             VARCHAR,
  phone             VARCHAR,
  address           VARCHAR,
  city              VARCHAR,
  state             VARCHAR,
  region            VARCHAR,
  kyc_status        VARCHAR,
  registration_date VARCHAR,
  is_student        VARCHAR,
  corporate_id      VARCHAR
);

-- Grant table access to PIPELINE role
GRANT SELECT, INSERT, DELETE ON ALL TABLES IN SCHEMA CITYRIDE_DB.RAW TO ROLE CITYRIDE_PIPELINE;
GRANT SELECT, INSERT, DELETE ON FUTURE TABLES IN SCHEMA CITYRIDE_DB.RAW TO ROLE CITYRIDE_PIPELINE;


-- ============================================================
-- VALIDATED  (typed, deduped, DQ-flagged)
-- ============================================================

USE SCHEMA CITYRIDE_DB.VALIDATED;

CREATE TABLE IF NOT EXISTS STATIONS (
  station_sk    BIGINT    NOT NULL AUTOINCREMENT PRIMARY KEY,
  station_id    VARCHAR   NOT NULL UNIQUE,
  station_name  VARCHAR   NOT NULL,
  latitude      FLOAT     NOT NULL,
  longitude     FLOAT     NOT NULL,
  capacity      INT       NOT NULL,
  neighborhood  VARCHAR,
  city_zone     VARCHAR   NOT NULL,
  install_date  DATE,
  status        VARCHAR,
  _dq_passed    BOOLEAN   NOT NULL DEFAULT TRUE,
  _dq_flags     VARIANT   COMMENT 'JSON array of failed rule names',
  _validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source_file  VARCHAR
);

CREATE TABLE IF NOT EXISTS BIKES (
  bike_sk           BIGINT    NOT NULL AUTOINCREMENT PRIMARY KEY,
  bike_id           VARCHAR   NOT NULL UNIQUE,
  bike_type         VARCHAR   NOT NULL,
  status            VARCHAR,
  purchase_date     DATE,
  last_service_date DATE,
  odometer_km       FLOAT,
  battery_level     INT,
  firmware_version  VARCHAR,
  _dq_passed        BOOLEAN   NOT NULL DEFAULT TRUE,
  _dq_flags         VARIANT,
  _validated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source_file      VARCHAR
);

CREATE TABLE IF NOT EXISTS RENTALS (
  rental_sk          BIGINT    NOT NULL AUTOINCREMENT PRIMARY KEY,
  rental_id          VARCHAR   NOT NULL UNIQUE,
  user_id            VARCHAR   NOT NULL,
  bike_id            VARCHAR   NOT NULL,
  start_station_id   VARCHAR   NOT NULL,
  end_station_id     VARCHAR,
  start_time         TIMESTAMP NOT NULL,
  end_time           TIMESTAMP,
  duration_sec       INT,
  distance_km        FLOAT,
  price              FLOAT,
  plan_type          VARCHAR,
  channel            VARCHAR,
  device_info        VARCHAR,
  start_lat          FLOAT,
  start_lon          FLOAT,
  end_lat            FLOAT,
  end_lon            FLOAT,
  is_flagged         BOOLEAN   DEFAULT FALSE,
  _dq_passed         BOOLEAN   NOT NULL DEFAULT TRUE,
  _dq_flags          VARIANT,
  _ref_checks_passed BOOLEAN,
  _validated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source_file       VARCHAR
);

CREATE TABLE IF NOT EXISTS USERS (
  user_sk           BIGINT    NOT NULL AUTOINCREMENT PRIMARY KEY,
  user_id           VARCHAR   NOT NULL UNIQUE,
  customer_name     VARCHAR,
  dob               DATE,
  gender            VARCHAR,
  email             VARCHAR,
  phone             VARCHAR,
  address           VARCHAR,
  city              VARCHAR,
  state             VARCHAR,
  region            VARCHAR   NOT NULL,
  kyc_status        VARCHAR,
  registration_date DATE,
  is_student        BOOLEAN,
  corporate_id      VARCHAR,
  _dq_passed        BOOLEAN   NOT NULL DEFAULT TRUE,
  _dq_flags         VARIANT,
  _validated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source_file      VARCHAR
);

GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA CITYRIDE_DB.VALIDATED TO ROLE CITYRIDE_PIPELINE;
GRANT SELECT, INSERT, UPDATE ON FUTURE TABLES IN SCHEMA CITYRIDE_DB.VALIDATED TO ROLE CITYRIDE_PIPELINE;
-- Analyst read
GRANT SELECT ON ALL TABLES IN SCHEMA CITYRIDE_DB.VALIDATED TO ROLE CITYRIDE_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CITYRIDE_DB.VALIDATED TO ROLE CITYRIDE_ANALYST;


-- ============================================================
-- CURATED  (SCD2 dimensions + fact table)
-- ============================================================

USE SCHEMA CITYRIDE_DB.CURATED;

CREATE TABLE IF NOT EXISTS DIM_STATION (
  station_sk     BIGINT    NOT NULL AUTOINCREMENT PRIMARY KEY,
  station_id     VARCHAR   NOT NULL,
  station_name   VARCHAR,
  latitude       FLOAT,
  longitude      FLOAT,
  capacity       INT,
  neighborhood   VARCHAR,
  city_zone      VARCHAR,
  install_date   DATE,
  status         VARCHAR,
  effective_from TIMESTAMP NOT NULL,
  effective_to   TIMESTAMP COMMENT 'NULL = current record',
  is_current     BOOLEAN   NOT NULL DEFAULT TRUE,
  _hash_diff     VARCHAR   COMMENT 'MD5 of SCD2-tracked columns',
  _curated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS DIM_BIKE (
  bike_sk           BIGINT    NOT NULL AUTOINCREMENT PRIMARY KEY,
  bike_id           VARCHAR   NOT NULL,
  bike_type         VARCHAR,
  status            VARCHAR,
  purchase_date     DATE,
  last_service_date DATE,
  odometer_km       FLOAT,
  battery_level     INT,
  firmware_version  VARCHAR,
  effective_from    TIMESTAMP NOT NULL,
  effective_to      TIMESTAMP,
  is_current        BOOLEAN   NOT NULL DEFAULT TRUE,
  _hash_diff        VARCHAR,
  _curated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS DIM_USER (
  user_sk           BIGINT    NOT NULL AUTOINCREMENT PRIMARY KEY,
  user_id           VARCHAR   NOT NULL,
  customer_name     VARCHAR,
  dob               DATE,
  gender            VARCHAR,
  email             VARCHAR,
  phone             VARCHAR,
  city              VARCHAR,
  state             VARCHAR,
  region            VARCHAR,
  city_zone         VARCHAR,
  kyc_status        VARCHAR,
  registration_date DATE,
  is_student        BOOLEAN,
  corporate_id      VARCHAR,
  effective_from    TIMESTAMP NOT NULL,
  effective_to      TIMESTAMP,
  is_current        BOOLEAN   NOT NULL DEFAULT TRUE,
  _hash_diff        VARCHAR,
  _curated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS DIM_DATE (
  date_sk      INT     NOT NULL PRIMARY KEY COMMENT 'YYYYMMDD integer',
  full_date    DATE    NOT NULL,
  year         INT,
  quarter      INT,
  month        INT,
  month_name   VARCHAR,
  week_of_year INT,
  day_of_week  INT     COMMENT '1 = Mon, 7 = Sun',
  day_name     VARCHAR,
  is_weekend   BOOLEAN DEFAULT FALSE,
  is_holiday   BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS FACT_RENTAL (
  rental_sk         BIGINT    NOT NULL AUTOINCREMENT PRIMARY KEY,
  rental_id         VARCHAR   NOT NULL UNIQUE,
  user_sk           BIGINT    REFERENCES DIM_USER(user_sk),
  bike_sk           BIGINT    REFERENCES DIM_BIKE(bike_sk),
  start_station_sk  BIGINT    REFERENCES DIM_STATION(station_sk),
  end_station_sk    BIGINT    REFERENCES DIM_STATION(station_sk),
  date_sk           INT       REFERENCES DIM_DATE(date_sk),
  plan_type         VARCHAR,
  channel           VARCHAR,
  device_info       VARCHAR,
  start_time        TIMESTAMP,
  end_time          TIMESTAMP,
  duration_sec      INT,
  distance_km       FLOAT,
  price             FLOAT,
  start_lat         FLOAT,
  start_lon         FLOAT,
  end_lat           FLOAT,
  end_lon           FLOAT,
  is_flagged        BOOLEAN   DEFAULT FALSE,
  anomaly_score     FLOAT     COMMENT '0-100; higher = more risk',
  anomaly_rules_hit VARIANT   COMMENT 'JSON array of triggered rule names',
  _curated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA CITYRIDE_DB.CURATED TO ROLE CITYRIDE_PIPELINE;
GRANT SELECT, INSERT, UPDATE ON FUTURE TABLES IN SCHEMA CITYRIDE_DB.CURATED TO ROLE CITYRIDE_PIPELINE;
-- Analyst read
GRANT SELECT ON ALL TABLES IN SCHEMA CITYRIDE_DB.CURATED TO ROLE CITYRIDE_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CITYRIDE_DB.CURATED TO ROLE CITYRIDE_ANALYST;
-- Ops read
GRANT SELECT ON ALL TABLES IN SCHEMA CITYRIDE_DB.CURATED TO ROLE CITYRIDE_OPS_NORTH;
GRANT SELECT ON ALL TABLES IN SCHEMA CITYRIDE_DB.CURATED TO ROLE CITYRIDE_OPS_SOUTH;
GRANT SELECT ON ALL TABLES IN SCHEMA CITYRIDE_DB.CURATED TO ROLE CITYRIDE_OPS_EAST;


-- ============================================================
-- ANALYTICS  (KPI snapshots + audit log)
-- ============================================================

USE SCHEMA CITYRIDE_DB.ANALYTICS;

CREATE TABLE IF NOT EXISTS AUDIT_LOG (
  log_id         BIGINT    NOT NULL AUTOINCREMENT PRIMARY KEY,
  batch_id       VARCHAR   NOT NULL,
  layer          VARCHAR   NOT NULL COMMENT 'RAW | VALIDATED | CURATED | ANALYTICS',
  domain         VARCHAR   COMMENT 'stations | bikes | rentals | users',
  table_name     VARCHAR,
  operation      VARCHAR   COMMENT 'COPY_INTO | VALIDATE | TRUNCATE_RAW | MERGE | KPI',
  rows_processed INT       DEFAULT 0,
  rows_inserted  INT       DEFAULT 0,
  rows_updated   INT       DEFAULT 0,
  rows_skipped   INT       DEFAULT 0,
  dq_failures    INT       DEFAULT 0,
  error_code     VARCHAR,
  error_message  VARCHAR,
  source_file    VARCHAR,
  started_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  finished_at    TIMESTAMP,
  duration_sec   FLOAT,
  status         VARCHAR   NOT NULL COMMENT 'SUCCESS | PARTIAL | FAILED'
);

CREATE TABLE IF NOT EXISTS KPI_ANOMALOUS_RENTAL_SCORE (
  snapshot_date       DATE  NOT NULL PRIMARY KEY,
  total_rentals       INT,
  flagged_rentals     INT,
  anomaly_probability FLOAT COMMENT '(flagged / total) * 100',
  top_rules_hit       VARIANT,
  _computed_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS KPI_STATION_AVAILABILITY (
  snapshot_date       DATE   NOT NULL,
  station_sk          BIGINT NOT NULL,
  station_id          VARCHAR,
  city_zone           VARCHAR,
  pct_time_available  FLOAT  COMMENT '% intervals with >=1 bike AND >=1 free dock',
  avg_bikes_available FLOAT,
  avg_docks_free      FLOAT,
  _computed_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (snapshot_date, station_sk)
);

CREATE TABLE IF NOT EXISTS KPI_RIDER_ENGAGEMENT (
  snapshot_date    DATE  NOT NULL PRIMARY KEY,
  total_registered INT,
  active_last_30d  INT,
  engagement_ratio FLOAT COMMENT '(active / total) * 100',
  _computed_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS KPI_FLEET_HEALTH (
  snapshot_date         DATE  NOT NULL PRIMARY KEY,
  total_bikes           INT,
  healthy_bikes         INT,
  health_index          FLOAT COMMENT '(healthy / total) * 100',
  ebikes_low_battery    INT,
  bikes_overdue_service INT,
  _computed_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS KPI_ARR_BY_CHANNEL (
  snapshot_date  DATE    NOT NULL,
  channel        VARCHAR NOT NULL COMMENT 'app | kiosk | corporate',
  plan_type      VARCHAR NOT NULL COMMENT 'payg | day_pass | monthly | annual',
  total_rentals  INT,
  total_revenue  FLOAT,
  avg_rental_rev FLOAT   COMMENT 'ARR = total_revenue / total_rentals',
  _computed_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (snapshot_date, channel, plan_type)
);

GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA CITYRIDE_DB.ANALYTICS TO ROLE CITYRIDE_PIPELINE;
GRANT SELECT, INSERT, UPDATE ON FUTURE TABLES IN SCHEMA CITYRIDE_DB.ANALYTICS TO ROLE CITYRIDE_PIPELINE;
-- All analyst + ops roles can read analytics
GRANT SELECT ON ALL TABLES IN SCHEMA CITYRIDE_DB.ANALYTICS TO ROLE CITYRIDE_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA CITYRIDE_DB.ANALYTICS TO ROLE CITYRIDE_OPS_NORTH;
GRANT SELECT ON ALL TABLES IN SCHEMA CITYRIDE_DB.ANALYTICS TO ROLE CITYRIDE_OPS_SOUTH;
GRANT SELECT ON ALL TABLES IN SCHEMA CITYRIDE_DB.ANALYTICS TO ROLE CITYRIDE_OPS_EAST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CITYRIDE_DB.ANALYTICS TO ROLE CITYRIDE_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CITYRIDE_DB.ANALYTICS TO ROLE CITYRIDE_OPS_NORTH;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CITYRIDE_DB.ANALYTICS TO ROLE CITYRIDE_OPS_SOUTH;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CITYRIDE_DB.ANALYTICS TO ROLE CITYRIDE_OPS_EAST;

-- Verify
SHOW TABLES IN SCHEMA CITYRIDE_DB.RAW;
SHOW TABLES IN SCHEMA CITYRIDE_DB.VALIDATED;
SHOW TABLES IN SCHEMA CITYRIDE_DB.CURATED;
SHOW TABLES IN SCHEMA CITYRIDE_DB.ANALYTICS;

--==============================
-- DATA INGESTION
--==============================

USE ROLE ACCOUNTADMIN;

-- ── Storage integration ───────────────────────────────────────

CREATE STORAGE INTEGRATION IF NOT EXISTS CITYRIDE_S3_INT
  TYPE                      = EXTERNAL_STAGE
  STORAGE_PROVIDER          = 'S3'
  ENABLED                   = TRUE
  STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::783764585001:role/citydriver'
  STORAGE_ALLOWED_LOCATIONS = ('s3://cityride-data')
  COMMENT                   = 'Snowflake to AWS S3 trust for CityRide';

DESC INTEGRATION CITYRIDE_S3_INT;


-- Grant integration usage to SYSADMIN so stages can be created
GRANT USAGE ON INTEGRATION CITYRIDE_S3_INT TO ROLE SYSADMIN;

-- ── File formats ──────────────────────────────────────────────

USE ROLE SYSADMIN;
USE SCHEMA CITYRIDE_DB.RAW;
 
CREATE FILE FORMAT IF NOT EXISTS CSV_FMT
  TYPE                         = CSV
  SKIP_HEADER                  = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF                      = ('NULL', 'null', '')
  EMPTY_FIELD_AS_NULL          = TRUE
  TRIM_SPACE                   = TRUE;
 
-- ── Single stage — all files in one bucket ────────────────────
 
CREATE STAGE IF NOT EXISTS CITYRIDE_DB.RAW.STG_CITYRIDE
  STORAGE_INTEGRATION = CITYRIDE_S3_INT
  URL                 = 's3://cityride-data/'
  FILE_FORMAT         = CSV_FMT
  COMMENT             = 'All CityRide source files — PATTERN routes per domain';
 
-- Verify
LIST @CITYRIDE_DB.RAW.STG_CITYRIDE;


-- ============================================================
-- SNOWPIPES
-- ============================================================

CREATE PIPE CITYRIDE_DB.RAW.PIPE_STATIONS
  AUTO_INGEST = TRUE
  COMMENT     = 'Event-driven ingest: stations_master.csv / stations_inc.csv'
AS
COPY INTO CITYRIDE_DB.RAW.STATIONS (
  _load_id, _loaded_at, _source_file,
  station_id, station_name, latitude, longitude,
  capacity, neighborhood, city_zone, install_date, status
)
FROM (
  SELECT
    'snowpipe'          AS _load_id,
    CURRENT_TIMESTAMP() AS _loaded_at,
    METADATA$FILENAME   AS _source_file,
    $1, $2, $3, $4, $5, $6, $7, $8, $9
  FROM @CITYRIDE_DB.RAW.STG_CITYRIDE
)
PATTERN  = '.*stations_(master|inc)\.csv'
ON_ERROR = CONTINUE;
 
 
-- ── BIKES  (3 meta + 8 data = 11 total) ───────────────────────
-- CSV cols: bike_id,bike_type,status,purchase_date,
--           last_service_date,odometer_km,battery_level,firmware_version
 
CREATE PIPE CITYRIDE_DB.RAW.PIPE_BIKES
  AUTO_INGEST = TRUE
  COMMENT     = 'Event-driven ingest: bikes_master.csv / bikes_inc.csv'
AS
COPY INTO CITYRIDE_DB.RAW.BIKES (
  _load_id, _loaded_at, _source_file,
  bike_id, bike_type, status,
  purchase_date, last_service_date,
  odometer_km, battery_level, firmware_version
)
FROM (
  SELECT
    'snowpipe'          AS _load_id,
    CURRENT_TIMESTAMP() AS _loaded_at,
    METADATA$FILENAME   AS _source_file,
    $1, $2, $3, $4, $5, $6, $7, $8
  FROM @CITYRIDE_DB.RAW.STG_CITYRIDE
)
PATTERN  = '.*bikes_(master|inc)\.csv'
ON_ERROR = CONTINUE;
 
 
-- ── RENTALS  (3 meta + 16 data = 19 total) ────────────────────
-- CSV cols: rental_id,user_id,bike_id,start_station_id,end_station_id,
--           start_time,end_time,duration_sec,distance_km,price,
--           plan_type,channel,device_info,start_gps,end_gps,is_flagged
 
CREATE PIPE CITYRIDE_DB.RAW.PIPE_RENTALS
  AUTO_INGEST = TRUE
  COMMENT     = 'Event-driven ingest: rentals_master.csv / rentals_inc.csv'
AS
COPY INTO CITYRIDE_DB.RAW.RENTALS (
  _load_id, _loaded_at, _source_file,
  rental_id, user_id, bike_id,
  start_station_id, end_station_id,
  start_time, end_time, duration_sec, distance_km,
  price, plan_type, channel, device_info,
  start_gps, end_gps, is_flagged
)
FROM (
  SELECT
    'snowpipe'          AS _load_id,
    CURRENT_TIMESTAMP() AS _loaded_at,
    METADATA$FILENAME   AS _source_file,
    $1,  $2,  $3,  $4,  $5,  $6,  $7,  $8,
    $9,  $10, $11, $12, $13, $14, $15, $16
  FROM @CITYRIDE_DB.RAW.STG_CITYRIDE
)
PATTERN  = '.*rentals_(master|inc)\.csv'
ON_ERROR = CONTINUE;
 
 
-- ── USERS  (3 meta + 14 data = 17 total) ──────────────────────
-- CSV cols: user_id,customer_name,dob,gender,email,phone,
--           address,city,state,region,kyc_status,
--           registration_date,is_student,corporate_id
 
CREATE PIPE CITYRIDE_DB.RAW.PIPE_USERS
  AUTO_INGEST = TRUE
  COMMENT     = 'Event-driven ingest: users_master.csv / users_inc.csv'
AS
COPY INTO CITYRIDE_DB.RAW.USERS (
  _load_id, _loaded_at, _source_file,
  user_id, customer_name, dob, gender,
  email, phone, address, city, state,
  region, kyc_status, registration_date,
  is_student, corporate_id
)
FROM (
  SELECT
    'snowpipe'          AS _load_id,
    CURRENT_TIMESTAMP() AS _loaded_at,
    METADATA$FILENAME   AS _source_file,
    $1,  $2,  $3,  $4,  $5,  $6,  $7,
    $8,  $9,  $10, $11, $12, $13, $14
  FROM @CITYRIDE_DB.RAW.STG_CITYRIDE
)
PATTERN  = '.*users_(master|inc)\.csv'
ON_ERROR = CONTINUE;
 
 
USE ROLE SYSADMIN;
 
GRANT OPERATE, MONITOR ON PIPE CITYRIDE_DB.RAW.PIPE_STATIONS TO ROLE CITYRIDE_PIPELINE;
GRANT OPERATE, MONITOR ON PIPE CITYRIDE_DB.RAW.PIPE_BIKES    TO ROLE CITYRIDE_PIPELINE;
GRANT OPERATE, MONITOR ON PIPE CITYRIDE_DB.RAW.PIPE_RENTALS  TO ROLE CITYRIDE_PIPELINE;
GRANT OPERATE, MONITOR ON PIPE CITYRIDE_DB.RAW.PIPE_USERS    TO ROLE CITYRIDE_PIPELINE;
 
-- Verify
SHOW GRANTS ON PIPE CITYRIDE_DB.RAW.PIPE_STATIONS;
SHOW GRANTS ON PIPE CITYRIDE_DB.RAW.PIPE_BIKES;
SHOW GRANTS ON PIPE CITYRIDE_DB.RAW.PIPE_RENTALS;
SHOW GRANTS ON PIPE CITYRIDE_DB.RAW.PIPE_USERS;

-- ── Get SQS ARNs for AWS event notification ───────────────────
SHOW PIPES IN SCHEMA CITYRIDE_DB.RAW;

SELECT SYSTEM$PIPE_STATUS('CITYRIDE_DB.RAW.PIPE_STATIONS');
SELECT SYSTEM$PIPE_STATUS('CITYRIDE_DB.RAW.PIPE_BIKES');
SELECT SYSTEM$PIPE_STATUS('CITYRIDE_DB.RAW.PIPE_RENTALS');
SELECT SYSTEM$PIPE_STATUS('CITYRIDE_DB.RAW.PIPE_USERS');

ALTER PIPE CITYRIDE_DB.RAW.PIPE_STATIONS REFRESH;
ALTER PIPE CITYRIDE_DB.RAW.PIPE_BIKES    REFRESH;
ALTER PIPE CITYRIDE_DB.RAW.PIPE_RENTALS  REFRESH;
ALTER PIPE CITYRIDE_DB.RAW.PIPE_USERS    REFRESH;

SELECT * FROM CITYRIDE_DB.RAW.STATIONS;
SELECT * FROM CITYRIDE_DB.RAW.BIKES;
SELECT * FROM CITYRIDE_DB.RAW.RENTALS;
SELECT * FROM CITYRIDE_DB.RAW.USERS;

--================================
-- RAW TO VALIDATION LAYER
--================================

-- ============================================================
-- CITYRIDE — 05_stored_procedures.sql (v10)
-- Fix: duration_sec = EXTRACT epoch diff, not DATEDIFF
-- Use: DATE_PART('epoch_second', CURRENT_TIMESTAMP()) - DATE_PART('epoch_second', v_start)
-- ============================================================

USE ROLE SYSADMIN;
USE DATABASE CITYRIDE_DB;

-- ============================================================
-- SP_TRUNCATE_RAW
-- ============================================================
USE SCHEMA CITYRIDE_DB.RAW;

CREATE OR REPLACE PROCEDURE SP_TRUNCATE_RAW(P_DOMAIN VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  v_table  VARCHAR;
  v_rows   INT       DEFAULT 0;
  v_start  TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  v_end    TIMESTAMP;
  v_dur    INT       DEFAULT 0;
BEGIN
  v_table := CASE P_DOMAIN
    WHEN 'stations' THEN 'CITYRIDE_DB.RAW.STATIONS'
    WHEN 'bikes'    THEN 'CITYRIDE_DB.RAW.BIKES'
    WHEN 'rentals'  THEN 'CITYRIDE_DB.RAW.RENTALS'
    WHEN 'users'    THEN 'CITYRIDE_DB.RAW.USERS'
    ELSE NULL
  END;

  IF (v_table IS NULL) THEN
    RETURN 'FAILED | unknown domain: ' || P_DOMAIN;
  END IF;

  SELECT COUNT(*) INTO v_rows FROM IDENTIFIER(:v_table);
  DELETE FROM IDENTIFIER(:v_table);

  v_end := CURRENT_TIMESTAMP();
  SELECT DATE_PART('epoch_second', :v_end) - DATE_PART('epoch_second', :v_start) INTO v_dur;

  INSERT INTO CITYRIDE_DB.ANALYTICS.AUDIT_LOG
    (batch_id, layer, domain, table_name, operation,
     rows_processed, status, started_at, finished_at, duration_sec)
  VALUES
    (TO_CHAR(:v_end, 'YYYYMMDDHH24MISS'), 'RAW', :P_DOMAIN,
     :v_table, 'TRUNCATE_RAW', :v_rows, 'SUCCESS',
     :v_start, :v_end, :v_dur);

  RETURN 'SUCCESS | deleted ' || :v_rows || ' rows | ' || :P_DOMAIN;
END;
$$;

GRANT USAGE ON PROCEDURE CITYRIDE_DB.RAW.SP_TRUNCATE_RAW(VARCHAR)
  TO ROLE CITYRIDE_PIPELINE;


-- ============================================================
-- SP_VALIDATE_STATIONS
-- ============================================================
USE SCHEMA CITYRIDE_DB.VALIDATED;

CREATE OR REPLACE PROCEDURE SP_VALIDATE_STATIONS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  v_rows_in INT       DEFAULT 0;
  v_rows_ok INT       DEFAULT 0;
  v_start   TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  v_end     TIMESTAMP;
  v_dur     INT       DEFAULT 0;
  v_status  VARCHAR   DEFAULT 'SUCCESS';
  v_err     VARCHAR   DEFAULT NULL;
BEGIN
  SELECT COUNT(*) INTO v_rows_in FROM CITYRIDE_DB.RAW.STATIONS;

  IF (v_rows_in = 0) THEN
    RETURN 'SKIPPED | RAW.STATIONS is empty';
  END IF;

  BEGIN
    INSERT INTO CITYRIDE_DB.VALIDATED.STATIONS
      (station_id, station_name, latitude, longitude, capacity,
       neighborhood, city_zone, install_date, status,
       _dq_passed, _dq_flags, _source_file)
    SELECT
      station_id, station_name,
      TRY_CAST(latitude  AS FLOAT),
      TRY_CAST(longitude AS FLOAT),
      TRY_CAST(capacity  AS INT),
      neighborhood, city_zone,
      TRY_TO_DATE(install_date),
      status,
      IFF(
        station_id IS NOT NULL AND station_id != ''
        AND TRY_CAST(latitude  AS FLOAT) IS NOT NULL
        AND TRY_CAST(longitude AS FLOAT) IS NOT NULL
        AND TRY_CAST(capacity  AS INT) > 0
        AND city_zone IS NOT NULL AND city_zone != '',
        TRUE, FALSE),
      TO_VARIANT(ARRAY_CONSTRUCT_COMPACT(
        IFF(station_id IS NULL OR station_id = '', 'MISSING_STATION_ID', NULL),
        IFF(TRY_CAST(latitude  AS FLOAT) IS NULL,  'INVALID_LATITUDE',   NULL),
        IFF(TRY_CAST(longitude AS FLOAT) IS NULL,  'INVALID_LONGITUDE',  NULL),
        IFF(TRY_CAST(capacity  AS INT) IS NULL
         OR TRY_CAST(capacity  AS INT) <= 0,       'INVALID_CAPACITY',   NULL),
        IFF(city_zone IS NULL OR city_zone = '',   'MISSING_CITY_ZONE',  NULL))),
      _source_file
    FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY _loaded_at DESC) AS rn
      FROM CITYRIDE_DB.RAW.STATIONS
    ) WHERE rn = 1;
  EXCEPTION WHEN OTHER THEN
    v_status := 'FAILED';
    v_err    := SQLERRM;
  END;

  SELECT COUNT(*) INTO v_rows_ok FROM CITYRIDE_DB.VALIDATED.STATIONS;
  v_end := CURRENT_TIMESTAMP();
  SELECT DATE_PART('epoch_second', :v_end) - DATE_PART('epoch_second', :v_start) INTO v_dur;

  INSERT INTO CITYRIDE_DB.ANALYTICS.AUDIT_LOG
    (batch_id, layer, domain, table_name, operation,
     rows_processed, rows_inserted, error_message, status,
     started_at, finished_at, duration_sec)
  VALUES
    (TO_CHAR(:v_end,'YYYYMMDDHH24MISS'), 'VALIDATED', 'stations',
     'CITYRIDE_DB.VALIDATED.STATIONS', 'VALIDATE',
     :v_rows_in, :v_rows_ok, :v_err, :v_status,
     :v_start, :v_end, :v_dur);

  IF (:v_status = 'SUCCESS') THEN
    CALL CITYRIDE_DB.RAW.SP_TRUNCATE_RAW('stations');
  END IF;

  RETURN :v_status || ' | in=' || :v_rows_in || ' ok=' || :v_rows_ok || ' | stations';
END;
$$;

GRANT USAGE ON PROCEDURE CITYRIDE_DB.VALIDATED.SP_VALIDATE_STATIONS()
  TO ROLE CITYRIDE_PIPELINE;


-- ============================================================
-- SP_VALIDATE_BIKES
-- ============================================================

CREATE OR REPLACE PROCEDURE SP_VALIDATE_BIKES()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  v_rows_in INT       DEFAULT 0;
  v_rows_ok INT       DEFAULT 0;
  v_start   TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  v_end     TIMESTAMP;
  v_dur     INT       DEFAULT 0;
  v_status  VARCHAR   DEFAULT 'SUCCESS';
  v_err     VARCHAR   DEFAULT NULL;
BEGIN
  SELECT COUNT(*) INTO v_rows_in FROM CITYRIDE_DB.RAW.BIKES;

  IF (v_rows_in = 0) THEN
    RETURN 'SKIPPED | RAW.BIKES is empty';
  END IF;

  BEGIN
    INSERT INTO CITYRIDE_DB.VALIDATED.BIKES
      (bike_id, bike_type, status, purchase_date, last_service_date,
       odometer_km, battery_level, firmware_version,
       _dq_passed, _dq_flags, _source_file)
    SELECT
      bike_id, bike_type, status,
      TRY_TO_DATE(purchase_date),
      TRY_TO_DATE(last_service_date),
      TRY_CAST(odometer_km   AS FLOAT),
      TRY_CAST(battery_level AS INT),
      firmware_version,
      IFF(
        bike_id IS NOT NULL AND bike_id != ''
        AND bike_type IN ('classic','ebike')
        AND TRY_CAST(battery_level AS INT) BETWEEN 0 AND 100
        AND TRY_CAST(odometer_km  AS FLOAT) >= 0,
        TRUE, FALSE),
      TO_VARIANT(ARRAY_CONSTRUCT_COMPACT(
        IFF(bike_id IS NULL OR bike_id = '',       'MISSING_BIKE_ID',      NULL),
        IFF(bike_type NOT IN ('classic','ebike'),  'INVALID_BIKE_TYPE',    NULL),
        IFF(TRY_CAST(battery_level AS INT) < 0
         OR TRY_CAST(battery_level AS INT) > 100,  'BATTERY_OUT_OF_RANGE', NULL),
        IFF(TRY_CAST(odometer_km  AS FLOAT) < 0,   'NEGATIVE_ODOMETER',    NULL))),
      _source_file
    FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY bike_id ORDER BY _loaded_at DESC) AS rn
      FROM CITYRIDE_DB.RAW.BIKES
    ) WHERE rn = 1;
  EXCEPTION WHEN OTHER THEN
    v_status := 'FAILED';
    v_err    := SQLERRM;
  END;

  SELECT COUNT(*) INTO v_rows_ok FROM CITYRIDE_DB.VALIDATED.BIKES;
  v_end := CURRENT_TIMESTAMP();
  SELECT DATE_PART('epoch_second', :v_end) - DATE_PART('epoch_second', :v_start) INTO v_dur;

  INSERT INTO CITYRIDE_DB.ANALYTICS.AUDIT_LOG
    (batch_id, layer, domain, table_name, operation,
     rows_processed, rows_inserted, error_message, status,
     started_at, finished_at, duration_sec)
  VALUES
    (TO_CHAR(:v_end,'YYYYMMDDHH24MISS'), 'VALIDATED', 'bikes',
     'CITYRIDE_DB.VALIDATED.BIKES', 'VALIDATE',
     :v_rows_in, :v_rows_ok, :v_err, :v_status,
     :v_start, :v_end, :v_dur);

  IF (:v_status = 'SUCCESS') THEN
    CALL CITYRIDE_DB.RAW.SP_TRUNCATE_RAW('bikes');
  END IF;

  RETURN :v_status || ' | in=' || :v_rows_in || ' ok=' || :v_rows_ok || ' | bikes';
END;
$$;

GRANT USAGE ON PROCEDURE CITYRIDE_DB.VALIDATED.SP_VALIDATE_BIKES()
  TO ROLE CITYRIDE_PIPELINE;


-- ============================================================
-- SP_VALIDATE_USERS
-- ============================================================

CREATE OR REPLACE PROCEDURE SP_VALIDATE_USERS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  v_rows_in INT       DEFAULT 0;
  v_rows_ok INT       DEFAULT 0;
  v_start   TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  v_end     TIMESTAMP;
  v_dur     INT       DEFAULT 0;
  v_status  VARCHAR   DEFAULT 'SUCCESS';
  v_err     VARCHAR   DEFAULT NULL;
BEGIN
  SELECT COUNT(*) INTO v_rows_in FROM CITYRIDE_DB.RAW.USERS;

  IF (v_rows_in = 0) THEN
    RETURN 'SKIPPED | RAW.USERS is empty';
  END IF;

  BEGIN
    INSERT INTO CITYRIDE_DB.VALIDATED.USERS
      (user_id, customer_name, dob, gender, email, phone,
       address, city, state, region, kyc_status,
       registration_date, is_student, corporate_id,
       _dq_passed, _dq_flags, _source_file)
    SELECT
      user_id, customer_name,
      TRY_TO_DATE(dob),
      gender, email, phone, address, city, state, region, kyc_status,
      TRY_TO_DATE(registration_date),
      TRY_CAST(is_student AS BOOLEAN),
      corporate_id,
      IFF(
        user_id IS NOT NULL AND user_id != ''
        AND region IS NOT NULL AND region != ''
        AND email  IS NOT NULL AND email  != '',
        TRUE, FALSE),
      TO_VARIANT(ARRAY_CONSTRUCT_COMPACT(
        IFF(user_id IS NULL OR user_id = '', 'MISSING_USER_ID', NULL),
        IFF(region  IS NULL OR region  = '', 'MISSING_REGION',  NULL),
        IFF(email   IS NULL OR email   = '', 'MISSING_EMAIL',   NULL))),
      _source_file
    FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY _loaded_at DESC) AS rn
      FROM CITYRIDE_DB.RAW.USERS
    ) WHERE rn = 1;
  EXCEPTION WHEN OTHER THEN
    v_status := 'FAILED';
    v_err    := SQLERRM;
  END;

  SELECT COUNT(*) INTO v_rows_ok FROM CITYRIDE_DB.VALIDATED.USERS;
  v_end := CURRENT_TIMESTAMP();
  SELECT DATE_PART('epoch_second', :v_end) - DATE_PART('epoch_second', :v_start) INTO v_dur;

  INSERT INTO CITYRIDE_DB.ANALYTICS.AUDIT_LOG
    (batch_id, layer, domain, table_name, operation,
     rows_processed, rows_inserted, error_message, status,
     started_at, finished_at, duration_sec)
  VALUES
    (TO_CHAR(:v_end,'YYYYMMDDHH24MISS'), 'VALIDATED', 'users',
     'CITYRIDE_DB.VALIDATED.USERS', 'VALIDATE',
     :v_rows_in, :v_rows_ok, :v_err, :v_status,
     :v_start, :v_end, :v_dur);

  IF (:v_status = 'SUCCESS') THEN
    CALL CITYRIDE_DB.RAW.SP_TRUNCATE_RAW('users');
  END IF;

  RETURN :v_status || ' | in=' || :v_rows_in || ' ok=' || :v_rows_ok || ' | users';
END;
$$;

GRANT USAGE ON PROCEDURE CITYRIDE_DB.VALIDATED.SP_VALIDATE_USERS()
  TO ROLE CITYRIDE_PIPELINE;


-- ============================================================
-- SP_VALIDATE_RENTALS  (run last)
-- ============================================================

CREATE OR REPLACE PROCEDURE SP_VALIDATE_RENTALS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  v_rows_in INT       DEFAULT 0;
  v_rows_ok INT       DEFAULT 0;
  v_start   TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  v_end     TIMESTAMP;
  v_dur     INT       DEFAULT 0;
  v_status  VARCHAR   DEFAULT 'SUCCESS';
  v_err     VARCHAR   DEFAULT NULL;
BEGIN
  SELECT COUNT(*) INTO v_rows_in FROM CITYRIDE_DB.RAW.RENTALS;

  IF (v_rows_in = 0) THEN
    RETURN 'SKIPPED | RAW.RENTALS is empty';
  END IF;

  BEGIN
    INSERT INTO CITYRIDE_DB.VALIDATED.RENTALS
      (rental_id, user_id, bike_id, start_station_id, end_station_id,
       start_time, end_time, duration_sec, distance_km, price,
       plan_type, channel, device_info,
       start_lat, start_lon, end_lat, end_lon,
       is_flagged, _dq_passed, _dq_flags, _ref_checks_passed, _source_file)
    SELECT
      r.rental_id, r.user_id, r.bike_id,
      r.start_station_id, r.end_station_id,
      TRY_TO_TIMESTAMP(r.start_time),
      TRY_TO_TIMESTAMP(r.end_time),
      TRY_CAST(r.duration_sec AS INT),
      TRY_CAST(r.distance_km  AS FLOAT),
      TRY_CAST(r.price        AS FLOAT),
      r.plan_type, r.channel, r.device_info,
      TRY_CAST(SPLIT_PART(r.start_gps,',',1) AS FLOAT),
      TRY_CAST(SPLIT_PART(r.start_gps,',',2) AS FLOAT),
      TRY_CAST(SPLIT_PART(r.end_gps,  ',',1) AS FLOAT),
      TRY_CAST(SPLIT_PART(r.end_gps,  ',',2) AS FLOAT),
      TRY_CAST(r.is_flagged AS BOOLEAN),
      IFF(
        r.rental_id IS NOT NULL
        AND r.user_id IS NOT NULL
        AND r.bike_id IS NOT NULL
        AND r.start_station_id IS NOT NULL
        AND TRY_TO_TIMESTAMP(r.start_time) IS NOT NULL
        AND TRY_CAST(r.duration_sec AS INT)   >= 0
        AND TRY_CAST(r.distance_km  AS FLOAT) >= 0,
        TRUE, FALSE),
      TO_VARIANT(ARRAY_CONSTRUCT_COMPACT(
        IFF(r.rental_id        IS NULL, 'MISSING_RENTAL_ID',  NULL),
        IFF(r.user_id          IS NULL, 'MISSING_USER_ID',    NULL),
        IFF(r.bike_id          IS NULL, 'MISSING_BIKE_ID',    NULL),
        IFF(r.start_station_id IS NULL, 'MISSING_START_STN',  NULL),
        IFF(TRY_TO_TIMESTAMP(r.start_time) IS NULL, 'INVALID_START_TIME', NULL),
        IFF(TRY_CAST(r.duration_sec AS INT)   < 0,  'NEGATIVE_DURATION',  NULL),
        IFF(TRY_CAST(r.distance_km  AS FLOAT) < 0,  'NEGATIVE_DISTANCE',  NULL))),
      IFF(
        EXISTS(SELECT 1 FROM CITYRIDE_DB.VALIDATED.BIKES     b WHERE b.bike_id     = r.bike_id)
        AND EXISTS(SELECT 1 FROM CITYRIDE_DB.VALIDATED.STATIONS s WHERE s.station_id = r.start_station_id)
        AND EXISTS(SELECT 1 FROM CITYRIDE_DB.VALIDATED.USERS    u WHERE u.user_id    = r.user_id),
        TRUE, FALSE),
      r._source_file
    FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY rental_id ORDER BY _loaded_at DESC) AS rn
      FROM CITYRIDE_DB.RAW.RENTALS r
    ) r WHERE rn = 1;
  EXCEPTION WHEN OTHER THEN
    v_status := 'FAILED';
    v_err    := SQLERRM;
  END;

  SELECT COUNT(*) INTO v_rows_ok FROM CITYRIDE_DB.VALIDATED.RENTALS;
  v_end := CURRENT_TIMESTAMP();
  SELECT DATE_PART('epoch_second', :v_end) - DATE_PART('epoch_second', :v_start) INTO v_dur;

  INSERT INTO CITYRIDE_DB.ANALYTICS.AUDIT_LOG
    (batch_id, layer, domain, table_name, operation,
     rows_processed, rows_inserted, error_message, status,
     started_at, finished_at, duration_sec)
  VALUES
    (TO_CHAR(:v_end,'YYYYMMDDHH24MISS'), 'VALIDATED', 'rentals',
     'CITYRIDE_DB.VALIDATED.RENTALS', 'VALIDATE',
     :v_rows_in, :v_rows_ok, :v_err, :v_status,
     :v_start, :v_end, :v_dur);

  IF (:v_status = 'SUCCESS') THEN
    CALL CITYRIDE_DB.RAW.SP_TRUNCATE_RAW('rentals');
  END IF;

  RETURN :v_status || ' | in=' || :v_rows_in || ' ok=' || :v_rows_ok || ' | rentals';
END;
$$;

GRANT USAGE ON PROCEDURE CITYRIDE_DB.VALIDATED.SP_VALIDATE_RENTALS()
  TO ROLE CITYRIDE_PIPELINE;
  
CALL CITYRIDE_DB.VALIDATED.SP_VALIDATE_STATIONS();
CALL CITYRIDE_DB.VALIDATED.SP_VALIDATE_BIKES();
CALL CITYRIDE_DB.VALIDATED.SP_VALIDATE_USERS();
CALL CITYRIDE_DB.VALIDATED.SP_VALIDATE_RENTALS();

-- ============================================================
-- SP_TRUNCATE_RAW now takes only P_DOMAIN (no timestamp)
-- Task calls no-arg SPs — clean and simple
-- ============================================================

USE ROLE SYSADMIN;
USE SCHEMA CITYRIDE_DB.RAW;

CREATE STREAM IF NOT EXISTS STREAM_STATIONS ON TABLE CITYRIDE_DB.RAW.STATIONS APPEND_ONLY = TRUE;
CREATE STREAM IF NOT EXISTS STREAM_BIKES    ON TABLE CITYRIDE_DB.RAW.BIKES    APPEND_ONLY = TRUE;
CREATE STREAM IF NOT EXISTS STREAM_RENTALS  ON TABLE CITYRIDE_DB.RAW.RENTALS  APPEND_ONLY = TRUE;
CREATE STREAM IF NOT EXISTS STREAM_USERS    ON TABLE CITYRIDE_DB.RAW.USERS    APPEND_ONLY = TRUE;

CREATE OR REPLACE TASK TASK_VALIDATE
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = '5 MINUTE'
  WHEN      SYSTEM$STREAM_HAS_DATA('CITYRIDE_DB.RAW.STREAM_STATIONS')
         OR SYSTEM$STREAM_HAS_DATA('CITYRIDE_DB.RAW.STREAM_BIKES')
         OR SYSTEM$STREAM_HAS_DATA('CITYRIDE_DB.RAW.STREAM_RENTALS')
         OR SYSTEM$STREAM_HAS_DATA('CITYRIDE_DB.RAW.STREAM_USERS')
AS
BEGIN
  CALL CITYRIDE_DB.VALIDATED.SP_VALIDATE_STATIONS();
  CALL CITYRIDE_DB.VALIDATED.SP_VALIDATE_BIKES();
  CALL CITYRIDE_DB.VALIDATED.SP_VALIDATE_USERS();
  CALL CITYRIDE_DB.VALIDATED.SP_VALIDATE_RENTALS();
END;

ALTER TASK TASK_VALIDATE RESUME;

-- ============================================================
-- SCD2 WITH CURATED LAYER
-- ============================================================

USE ROLE SYSADMIN;
USE DATABASE CITYRIDE_DB;


-- ============================================================
-- SP_CURATE_STATIONS  (SCD2 via MERGE)
-- ============================================================
USE SCHEMA CITYRIDE_DB.CURATED;

CREATE OR REPLACE PROCEDURE SP_CURATE_STATIONS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  v_rows_in  INT       DEFAULT 0;
  v_inserted INT       DEFAULT 0;
  v_updated  INT       DEFAULT 0;
  v_start    TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  v_end      TIMESTAMP;
  v_dur      INT       DEFAULT 0;
  v_status   VARCHAR   DEFAULT 'SUCCESS';
  v_err      VARCHAR   DEFAULT NULL;
BEGIN
  SELECT COUNT(*) INTO v_rows_in
  FROM CITYRIDE_DB.VALIDATED.STATIONS
  WHERE _dq_passed = TRUE;

  IF (v_rows_in = 0) THEN
    RETURN 'SKIPPED | no valid stations in VALIDATED';
  END IF;

  BEGIN
    -- ── Pass 1: close rows where data has changed ─────────────
    MERGE INTO CITYRIDE_DB.CURATED.DIM_STATION tgt
    USING (
      SELECT
        station_id,
        MD5(COALESCE(station_name,'') || '|' ||
            COALESCE(capacity::VARCHAR,'') || '|' ||
            COALESCE(city_zone,'') || '|' ||
            COALESCE(status,'')) AS new_hash
      FROM CITYRIDE_DB.VALIDATED.STATIONS
      WHERE _dq_passed = TRUE
    ) src
    ON tgt.station_id = src.station_id
    AND tgt.is_current = TRUE
    WHEN MATCHED AND tgt._hash_diff <> src.new_hash THEN
      UPDATE SET
        tgt.effective_to = CURRENT_TIMESTAMP(),
        tgt.is_current   = FALSE;

    v_updated := SQLROWCOUNT;

    -- ── Pass 2: insert new + changed (now no current row) ─────
    MERGE INTO CITYRIDE_DB.CURATED.DIM_STATION tgt
    USING (
      SELECT
        station_id, station_name, latitude, longitude, capacity,
        neighborhood, city_zone, install_date, status,
        MD5(COALESCE(station_name,'') || '|' ||
            COALESCE(capacity::VARCHAR,'') || '|' ||
            COALESCE(city_zone,'') || '|' ||
            COALESCE(status,'')) AS new_hash
      FROM CITYRIDE_DB.VALIDATED.STATIONS
      WHERE _dq_passed = TRUE
    ) src
    ON tgt.station_id = src.station_id
    AND tgt.is_current = TRUE
    WHEN NOT MATCHED THEN
      INSERT (station_id, station_name, latitude, longitude, capacity,
              neighborhood, city_zone, install_date, status,
              effective_from, effective_to, is_current, _hash_diff)
      VALUES (src.station_id, src.station_name, src.latitude, src.longitude, src.capacity,
              src.neighborhood, src.city_zone, src.install_date, src.status,
              CURRENT_TIMESTAMP(), NULL, TRUE, src.new_hash);

    v_inserted := SQLROWCOUNT;
  EXCEPTION WHEN OTHER THEN
    v_status := 'FAILED';
    v_err    := SQLERRM;
  END;

  v_end := CURRENT_TIMESTAMP();
  SELECT DATE_PART('epoch_second',:v_end) - DATE_PART('epoch_second',:v_start) INTO v_dur;

  INSERT INTO CITYRIDE_DB.ANALYTICS.AUDIT_LOG
    (batch_id, layer, domain, table_name, operation,
     rows_processed, rows_inserted, rows_updated, error_message, status,
     started_at, finished_at, duration_sec)
  VALUES
    (TO_CHAR(:v_end,'YYYYMMDDHH24MISS'), 'CURATED', 'stations',
     'CITYRIDE_DB.CURATED.DIM_STATION', 'SCD2_MERGE',
     :v_rows_in, :v_inserted, :v_updated, :v_err, :v_status,
     :v_start, :v_end, :v_dur);

  RETURN :v_status || ' | in=' || :v_rows_in || ' inserted=' || :v_inserted || ' closed=' || :v_updated || ' | dim_station';
END;
$$;

GRANT USAGE ON PROCEDURE CITYRIDE_DB.CURATED.SP_CURATE_STATIONS() TO ROLE CITYRIDE_PIPELINE;


-- ============================================================
-- SP_CURATE_BIKES  (SCD2 via MERGE)
-- ============================================================

CREATE OR REPLACE PROCEDURE SP_CURATE_BIKES()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  v_rows_in  INT       DEFAULT 0;
  v_inserted INT       DEFAULT 0;
  v_updated  INT       DEFAULT 0;
  v_start    TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  v_end      TIMESTAMP;
  v_dur      INT       DEFAULT 0;
  v_status   VARCHAR   DEFAULT 'SUCCESS';
  v_err      VARCHAR   DEFAULT NULL;
BEGIN
  SELECT COUNT(*) INTO v_rows_in
  FROM CITYRIDE_DB.VALIDATED.BIKES
  WHERE _dq_passed = TRUE;

  IF (v_rows_in = 0) THEN
    RETURN 'SKIPPED | no valid bikes in VALIDATED';
  END IF;

  BEGIN
    -- Pass 1: close changed rows
    MERGE INTO CITYRIDE_DB.CURATED.DIM_BIKE tgt
    USING (
      SELECT
        bike_id,
        MD5(COALESCE(status,'') || '|' ||
            COALESCE(battery_level::VARCHAR,'') || '|' ||
            COALESCE(odometer_km::VARCHAR,'') || '|' ||
            COALESCE(firmware_version,'')) AS new_hash
      FROM CITYRIDE_DB.VALIDATED.BIKES
      WHERE _dq_passed = TRUE
    ) src
    ON tgt.bike_id = src.bike_id
    AND tgt.is_current = TRUE
    WHEN MATCHED AND tgt._hash_diff <> src.new_hash THEN
      UPDATE SET
        tgt.effective_to = CURRENT_TIMESTAMP(),
        tgt.is_current   = FALSE;

    v_updated := SQLROWCOUNT;

    -- Pass 2: insert new + changed
    MERGE INTO CITYRIDE_DB.CURATED.DIM_BIKE tgt
    USING (
      SELECT
        bike_id, bike_type, status, purchase_date, last_service_date,
        odometer_km, battery_level, firmware_version,
        MD5(COALESCE(status,'') || '|' ||
            COALESCE(battery_level::VARCHAR,'') || '|' ||
            COALESCE(odometer_km::VARCHAR,'') || '|' ||
            COALESCE(firmware_version,'')) AS new_hash
      FROM CITYRIDE_DB.VALIDATED.BIKES
      WHERE _dq_passed = TRUE
    ) src
    ON tgt.bike_id = src.bike_id
    AND tgt.is_current = TRUE
    WHEN NOT MATCHED THEN
      INSERT (bike_id, bike_type, status, purchase_date, last_service_date,
              odometer_km, battery_level, firmware_version,
              effective_from, effective_to, is_current, _hash_diff)
      VALUES (src.bike_id, src.bike_type, src.status, src.purchase_date, src.last_service_date,
              src.odometer_km, src.battery_level, src.firmware_version,
              CURRENT_TIMESTAMP(), NULL, TRUE, src.new_hash);

    v_inserted := SQLROWCOUNT;
  EXCEPTION WHEN OTHER THEN
    v_status := 'FAILED';
    v_err    := SQLERRM;
  END;

  v_end := CURRENT_TIMESTAMP();
  SELECT DATE_PART('epoch_second',:v_end) - DATE_PART('epoch_second',:v_start) INTO v_dur;

  INSERT INTO CITYRIDE_DB.ANALYTICS.AUDIT_LOG
    (batch_id, layer, domain, table_name, operation,
     rows_processed, rows_inserted, rows_updated, error_message, status,
     started_at, finished_at, duration_sec)
  VALUES
    (TO_CHAR(:v_end,'YYYYMMDDHH24MISS'), 'CURATED', 'bikes',
     'CITYRIDE_DB.CURATED.DIM_BIKE', 'SCD2_MERGE',
     :v_rows_in, :v_inserted, :v_updated, :v_err, :v_status,
     :v_start, :v_end, :v_dur);

  RETURN :v_status || ' | in=' || :v_rows_in || ' inserted=' || :v_inserted || ' closed=' || :v_updated || ' | dim_bike';
END;
$$;

GRANT USAGE ON PROCEDURE CITYRIDE_DB.CURATED.SP_CURATE_BIKES() TO ROLE CITYRIDE_PIPELINE;


-- ============================================================
-- SP_CURATE_USERS  (SCD2 via MERGE)
-- ============================================================

CREATE OR REPLACE PROCEDURE SP_CURATE_USERS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  v_rows_in  INT       DEFAULT 0;
  v_inserted INT       DEFAULT 0;
  v_updated  INT       DEFAULT 0;
  v_start    TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  v_end      TIMESTAMP;
  v_dur      INT       DEFAULT 0;
  v_status   VARCHAR   DEFAULT 'SUCCESS';
  v_err      VARCHAR   DEFAULT NULL;
BEGIN
  SELECT COUNT(*) INTO v_rows_in
  FROM CITYRIDE_DB.VALIDATED.USERS
  WHERE _dq_passed = TRUE;

  IF (v_rows_in = 0) THEN
    RETURN 'SKIPPED | no valid users in VALIDATED';
  END IF;

  BEGIN
    -- Pass 1: close changed rows
    MERGE INTO CITYRIDE_DB.CURATED.DIM_USER tgt
    USING (
      SELECT
        user_id,
        MD5(COALESCE(customer_name,'') || '|' ||
            COALESCE(region,'') || '|' ||
            COALESCE(kyc_status,'') || '|' ||
            COALESCE(city,'')) AS new_hash
      FROM CITYRIDE_DB.VALIDATED.USERS
      WHERE _dq_passed = TRUE
    ) src
    ON tgt.user_id = src.user_id
    AND tgt.is_current = TRUE
    WHEN MATCHED AND tgt._hash_diff <> src.new_hash THEN
      UPDATE SET
        tgt.effective_to = CURRENT_TIMESTAMP(),
        tgt.is_current   = FALSE;

    v_updated := SQLROWCOUNT;

    -- Pass 2: insert new + changed
    MERGE INTO CITYRIDE_DB.CURATED.DIM_USER tgt
    USING (
      SELECT
        user_id, customer_name, dob, gender, email, phone,
        user_address, city, state, region, kyc_status,
        registration_date, is_student, corporate_id,
        MD5(COALESCE(customer_name,'') || '|' ||
            COALESCE(region,'') || '|' ||
            COALESCE(kyc_status,'') || '|' ||
            COALESCE(city,'')) AS new_hash
      FROM CITYRIDE_DB.VALIDATED.USERS
      WHERE _dq_passed = TRUE
    ) src
    ON tgt.user_id = src.user_id
    AND tgt.is_current = TRUE
    WHEN NOT MATCHED THEN
      INSERT (user_id, customer_name, dob, gender, email, phone,
              user_address, city, state, region, kyc_status,
              registration_date, is_student, corporate_id,
              effective_from, effective_to, is_current, _hash_diff)
      VALUES (src.user_id, src.customer_name, src.dob, src.gender, src.email, src.phone,
              src.user_address, src.city, src.state, src.region, src.kyc_status,
              src.registration_date, src.is_student, src.corporate_id,
              CURRENT_TIMESTAMP(), NULL, TRUE, src.new_hash);

    v_inserted := SQLROWCOUNT;
  EXCEPTION WHEN OTHER THEN
    v_status := 'FAILED';
    v_err    := SQLERRM;
  END;

  v_end := CURRENT_TIMESTAMP();
  SELECT DATE_PART('epoch_second',:v_end) - DATE_PART('epoch_second',:v_start) INTO v_dur;

  INSERT INTO CITYRIDE_DB.ANALYTICS.AUDIT_LOG
    (batch_id, layer, domain, table_name, operation,
     rows_processed, rows_inserted, rows_updated, error_message, status,
     started_at, finished_at, duration_sec)
  VALUES
    (TO_CHAR(:v_end,'YYYYMMDDHH24MISS'), 'CURATED', 'users',
     'CITYRIDE_DB.CURATED.DIM_USER', 'SCD2_MERGE',
     :v_rows_in, :v_inserted, :v_updated, :v_err, :v_status,
     :v_start, :v_end, :v_dur);

  RETURN :v_status || ' | in=' || :v_rows_in || ' inserted=' || :v_inserted || ' closed=' || :v_updated || ' | dim_user';
END;
$$;

GRANT USAGE ON PROCEDURE CITYRIDE_DB.CURATED.SP_CURATE_USERS() TO ROLE CITYRIDE_PIPELINE;


-- ============================================================
-- SP_CURATE_RENTALS  (MERGE into fact_rental)
-- Resolves surrogate keys from current dim rows
-- ============================================================

CREATE OR REPLACE PROCEDURE SP_CURATE_RENTALS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  v_rows_in  INT       DEFAULT 0;
  v_inserted INT       DEFAULT 0;
  v_start    TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  v_end      TIMESTAMP;
  v_dur      INT       DEFAULT 0;
  v_status   VARCHAR   DEFAULT 'SUCCESS';
  v_err      VARCHAR   DEFAULT NULL;
BEGIN
  SELECT COUNT(*) INTO v_rows_in
  FROM CITYRIDE_DB.VALIDATED.RENTALS
  WHERE _dq_passed = TRUE;

  IF (v_rows_in = 0) THEN
    RETURN 'SKIPPED | no valid rentals in VALIDATED';
  END IF;

  BEGIN
    MERGE INTO CITYRIDE_DB.CURATED.FACT_RENTAL tgt
    USING (
      SELECT
        v.rental_id,
        du.user_sk,
        db.bike_sk,
        ds.station_sk   AS start_station_sk,
        de.station_sk   AS end_station_sk,
        CAST(TO_CHAR(v.start_time::DATE,'YYYYMMDD') AS INT) AS date_sk,
        v.plan_type, v.channel, v.device_info,
        v.start_time, v.end_time, v.duration_sec,
        v.distance_km, v.price,
        v.start_lat, v.start_lon, v.end_lat, v.end_lon,
        v.is_flagged
      FROM CITYRIDE_DB.VALIDATED.RENTALS v
      LEFT JOIN CITYRIDE_DB.CURATED.DIM_USER    du ON du.user_id    = v.user_id    AND du.is_current = TRUE
      LEFT JOIN CITYRIDE_DB.CURATED.DIM_BIKE    db ON db.bike_id    = v.bike_id    AND db.is_current = TRUE
      LEFT JOIN CITYRIDE_DB.CURATED.DIM_STATION ds ON ds.station_id = v.start_station_id AND ds.is_current = TRUE
      LEFT JOIN CITYRIDE_DB.CURATED.DIM_STATION de ON de.station_id = v.end_station_id   AND de.is_current = TRUE
      WHERE v._dq_passed = TRUE
    ) src
    ON tgt.rental_id = src.rental_id
    WHEN NOT MATCHED THEN
      INSERT (rental_id, user_sk, bike_sk, start_station_sk, end_station_sk, date_sk,
              plan_type, channel, device_info,
              start_time, end_time, duration_sec, distance_km, price,
              start_lat, start_lon, end_lat, end_lon,
              is_flagged, anomaly_score, anomaly_rules_hit)
      VALUES (src.rental_id, src.user_sk, src.bike_sk, src.start_station_sk, src.end_station_sk, src.date_sk,
              src.plan_type, src.channel, src.device_info,
              src.start_time, src.end_time, src.duration_sec, src.distance_km, src.price,
              src.start_lat, src.start_lon, src.end_lat, src.end_lon,
              src.is_flagged, 0, NULL);

    v_inserted := SQLROWCOUNT;
  EXCEPTION WHEN OTHER THEN
    v_status := 'FAILED';
    v_err    := SQLERRM;
  END;

  v_end := CURRENT_TIMESTAMP();
  SELECT DATE_PART('epoch_second',:v_end) - DATE_PART('epoch_second',:v_start) INTO v_dur;

  INSERT INTO CITYRIDE_DB.ANALYTICS.AUDIT_LOG
    (batch_id, layer, domain, table_name, operation,
     rows_processed, rows_inserted, error_message, status,
     started_at, finished_at, duration_sec)
  VALUES
    (TO_CHAR(:v_end,'YYYYMMDDHH24MISS'), 'CURATED', 'rentals',
     'CITYRIDE_DB.CURATED.FACT_RENTAL', 'MERGE',
     :v_rows_in, :v_inserted, :v_err, :v_status,
     :v_start, :v_end, :v_dur);

  RETURN :v_status || ' | in=' || :v_rows_in || ' inserted=' || :v_inserted || ' | fact_rental';
END;
$$;

GRANT USAGE ON PROCEDURE CITYRIDE_DB.CURATED.SP_CURATE_RENTALS() TO ROLE CITYRIDE_PIPELINE;

CALL CITYRIDE_DB.CURATED.SP_CURATE_STATIONS();
SELECT * FROM CITYRIDE_DB.CURATED.DIM_STATION;

CALL CITYRIDE_DB.CURATED.SP_CURATE_BIKES();
SELECT * FROM CITYRIDE_DB.CURATED.DIM_BIKE;

CALL CITYRIDE_DB.CURATED.SP_CURATE_USERS();
SELECT * FROM CITYRIDE_DB.CURATED.DIM_USER;

CALL CITYRIDE_DB.CURATED.SP_CURATE_RENTALS();
SELECT * FROM CITYRIDE_DB.CURATED.FACT_RENTAL;

USE ROLE SYSADMIN;
USE SCHEMA CITYRIDE_DB.RAW;

CREATE OR REPLACE TASK CITYRIDE_DB.RAW.TASK_CURATE
  WAREHOUSE = COMPUTE_WH
  AFTER     CITYRIDE_DB.RAW.TASK_VALIDATE
AS
BEGIN
  CALL CITYRIDE_DB.CURATED.SP_CURATE_STATIONS();
  CALL CITYRIDE_DB.CURATED.SP_CURATE_BIKES();
  CALL CITYRIDE_DB.CURATED.SP_CURATE_USERS();
  CALL CITYRIDE_DB.CURATED.SP_CURATE_RENTALS();
END;

ALTER TASK CITYRIDE_DB.RAW.TASK_CURATE RESUME;



--======================================================
-- CURATED TO KPI'S
--======================================================


USE ROLE SYSADMIN;
USE DATABASE CITYRIDE_DB;
USE SCHEMA CITYRIDE_DB.ANALYTICS;

-- ============================================================
-- KPI 1 — Anomalous Rental Probability Score
-- (flagged rentals / total rentals) * 100
-- ============================================================

CREATE OR REPLACE PROCEDURE SP_KPI_ANOMALOUS_RENTAL()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  v_start   TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  v_end     TIMESTAMP;
  v_dur     INT       DEFAULT 0;
  v_status  VARCHAR   DEFAULT 'SUCCESS';
  v_err     VARCHAR   DEFAULT NULL;
BEGIN
  BEGIN
    INSERT INTO CITYRIDE_DB.ANALYTICS.KPI_ANOMALOUS_RENTAL_SCORE
      (snapshot_date, total_rentals, flagged_rentals, anomaly_probability, top_rules_hit)
    SELECT
      CURRENT_DATE(),
      COUNT(*)                                                          AS total_rentals,
      SUM(IFF(is_flagged = TRUE, 1, 0))                                AS flagged_rentals,
      ROUND(SUM(IFF(is_flagged = TRUE, 1, 0)) / NULLIF(COUNT(*),0) * 100, 2) AS anomaly_probability,
      NULL                                                              AS top_rules_hit
    FROM CITYRIDE_DB.CURATED.FACT_RENTAL;
  EXCEPTION WHEN OTHER THEN
    v_status := 'FAILED';
    v_err    := SQLERRM;
  END;

  v_end := CURRENT_TIMESTAMP();
  SELECT DATE_PART('epoch_second',:v_end) - DATE_PART('epoch_second',:v_start) INTO v_dur;

  INSERT INTO CITYRIDE_DB.ANALYTICS.AUDIT_LOG
    (batch_id, layer, domain, table_name, operation, error_message, status, started_at, finished_at, duration_sec)
  VALUES
    (TO_CHAR(:v_end,'YYYYMMDDHH24MISS'), 'ANALYTICS', 'rentals',
     'KPI_ANOMALOUS_RENTAL_SCORE', 'KPI', :v_err, :v_status, :v_start, :v_end, :v_dur);

  RETURN :v_status || ' | KPI_ANOMALOUS_RENTAL_SCORE';
END;
$$;

GRANT USAGE ON PROCEDURE CITYRIDE_DB.ANALYTICS.SP_KPI_ANOMALOUS_RENTAL() TO ROLE CITYRIDE_PIPELINE;


-- ============================================================
-- KPI 2 — Station Availability Score
-- % of stations that have at least 1 bike available
-- (approximated from current dim_station capacity vs fact rentals)
-- ============================================================

CREATE OR REPLACE PROCEDURE SP_KPI_STATION_AVAILABILITY()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  v_start   TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  v_end     TIMESTAMP;
  v_dur     INT       DEFAULT 0;
  v_status  VARCHAR   DEFAULT 'SUCCESS';
  v_err     VARCHAR   DEFAULT NULL;
BEGIN
  BEGIN
    INSERT INTO CITYRIDE_DB.ANALYTICS.KPI_STATION_AVAILABILITY
      (snapshot_date, station_sk, station_id, city_zone,
       pct_time_available, avg_bikes_available, avg_docks_free)
    SELECT
      CURRENT_DATE(),
      s.station_sk,
      s.station_id,
      s.city_zone,
      -- availability: stations with rentals / total capacity as proxy
      ROUND(
        (s.capacity - COUNT(f.rental_sk)) / NULLIF(s.capacity, 0) * 100
      , 2)                                       AS pct_time_available,
      s.capacity - COUNT(f.rental_sk)            AS avg_bikes_available,
      COUNT(f.rental_sk)                         AS avg_docks_free
    FROM CITYRIDE_DB.CURATED.DIM_STATION s
    LEFT JOIN CITYRIDE_DB.CURATED.FACT_RENTAL f
           ON f.start_station_sk = s.station_sk
          AND f.start_time::DATE = CURRENT_DATE()
    WHERE s.is_current = TRUE
    GROUP BY s.station_sk, s.station_id, s.city_zone, s.capacity;
  EXCEPTION WHEN OTHER THEN
    v_status := 'FAILED';
    v_err    := SQLERRM;
  END;

  v_end := CURRENT_TIMESTAMP();
  SELECT DATE_PART('epoch_second',:v_end) - DATE_PART('epoch_second',:v_start) INTO v_dur;

  INSERT INTO CITYRIDE_DB.ANALYTICS.AUDIT_LOG
    (batch_id, layer, domain, table_name, operation, error_message, status, started_at, finished_at, duration_sec)
  VALUES
    (TO_CHAR(:v_end,'YYYYMMDDHH24MISS'), 'ANALYTICS', 'stations',
     'KPI_STATION_AVAILABILITY', 'KPI', :v_err, :v_status, :v_start, :v_end, :v_dur);

  RETURN :v_status || ' | KPI_STATION_AVAILABILITY';
END;
$$;

GRANT USAGE ON PROCEDURE CITYRIDE_DB.ANALYTICS.SP_KPI_STATION_AVAILABILITY() TO ROLE CITYRIDE_PIPELINE;


-- ============================================================
-- KPI 3 — Active Rider Engagement Ratio
-- % of registered users with >= 1 rental in last 30 days
-- ============================================================

CREATE OR REPLACE PROCEDURE SP_KPI_RIDER_ENGAGEMENT()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  v_start   TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  v_end     TIMESTAMP;
  v_dur     INT       DEFAULT 0;
  v_status  VARCHAR   DEFAULT 'SUCCESS';
  v_err     VARCHAR   DEFAULT NULL;
BEGIN
  BEGIN
    INSERT INTO CITYRIDE_DB.ANALYTICS.KPI_RIDER_ENGAGEMENT
      (snapshot_date, total_registered, active_last_30d, engagement_ratio)
    SELECT
      CURRENT_DATE(),
      COUNT(DISTINCT u.user_sk)                                              AS total_registered,
      COUNT(DISTINCT CASE
        WHEN f.start_time >= DATEADD('day', -30, CURRENT_DATE()) THEN f.user_sk
      END)                                                                   AS active_last_30d,
      ROUND(
        COUNT(DISTINCT CASE
          WHEN f.start_time >= DATEADD('day', -30, CURRENT_DATE()) THEN f.user_sk
        END) / NULLIF(COUNT(DISTINCT u.user_sk), 0) * 100
      , 2)                                                                   AS engagement_ratio
    FROM CITYRIDE_DB.CURATED.DIM_USER u
    LEFT JOIN CITYRIDE_DB.CURATED.FACT_RENTAL f ON f.user_sk = u.user_sk
    WHERE u.is_current = TRUE;
  EXCEPTION WHEN OTHER THEN
    v_status := 'FAILED';
    v_err    := SQLERRM;
  END;

  v_end := CURRENT_TIMESTAMP();
  SELECT DATE_PART('epoch_second',:v_end) - DATE_PART('epoch_second',:v_start) INTO v_dur;

  INSERT INTO CITYRIDE_DB.ANALYTICS.AUDIT_LOG
    (batch_id, layer, domain, table_name, operation, error_message, status, started_at, finished_at, duration_sec)
  VALUES
    (TO_CHAR(:v_end,'YYYYMMDDHH24MISS'), 'ANALYTICS', 'users',
     'KPI_RIDER_ENGAGEMENT', 'KPI', :v_err, :v_status, :v_start, :v_end, :v_dur);

  RETURN :v_status || ' | KPI_RIDER_ENGAGEMENT';
END;
$$;

GRANT USAGE ON PROCEDURE CITYRIDE_DB.ANALYTICS.SP_KPI_RIDER_ENGAGEMENT() TO ROLE CITYRIDE_PIPELINE;


-- ============================================================
-- KPI 4 — Fleet Maintenance Health Index
-- % of bikes within health thresholds:
--   no error (status != 'maintenance')
--   ebike battery >= 25%
--   odometer within limits (< 50000 km)
-- ============================================================

CREATE OR REPLACE PROCEDURE SP_KPI_FLEET_HEALTH()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  v_start   TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  v_end     TIMESTAMP;
  v_dur     INT       DEFAULT 0;
  v_status  VARCHAR   DEFAULT 'SUCCESS';
  v_err     VARCHAR   DEFAULT NULL;
BEGIN
  BEGIN
    INSERT INTO CITYRIDE_DB.ANALYTICS.KPI_FLEET_HEALTH
      (snapshot_date, total_bikes, healthy_bikes, health_index,
       ebikes_low_battery, bikes_overdue_service)
    SELECT
      CURRENT_DATE(),
      COUNT(*)                                                           AS total_bikes,
      SUM(IFF(
        status != 'maintenance'
        AND (bike_type = 'classic' OR battery_level >= 25)
        AND odometer_km < 50000,
        1, 0))                                                           AS healthy_bikes,
      ROUND(SUM(IFF(
        status != 'maintenance'
        AND (bike_type = 'classic' OR battery_level >= 25)
        AND odometer_km < 50000,
        1, 0)) / NULLIF(COUNT(*), 0) * 100, 2)                          AS health_index,
      SUM(IFF(bike_type = 'ebike' AND battery_level < 25, 1, 0))        AS ebikes_low_battery,
      SUM(IFF(odometer_km >= 50000, 1, 0))                              AS bikes_overdue_service
    FROM CITYRIDE_DB.CURATED.DIM_BIKE
    WHERE is_current = TRUE;
  EXCEPTION WHEN OTHER THEN
    v_status := 'FAILED';
    v_err    := SQLERRM;
  END;

  v_end := CURRENT_TIMESTAMP();
  SELECT DATE_PART('epoch_second',:v_end) - DATE_PART('epoch_second',:v_start) INTO v_dur;

  INSERT INTO CITYRIDE_DB.ANALYTICS.AUDIT_LOG
    (batch_id, layer, domain, table_name, operation, error_message, status, started_at, finished_at, duration_sec)
  VALUES
    (TO_CHAR(:v_end,'YYYYMMDDHH24MISS'), 'ANALYTICS', 'bikes',
     'KPI_FLEET_HEALTH', 'KPI', :v_err, :v_status, :v_start, :v_end, :v_dur);

  RETURN :v_status || ' | KPI_FLEET_HEALTH';
END;
$$;

GRANT USAGE ON PROCEDURE CITYRIDE_DB.ANALYTICS.SP_KPI_FLEET_HEALTH() TO ROLE CITYRIDE_PIPELINE;


-- ============================================================
-- KPI 5 — Average Rental Revenue (ARR) by Channel
-- Total revenue / total rentals grouped by channel + plan_type
-- ============================================================

CREATE OR REPLACE PROCEDURE SP_KPI_ARR_BY_CHANNEL()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  v_start   TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  v_end     TIMESTAMP;
  v_dur     INT       DEFAULT 0;
  v_status  VARCHAR   DEFAULT 'SUCCESS';
  v_err     VARCHAR   DEFAULT NULL;
BEGIN
  BEGIN
    INSERT INTO CITYRIDE_DB.ANALYTICS.KPI_ARR_BY_CHANNEL
      (snapshot_date, channel, plan_type, total_rentals, total_revenue, avg_rental_rev)
    SELECT
      CURRENT_DATE(),
      COALESCE(channel,   'unknown')   AS channel,
      COALESCE(plan_type, 'unknown')   AS plan_type,
      COUNT(*)                         AS total_rentals,
      ROUND(SUM(price), 2)             AS total_revenue,
      ROUND(SUM(price) / NULLIF(COUNT(*), 0), 2) AS avg_rental_rev
    FROM CITYRIDE_DB.CURATED.FACT_RENTAL
    GROUP BY channel, plan_type;
  EXCEPTION WHEN OTHER THEN
    v_status := 'FAILED';
    v_err    := SQLERRM;
  END;

  v_end := CURRENT_TIMESTAMP();
  SELECT DATE_PART('epoch_second',:v_end) - DATE_PART('epoch_second',:v_start) INTO v_dur;

  INSERT INTO CITYRIDE_DB.ANALYTICS.AUDIT_LOG
    (batch_id, layer, domain, table_name, operation, error_message, status, started_at, finished_at, duration_sec)
  VALUES
    (TO_CHAR(:v_end,'YYYYMMDDHH24MISS'), 'ANALYTICS', 'rentals',
     'KPI_ARR_BY_CHANNEL', 'KPI', :v_err, :v_status, :v_start, :v_end, :v_dur);

  RETURN :v_status || ' | KPI_ARR_BY_CHANNEL';
END;
$$;

GRANT USAGE ON PROCEDURE CITYRIDE_DB.ANALYTICS.SP_KPI_ARR_BY_CHANNEL() TO ROLE CITYRIDE_PIPELINE;

CALL CITYRIDE_DB.ANALYTICS.SP_KPI_ANOMALOUS_RENTAL();
SELECT * FROM CITYRIDE_DB.ANALYTICS.KPI_ANOMALOUS_RENTAL_SCORE;

CALL CITYRIDE_DB.ANALYTICS.SP_KPI_STATION_AVAILABILITY();
SELECT * FROM CITYRIDE_DB.ANALYTICS.KPI_STATION_AVAILABILITY;

CALL CITYRIDE_DB.ANALYTICS.SP_KPI_RIDER_ENGAGEMENT();
SELECT * FROM CITYRIDE_DB.ANALYTICS.KPI_RIDER_ENGAGEMENT;

CALL CITYRIDE_DB.ANALYTICS.SP_KPI_FLEET_HEALTH();
SELECT * FROM CITYRIDE_DB.ANALYTICS.KPI_FLEET_HEALTH;

CALL SP_KPI_ARR_BY_CHANNEL();
SELECT * FROM CITYRIDE_DB.ANALYTICS.KPI_ARR_BY_CHANNEL;

USE ROLE SYSADMIN;
USE SCHEMA CITYRIDE_DB.RAW;



CREATE OR REPLACE TASK CITYRIDE_DB.RAW.TASK_KPIS
  WAREHOUSE = COMPUTE_WH
  AFTER     CITYRIDE_DB.RAW.TASK_CURATE
AS
BEGIN
  CALL CITYRIDE_DB.ANALYTICS.SP_KPI_ANOMALOUS_RENTAL();
  CALL CITYRIDE_DB.ANALYTICS.SP_KPI_STATION_AVAILABILITY();
  CALL CITYRIDE_DB.ANALYTICS.SP_KPI_RIDER_ENGAGEMENT();
  CALL CITYRIDE_DB.ANALYTICS.SP_KPI_FLEET_HEALTH();
  CALL CITYRIDE_DB.ANALYTICS.SP_KPI_ARR_BY_CHANNEL();
END;
 
-- Activate (children first, then root)
ALTER TASK CITYRIDE_DB.RAW.TASK_KPIS   RESUME;
ALTER TASK CITYRIDE_DB.RAW.TASK_CURATE    RESUME;
-- TASK_VALIDATE is already running — root task stays active
 
SHOW TASKS IN SCHEMA CITYRIDE_DB.RAW;
