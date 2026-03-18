# CityRide Data Platform

End-to-end Snowflake data platform for a bike rental analytics system. Built for a hackathon covering data ingestion, transformation, dimensional modelling, KPI computation, data governance, and a Streamlit BI dashboard.

---

## Architecture Overview

```
S3 (CSV files)
    └── Snowpipe (AUTO_INGEST + SQS)
        └── RAW layer
            └── TASK_VALIDATE (every 5 min)
                └── VALIDATED layer (DQ checks)
                    └── TASK_CURATE (after TASK_VALIDATE)
                        └── CURATED layer (SCD2 dims + fact)
                            └── TASK_KPIS (after TASK_CURATE)
                                └── ANALYTICS layer (5 KPIs)
                                    └── Streamlit Dashboard
```

### Schema Design — Star Schema

| Schema | Purpose |
|---|---|
| `RAW` | Exact source copy from S3, all VARCHAR, no transformations |
| `VALIDATED` | Typed, DQ-checked, one row per entity (MERGE UPDATE on change) |
| `CURATED` | Dimensional model — SCD2 dims + fact table |
| `ANALYTICS` | KPI tables, audit log, masking policies |

---

## File Structure

```
01_database.sql              — Database + schema creation
02_roles.sql                 — RBAC roles, hierarchy, warehouse grants
03_storage_integration.sql   — AWS S3 integration, stage, Snowpipes
04_tables.sql                — All tables across 4 schemas
05_stored_procedures.sql     — RAW → VALIDATED (4 domain SPs + truncate)
06_tasks.sql                 — TASK_VALIDATE + append-only streams
07_sp_curate.sql             — VALIDATED → CURATED (SCD2 + fact_rental)
08_sp_kpis.sql               — 5 KPI stored procedures
09_tasks_curate_kpi.sql      — TASK_CURATE + TASK_KPIS (task chain)
10_data_masking.sql          — Dynamic masking on PII + GPS columns
11_streamlit_dashboard.py    — 7-tab Streamlit BI dashboard
12_dim_date.sql              — DIM_DATE calendar population (2020–2030)
```

---

## Data Model

### Domains
- **Stations** — bike docking stations with capacity and zone
- **Bikes** — fleet with type (classic/ebike), battery, odometer
- **Users** — registered customers with region and KYC status
- **Rentals** — trip records linking all four domains

### CURATED tables

| Table | Type | Key columns |
|---|---|---|
| `DIM_STATION` | SCD2 | `station_sk`, `station_id`, `effective_from`, `is_current` |
| `DIM_BIKE` | SCD2 | `bike_sk`, `bike_id`, `effective_from`, `is_current` |
| `DIM_USER` | SCD2 | `user_sk`, `user_id`, `effective_from`, `is_current` |
| `DIM_DATE` | Static calendar | `date_sk` (YYYYMMDD INT), 2020–2030 |
| `FACT_RENTAL` | Fact | `rental_sk`, `user_sk`, `bike_sk`, `start_station_sk`, `date_sk` |

### Why Star Schema
Chosen over snowflake schema because:
- Small dimension sizes (10–100 rows per domain) make normalization storage savings negligible
- Simpler KPI queries — no chained joins across normalized tables
- Snowflake platform's columnar engine eliminates the query cost of denormalization
- SCD2 already adds complexity — normalization on top would make curate SPs hard to maintain

---

## SCD2 Logic

Each dimension SP runs two MERGE passes:

**Pass 1** — detect changes via `MD5(_hash_diff)` on tracked columns, close old row:
```sql
MERGE INTO DIM_STATION tgt USING (...) src
ON tgt.station_id = src.station_id AND tgt.is_current = TRUE
WHEN MATCHED AND tgt._hash_diff <> src.new_hash THEN
  UPDATE SET effective_to = CURRENT_TIMESTAMP(), is_current = FALSE
```

**Pass 2** — insert new version for new or changed entities:
```sql
MERGE INTO DIM_STATION tgt USING (...) src
ON tgt.station_id = src.station_id AND tgt.is_current = TRUE
WHEN NOT MATCHED THEN INSERT (...)
```

**Hash columns tracked per dimension:**

| Dimension | Tracked columns |
|---|---|
| `DIM_STATION` | station_name, capacity, city_zone, status |
| `DIM_BIKE` | status, battery_level, odometer_km, firmware_version |
| `DIM_USER` | customer_name, region, kyc_status, city |

---

## KPIs

| KPI | Table | Description |
|---|---|---|
| 1 | `KPI_ANOMALOUS_RENTAL_SCORE` | (Flagged rentals / Total) × 100 |
| 2 | `KPI_STATION_AVAILABILITY` | % of capacity available per station |
| 3 | `KPI_RIDER_ENGAGEMENT` | % of users with ≥1 rental in last 30 days |
| 4 | `KPI_FLEET_HEALTH` | % of bikes within all health thresholds |
| 5 | `KPI_ARR_BY_CHANNEL` | Avg revenue per rental by channel + plan type |

---

## Pipeline Automation

### Task chain (all tasks in `CITYRIDE_DB.RAW`)

```
TASK_VALIDATE  →  TASK_CURATE  →  TASK_KPIS
(5 min + stream)   (after validate)  (after curate)
```

### TASK_VALIDATE trigger logic
Only fires when streams detect new Snowpipe rows:
```sql
WHEN SYSTEM$STREAM_HAS_DATA('STREAM_STATIONS')
  OR SYSTEM$STREAM_HAS_DATA('STREAM_BIKES')
  OR SYSTEM$STREAM_HAS_DATA('STREAM_RENTALS')
  OR SYSTEM$STREAM_HAS_DATA('STREAM_USERS')
```

### End-to-end latency
S3 upload → KPIs updated: **~10 minutes**

---

## Stored Procedures

### RAW → VALIDATED

| SP | What it does |
|---|---|
| `SP_TRUNCATE_RAW(domain)` | Deletes all rows from RAW table after successful validation |
| `SP_VALIDATE_STATIONS()` | DQ checks + INSERT into VALIDATED.STATIONS |
| `SP_VALIDATE_BIKES()` | DQ checks + INSERT into VALIDATED.BIKES |
| `SP_VALIDATE_USERS()` | DQ checks + INSERT into VALIDATED.USERS |
| `SP_VALIDATE_RENTALS()` | DQ checks + ref integrity + INSERT into VALIDATED.RENTALS |

### VALIDATED → CURATED

| SP | What it does |
|---|---|
| `SP_CURATE_STATIONS()` | SCD2 MERGE into DIM_STATION |
| `SP_CURATE_BIKES()` | SCD2 MERGE into DIM_BIKE |
| `SP_CURATE_USERS()` | SCD2 MERGE into DIM_USER |
| `SP_CURATE_RENTALS()` | Surrogate key resolution + MERGE into FACT_RENTAL |

### ANALYTICS → KPI

| SP | What it does |
|---|---|
| `SP_KPI_ANOMALOUS_RENTAL()` | Computes anomaly probability score |
| `SP_KPI_STATION_AVAILABILITY()` | Computes per-station availability |
| `SP_KPI_RIDER_ENGAGEMENT()` | Computes 30-day active rider ratio |
| `SP_KPI_FLEET_HEALTH()` | Computes fleet health index |
| `SP_KPI_ARR_BY_CHANNEL()` | Computes revenue by channel + plan type |

---

## Data Governance

### Roles

| Role | Access |
|---|---|
| `CITYRIDE_SYSADMIN` | Full DDL + DML on all schemas |
| `CITYRIDE_PIPELINE` | INSERT on all schemas, runs SP chain |
| `CITYRIDE_ANALYST` | SELECT on VALIDATED + CURATED + ANALYTICS, PII masked |
| `CITYRIDE_OPS_NORTH/SOUTH/EAST` | SELECT on CURATED + ANALYTICS, PII fully masked |
| `CITYRIDE_GOVERNANCE` | Manages masking + row access policies |

### Masking Policies

| Policy | Column | SYSADMIN/PIPELINE | ANALYST | OPS/Others |
|---|---|---|---|---|
| `MASK_EMAIL` | email | Full | `ph***@mail.com` | `***MASKED***` |
| `MASK_PHONE` | phone | Full | Full | `XXXXXX1234` |
| `MASK_GPS` | lat/lon FLOAT | Full | Full | `NULL` |

Applied to: `VALIDATED.USERS`, `VALIDATED.RENTALS`, `CURATED.DIM_USER`, `CURATED.FACT_RENTAL`

---

## Streamlit Dashboard

7 tabs powered by Snowflake session queries:

| Tab | Content |
|---|---|
| Overview | 4 KPI metrics, rentals/revenue trend, SCD2 history tables, audit log |
| KPI 1 — Anomaly | Score metric + trend line + flagged rentals table |
| KPI 2 — Availability | Per-station bar charts + low availability warnings |
| KPI 3 — Engagement | Area chart trend + rider activity breakdown |
| KPI 4 — Fleet Health | Health index trend + status distribution + SCD2 bike versions |
| KPI 5 — Revenue | Channel/plan bar charts + revenue trend by channel |
| Date Analytics | Rentals by DOW, month, weekday/weekend, quarter (DIM_DATE joins) |

---

## Deployment

### Inside Snowflake (Streamlit in Snowflake)
1. Snowflake UI → Projects → Streamlit → + Streamlit App
2. Set database: `CITYRIDE_DB`, schema: `ANALYTICS`
3. Paste `11_streamlit_dashboard.py` into editor → Run

### External (Streamlit Cloud / local)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure credentials
cp .env.example .env
# fill in SNOWFLAKE_ACCOUNT, USER, PASSWORD, etc.

# 3. Run
streamlit run app.py
```

For Streamlit Cloud — push to GitHub, connect repo at share.streamlit.io, add credentials as Secrets.

---

## Setup — Run Order

```sql
-- 1. Infrastructure
01_database.sql
02_roles.sql
03_storage_integration.sql
04_tables.sql

-- 2. Pipeline
05_stored_procedures.sql
06_tasks.sql
07_sp_curate.sql
08_sp_kpis.sql
09_tasks_curate_kpi.sql

-- 3. Governance
10_data_masking.sql

-- 4. Calendar dimension (run once)
12_dim_date.sql

-- 5. First manual trigger (existing RAW data)
CALL CITYRIDE_DB.VALIDATED.SP_VALIDATE_STATIONS();
CALL CITYRIDE_DB.VALIDATED.SP_VALIDATE_BIKES();
CALL CITYRIDE_DB.VALIDATED.SP_VALIDATE_USERS();
CALL CITYRIDE_DB.VALIDATED.SP_VALIDATE_RENTALS();

CALL CITYRIDE_DB.CURATED.SP_CURATE_STATIONS();
CALL CITYRIDE_DB.CURATED.SP_CURATE_BIKES();
CALL CITYRIDE_DB.CURATED.SP_CURATE_USERS();
CALL CITYRIDE_DB.CURATED.SP_CURATE_RENTALS();

CALL CITYRIDE_DB.ANALYTICS.SP_KPI_ANOMALOUS_RENTAL();
CALL CITYRIDE_DB.ANALYTICS.SP_KPI_STATION_AVAILABILITY();
CALL CITYRIDE_DB.ANALYTICS.SP_KPI_RIDER_ENGAGEMENT();
CALL CITYRIDE_DB.ANALYTICS.SP_KPI_FLEET_HEALTH();
CALL CITYRIDE_DB.ANALYTICS.SP_KPI_ARR_BY_CHANNEL();
```

From this point every S3 file upload triggers the full pipeline automatically.

---

## Tech Stack

| Component | Technology |
|---|---|
| Cloud data warehouse | Snowflake |
| Storage | AWS S3 |
| Ingestion | Snowpipe + SQS event notifications |
| Transformation | Snowflake SQL stored procedures |
| Orchestration | Snowflake Tasks + Streams |
| Dimensional model | Star schema with SCD2 |
| Governance | Dynamic data masking, RBAC |
| Dashboard | Streamlit (Snowflake-native + external) |
| Language | SQL (Snowflake scripting), Python |

---

## AWS S3 Setup

1. Create bucket `s3://cityride-data/`
2. Create SQS queues — one per domain
3. Configure S3 event notifications → SQS on `s3:ObjectCreated:*`
4. Run `03_storage_integration.sql` as ACCOUNTADMIN
5. Grant S3 bucket access to the IAM role from `SHOW INTEGRATIONS`

### File naming convention
```
stations_master.csv   — initial full load
stations_inc.csv      — incremental updates
bikes_master.csv
bikes_inc.csv
rentals_master.csv
rentals_inc.csv
users_master.csv
users_inc.csv
```
