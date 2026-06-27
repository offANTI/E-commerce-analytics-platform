# E-commerce Analytics Platform

> End-to-end data pipeline with Medallion Architecture, dbt transformations, Airflow orchestration, and Metabase analytics dashboard.

---

## Overview

Production-style analytics engineering project built on real e-commerce data from two public APIs. Implements a full Bronze → Silver → Gold data pipeline with automated data quality tests, incremental data loading, scheduled orchestration via Airflow, containerized infrastructure, and a business analytics dashboard.

---

## Architecture

```
┌─────────────────┐    ┌─────────────────┐
│   DummyJSON API │    │   Escuela API   │
└────────┬────────┘    └────────┬────────┘
         │                      │
         └──────────┬───────────┘
                    ▼
         ┌─────────────────────┐
         │   Python ETL App    │
         │  (extract + load)   │
         └──────────┬──────────┘
                    ▼
         ┌─────────────────────┐
         │  BRONZE  (raw)      │
         │  PostgreSQL schemas │
         └──────────┬──────────┘
                    ▼
         ┌─────────────────────┐
         │  SILVER  (cleaned)  │
         │  dbt views +        │
         │  incremental model  │
         └──────────┬──────────┘
                    ▼
         ┌─────────────────────┐
         │  GOLD  (aggregated) │
         │  dbt tables         │
         └──────────┬──────────┘
                    ▼
         ┌─────────────────────┐
         │  Metabase Dashboard │
         └─────────────────────┘

      Orchestrated nightly by Airflow (dbt run → dbt test)
```

---

## Tech Stack

| Layer            | Technology              |
|------------------|--------------------------|
| Ingestion        | Python, Requests         |
| Storage          | PostgreSQL 15            |
| Transformation   | dbt (dbt-postgres 1.7)   |
| Orchestration    | Apache Airflow 2.8        |
| Containerization | Docker Compose           |
| Visualization    | Metabase                 |
| Testing          | pytest, dbt tests        |
| Code Quality     | Ruff                     |
| Version Control  | Git / GitHub             |

---

## Data Pipeline

### Bronze Layer

Raw data ingested from APIs and stored as-is in PostgreSQL:

- `bronze.orders` — transaction records
- `bronze.dummy_products` — product catalog (DummyJSON)
- `bronze.dummy_categories` — product categories
- `bronze.escuela_products` — product catalog (Escuela)
- `bronze.escuela_users` — customer records

### Silver Layer (dbt views + incremental model)

Cleaned and validated transformations on top of Bronze:

- `silver.silver_orders` — validated orders, nulls and negatives filtered (view, full refresh)
- `silver.silver_orders_incremental` — same validation, but materialized incrementally using a `created_at` watermark and `unique_key='order_id'` for MERGE-based updates
- `silver.silver_products` — parsed product attributes from JSONB
- `silver.silver_users` — parsed customer profiles from JSONB

### Gold Layer (dbt tables)

Aggregated business metrics ready for dashboards:

- `gold.gold_revenue_summary` — daily revenue by store
- `gold.gold_monthly_revenue` — monthly revenue and order counts by store
- `gold.gold_customer_ltv` — customer lifetime value with ranking

---

## Incremental Loading

`silver_orders_incremental` demonstrates a watermark-based incremental pattern:

```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    schema='silver'
) }}

SELECT ...
FROM {{ source('bronze', 'orders') }}
WHERE total_amount > 0
  AND quantity > 0
  AND order_date IS NOT NULL

{% if is_incremental() %}
  AND created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
```

On the first run, dbt performs a full load. On subsequent runs, only rows newer than the existing watermark are processed and merged by `order_id`:

```
1st run:  SELECT 5000  → full load (table doesn't exist yet)
2nd run:  INSERT 0 0   → no new rows, full scan skipped
3rd run:  INSERT 0 1   → one new row detected and merged
```

This pattern is applied only to the growing fact table (`orders`). Reference tables (`products`, `users`) remain full-refresh views, since incremental logic adds complexity that isn't justified for small, slowly-changing datasets.

---

## Orchestration (Airflow)

A scheduled DAG (`dags/ecommerce_pipeline.py`) runs nightly at 02:00:

```
dbt_run → dbt_test
```

- `dbt_run` rebuilds all Silver and Gold models (including the incremental model)
- `dbt_test` runs all 19 data quality tests against the rebuilt models
- If `dbt_run` fails, `dbt_test` is skipped — Gold tables are not validated against a broken build

dbt is installed directly into the Airflow image (`Dockerfile.airflow`) rather than invoking a separate dbt container at runtime. This avoids mounting the Docker socket into the Airflow container, which would grant host-level root access — an unnecessary security trade-off for a local dev pipeline.

---

## Data Quality

19 automated dbt tests across all layers:

- `not_null` — critical fields validated at Silver and Gold
- `unique` — primary keys enforced
- `accepted_values` — store names validated against known sources

```bash
dbt test --profiles-dir /usr/app/dbt
# Done. PASS=19 WARN=0 ERROR=0 SKIP=0 TOTAL=19
```

---

## Dashboard

Built in Metabase on top of Gold layer tables.

**KPIs:** Orders · Items Sold · Repeat Rate · Customers

**Charts:**
- Monthly Revenue Trend by store
- Revenue by Store (total)
- Average Order Value by store
- Top Products by Revenue
- Customer LTV segments

![Dashboard](docs/dashboard.jpg)

---

## Data Insights

Two stores with fundamentally different business profiles:

| Metric          | Escuela        | DummyJSON      |
|------------------|----------------|----------------|
| Total Revenue    | ~$30M          | ~$300K         |
| Avg Order Value  | ~$10,000       | ~$700          |
| Profile          | Premium / B2B  | Budget / Demo  |

Monthly revenue peaks in April 2026 across both stores, suggesting a seasonal or promotional pattern worth investigating further.

---

## Project Structure

```
E_commerce_Analytics_Platform/
├── config/
│   ├── settings.py
│   └── __init__.py
├── dags/
│   └── ecommerce_pipeline.py
├── database/
│   ├── bronze_tables.sql
│   ├── silver_views.sql
│   ├── gold_analytics.sql
│   └── init_schema.sql
├── dbt/
│   └── ecommerce_analytics/
│       ├── models/
│       │   ├── silver/
│       │   │   ├── silver_orders.sql
│       │   │   ├── silver_orders_incremental.sql
│       │   │   ├── silver_products.sql
│       │   │   ├── silver_users.sql
│       │   │   ├── sources.yml
│       │   │   └── schema.yml
│       │   └── gold/
│       │       ├── gold_revenue_summary.sql
│       │       ├── gold_monthly_revenue.sql
│       │       ├── gold_customer_ltv.sql
│       │       └── schema.yml
│       ├── macros/
│       │   └── generate_schema_name.sql
│       └── dbt_project.yml
├── src/
│   ├── extract/
│   │   └── api_client.py
│   ├── load/
│   │   └── postgres_loader.py
│   └── transform/
│       ├── orders.py
│       └── pipeline.py
├── tests/
│   ├── test_extract.py
│   ├── test_load.py
│   └── test_transform.py
├── utils/
│   └── logger.py
├── docker-compose.yml
├── Dockerfile
├── Dockerfile.airflow
├── main.py
└── requirements.txt
```

---

## Quick Start

**Prerequisites:** Docker, Docker Compose

```bash
# Clone the repository
git clone https://github.com/offANTI/E-commerce-analytics-platform.git
cd E-commerce-analytics-platform

# Create a .env file with DB credentials and API URLs (see below)

# Build and start all services
docker-compose up -d

# Run ETL pipeline
docker exec -it bsg_etl_app python main.py

# Run dbt transformations manually (optional — Airflow runs this nightly)
docker exec -it bsg_dbt bash
cd /usr/app/dbt/ecommerce_analytics
dbt run --profiles-dir /usr/app/dbt
dbt test --profiles-dir /usr/app/dbt

# Open Airflow UI (trigger/monitor the nightly pipeline)
# http://localhost:8080  (user: admin / admin)

# Open Metabase dashboard
# http://localhost:3000
```

### Required `.env` variables

```
LOG_LEVEL=INFO
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ecommerce_analytics
DUMMY_PRODUCTS_URL=https://dummyjson.com/products
DUMMY_CATEGORIES_URL=https://dummyjson.com/products/categories
ESCUELA_PRODUCTS_URL=https://api.escuelajs.co/api/v1/products
ESCUELA_CATEGORIES_URL=https://api.escuelajs.co/api/v1/categories
ESCUELA_USERS_URL=https://api.escuelajs.co/api/v1/users
```

---

## Data Sources

- [DummyJSON API](https://dummyjson.com) — products, categories
- [Escuela API](https://api.escuelajs.co/api/v1/products) — products, categories, users

---

## Key Metrics (Sample Data)

| Metric           | Value  |
|-------------------|--------|
| Total Orders      | 5,000  |
| Total Items Sold  | 15,217 |
| Unique Customers  | 649    |
| Repeat Rate       | 90%    |

---

## Architecture Decisions

**Why dbt instead of raw SQL views?**
Built-in data quality tests, dependency graphs via `ref()`, and auto-generated documentation — all version-controlled in Git.

**Why incremental only on `orders`?**
Incremental models add complexity (watermark logic, MERGE behavior). This is justified only for tables that grow continuously. `products` and `users` are small reference tables — full refresh is simpler and fast enough.

**Why is dbt installed inside the Airflow image instead of calling a separate dbt container?**
Running `docker exec` from inside the Airflow container isn't possible without mounting `/var/run/docker.sock`, which grants the container root access to the host. Embedding dbt in the Airflow image avoids this security trade-off entirely, at the cost of a duplicated dbt installation across two containers — an acceptable trade-off for a local development pipeline.

---
## Known Limitations/ArArchitecture Decisions

**ETL not yet orchestrated via Airflow:**
The ETL pipeline (`main.py`) is currently triggered manually. 
Integrating it into the Airflow DAG caused dependency conflicts — 
the ETL app requires Python 3.11 + pydantic v2, while Airflow 2.8 
runs on Python 3.8. The correct production solution would be 
`KubernetesPodOperator` — running ETL in an isolated pod with its 
own Python environment, keeping Airflow lightweight.
## Author

Ruslan Tuliei
GitHub: [@offANTI](https://github.com/offANTI)