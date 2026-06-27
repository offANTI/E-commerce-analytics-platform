# E-commerce Analytics Platform

End-to-end data pipeline built on real e-commerce data from two public APIs. Bronze → Silver → Gold Medallion Architecture, dbt transformations, Airflow orchestration, and a Metabase dashboard.

---

## What this project does

Two public APIs (DummyJSON and Escuela) feed raw data into a PostgreSQL Bronze layer. A Python ETL app handles the extraction and loading. From there, dbt transforms the data through Silver (cleaning and validation) into Gold (business-ready aggregates). Airflow schedules the dbt pipeline nightly, and Metabase sits on top of the Gold layer for dashboards.

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

| Layer | Technology |
|---|---|
| Ingestion | Python, Requests |
| Storage | PostgreSQL 15 |
| Transformation | dbt (dbt-postgres 1.7) |
| Orchestration | Apache Airflow 2.8 |
| Containerization | Docker Compose |
| Visualization | Metabase |
| Testing | pytest, dbt tests |
| Code Quality | Ruff |
| Version Control | Git / GitHub |

---

## Data layers

**Bronze** stores everything as-is from the APIs: orders, products, categories, users. No transformation, no filtering.

**Silver** cleans and validates. Nulls and negative amounts are filtered out, JSONB fields are unpacked into typed columns, and duplicates are handled. The `silver_orders_incremental` model uses a watermark-based pattern so only new rows are processed on each run instead of rebuilding the full table every night.

**Gold** aggregates for the dashboard: daily and monthly revenue by store, customer lifetime value with ranking.

---

## Incremental loading

The `silver_orders_incremental` model only processes rows that arrived after the last run:

```sql
{% if is_incremental() %}
  AND created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
```

On the first run it loads everything. After that it only touches new rows:

```
1st run — SELECT 5000   (full load, table didn't exist)
2nd run — INSERT 0 0    (nothing new, scan skipped entirely)
3rd run — INSERT 0 1    (one new row detected and merged)
```

This pattern only makes sense for a growing fact table like orders. Products and users are small reference tables — rebuilding them from scratch each night takes milliseconds and adding incremental logic would just add unnecessary complexity.

---

## Airflow orchestration

A DAG runs nightly at 02:00:

```
dbt_run → dbt_test
```

If `dbt_run` fails, `dbt_test` is skipped automatically. dbt is installed directly into the Airflow image via `Dockerfile.airflow` rather than calling a separate dbt container. This avoids needing to mount the Docker socket, which would give the Airflow container root-level access to the host machine.

---

## Data quality

19 dbt tests run on every pipeline execution: `not_null` and `unique` checks on primary keys, and `accepted_values` validation on store names.

```
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 TOTAL=19
```

---

## Dashboard

Built in Metabase on top of the Gold layer. KPIs: Orders, Items Sold, Repeat Rate, Customers. Charts cover monthly revenue trend, revenue and average order value by store, top products, and customer LTV segments.

![Dashboard](docs/dashboard.jpg)

---

## Data insights

The two stores have very different profiles:

| Metric | Escuela | DummyJSON |
|---|---|---|
| Total Revenue | ~$30M | ~$300K |
| Avg Order Value | ~$10,000 | ~$700 |
| Profile | Premium / B2B | Budget / Demo |

Revenue peaks in April 2026 across both stores, which could suggest a seasonal or promotional pattern worth investigating.

---

## Project structure

```
E_commerce_Analytics_Platform/
├── config/
├── dags/
│   └── ecommerce_pipeline.py
├── database/
├── dbt/
│   └── ecommerce_analytics/
│       ├── models/
│       │   ├── silver/
│       │   └── gold/
│       └── macros/
├── src/
│   ├── extract/
│   ├── load/
│   └── transform/
├── tests/
├── utils/
├── docker-compose.yml
├── Dockerfile
├── Dockerfile.airflow
├── main.py
└── requirements.txt
```

---

## Quick start

Prerequisites: Docker and Docker Compose.

```bash
git clone https://github.com/offANTI/E-commerce-analytics-platform.git
cd E-commerce-analytics-platform

# create .env with your credentials (see below)

docker-compose up -d

# load data into Bronze
docker exec -it bsg_etl_app python main.py

# Airflow UI — http://localhost:8080 (admin / admin)
# Metabase  — http://localhost:3000
```

Required `.env` variables:

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

## Key metrics (sample data)

5,000 orders, 15,217 items sold, 649 unique customers, 90% repeat rate.

---

## Known limitations

The ETL pipeline runs manually for now. Integrating it into the Airflow DAG hit a Python version conflict: the ETL app needs Python 3.11 and pydantic v2, while Airflow 2.8 runs on Python 3.8. The right production solution is `KubernetesPodOperator` — ETL runs in an isolated pod with its own Python environment, and Airflow stays a lightweight orchestrator without carrying the ETL's dependencies.

---

## Author

Ruslan Tuliei — [@offANTI](https://github.com/offANTI)