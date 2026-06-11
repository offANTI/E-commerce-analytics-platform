# E-commerce Analytics Platform

> End-to-end data pipeline with Medallion Architecture, dbt transformations, and Metabase analytics dashboard.

---

## Overview

Production-style analytics engineering project built on real e-commerce data from two public APIs. Implements a full Bronze → Silver → Gold data pipeline with automated data quality tests, containerized infrastructure, and a business analytics dashboard.

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
         │  dbt views          │
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
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, Requests |
| Storage | PostgreSQL 15 |
| Transformation | dbt (dbt-postgres 1.7) |
| Orchestration | Docker Compose |
| Visualization | Metabase |
| Testing | pytest, dbt tests |
| Version Control | Git / GitHub |
| Code Quality     | Ruff        |
---

## Data Pipeline

### Bronze Layer
Raw data ingested from APIs and stored as-is in PostgreSQL:
- `bronze.orders` — transaction records
- `bronze.dummy_products` — product catalog (DummyJSON)
- `bronze.dummy_categories` — product categories
- `bronze.escuela_products` — product catalog (Escuela)
- `bronze.escuela_users` — customer records

### Silver Layer (dbt views)
Cleaned and validated transformations on top of Bronze:
- `silver.silver_orders` — validated orders, nulls and negatives filtered
- `silver.silver_products` — parsed product attributes from JSONB
- `silver.silver_users` — parsed customer profiles from JSONB

### Gold Layer (dbt tables)
Aggregated business metrics ready for dashboards:
- `gold.gold_revenue_summary` — daily revenue by store
- `gold.gold_customer_ltv` — customer lifetime value with ranking

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
- Daily Revenue Trend by store
- Revenue by Store (total)
- Average Order Value by store
- Top Products by Revenue
- Customer LTV segments

![Dashboard](docs/dashboard.jpg)

---

## Project Structure

```
E_commerce_Analytics_Platform/
├── config/
│   ├── settings.py
│   └── __init__.py
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
│       │   │   ├── silver_products.sql
│       │   │   ├── silver_users.sql
│       │   │   ├── sources.yml
│       │   │   └── schema.yml
│       │   └── gold/
│       │       ├── gold_revenue_summary.sql
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
├── main.py
└── requirements.txt
```

---

## Quick Start

**Prerequisites:** Docker, Docker Compose

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/e-commerce-analytics-platform.git
cd e-commerce-analytics-platform

# Set up environment variables
cp .env.example .env

# Start all services
docker-compose up -d

# Run ETL pipeline
docker exec -it bsg_etl_app python main.py

# Run dbt transformations
docker exec -it bsg_dbt bash
cd /usr/app/dbt/ecommerce_analytics
dbt run --profiles-dir /usr/app/dbt
dbt test --profiles-dir /usr/app/dbt

# Open Metabase dashboard
# http://localhost:3000
```

---

## Data Sources

- [DummyJSON API](https://dummyjson.com) — products, categories
- [Escuela API](https://api.escuelajs.co/api/v1/products) — products, categories, users

---

## Key Metrics (Sample Data)

| Metric | Value |
|---|---|
| Total Orders | 5,000 |
| Total Items Sold | 15,217 |
| Unique Customers | 649 |
| Repeat Rate | 90% |

## Data Insights

Two stores with fundamentally different business profiles:

| Metric | Escuela | DummyJSON |
|---|---|---|
| Total Revenue | ~$30M | ~$300K |
| Avg Order Value | ~$10,000 | ~$700 |
| Profile | Premium / B2B | Budget / Demo |

Revenue peaks in April 2026 across both stores suggest 
a seasonal or promotional pattern worth investigating.
---
## Author

Ruslan Tuliei

GitHub: https://github.com/offANTI