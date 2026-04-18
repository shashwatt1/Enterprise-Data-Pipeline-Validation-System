# Enterprise Data Pipeline & Validation System

A production-grade data pipeline system that demonstrates end-to-end data ingestion, schema mapping, transformation, validation, and SQL-based querying — built with **FastAPI**, **Pandas**, and **SQLAlchemy**.

---

## Architecture

```
┌─────────────────────┐     ┌──────────────────────┐
│   CSV File Upload   │     │  JSON REST API Input  │
└────────┬────────────┘     └──────────┬───────────┘
         │                             │
         └──────────┬──────────────────┘
                    ▼
         ┌──────────────────┐
         │  Ingestion Layer │  ← Stores raw data (JSON blobs)
         └────────┬─────────┘
                  ▼
         ┌──────────────────┐
         │  Schema Mapper   │  ← YAML-driven column mapping
         └────────┬─────────┘
                  ▼
         ┌──────────────────┐
         │  Transformation  │  ← Dedup, clean, standardize
         │     Engine       │
         └────────┬─────────┘
                  ▼
         ┌──────────────────┐
         │  Validation      │  ← Null, unique, range, pattern
         │     Layer        │
         └────────┬─────────┘
                  ▼
         ┌──────────────────┐
         │  Normalized      │  ← customers & transactions tables
         │  Storage (SQL)   │
         └────────┬─────────┘
                  ▼
    ┌─────────────┴─────────────┐
    │                           │
    ▼                           ▼
┌───────────┐          ┌──────────────────┐
│ REST APIs │          │ Streamlit        │
│ (FastAPI) │          │ Dashboard (opt.) │
└───────────┘          └──────────────────┘
```

### Pipeline Flow

| Step | Component | Description |
|------|-----------|-------------|
| 1 | **Ingestion** | Accept CSV uploads or JSON payloads; store as raw records |
| 2 | **Schema Validation** | Verify source columns match expected schema |
| 3 | **Column Mapping** | Rename columns from source → target using YAML config |
| 4 | **Transformation** | Remove duplicates, parse dates/currency, standardize formats |
| 5 | **Validation** | Run null, uniqueness, range, and pattern checks |
| 6 | **Storage** | Write cleaned data to normalized relational tables |

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| Backend API | FastAPI / Uvicorn |
| Data Processing | Pandas |
| Database | SQLite (default) / PostgreSQL / MySQL |
| ORM | SQLAlchemy 2.0 |
| Schema Config | YAML (PyYAML) |
| Dashboard | Streamlit + Plotly |
| Testing | Pytest + FastAPI TestClient |

---

## Project Structure

```
enterprise-data-pipeline/
├── app/
│   ├── api/
│   │   ├── routes_ingest.py       # Upload & ingestion endpoints
│   │   ├── routes_pipeline.py     # Pipeline trigger & status
│   │   ├── routes_data.py         # Data retrieval & aggregations
│   │   └── routes_validation.py   # Validation report endpoints
│   ├── services/
│   │   ├── ingestion.py           # CSV/JSON parsing & raw storage
│   │   ├── schema_mapper.py       # YAML config & column mapping
│   │   ├── transformer.py         # Data cleaning & standardization
│   │   ├── validator.py           # Validation checks & reporting
│   │   ├── pipeline.py            # Orchestrates the full flow
│   │   └── query_service.py       # SQL joins & aggregations
│   ├── models/
│   │   └── database.py            # SQLAlchemy ORM models
│   ├── schemas/
│   │   └── api_schemas.py         # Pydantic request/response models
│   ├── utils/
│   │   ├── db.py                  # Database engine & session
│   │   └── logger.py              # Structured logging
│   └── core/
│       └── config.py              # Environment settings
├── data/
│   ├── sample_customers.csv       # 50 customer records (with issues)
│   └── sample_transactions.csv    # 80 transaction records (with issues)
├── config/
│   └── mapping_config.yaml        # Schema mapping & transformation rules
├── tests/
│   ├── test_ingestion.py
│   ├── test_transformer.py
│   ├── test_validator.py
│   └── test_api.py
├── main.py                        # FastAPI entry point
├── streamlit_app.py               # Optional dashboard
├── requirements.txt
├── .env.example
└── README.md
```

---

## Setup

### 1. Clone & create virtual environment

```bash
cd enterprise-data-pipeline
python3 -m venv venv
source venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment

```bash
cp .env.example .env
# Edit .env if needed (default SQLite works out of the box)
```

### 4. Start the API server

```bash
uvicorn main:app --reload --port 8000
```

### 5. Open API docs

Visit: [http://localhost:8000/docs](http://localhost:8000/docs)

### 6. (Optional) Launch the dashboard

```bash
streamlit run streamlit_app.py
```

---

## API Endpoints

### Ingestion

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/ingest/csv` | Upload a CSV file (multipart form) |
| `POST` | `/api/v1/ingest/json` | Submit JSON records |

### Pipeline

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/pipeline/run/{entity_type}?pipeline_run_id=XXX` | Trigger pipeline |
| `GET`  | `/api/v1/pipeline/status/{run_id}` | Check pipeline status |
| `GET`  | `/api/v1/pipeline/logs/{run_id}` | Get step-by-step logs |

### Data Retrieval

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET`  | `/api/v1/data/customers?skip=0&limit=50&state=CA` | List customers |
| `GET`  | `/api/v1/data/transactions?status=completed&min_amount=100` | List transactions |
| `GET`  | `/api/v1/data/customer-transactions/{customer_id}` | Customer + transactions join |
| `GET`  | `/api/v1/data/aggregations/spend-by-customer` | Top spenders |
| `GET`  | `/api/v1/data/aggregations/monthly-summary` | Monthly trends |
| `GET`  | `/api/v1/data/aggregations/category-breakdown` | Spend by category |

### Validation

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET`  | `/api/v1/validation/report/{run_id}` | Detailed validation report |
| `GET`  | `/api/v1/validation/summary` | All validation run summaries |

---

## Quick Start: End-to-End Demo

### Step 1: Ingest sample customers

```bash
curl -X POST http://localhost:8000/api/v1/ingest/csv \
  -F "file=@data/sample_customers.csv" \
  -F "entity_type=customers"
```

Response:
```json
{
  "pipeline_run_id": "a1b2c3d4e5f6",
  "entity_type": "customers",
  "rows_ingested": 50,
  "source": "sample_customers.csv"
}
```

### Step 2: Run the pipeline

```bash
curl -X POST "http://localhost:8000/api/v1/pipeline/run/customers?pipeline_run_id=a1b2c3d4e5f6"
```

### Step 3: Check validation report

```bash
curl http://localhost:8000/api/v1/validation/report/a1b2c3d4e5f6
```

### Step 4: Query processed data

```bash
# All customers
curl http://localhost:8000/api/v1/data/customers

# Customers in Texas
curl "http://localhost:8000/api/v1/data/customers?state=TX"

# Top spenders
curl http://localhost:8000/api/v1/data/aggregations/spend-by-customer
```

---

## Sample SQL Queries (via Query Service)

### Customer–Transaction Join
```sql
SELECT c.customer_id, c.name, c.city, c.state,
       t.transaction_id, t.amount, t.category, t.transaction_date
FROM customers c
JOIN transactions t ON c.customer_id = t.customer_id
WHERE c.state = 'TX'
ORDER BY t.amount DESC;
```

### Monthly Revenue Aggregation
```sql
SELECT strftime('%Y-%m', transaction_date) AS month,
       COUNT(*) AS txn_count,
       SUM(amount) AS total_revenue,
       AVG(amount) AS avg_transaction
FROM transactions
WHERE transaction_date IS NOT NULL
GROUP BY month
ORDER BY month;
```

### Top 10 Customers by Spend
```sql
SELECT c.customer_id, c.name,
       SUM(t.amount) AS total_spend,
       COUNT(t.id) AS num_transactions
FROM customers c
JOIN transactions t ON c.customer_id = t.customer_id
GROUP BY c.customer_id, c.name
ORDER BY total_spend DESC
LIMIT 10;
```

---

## Data Quality Issues in Sample Data

The sample datasets include intentional data quality problems to demonstrate the pipeline's handling:

| Issue | Dataset | Example |
|-------|---------|---------|
| Missing emails | Customers | Rows 4, 12, 21, 26, 30, 35, 40, 45 |
| Inconsistent date formats | Both | `2023-01-15`, `01/20/2023`, `15-Mar-2023` |
| Mixed case | Customers | `new york`, `CHICAGO`, `los angeles` |
| Extra whitespace | Customers | `  Alice Johnson  `, `  PHILADELPHIA  ` |
| Duplicate records | Customers | Row 47 duplicates row 1 |
| Currency formatting | Transactions | `$1,200.50`, `$150.00` |
| Negative amounts | Transactions | Row 1051: `$-25.00` |
| Extreme values | Transactions | Row 1053: `$999,999.99` |
| Missing categories | Transactions | Rows 1023, 1034, 1054 |
| Duplicate txn IDs | Transactions | ID 1079 appears twice |
| Orphan references | Transactions | customer_id 999, 1001 don't exist |

---

## Running Tests

```bash
pytest tests/ -v
```

---

## Switching to PostgreSQL

1. Install PostgreSQL and create a database
2. Update `.env`:
   ```
   DATABASE_URL=postgresql://user:password@localhost:5432/pipeline_db
   ```
3. Install the driver: `pip install psycopg2-binary`
4. Restart the server — tables are auto-created

---

## License

MIT
