# Walmart Retail Data Engineering Pipeline

An end-to-end retail data engineering project that demonstrates how source data can be ingested, processed, tested, historized, and prepared for analytics using **PostgreSQL, Databricks, Apache Airflow, dbt, Python, and Docker**.

> This repository is an educational / portfolio implementation. It uses a sample Walmart-style dataset and a Databricks job placeholder for the CDC ingestion step. Cloud resources and credentials must be configured before the pipeline can run end to end.

## Project at a glance

The project builds a repeatable workflow for six retail entities:

- Customers
- Stores
- Products
- Employees
- Orders
- Order items

The intended data flow is:

```text
CSV files
   |
   v
Python loader (psycopg2)
   |
   v
PostgreSQL raw schema
   |
   v
Databricks job / CDC ingestion
   |
   v
Bronze source tables in Databricks
   |
   v
dbt Silver Technical models
   |
   v
dbt Silver Business model (OBT)
   |
   v
dbt Gold helper models + snapshots + fact model
   |
   v
Analytics-ready data
```

Apache Airflow coordinates the workflow and controls task dependencies. Docker Compose is used to run the local Airflow environment and its supporting services.

## Why this project?

Retail data is spread across customers, stores, products, employees, and transactions. Before it can be used reliably for reporting, it needs to be loaded consistently, cleaned, joined, checked, and shaped into analytics-friendly models.

This project demonstrates common data engineering practices:

- Batch loading CSV data into PostgreSQL.
- Triggering a Databricks job from an Airflow DAG.
- Transforming data in layers with dbt.
- Running source freshness checks and dbt tests.
- Building a consolidated business dataset.
- Preserving dimension history with dbt snapshots.
- Producing a final fact model for downstream analysis.
- Running the orchestration stack locally with Docker Compose.

## Technology stack

| Technology | Role in this project |
|---|---|
| Python | Loads CSV files into PostgreSQL and contains orchestration support logic |
| PostgreSQL | Relational landing / raw database for the sample retail entities |
| Databricks | Executes the configured ingestion / CDC job and hosts the analytical source tables |
| Apache Airflow | Schedules and orchestrates the pipeline tasks |
| dbt Core | Implements SQL transformations, tests, source freshness, and snapshots |
| PySpark / Databricks SDK | Supports the Databricks processing and job integration |
| Docker Compose | Runs Airflow services and supporting PostgreSQL / Redis containers |
| Git / GitHub | Version control |

## Data model

The sample data contains six related entities.

| Dataset | Purpose | Important relationships |
|---|---|---|
| `customers` | Customer profile and contact details | Referenced by orders |
| `stores` | Store name and location attributes | Referenced by orders and employees |
| `products` | Product, category, brand, and price | Referenced by order items |
| `employees` | Employee details, store assignment, role, and salary | References stores |
| `orders` | Order header, customer, store, timestamp, payment, and status | References customers and stores |
| `order_items` | Products and quantities purchased in each order | References orders and products |

The schema script defines the relational tables. The CSV loader maps each CSV file to its corresponding table in the PostgreSQL `raw` schema.

## Pipeline walkthrough

### 1. Source data loading

The Python loader uses `psycopg2` to connect to PostgreSQL. It maintains a mapping between CSV filenames and destination tables, checks whether each file exists, and uses PostgreSQL's `COPY` command to load rows with the header row handled by the CSV format option.

Conceptually:

1. Connect to PostgreSQL.
2. Iterate through the configured CSV-to-table mapping.
3. Check that the CSV file exists.
4. Load it using `COPY ... FROM STDIN`.
5. Commit the load and report success or failure.

Before running it, update the connection configuration to use environment variables or a secrets manager. Do not put a real database connection string in source code.

### 2. Databricks ingestion / CDC

The Airflow DAG contains an `ingest_cdc` task. It creates a Databricks `WorkspaceClient`, starts a configured Databricks job, and polls the run status until the job terminates.

- A successful Databricks run allows the DAG to continue.
- A failed, skipped, or internally failed run raises an exception and stops downstream tasks.
- The host, token, and job ID shown in the source are placeholders and must be configured.

The repository's DAG assumes the Databricks job is already created and that its output is available to dbt as the configured Bronze source tables. The Databricks workspace/job itself is not provisioned automatically by this Airflow task.

### 3. dbt source freshness

Airflow runs:

```bash
dbt source freshness
```

This checks the freshness rules configured for dbt sources. It helps identify source data that has not been updated within the expected time window.

### 4. Silver Technical layer

Airflow runs the selected technical models:

```bash
dbt run --select silver_t
dbt test --select silver_t
```

This is the first dbt transformation layer after the Bronze sources. It is intended to standardize source entities and prepare them for business-level joins. The test task validates the technical models before the next layer runs.

### 5. Silver Business layer

Airflow then runs:

```bash
dbt run --select silver_b
dbt test --select silver_b
```

The business layer builds an **One Big Table (OBT)** by combining the technical models. A wide, joined dataset can simplify downstream consumption when analysts need order, customer, product, store, and employee context together.

### 6. Gold layer

The DAG executes three Gold-stage activities:

```bash
dbt run --select gold/ephermeral
dbt snapshot
dbt run --select gold/fact
```

- **Ephemeral/helper models:** reusable intermediate logic that dbt can inline into downstream models rather than materializing as standalone tables.
- **Snapshots:** preserve historical versions of selected records so changes to dimensions can be analyzed over time. Snapshot behavior depends on the configured strategy and unique key.
- **Fact model:** builds the final analytical fact dataset from the prepared business data.

The DAG's task order ensures the technical and business tests run before the downstream Gold steps.

## Airflow orchestration

The DAG is named `orchestrate`. Its intended dependency chain is:

```text
ingest_cdc
   |
clean_target
   |
source_freshness
   |
silver_technical
   |
silver_technical_tests
   |
silver_business
   |
silver_business_tests
   |
gold_ephemeral
   |
gold_dimensions (dbt snapshot)
   |
gold_facts
```

The `clean_target` task removes dbt's generated `target` and `logs` directories before the run. This clears generated artifacts; it does not delete the source data tables.

Airflow uses task dependencies to ensure each step waits for the preceding step to complete successfully. If an upstream task fails, downstream tasks are not run under the default dependency behavior.

## Running locally

### Prerequisites

- Git
- Docker Desktop with Docker Compose
- A Databricks workspace and a configured Databricks job
- PostgreSQL access for the source loading step
- The required Python / dbt dependencies specified by the project

Airflow's Docker Compose setup recommends at least 4 GB of memory, 2 CPUs, and 10 GB of free disk space.

### 1. Clone the repository

```bash
git clone https://github.com/nishu2423/Walmart-Data-Engineering-Pipeline.git
cd Walmart-Data-Engineering-Pipeline
```

### 2. Configure environment variables and credentials

Create the environment file expected by the Compose configuration and provide the required Airflow settings and Databricks connection details. Use the variable names referenced by the project configuration.

At minimum, replace the Databricks placeholders in the DAG with secure configuration for:

- Databricks workspace host
- Databricks access token
- Databricks job ID

Prefer Airflow Connections, environment variables, or a secrets backend. Never commit `.env`, access tokens, passwords, or private connection strings.

### 3. Start Airflow

Run Docker Compose from the directory containing the Airflow Compose file:

```bash
docker compose up --build
```

Open the Airflow UI:

```text
http://localhost:8080
```

The Compose configuration shown in this project initializes an Airflow administrator with development defaults. Change the credentials before exposing the service beyond your local machine.

### 4. Load the source CSV files

Configure PostgreSQL and the database schema first. Then run the Python CSV loader from the directory where its relative `data` directory is available:

```bash
python load_data.py
```

Adjust the command to the actual location of the loader in your checkout. Confirm that the six source tables have been populated before triggering the downstream workflow.

### 5. Configure and run dbt

Configure the dbt profile for your Databricks workspace and catalog/schema. Keep tokens outside version control. From the dbt project directory, validate the connection and models:

```bash
dbt debug
dbt source freshness
dbt run
dbt test
dbt snapshot
```

The Airflow DAG uses selectors such as `silver_t`, `silver_b`, and `gold/fact`; make sure those selectors match the model names and paths in your dbt project.

### 6. Trigger the DAG

In the Airflow UI:

1. Find the `orchestrate` DAG.
2. Unpause it if it is paused.
3. Trigger a manual run.
4. Open the DAG graph and monitor each task.
5. Inspect task logs if a step fails.

A successful Airflow run requires the Databricks job, source tables, dbt profile, and all required credentials to be correctly configured.

## Configuration and security notes

Before using this project beyond a local learning environment:

- Replace hardcoded Databricks host, token, and job ID placeholders with secure configuration.
- Replace the PostgreSQL connection-string placeholder with environment-based configuration.
- Do not commit `.env` files, passwords, API tokens, or private keys.
- Avoid committing Airflow task logs and generated dbt `target/` or `logs/` directories.
- Change default Airflow credentials and development secrets.
- Review the Docker Compose configuration before exposing Airflow or PostgreSQL ports publicly.
- Confirm that the Databricks job really performs the expected CDC ingestion and writes the source tables used by dbt.

## Current scope and limitations

This repository provides the orchestration and transformation workflow, plus sample source data and schema/loading components. It should not be described as a fully deployed production system until the external Databricks job, PostgreSQL environment, dbt models, credentials, and end-to-end execution have been configured and validated.

The README describes the workflow visible in the repository. Exact runtime behavior still depends on the deployed Databricks job and the dbt model/profile configuration.

## Learning roadmap

If you are studying the project, follow this order:

1. Understand the six source tables and their relationships.
2. Read the SQL DDL and learn the purpose of each column and key.
3. Trace how the Python loader moves CSV rows into PostgreSQL.
4. Understand how the Databricks SDK starts and monitors a job.
5. Read the Airflow DAG and draw its task dependency graph.
6. Learn dbt sources, models, selectors, tests, and snapshots.
7. Trace one order from the source tables through the Silver OBT to the Gold fact model.

## Reference

The repository's initial project notes link to a walkthrough video:

[Watch the project walkthrough on YouTube](https://youtu.be/ZEE-jNAthB0)

---

**Author:** [nishu2423](https://github.com/nishu2423)  
**Repository:** [Walmart Data Engineering Pipeline](https://github.com/nishu2423/Walmart-Data-Engineering-Pipeline)
