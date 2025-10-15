# Tottenham Analytics Pipeline

## Overview

This project is a data pipeline built to process and analyze football (soccer) matchday data from StatsBomb.

It covers the full flow: data ingestion, validation, transformation, and metric generation, all orchestrated with **Dagster** and stored in **PostgreSQL**.

---

## Solution Design

### Assumptions

The current setup works under a few key assumptions:

1. Input files are placed in a local folder called `data/`.
2. Files come in pairs one *lineups* file and one *events* file for each match.
3. All files are in **JSON** format.
4. The JSON follows the expected StatsBomb schema.

---

### Data Flow

Here’s how the data moves through the pipeline:

```
Files dropped into `data/`  
    ↓  
Validated for required fields  
    ↓  
Metrics generated using Python (Pandas)  
    ↓  
Loaded into PostgreSQL  
    ↓  
SQL used to generate other metrics
```

This setup ensures the data is validated, transformed, and analyzed in a repeatable way.

---

## How to Run the Project

You can spin this up locally in a few simple steps.

### 1. Create a virtual environment

```bash
python3 -m venv .venv
```

### 2. Activate the environment

| OS | Command |
| --- | --- |
| macOS / Linux | `source .venv/bin/activate` |
| Windows | `.venv\Scripts\activate` |

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Start the PostgreSQL container

```bash
docker compose -f docker-compose.yml up -d
```

### 5. Run Dagster

```bash
dg dev
```

Then open your browser at [http://localhost:3000](http://localhost:3000) to see everything running.

### 6. Shut things down

When you’re done, stop Dagster with `CTRL + C`,  
then bring down the database container:

```bash
docker compose -f docker-compose.yml down
```

---

## Tools Used

Here’s what powers the project:

- **Python** (Pandas, Dagster) → for data processing, validation, and orchestration  
- **SQL / PostgreSQL** → for data storage and analytics  
- **Docker / Docker Compose** → for local containerized setup  
- **Dagster** → for workflow management and observability

---

## Project Structure

```bash
.
├── data/                       # Input files (JSON)
│   ├── events_4028837.json
│   └── lineups_4028837.json
├── docker-compose.yml
├── Makefile
├── output/                     # CSV outputs for various metrics
│   ├── 2a_player_time_on_pitch.csv
│   ├── 2b_match_duration.csv
│   ├── 2c_total_passes_per_player.csv
│   ├── 2d_get_goal_minutes.csv
│   ├── 2e_first_foul_in_second_half.csv
│   ├── 3f_furthest_shot_from_goal.csv
│   ├── 3g_penalty_box_events.csv
│   └── 3h_longest_shot_gap.csv
├── pyproject.toml
├── requirements.txt
├── src/
│   └── matchday_pipeline/
│       ├── definitions.py
│       ├── defs/
│       │   ├── assets/          # Data assets and processing logic
│       │   │   ├── analysis.py
│       │   │   ├── ingestion.py
│       │   │   ├── load_db.py
│       │   │   ├── sql_queries.py
│       │   │   └── validation.py
│       │   ├── checks/
│       │   │   └── asset_checks.py
│       │   ├── config.py
│       │   ├── jobs/
│       │   │   └── pipelines.py
│       │   ├── models.py
│       │   ├── resources.py
│       │   └── utils.py
└── tests/
    └── __init__.py
```

---

## Possible Improvements

There are a few areas where this could evolve into something closer to a production pipeline.

### **Data Ingestion**

- Currently, files are dropped into a folder manually.

  In production, data would likely come through (push based system / pull based system) an **API**, so adding API ingestion (with schema/version validation) would be a natural next step.

### **Transformation**

- Using a tool like **dbt** would make SQL transformations cleaner and more maintainable.
- Adding **data quality tests** (e.g., Great Expectations) would help validate results end-to-end.

### **Storage**

- Moving to a **medallion architecture** (bronze → silver → gold) would improve scalability.
- Cloud storage (like S3 or GCS) could support larger datasets.

### **Serving / Analytics**

- Hooking up **BI tools** like Metabase or Superset would make analysis easier.
- There’s also room to add **ML components** for player or team performance predictions.

### **Security**

- Add **role-based access control (RBAC)** for data access.
- Use environment variables or a secrets manager for credentials.

### **Engineering Practices**

- Add **CI/CD pipelines** for automated testing and deployment.
- Write **unit and integration tests** for each Dagster asset.
- Expand **logging and monitoring** with tools like **Prometheus + Grafana** or **Dagster Cloud alerts** to track job performance, latency, and data freshness in real time.
