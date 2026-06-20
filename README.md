## Overview
The Eskom Grid Observability System is an end-to-end, locally running analytical data pipeline. It extracts real-time national loadshedding data from the EskomSePush API, transforms the nested JSON structures into a clean dimensional model, and orchestrates the entire workflow for automated execution.

This project demonstrates modern Data Engineering and Analytics Engineering principles, specifically focusing on handling messy, overlapping time-series data using batch-processing methodologies.

## Architecture Design
This system follows a Modern Data Stack (MDS) architecture running entirely locally:

1. **Extraction (Python / REST API):** - A Python worker queries the EskomSePush API.
   - Raw JSON payloads are ingested and saved to a local Data Lake directory.

2. **Storage & Compute (DuckDB):** - DuckDB acts as the local analytical data warehouse.
   - It directly queries the local JSON files and handles high-speed columnar data processing.

3. **Transformation (dbt - Data Build Tool):** - SQL-based dbt models clean the raw JSON.
   - Complex nested arrays (merged loadshedding time slots) are flattened and normalized into a clean, queryable dimensional schema (Fact and Dimension tables).
   - dbt handles data quality testing to ensure schema integrity.

4. **Orchestration (Dagster):** - Dagster binds the Python extraction scripts and dbt transformations into a unified lineage graph using Software-Defined Assets (SDAs).
   - Provides a UI to monitor pipeline execution and dependencies.

## Project Structure
```text
eskom-grid-observability/
├── data/                   # Local Data Lake (Raw JSON payloads)
├── dbt_project/            # dbt models and configuration
├── orchestrator/           # Dagster asset definitions
├── scripts/                # Python extraction logic
├── .env                    # Environment variables (API Keys - NOT in version control)
├── .gitignore
├── README.md
└── requirements.txt        # Python dependencies