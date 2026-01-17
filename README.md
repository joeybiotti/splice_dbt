# Splice dbt

A minimalist data project.

## The Goal
To transform messy, raw movie data into a structured analytical warehouse using a local-first stack.

## The Stack
* **Python 3.12**: The muscle. Handles raw ingestion.
* **DuckDB**: The engine. A high-performance analytical database that lives in a single file.
* **dbt (Data Build Tool)**: The logic. Manages transformations, testing, and documentation.

## Project Structure
* `/data`: Raw CSV source files.
* `/scripts`: Python ingestion scripts (`ingest.py`).
* `/transform`: The dbt project containing models, tests, and analysis.
* `splice_warehouse.duckdb`: The local database (ignored by git).

## Getting Started
1. **Environment**: `source .venv/bin/activate`
2. **Ingest**: `python scripts/ingest.py`
3. **Transform**: `cd transform && dbt run`