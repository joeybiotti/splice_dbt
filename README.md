# Splice Data Warehouse
### Modern Analytics with dbt and DuckDB

This project transforms raw, high-variance movie datasets into a structured Star Schema analytical warehouse. The goal was to move beyond flat-file analysis and implement a modular architecture that ensures data integrity and supports complex many-to-many relationships.

## Data Lineage
The following Directed Acyclic Graph (DAG) represents the end-to-end transformation flow.

![Project Lineage Graph](./assets/lineage_graph.png)

The pipeline is organized into three distinct layers:
* **Staging**: Initial schema enforcement and type casting of raw CSV sources.
* **Intermediate**: A clean room layer where data is deduplicated and pipe-delimited strings are parsed into native database arrays.
* **Marts**: The final dimensional model, consisting of core dimensions, a performance-optimized fact table, and a bridge table to resolve many-to-many genre relationships.

## Architectural Decisions

### Dimensional Modeling
I implemented a Star Schema to separate descriptive metadata from quantitative metrics. This ensures that movie attributes (Dimensions) are stored once, while financial performance (Facts) is aggregated without data fan-out.

### Bridge Table Implementation
To handle movies belonging to multiple genres, I utilized a bridge table (bridge_movie_genres). This allows for granular genre-level analysis without duplicating revenue figures in the central fact table—a critical step for maintaining a Single Source of Truth.

### Data Quality and Governance
Reliability is managed through dbt's native testing framework.
* **Primary Key Validation**: Ensuring 1:1 grain in core dimensions.
* **Referential Integrity**: Relationship tests verify that every fact record correctly maps back to its parent dimension.
* **Schema Testing**: Null-checks and type-validation on all critical columns.

## Tech Stack
* **Engine**: DuckDB
* **Transformation**: dbt (Data Build Tool)
* **Environment**: Python 3.12

## Getting Started
1. Activate the environment: `source .venv/bin/activate`
2. Run ingestion: `python scripts/ingest.py`
3. Build and test the warehouse: `cd transform && dbt build`
4. View documentation: `dbt docs generate && dbt docs serve`