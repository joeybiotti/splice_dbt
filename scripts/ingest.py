import duckdb
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
RAW_DATA = BASE_DIR / "data" / "movies.raw.csv"
DB_PATH = BASE_DIR / "splice_warehouse.duckdb"


def run_ingest():
    """
    Loads raw CSV data into local DuckDB instance
    The read_csv_auto function handles schema detection and dirty data
    """
    print(f"--- Starting Ingest: {RAW_DATA.name} ---")

    if not RAW_DATA.exists():
        print(f"Error: File not found at {RAW_DATA}")
        return

   # Connect to the persistent database file
    with duckdb.connect(str(DB_PATH)) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")

        print("Loading data into raw.movies...")
        conn.execute(f"""
                    CREATE OR REPLACE TABLE raw.movies AS 
                    SELECT * FROM read_csv_auto('{RAW_DATA}')
                    """)

        # Quick validation
        row_count = conn.execute("SELECT count(*) FROM raw.movies").fetchone()[0]
        print(f"Success. Landed {row_count} rows in 'raw.movies'")


if __name__ == "__main__":
    run_ingest()
