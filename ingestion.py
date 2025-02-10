import sys
import os
import duckdb

DB_NAME = "sup-san-reviews.ddb"

def create_db_and_table_if_not_exists():
    with duckdb.connect(DB_NAME) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_messages (
                timestamp TIMESTAMP NOT NULL,
                uuid UUID NOT NULL PRIMARY KEY,
                message TEXT NOT NULL
            );
        """)

def ingest_csv(file_path):
    """Read CSV file and insert rows into raw_messages table."""
    with duckdb.connect(DB_NAME) as conn:
        conn.execute(f"""
            COPY raw_messages FROM '{file_path}' (DELIMITER ';', HEADER TRUE, QUOTE '"')
            ON CONFLICT(uuid) DO NOTHING;
        """)

def main():
    if len(sys.argv) < 2:
        print("Usage: python ingestion.py <csv_file_path>")
        sys.exit(1)

    csv_file_path = sys.argv[1]
    
    if not os.path.exists(csv_file_path):
        print(f"Error: File '{csv_file_path}' not found.")
        sys.exit(1)

    create_db_and_table_if_not_exists()
    ingest_csv(csv_file_path)
    print("Ingestion completed successfully.")

if __name__ == "__main__":
    main()