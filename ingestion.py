import sys
import os
import duckdb

DB_NAME = "sup-san-reviews.ddb"

def create_db_and_table_if_not_exists():
    """Create the database (if not exists) and the raw_messages table (if not exists)."""
    conn = duckdb.connect(DB_NAME)
    
    create_raw_messages_table = """
    CREATE TABLE IF NOT EXISTS raw_messages (
        timestamp TIMESTAMP NOT NULL,
        uuid UUID NOT NULL,
        message TEXT NOT NULL
    );
    """
    conn.execute(create_raw_messages_table)
    conn.close()

def ingest_csv(file_path):
    """Read CSV file and insert rows into raw_messages table."""
    conn = duckdb.connect(DB_NAME)
    
    # Ingest CSV directly into the database
    conn.execute(f"""
        COPY raw_messages FROM '{file_path}' (DELIMITER ';', HEADER TRUE);
    """)
    
    conn.close()

def main():
    if len(sys.argv) < 2:
        print("Usage: python ingestion.py <csv_file_path>")
        sys.exit(1)

    csv_file_path = sys.argv[1]

    if not os.path.exists(csv_file_path):
        print(f"Error: File '{csv_file_path}' not found.")
        sys.exit(1)

    # Create DB and table if not exist
    create_db_and_table_if_not_exists()

    # Ingest CSV data
    ingest_csv(csv_file_path)

    print("Ingestion completed successfully.")

if __name__ == "__main__":
    main()