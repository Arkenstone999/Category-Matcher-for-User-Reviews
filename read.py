import sys
import json
import duckdb
from datetime import datetime

DB_NAME = "sup-san-reviews.ddb"

def read_processed_messages(date_from):
    """
    Reads messages from proc_messages where timestamp >= date_from.
    Returns a list of dicts with the records.
    """
    conn = duckdb.connect(DB_NAME)

    # Proper cast for both the column and the input date
    query = """
    SELECT timestamp, uuid, message, category, num_lemm, num_char
    FROM proc_messages
    WHERE CAST(timestamp AS DATE) >= CAST(? AS DATE)
    """
    rows = conn.execute(query, (date_from,)).fetchall()
    conn.close()

    messages = []
    for row in rows:
        timestamp, uuid_val, message, category, num_lemm, num_char = row
        messages.append({
            "timestamp": str(timestamp),
            "uuid": str(uuid_val),
            "message": message,
            "category": category,
            "num_lemm": num_lemm,
            "num_char": num_char
        })
    return messages

def main():
    if len(sys.argv) < 2:
        print("Usage: python read.py <date_from (YYYY-MM-DD)>")
        sys.exit(1)

    date_from = sys.argv[1]

    # Validate input date format (YYYY-MM-DD)
    try:
        datetime.strptime(date_from, "%Y-%m-%d")
    except ValueError:
        print("Invalid date format. Please use YYYY-MM-DD (e.g., 2025-01-01).")
        sys.exit(1)

    # Retrieve the records
    processed_msgs = read_processed_messages(date_from)

    # Build output structure
    output_data = {
        "num": len(processed_msgs),
        "messages": processed_msgs
    }

    # Write to JSON file
    with open("messages.json", "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2)

    print(f"{len(processed_msgs)} messages written to messages.json")

if __name__ == "__main__":
    main()
