import sys
import json
import duckdb
from datetime import datetime

DB_NAME = "sup-san-reviews.ddb"

def read_processed_messages(date_from):
    with duckdb.connect(DB_NAME) as conn:
        rows = conn.execute("""
            SELECT timestamp, uuid, message, category, num_lemm, num_char
            FROM proc_messages
            WHERE CAST(timestamp AS DATE) >= CAST(? AS DATE)
        """, (date_from,)).fetchall()

    return [
        {
            "timestamp": str(row[0]),
            "uuid": str(row[1]),
            "message": row[2],
            "category": row[3],
            "num_lemm": row[4],
            "num_char": row[5]
        } for row in rows
    ]

def main():
    if len(sys.argv) < 2:
        print("Usage: python read.py <date_from (YYYY-MM-DD)>")
        sys.exit(1)

    date_from = sys.argv[1]
    try:
        datetime.strptime(date_from, "%Y-%m-%d")
    except ValueError:
        print("Invalid date format. Use YYYY-MM-DD.")
        sys.exit(1)

    messages = read_processed_messages(date_from)

    with open("messages.json", "w", encoding="utf-8") as f:
        json.dump({"num": len(messages), "messages": messages}, f, indent=2)

    print(f"{len(messages)} messages written to messages.json")

if __name__ == "__main__":
    main()

