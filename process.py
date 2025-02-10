import duckdb
import spacy
from datetime import datetime

DB_NAME = "sup-san-reviews.ddb"

FOOD_LEMMAS = {"sandwich", "bread", "meat", "cheese", "ham", "omelette", "food", "meal"}
SERVICE_LEMMAS = {"waiter", "service", "table"}

def create_proc_tables():
    with duckdb.connect(DB_NAME) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS proc_messages (
                timestamp TIMESTAMP NOT NULL,
                uuid UUID NOT NULL PRIMARY KEY,
                message TEXT NOT NULL,
                category TEXT NOT NULL,
                num_lemm INTEGER NOT NULL,
                num_char INTEGER NOT NULL
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS proc_log (
                uuid UUID NOT NULL PRIMARY KEY,
                proc_time TIMESTAMP
            );
        """)

def update_proc_log():
    with duckdb.connect(DB_NAME) as conn:
        conn.execute("""
            INSERT INTO proc_log (uuid, proc_time)
            SELECT r.uuid, NULL
            FROM raw_messages r
            LEFT JOIN proc_log p ON r.uuid = p.uuid
            WHERE p.uuid IS NULL;
        """)

def analyze_message(text, nlp):
    doc = nlp(text)
    food_score = sum(1 for token in doc if token.lemma_.lower() in FOOD_LEMMAS)
    service_score = sum(1 for token in doc if token.lemma_.lower() in SERVICE_LEMMAS)
    service_score += sum(1 for ent in doc.ents if ent.label_ == "MONEY")

    if service_score > food_score:
        category = "SERVICE"
    elif food_score >= service_score and food_score > 0:
        category = "FOOD"
    else:
        category = "GENERAL"

    return category, len(doc), len(text)

def process_messages():
    nlp = spacy.load("en_core_web_sm")

    with duckdb.connect(DB_NAME) as conn:
        rows = conn.execute("""
            SELECT r.timestamp, r.uuid, r.message
            FROM raw_messages r
            JOIN proc_log p ON r.uuid = p.uuid
            WHERE p.proc_time IS NULL
        """).fetchall()

        current_time = datetime.now()

        for row in rows:
            timestamp, uuid_val, message = row
            category, num_lemm, num_char = analyze_message(message, nlp)

            conn.execute("""
                INSERT INTO proc_messages (timestamp, uuid, message, category, num_lemm, num_char)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (timestamp, uuid_val, message, category, num_lemm, num_char))

            conn.execute("""
                UPDATE proc_log
                SET proc_time = ?
                WHERE uuid = ?;
            """, (current_time, uuid_val))

def main():
    create_proc_tables()
    update_proc_log()
    process_messages()
    print("Processing completed successfully.")

if __name__ == "__main__":
    main()