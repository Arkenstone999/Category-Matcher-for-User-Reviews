import duckdb
import spacy
from datetime import datetime

DB_NAME = "sup-san-reviews.ddb"

# Define lemma sets
FOOD_LEMMAS = {"sandwich", "bread", "meat", "cheese", "ham", "omelette", "food", "meal"}
SERVICE_LEMMAS = {"waiter", "service", "table"}

def create_tables_if_not_exists():
    """Create tables proc_messages and proc_log if they do not exist."""
    conn = duckdb.connect(DB_NAME)

    # Create proc_messages table if not exists
    create_proc_messages_table = """
    CREATE TABLE IF NOT EXISTS proc_messages (
        timestamp TIMESTAMP NOT NULL,
        uuid UUID NOT NULL,
        message TEXT NOT NULL,
        category TEXT NOT NULL,
        num_lemm INTEGER NOT NULL,
        num_char INTEGER NOT NULL
    );
    """
    conn.execute(create_proc_messages_table)

    # Create proc_log if not exists
    create_proc_log_table = """
    CREATE TABLE IF NOT EXISTS proc_log (
        uuid UUID NOT NULL PRIMARY KEY,
        proc_time TIMESTAMP
    );
    """
    conn.execute(create_proc_log_table)

    conn.close()

def update_proc_log():
    """
    1. Insert into proc_log all UUIDs from raw_messages that are not in proc_log.
       We insert (uuid, NULL) as proc_time.
    """
    conn = duckdb.connect(DB_NAME)

    # Insert missing uuids into proc_log
    insert_missing_uuids = """
    INSERT INTO proc_log (uuid, proc_time)
    SELECT r.uuid, NULL
    FROM raw_messages r
    LEFT JOIN proc_log p ON r.uuid = p.uuid
    WHERE p.uuid IS NULL;
    """
    conn.execute(insert_missing_uuids)
    conn.commit()
    conn.close()

def process_messages():
    """
    2. Read all messages from raw_messages where proc_log has an empty time field.
    3. Process them and insert into proc_messages.
    4. Update proc_time in proc_log for those messages.
    """
    nlp = spacy.load("en_core_web_sm")

    conn = duckdb.connect(DB_NAME)

    # Step 2: Read all unprocessed messages
    select_unprocessed = """
    SELECT r.timestamp, r.uuid, r.message
    FROM raw_messages r
    JOIN proc_log p ON r.uuid = p.uuid
    WHERE p.proc_time IS NULL
    """
    rows = conn.execute(select_unprocessed).fetchall()

    # Step 3: Process them and insert into proc_messages
    for row in rows:
        timestamp, uuid_val, message = row
        category, num_lemm, num_char = analyze_message(message, nlp)
        
        insert_proc = """
        INSERT INTO proc_messages
            (timestamp, uuid, message, category, num_lemm, num_char)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        conn.execute(insert_proc, (timestamp, uuid_val, message, category, num_lemm, num_char))

    # Step 4: Update proc_time in proc_log for these messages
    current_time = datetime.now().isoformat()
    update_proc_log_sql = """
    UPDATE proc_log
    SET proc_time = ?
    WHERE uuid IN (
        SELECT r.uuid
        FROM raw_messages r
        JOIN proc_log p ON r.uuid = p.uuid
        WHERE p.proc_time IS NULL
    )
    """
    conn.execute(update_proc_log_sql, (current_time,))
    conn.commit()
    conn.close()

def analyze_message(text, nlp):
    """
    Analyzes the message text with spaCy to determine:
    1) category (FOOD, SERVICE, GENERAL)
    2) num_lemm (# of lemmas/tokens)
    3) num_char (# of characters in the message)
    """
    doc = nlp(text)
    food_score = 0
    service_score = 0

    # Count lemmas
    for token in doc:
        # We take the lemma in lowercase
        lemma = token.lemma_.lower()
        if lemma in FOOD_LEMMAS:
            food_score += 1
        if lemma in SERVICE_LEMMAS:
            service_score += 1

    # Check for MONEY entities (increment service)
    for ent in doc.ents:
        if ent.label_ == "MONEY":
            service_score += 1

    # Determine category
    if service_score > food_score:
        category = "SERVICE"
    elif food_score >= service_score and food_score > 0:
        category = "FOOD"
    else:
        category = "GENERAL"

    num_lemm = len([token for token in doc])  # total token count
    num_char = len(text)

    return category, num_lemm, num_char

def main():
    # Create necessary tables if not exists
    create_tables_if_not_exists()

    # 1. Insert missing uuids into proc_log
    update_proc_log()

    # 2, 3, 4. Process unprocessed messages, insert into proc_messages, update proc_log
    process_messages()

    print("Processing completed successfully.")

if __name__ == "__main__":
    main()