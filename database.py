import sqlite3

def connect_db(db_path="apache_logs.db"):
    return sqlite3.connect(db_path)

def create_tables(conn):
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ip TEXT,
        timestamp TEXT,
        method TEXT,
        path TEXT,
        protocol TEXT,
        status INTEGER,
        bytes INTEGER,
        referrer TEXT,
        user_agent TEXT,
        signature_hash TEXT UNIQUE
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS errors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        raw_line TEXT,
        error_reason TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.commit()

def insert_logs(conn, logs_df):
    cursor = conn.cursor()
    for _, row in logs_df.iterrows():
        try:
            cursor.execute("""
            INSERT INTO logs (
                ip, timestamp, method, path, protocol, status,
                bytes, referrer, user_agent, signature_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row["ip"],
                row["timestamp"],
                row["method"],
                row["path"],
                row["protocol"],
                row["status"],
                row["bytes_sent"],  # this maps to `bytes` in DB
                row["referrer"],
                row["user_agent"],
                row["signature_hash"]
            ))
        except sqlite3.IntegrityError:
            continue  # Skip duplicates
    conn.commit()

def insert_errors(conn, errors_df):
    cursor = conn.cursor()
    for _, row in errors_df.iterrows():
        cursor.execute("""
            INSERT INTO errors (raw_line, error_reason)
            VALUES (?, ?)
        """, (row["raw_log"], row["error_reason"]))
    conn.commit()
