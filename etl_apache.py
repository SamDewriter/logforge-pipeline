# etl_apache.py — Entry-point CLI tool for ETL
import argparse
import pandas as pd
from parser import transform_logs
from database import connect_db, create_tables, insert_logs, insert_errors
import chardet

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        raw = f.read(10000)  # sample
    return chardet.detect(raw)['encoding']

def extract_logs(file_path):
    """Reads raw log lines into a DataFrame."""
    encoding = detect_encoding(file_path)
    with open(file_path, 'r', encoding=encoding,errors='replace' ) as f:
        lines = f.readlines()
    return pd.DataFrame({'raw_log': [line.strip() for line in lines]})

def run_etl(log_path):
    print("🔄 Starting ETL pipeline...")

    raw_df = extract_logs(log_path)

    # Connect to SQLite and set up schema
    conn = connect_db()
    create_tables(conn)

    # Transform logs
    cleaned_df, malformed_df = transform_logs(raw_df)

    # Load valid logs
    if not cleaned_df.empty:
        insert_logs(conn, cleaned_df)
        print(f"✅ Inserted {len(cleaned_df)} valid logs.")

    # Load errors
    if not malformed_df.empty:
        insert_errors(conn, malformed_df)
        print(f"⚠️ Logged {len(malformed_df)} malformed lines.")

    conn.close()
    print(" ETL process completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Apache Log ETL")
    parser.add_argument('--log', type=str, required=True, help='Path to Apache log file')
    args = parser.parse_args()

    run_etl(args.log)
