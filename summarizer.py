# summarizer.py
import sqlite3
import pandas as pd
import json

def generate_daily_summary(date_str):
    conn = sqlite3.connect("apache_logs.db")
    query = f"""
    SELECT date(timestamp) AS log_date, status, COUNT(*) AS hits
    FROM logs
    WHERE date(timestamp) = '{date_str}'
    GROUP BY status
    ORDER BY status
    """
    df = pd.read_sql_query(query, conn)
    
    # Save as JSON instead of CSV
    output_filename = f"summary_{date_str}.json"
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(df.to_dict(orient='records'), f, indent=2)

    print(f"📦 Daily summary saved as {output_filename}")
    conn.close()
