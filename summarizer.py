# Report Generation
import sqlite3
import pandas as pd

def generate_daily_summary(date_str):
    conn = sqlite3.connect("apache_logs.db")
    query = f"""
    SELECT date(timestamp) as log_date, status, COUNT(*) as hits
    FROM logs
    WHERE date(timestamp) = '{date_str}'
    GROUP BY status
    """
    df = pd.read_sql_query(query, conn)
    df.to_csv(f"summary_{date_str}.csv", index=False)
    print(f"📦 Daily summary saved as summary_{date_str}.csv")
    conn.close()
