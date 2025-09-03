import argparse
import logging
import os
from summarizer import summarize_log
from database import (create_connection, create_table, 
                        insert_log_entries, insert_error_entries, is_db_up_to_date,
                        is_db_empty)
from parser import parse_log
from extractor import extract
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# def extract(log_file):
#     """
#     Extracts log data from the specified log file.
    
#     Args:
#         log_file (str): Path to the Apache log file.
        
#     Returns:
#         str: Content of the log file.
#     """
#     try:
#         with open(log_file, 'r') as file:
#             content = file.read()
#         logger.info(f"Extracted {len(content)} characters from {log_file}")
#         return content
#     except Exception as e:
#         logger.error(f"Error extracting log file: {e}")
#         raise

log_source = extract()

def transform(log_source):
    """
    Transforms the log content into structured data.
    
    Args:
        log_content (str): Content of the log file.
        
    Returns:
        list: List of parsed log entries.
    """
    try:
        parsed_entries, error_entries = parse_log(log_source)
        logger.info(f"Transformed log content into {len(parsed_entries)} entries")
        logger.info(f"Found {len(error_entries)} error entries during parsing")
        # Print a sample of error entries for debugging
        if error_entries:
            logger.debug(f"Sample error entries: {error_entries[:5]}")
        return parsed_entries, error_entries
    except Exception as e:
        logger.error(f"Error transforming log content: {e}")
        raise

transform(log_source)

# def load(parsed_entries, error_entries, db_file):
#     """
#     Loads the parsed log entries into the SQLite database.
    
#     Args:
#         parsed_entries (list): List of parsed log entries.
#         db_file (str): Path to the SQLite database file.
#     """
#     conn = create_connection(db_file)
#     if conn is not None:
#         create_table(conn)
#         insert_log_entries(conn, parsed_entries)
#         insert_error_entries(conn, error_entries)
#         conn.commit()
#         conn.close()
#         logger.info(f"Loaded {len(parsed_entries)} entries into the database")
#     else:
#         logger.error("Failed to connect to the database")


# def summarize(db_file, output_file, output_format):
#     """
#     Summarizes the logs and saves the summary to a file.
    
#     Args:
#         db_file (str): Path to the SQLite database file.
#         output_file (str): Path to save the summary output.
#         output_format (str): Format of the output file ('json' or 'csv').
#     """
#     summary = summarize_log(db_file)

#     if summary is None:
#         logger.error("No summary generated, possibly due to database connection issues")
#         return
    
#     if output_format == 'json':
#         import json
#         with open(output_file, 'w') as f:
#             json.dump(summary, f, indent=4)
#     elif output_format == 'csv':
#         import csv
#         with open(output_file, 'w', newline='') as f:
#             writer = csv.writer(f)
#             writer.writerow(['Path', 'Count'])
#             for entry in summary['top_endpoints']:
#                 writer.writerow([entry['path'], entry['count']])
#     logger.info(f"Summary saved to {output_file}")

# def run_etl_if_needed(input_path, db_file, first_run=False):
#     db_exists = os.path.exists(db_file)
#     db_up_to_date = is_db_up_to_date(db_file, input_path) if db_exists else False
#     db_empty = is_db_empty(create_connection(db_file)) if db_exists else True

#     if first_run and db_exists and db_up_to_date and not db_empty:
#         logger.info("First run, but DB already populated and up-to-date. Skipping ETL.")
#         return

#     if not first_run and db_exists and db_up_to_date and not db_empty:
#         logger.info("DB is already populated and up-to-date. Skipping ETL.")
#         return

#     logger.info("Running ETL process...")
#     log_content = extract(input_path)
#     parsed_entries, error_entries = transform(log_source=log_content)
#     load(parsed_entries, error_entries, db_file)
#     logger.info("ETL process completed successfully.")

# def main():
#     parser = argparse.ArgumentParser(description="Apache Log ETL Summary Tool")
#     parser.add_argument('--task', type=str, choices=['summary'], required=True)
#     parser.add_argument('--input', type=str, required=True, help='Path to Apache log file')
#     parser.add_argument('--db', type=str, required=True, help='Path to SQLite database')
#     parser.add_argument('--output', type=str, required=True, help='Path to save the summary output')
#     parser.add_argument('--first-run', action='store_true', help='Force ETL to run even if DB looks up-to-date')
#     parser.add_argument('--output-format', type=str, choices=['json', 'csv'], required=True)

#     args = parser.parse_args()

#     print(f"Task: {args.task}, Input: {args.input}, DB: {args.db}, Output: {args.output}, Format: {args.output_format}, First Run: {args.first_run}")
#     print(f"Is DB empty: {is_db_empty(create_connection(args.db))}")

#     if args.task == 'summary':
#         run_etl_if_needed(args.input, args.db, args.first_run)
#         summarize(args.db, args.output, args.output_format)

# if __name__ == "__main__":
#     main()


# Sample CLI Usage
# python etl_apache.py --task summary --input data/logs/apache_logs --db db/logs.db --output /home/mubarak/logforge-pipeline/data/summary.json --output-format json
# python etl_apache