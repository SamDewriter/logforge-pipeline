import re
import pandas as pd
import hashlib
from datetime import datetime

# regex pattern
log_pattern = re.compile(
    r'(?P<ip>\d+\.\d+\.\d+\.\d+)\s-\s-\s'
    r'\[(?P<timestamp>[^\]]+)\]\s'
    r'"(?P<method>\w+)\s(?P<path>.*?)\s(?P<protocol>HTTP/\d\.\d)"\s'
    r'(?P<status>\d{3})\s(?P<bytes>\d+)\s'
    r'"(?P<referrer>[^"]*)"\s'
    r'"(?P<user_agent>[^"]*)"'
)

# Parse timestamp
def parse_timestamp(raw: str) -> str | None:
    try:
        return datetime.strptime(raw, "%d/%b/%Y:%H:%M:%S %z").isoformat()
    except Exception:
        return None

# Generate unique hash for deduplication
def generate_hash(ip: str, timestamp: str, path: str) -> str:
    return hashlib.md5(f"{ip}_{timestamp}_{path}".encode()).hexdigest()

# Main transform function
def transform_logs(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    structured_logs = []
    malformed_logs = []

    for raw in df["raw_log"]:
        raw = raw.strip()
        match = log_pattern.match(raw)
        if not match:
            malformed_logs.append({"raw_log": raw, "error_reason": "Regex match failed"})
            continue

        data = match.groupdict()
        timestamp = parse_timestamp(data["timestamp"])
        if not timestamp:
            malformed_logs.append({"raw_log": raw, "error_reason": "Invalid timestamp"})
            continue

        uid = generate_hash(data["ip"], timestamp, data["path"])

        structured_logs.append({
            "signature_hash": uid,
            "ip": data["ip"],
            "timestamp": timestamp,
            "method": data["method"],
            "path": data["path"],
            "protocol": data["protocol"],
            "status": int(data["status"]),
            "bytes_sent": int(data["bytes"]),
            "referrer": data["referrer"],
            "user_agent": data["user_agent"]
        })

    return pd.DataFrame(structured_logs), pd.DataFrame(malformed_logs)
