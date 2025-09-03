# Log Parsing Logic
import json
import re 
import os


def validate_source(log_source):
        """
        Validates the log source input (This is done to enable backward compatibility)
        """
        if isinstance(log_source, str) and os.path.isfile(log_source) and log_source.lower().endswith('.json'):
            with open(log_source, 'r') as file:
                try:
                    data = json.load(file)
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid JSON file: {e}")

                if isinstance(data, dict):
                    if data.get('raw_logs'):
                        raw_logs = data['raw_logs']
                        if isinstance(raw_logs, list):
                            log_content = "\n".join(str(line) for line in raw_logs)
                        elif isinstance(raw_logs, str):
                            log_content = raw_logs.replace('\r\n', '\n')
                        else:
                            raise ValueError("Invalid 'raw_logs' format")
                    else:
                        raise ValueError("No 'raw_logs' key found in JSON file")
                else:
                    raise ValueError("Invalid JSON structure")
        log_content = log_source

        return log_content


def parse_log(log_source):
    """
    Parses the log content and extracts relevant information.
    
    Args:
        log_content (str): The content of the log file.
        
    Returns:
        list: A list of dictionaries containing parsed log entries.
    """
    log_entries = []
    error_entries = []

    log_content = validate_source(log_source)

    # Regular expression to match log entries
    log_pattern = re.compile(
        r'^(?P<ipaddress>\d{1,3}(?:\.\d{1,3}){3}) \- - \[(?P<timestamp>[^\]]+)\] "(?P<method>[A-Z]+) (?P<path>[^"]+) (?P<protocol>[^"]+)" (?P<status_code>\d{3}) (?P<bytes_sent>\d+) "(?P<referrer>[^"]+)" "(?P<user_agent>[^"]+)"$',
        re.MULTILINE
    )
    
    for line in log_content.splitlines():
        match = log_pattern.match(line)
        if match:
            entry = {
                'ipaddress': match.group('ipaddress'),
                'timestamp': match.group('timestamp'),
                'method': match.group('method'),
                'path': match.group('path'),
                'protocol': match.group('protocol'),
                'status_code': match.group('status_code'),
                'bytes_sent': match.group('bytes_sent'),
                'referrer': match.group('referrer'),
                'user_agent': match.group('user_agent')
            }
            log_entries.append(entry)
        else:
            error_entries.append(line)

    return log_entries, error_entries