import etl

logs_path = 'data-src/logs.jsonl'

logs = etl.load_logs(logs_path)