"""
ETL run logs. DynamoDB substitute: one log record per run/stage.
Writes to logs/ folder (CSV or JSON) and optionally to db etl_run_logs table.
"""
import os
import json
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGS_DIR = os.path.join(ROOT, "logs")


def ensure_logs_dir():
    os.makedirs(LOGS_DIR, exist_ok=True)


def log_run(run_id: str, stage: str, status: str, row_count: int = None, error_message: str = None):
    """Append one log entry to logs/etl_runs.jsonl and optionally DB."""
    ensure_logs_dir()
    entry = {
        "run_id": run_id,
        "stage": stage,
        "status": status,
        "row_count": row_count,
        "error_message": error_message,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    path = os.path.join(LOGS_DIR, "etl_runs.jsonl")
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")
    return entry
