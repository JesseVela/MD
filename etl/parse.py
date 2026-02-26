"""
Parse CSV/Excel from upload path. Lambda equivalent: Get object from S3 → Parse.
Input: file path (local = data/upload/<client_id>/<file>).
Output: list of dicts (rows) + column names.
"""
import os
import csv

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def parse_csv(file_path: str) -> tuple[list[dict], list[str]]:
    """Parse CSV; return (rows as dicts, column names)."""
    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        columns = list(reader.fieldnames) if reader.fieldnames else []
    return rows, columns


def get_file_path(client_id: str, file_name: str) -> str:
    """Local S3-style path: data/upload/<client_id>/<file_name>."""
    return os.path.join(ROOT, "data", "upload", client_id, file_name)


def parse_upload(client_id: str, file_name: str) -> tuple[list[dict], list[str]]:
    """Parse file from upload folder for client. Returns (rows, columns)."""
    path = get_file_path(client_id, file_name)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Upload not found: {path}")
    return parse_csv(path)


if __name__ == "__main__":
    # Quick test (canonical client_id = acme)
    rows, cols = parse_upload("acme", "sample_upload.csv")
    print("Columns:", cols)
    print("Rows:", len(rows))
    if rows:
        print("First row:", rows[0])
