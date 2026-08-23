import csv
import os
from datetime import datetime
from typing import List, Any

# Paths whose header has already been verified this run (the check is once per file).
_header_checked: set = set()

def ensure_dir(dir_path: str):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)

def _rotate_if_header_changed(file_path: str, header: List[str]):
    """Move an existing log aside when its columns no longer match.

    The strategy's columns change when the strategy does, and appending new-shaped rows
    under an old header silently corrupts the whole file — every row after the change is
    misaligned, and nothing in it says so. Rotating keeps the old run readable and starts
    a clean file with the new header.
    """
    if file_path in _header_checked or not os.path.exists(file_path):
        return
    try:
        with open(file_path, "r", newline="", encoding="utf-8") as f:
            existing = next(csv.reader(f), None)
        if existing is not None and existing != list(header):
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base, ext = os.path.splitext(file_path)
            os.rename(file_path, f"{base}_{stamp}{ext or '.csv'}")
    except Exception:
        pass

def append_csv_row(file_path: str, header: List[str], row: List[Any]):
    ensure_dir(os.path.dirname(file_path))
    _rotate_if_header_changed(file_path, header)
    _header_checked.add(file_path)
    file_exists = os.path.exists(file_path)

    with open(file_path, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)
        writer.writerow(row)
