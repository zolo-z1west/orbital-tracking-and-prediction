from pathlib import Path
import csv
from typing import List, Dict, Any

def ensure_parent_dir(file_path: str) -> None:
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)

def append_rows_to_csv(
    rows: List[Dict[str, Any]],
    columns: List[str],
    file_path: str
) -> None:
    ensure_parent_dir(file_path)
    file_exists = Path(file_path).exists()
    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)
