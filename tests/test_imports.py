# tests/test_imports.py
import sys

modules = [
    "src.compute_positions",
    "src.io_utils",
    "src.data_utils",
    "src.schema",
    "src.features.derive_features"
]

for m in modules:
    try:
        __import__(m)
        print(f"{m} imported successfully")
    except Exception as e:
        print(f"Failed to import {m}: {e}")
        sys.exit(1)
