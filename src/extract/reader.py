"""
extract/reader.py – Reads CSV or JSON files using pandas.
Robust ingestion layer for ETL pipeline.
"""

import os
import pandas as pd
from typing import List, Dict, Any
from src.utils.logger import get_logger
from src.utils.config_loader import InputConfig

logger = get_logger("extract.reader")


# ============================================================
# MAIN ENTRY
# ============================================================

def read_all(config: InputConfig) -> List[Dict[str, Any]]:
    path = config.input_path
    suffix = config.input_suffix
    input_type = config.input_type.lower()

    if not os.path.isdir(path):
        raise FileNotFoundError(f"Input directory not found: {path!r}")

    files = sorted([f for f in os.listdir(path) if f.endswith(suffix)])

    if not files:
        raise FileNotFoundError(f"No {suffix} files found in {path!r}")

    readers = {
        "csv": _read_csv,
        "json": _read_json,
    }

    if input_type not in readers:
        raise ValueError(f"Unsupported input_type: {input_type!r}")

    read_fn = readers[input_type]

    records = []
    failed_files = []

    for file in files:
        filepath = os.path.join(path, file)

        try:
            df = read_fn(filepath)

            logger.info("Loaded %5d rows <- %s", len(df), file)

            # convert DataFrame → dicts for downstream ETL
            records.extend(df.to_dict(orient="records"))

        except Exception as exc:
            logger.error("Failed reading %s: %s", file, exc)
            failed_files.append(file)
            continue

    logger.info("Extract complete – total rows: %d", len(records))

    if failed_files:
        logger.warning("Skipped corrupted files: %s", failed_files)

    return records


# ============================================================
# CSV READER (pandas)
# ============================================================

def _read_csv(filepath: str) -> pd.DataFrame:
    encodings = ["utf-8", "utf-8-sig", "latin1"]

    last_error = None

    for enc in encodings:
        try:
            return pd.read_csv(filepath, encoding=enc)
        except UnicodeDecodeError as e:
            last_error = e
            continue

    raise ValueError(f"Cannot decode CSV file {filepath}: {last_error}")


# ============================================================
# JSON READER (pandas)
# ============================================================

def _read_json(filepath: str) -> pd.DataFrame:
    try:
        df = pd.read_json(filepath)
    except ValueError as exc:
        raise ValueError(f"Invalid JSON file {filepath}: {exc}") from exc

    # Handle nested JSON structures
    if isinstance(df, pd.DataFrame):
        return df

    raise ValueError(f"Unsupported JSON structure in {filepath}")