"""
src/load/loader.py
==================
Task 5 – Data Loading (Improved Production Version)
"""

import os
import pandas as pd
from src.utils.logger import get_logger

logger = get_logger("load.loader")

TABLE_PKS = {
    "dim_customers": "CustomerID",
    "dim_accounts": "AccountID",
    "dim_cards": "CardID",
    "dim_loans": "LoanID",
    "fact_transactions": "TransactionID",
    "fact_support_calls": "CallID",
}


def load_all(datasets: dict, output_path: str, connection_string: str = None) -> dict:
    os.makedirs(output_path, exist_ok=True)
    summary = {}

    for table_name, df in datasets.items():

        if df is None or df.empty:
            logger.warning(f"[load] Skipping {table_name} (empty)")
            summary[table_name] = 0
            continue

        if connection_string:
            rows = _load_to_sql(df, table_name, connection_string)
        else:
            rows = _load_to_csv(df, table_name, output_path)

        summary[table_name] = rows

    _log_summary(summary)
    return summary


# ─────────────────────────────────────────────
# SQL LOADER 
# ─────────────────────────────────────────────

def _load_to_sql(df: pd.DataFrame, table_name: str, connection_string: str) -> int:
    from sqlalchemy import create_engine

    engine = create_engine(connection_string)
    pk = TABLE_PKS.get(table_name)

    with engine.begin() as conn:

        # ── SAFE APPROACH: remove only existing PKs (batched) ──
        if pk and pk in df.columns:
            pk_values = tuple(df[pk].dropna().unique())

            if len(pk_values) > 0:
                placeholders = ",".join(["?"] * len(pk_values))

                conn.exec_driver_sql(
                    f"DELETE FROM {table_name} WHERE {pk} IN ({','.join(['?' for _ in pk_values])})",
                    pk_values
                )

                logger.info(f"[load] Cleaned {len(pk_values)} existing rows from {table_name}")

        # ── INSERT ──
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi"
        )

    logger.info(f"[load] Loaded {len(df)} rows → {table_name}")
    return len(df)


# ─────────────────────────────────────────────
# CSV FALLBACK
# ─────────────────────────────────────────────

def _load_to_csv(df: pd.DataFrame, table_name: str, output_path: str) -> int:
    path = os.path.join(output_path, f"{table_name}.csv")
    df.to_csv(path, index=False)
    logger.info(f"[load] Saved {len(df)} rows → {path}")
    return len(df)


# ─────────────────────────────────────────────
# REJECTED DATA
# ─────────────────────────────────────────────

def save_rejected(rejected: dict, path: str) -> None:
    os.makedirs(path, exist_ok=True)

    for name, df in rejected.items():
        if df is None or df.empty:
            continue

        file = os.path.join(path, f"rejected_{name}.csv")
        df.to_csv(file, index=False)

        logger.warning(f"[load] Rejected {len(df)} rows → {file}")


# ─────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────

def _log_summary(summary: dict):
    logger.info("=" * 50)
    logger.info("LOAD SUMMARY")
    logger.info("=" * 50)

    total = 0
    for t, r in summary.items():
        logger.info(f"{t:<25} {r:>6} rows")
        total += r

    logger.info("-" * 50)
    logger.info(f"{'TOTAL':<25} {total:>6} rows")
    logger.info("=" * 50)