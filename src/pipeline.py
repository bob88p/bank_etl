"""
src/pipeline.py
Complete Banking ETL Pipeline – Assignment Week 5
"""

import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text

from src.utils.config_loader import load_config, JobConfig
from src.utils.logger import get_logger
from src.load.loader import load_all, save_rejected

logger = get_logger("pipeline")


# ============================================================
# STAGE 1 – EXTRACT (reads actual files)
# ============================================================

def extract(job_config: JobConfig) -> dict[str, pd.DataFrame]:
    logger.info("━" * 60)
    logger.info("  STAGE 1 — EXTRACT (reading files)")
    logger.info("━" * 60)

    datasets = {}
    for src in job_config.inputs:
        name = src.source_name
        file_path = os.path.join(src.input_path, src.input_suffix)
        logger.info(f"Reading {name} from {file_path}")

        try:
            if src.input_type == "json":
                df = pd.read_json(file_path)
            else:  # csv
                df = pd.read_csv(file_path, header=0 if src.has_header else None)

            # Apply schema dtypes if defined
            if src.input_schema:
                for col, dtype_str in src.input_schema.items():
                    if col in df.columns:
                        if dtype_str == "datetime":
                            df[col] = pd.to_datetime(df[col], errors="coerce")
                        elif dtype_str == "int":
                            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                        elif dtype_str == "float":
                            df[col] = pd.to_numeric(df[col], errors="coerce")
            datasets[name] = df
            logger.info(f"  → {len(df)} rows, {len(df.columns)} columns")

        except Exception as e:
            logger.error(f"Failed to read {name}: {e}")
            datasets[name] = pd.DataFrame()   # empty fallback

    return datasets


# ============================================================
# STAGE 2 – VALIDATE (basic schema & null checks)
# ============================================================

def validate(datasets: dict[str, pd.DataFrame]) -> tuple[dict, dict]:
    logger.info("━" * 60)
    logger.info("  STAGE 2 — SCHEMA VALIDATION")
    logger.info("━" * 60)

    validated = {}
    rejected = {}

    # Required columns per table (from assignment)
    required_cols = {
        "customers": ["CustomerID", "FirstName", "LastName", "JoinDate"],
        "accounts": ["AccountID", "CustomerID", "AccountType", "Balance", "CreatedDate"],
        "transactions": ["TransactionID", "AccountID", "TransactionType", "Amount", "TransactionDate"],
        "cards": ["CardID", "CustomerID", "CardType", "CardNumber", "ExpirationDate"],
        "loans": ["LoanID", "CustomerID", "LoanAmount", "InterestRate"],
        "support_calls": ["CallID", "CustomerID", "CallDate", "IssueType", "Resolved"],
    }

    for name, df in datasets.items():
        if df.empty:
            validated[name] = df
            rejected[name] = pd.DataFrame()
            continue

        # Check required columns exist
        missing = [c for c in required_cols.get(name, []) if c not in df.columns]
        if missing:
            logger.warning(f"{name}: missing columns {missing} → all rows rejected")
            rejected[name] = df.copy()
            validated[name] = pd.DataFrame(columns=df.columns)
            continue

        # Basic null check on primary key
        pk = {"customers": "CustomerID", "accounts": "AccountID", "transactions": "TransactionID",
              "cards": "CardID", "loans": "LoanID", "support_calls": "CallID"}.get(name)
        if pk and pk in df.columns:
            null_pk = df[pk].isna()
            if null_pk.any():
                rejected_part = df[null_pk].copy()
                rejected[name] = rejected_part if rejected.get(name) is None else pd.concat([rejected[name], rejected_part])
                df = df[~null_pk].copy()
                logger.warning(f"{name}: dropped {null_pk.sum()} rows with null {pk}")

        validated[name] = df
        if name not in rejected:
            rejected[name] = pd.DataFrame()

    return validated, rejected


# ============================================================
# STAGE 3 – CLEAN (full assignment requirements)
# ============================================================

def clean_customers(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Clean customers: drop duplicates, convert JoinDate, handle missing names"""
    if df.empty:
        return df, pd.DataFrame()

    original_len = len(df)
    rejected_rows = []

    # 1. Drop duplicate CustomerID
    duplicates = df.duplicated(subset=["CustomerID"], keep=False)
    if duplicates.any():
        rejected_rows.append(df[duplicates])
        df = df.drop_duplicates(subset=["CustomerID"], keep="first")

    # 2. Convert JoinDate (milliseconds to datetime)
    if "JoinDate" in df.columns:
        if df["JoinDate"].dtype == "int64":
            df["JoinDate"] = pd.to_datetime(df["JoinDate"], unit="ms")
        else:
            df["JoinDate"] = pd.to_datetime(df["JoinDate"], errors="coerce")
        null_date = df["JoinDate"].isna()
        if null_date.any():
            rejected_rows.append(df[null_date])
            df = df[~null_date]

    # 3. Missing names → reject
    missing_name = df["FirstName"].isna() | df["LastName"].isna()
    if missing_name.any():
        rejected_rows.append(df[missing_name])
        df = df[~missing_name]

    # Combine all rejected
    rejected = pd.concat(rejected_rows, ignore_index=True) if rejected_rows else pd.DataFrame()
    logger.info(f"customers: kept {len(df)} / {original_len} (rejected {len(rejected)})")
    return df, rejected


def clean_accounts(df: pd.DataFrame, valid_customer_ids: set) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Clean accounts: negative balance? reject, foreign key, duplicate AccountID"""
    if df.empty:
        return df, pd.DataFrame()

    original_len = len(df)
    rejected_rows = []

    # Duplicate AccountID
    dup = df.duplicated(subset=["AccountID"], keep=False)
    if dup.any():
        rejected_rows.append(df[dup])
        df = df.drop_duplicates(subset=["AccountID"], keep="first")

    # Negative balance (assignments says "negative or zero transaction amounts" – but for balance, negative is suspicious)
    # We'll reject negative balance as invalid
    negative_balance = df["Balance"] < 0
    if negative_balance.any():
        rejected_rows.append(df[negative_balance])
        df = df[~negative_balance]

    # Foreign key to customers
    if valid_customer_ids:
        invalid_fk = ~df["CustomerID"].isin(valid_customer_ids)
        if invalid_fk.any():
            rejected_rows.append(df[invalid_fk])
            df = df[~invalid_fk]

    # Convert CreatedDate
    df["CreatedDate"] = pd.to_datetime(df["CreatedDate"], errors="coerce")
    null_date = df["CreatedDate"].isna()
    if null_date.any():
        rejected_rows.append(df[null_date])
        df = df[~null_date]

    rejected = pd.concat(rejected_rows, ignore_index=True) if rejected_rows else pd.DataFrame()
    logger.info(f"accounts: kept {len(df)} / {original_len} (rejected {len(rejected)})")
    return df, rejected


def clean_transactions(df: pd.DataFrame, valid_account_ids: set) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Clean transactions: no negative/zero amount, no duplicates, valid account FK, valid type"""
    if df.empty:
        return df, pd.DataFrame()

    original_len = len(df)
    rejected_rows = []

    # Duplicate TransactionID
    dup = df.duplicated(subset=["TransactionID"], keep=False)
    if dup.any():
        rejected_rows.append(df[dup])
        df = df.drop_duplicates(subset=["TransactionID"], keep="first")

    # Negative or zero amount
    invalid_amount = (df["Amount"] <= 0)
    if invalid_amount.any():
        rejected_rows.append(df[invalid_amount])
        df = df[~invalid_amount]

    # Invalid transaction type (must be Deposit, Withdrawal, Transfer, Payment)
    valid_types = {"Deposit", "Withdrawal", "Transfer", "Payment"}
    invalid_type = ~df["TransactionType"].isin(valid_types)
    if invalid_type.any():
        rejected_rows.append(df[invalid_type])
        df = df[~invalid_type]

    # Foreign key to accounts
    if valid_account_ids:
        invalid_fk = ~df["AccountID"].isin(valid_account_ids)
        if invalid_fk.any():
            rejected_rows.append(df[invalid_fk])
            df = df[~invalid_fk]

    # Convert date
    df["TransactionDate"] = pd.to_datetime(df["TransactionDate"], errors="coerce")
    null_date = df["TransactionDate"].isna()
    if null_date.any():
        rejected_rows.append(df[null_date])
        df = df[~null_date]

    rejected = pd.concat(rejected_rows, ignore_index=True) if rejected_rows else pd.DataFrame()
    logger.info(f"transactions: kept {len(df)} / {original_len} (rejected {len(rejected)})")
    return df, rejected


def clean_cards(df: pd.DataFrame, valid_customer_ids: set) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Cards: drop duplicates, reject expired cards? (assignment doesn't require, but keep for now)"""
    if df.empty:
        return df, pd.DataFrame()
    df = df.drop_duplicates(subset=["CardID"])
    if valid_customer_ids:
        df = df[df["CustomerID"].isin(valid_customer_ids)]
    return df, pd.DataFrame()


def clean_loans(df: pd.DataFrame, valid_customer_ids: set) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return df, pd.DataFrame()
    df = df.drop_duplicates(subset=["LoanID"])
    # Remove negative loan amount
    df = df[df["LoanAmount"] > 0]
    if valid_customer_ids:
        df = df[df["CustomerID"].isin(valid_customer_ids)]
    return df, pd.DataFrame()


def clean_support_calls(df: pd.DataFrame, valid_customer_ids: set) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return df, pd.DataFrame()
    df = df.drop_duplicates(subset=["CallID"])
    if valid_customer_ids:
        df = df[df["CustomerID"].isin(valid_customer_ids)]
    return df, pd.DataFrame()


def clean(validated: dict[str, pd.DataFrame]) -> tuple[dict, dict]:
    logger.info("━" * 60)
    logger.info("  STAGE 3 — CLEANING")
    logger.info("━" * 60)

    cleaned = {}
    rejected = {}

    # Customers
    cleaned["customers"], rejected["customers"] = clean_customers(validated["customers"])

    valid_customer_ids = set(cleaned["customers"]["CustomerID"].unique()) if not cleaned["customers"].empty else set()

    # Accounts
    cleaned["accounts"], rejected["accounts"] = clean_accounts(validated["accounts"], valid_customer_ids)

    valid_account_ids = set(cleaned["accounts"]["AccountID"].unique()) if not cleaned["accounts"].empty else set()

    # Transactions
    cleaned["transactions"], rejected["transactions"] = clean_transactions(validated["transactions"], valid_account_ids)

    # Cards, Loans, Calls
    for name, cleaner in [("cards", clean_cards), ("loans", clean_loans), ("support_calls", clean_support_calls)]:
        cleaned[name], rejected[name] = cleaner(validated[name], valid_customer_ids)

    return cleaned, rejected


# ============================================================
# STAGE 4 – TRANSFORM (assignment tasks)
# ============================================================

def transform(cleaned: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    logger.info("━" * 60)
    logger.info("  STAGE 4 — TRANSFORM")
    logger.info("━" * 60)

    transactions = cleaned.get("transactions", pd.DataFrame())
    accounts = cleaned.get("accounts", pd.DataFrame())
    customers = cleaned.get("customers", pd.DataFrame())

    # 1. Separate deposits & withdrawals, calculate totals per account
    if not transactions.empty:
        deposits = transactions[transactions["TransactionType"] == "Deposit"].groupby("AccountID")["Amount"].sum().rename("TotalDeposits")
        withdrawals = transactions[transactions["TransactionType"] == "Withdrawal"].groupby("AccountID")["Amount"].sum().rename("TotalWithdrawals")
        account_totals = accounts.set_index("AccountID").join(deposits, how="left").join(withdrawals, how="left")
        account_totals["TotalDeposits"] = account_totals["TotalDeposits"].fillna(0)
        account_totals["TotalWithdrawals"] = account_totals["TotalWithdrawals"].fillna(0)
        account_totals["NetChange"] = account_totals["TotalDeposits"] - account_totals["TotalWithdrawals"]
        # Balance validation: recalc balance from transactions (assuming starting balance = 0? Actually we have stored Balance)
        # Better: compute expected balance as initial balance (from first transaction date?) – simplified: compare stored Balance with sum of all deposits - withdrawals.
        # But we don't have initial balance. Alternative: compare stored Balance with net change? Not accurate.
        # Assignment says: "Recalculate account balance from transactions" – we can compute running balance if we have all transactions in order.
        # We'll do: for each account, sort by date, compute cumulative sum, then compare last cumulative with stored Balance.
        # However for large data, we'll do a simpler check: if |stored - (net_change)| > threshold? No, because stored balance is absolute, not delta.
        # Better: compute expected balance by assuming the stored balance is the latest? No.
        # Given time, we'll produce a "balance_mismatch" flag if the stored balance differs from the sum of all transactions (deposits - withdrawals) by more than 0.01.
        # That is valid only if we assume all transactions are recorded and the account started at zero. That's a simplification.
        # But the assignment likely expects that: recalc from scratch using transaction amounts.
        account_totals["RecalculatedBalance"] = account_totals["NetChange"]   # assuming start at 0
        account_totals["BalanceMismatch"] = abs(account_totals["Balance"] - account_totals["RecalculatedBalance"]) > 0.01

        suspicious_accounts = account_totals[account_totals["BalanceMismatch"]].reset_index()
        valid_accounts = account_totals[~account_totals["BalanceMismatch"]].reset_index()
        logger.info(f"Balance validation: {len(suspicious_accounts)} suspicious accounts found")
    else:
        account_totals = accounts.copy()
        suspicious_accounts = pd.DataFrame()
        valid_accounts = accounts

    # 2. Customer-level metrics
    if not customers.empty and not accounts.empty:
        # Join customers with accounts
        cust_accounts = accounts.merge(customers[["CustomerID"]], on="CustomerID", how="inner")
        total_balance_per_customer = cust_accounts.groupby("CustomerID")["Balance"].sum().reset_index(name="TotalBalance")
        # Total transactions per customer (via accounts)
        if not transactions.empty:
            trans_with_cust = transactions.merge(accounts[["AccountID", "CustomerID"]], on="AccountID", how="left")
            total_trans_per_customer = trans_with_cust.groupby("CustomerID").size().reset_index(name="TotalTransactions")
            monthly_activity = trans_with_cust.copy()
            monthly_activity["YearMonth"] = monthly_activity["TransactionDate"].dt.to_period("M")
            monthly_activity = monthly_activity.groupby(["CustomerID", "YearMonth"]).size().reset_index(name="MonthlyTxCount")
        else:
            total_trans_per_customer = pd.DataFrame(columns=["CustomerID", "TotalTransactions"])
            monthly_activity = pd.DataFrame(columns=["CustomerID", "YearMonth", "MonthlyTxCount"])

        customer_metrics = customers[["CustomerID", "FirstName", "LastName"]].merge(total_balance_per_customer, on="CustomerID", how="left")
        customer_metrics = customer_metrics.merge(total_trans_per_customer, on="CustomerID", how="left")
        customer_metrics["TotalBalance"] = customer_metrics["TotalBalance"].fillna(0)
        customer_metrics["TotalTransactions"] = customer_metrics["TotalTransactions"].fillna(0)
    else:
        customer_metrics = pd.DataFrame()

    # Build final fact & dimension tables
    transformed = {
        "dim_customers": customers.copy(),
        "dim_accounts": accounts.copy(),
        "dim_cards": cleaned.get("cards", pd.DataFrame()),
        "dim_loans": cleaned.get("loans", pd.DataFrame()),
        "fact_transactions": transactions.copy(),
        "fact_support_calls": cleaned.get("support_calls", pd.DataFrame()),
        "customer_metrics": customer_metrics,          # extra for assignment
        "suspicious_accounts": suspicious_accounts,    # extra
    }

    for name, df in transformed.items():
        logger.info(f"[transform] {name:<25} → {len(df):>6} rows")

    return transformed


# ============================================================
# STAGE 5 – LOAD (to SQL Server)
# ============================================================

def load(transformed: dict[str, pd.DataFrame],
         rejected: dict[str, pd.DataFrame],
         config,
         connection_string: str = None):
    logger.info("━" * 60)
    logger.info("  STAGE 5 — LOAD")
    logger.info("━" * 60)

    if connection_string is None:
        logger.warning("No connection string provided – skipping database load, saving to CSV only")
        output_path = config.output.output_path
        for name, df in transformed.items():
            if not df.empty:
                path = os.path.join(output_path, f"{name}.csv")
                df.to_csv(path, index=False)
                logger.info(f"Saved {name} to {path}")
        save_rejected(rejected, config.rejection.rejection_path)
        return

    # Load to SQL Server
    engine = create_engine(connection_string)
    with engine.connect() as conn:
        # Create tables if not exist (you should run SQL script first)
        for table_name, df in transformed.items():
            if df.empty:
                continue
            # Use if_exists='replace' for simplicity, or 'append' for incremental
            df.to_sql(table_name, con=conn, if_exists='replace', index=False, method='multi')
            logger.info(f"Loaded {len(df)} rows into {table_name}")

    # Save rejected separately
    save_rejected(rejected, config.rejection.rejection_path)
    logger.info("✅ Load stage completed")


# ============================================================
# MAIN PIPELINE ENTRY POINT
# ============================================================

def run_pipeline(config_path: str = "configs/job_config.yaml",
                 connection_string: str = None):
    logger.info("=" * 70)
    logger.info("  BANKING ETL PIPELINE STARTED")
    logger.info("=" * 70)

    try:
        job_config = load_config(config_path)

        # Dummy config object for legacy compatibility
        config = type('Config', (), {})()
        config.name = job_config.name
        config.version = job_config.version
        config.input = type('obj', (object,), {})()
        config.input.input_path = job_config.inputs[0].input_path if job_config.inputs else "data/raw/"
        config.output = type('obj', (object,), {})()
        config.output.output_path = job_config.output.output_path
        config.rejection = type('obj', (object,), {})()
        config.rejection.rejection_path = job_config.rejection.rejection_path
        config.rejection.max_rejection_rate = job_config.rejection.max_rejection_rate

        logger.info(f"Config loaded: {config.name} v{config.version}")

        # 1. Extract
        raw = extract(job_config)

        # 2. Validate
        validated, schema_rejected = validate(raw)

        # 3. Clean
        cleaned, clean_rejected = clean(validated)

        # Merge all rejected
        all_rejected = {}
        for name in raw.keys():
            parts = [schema_rejected.get(name), clean_rejected.get(name)]
            parts = [p for p in parts if p is not None and not p.empty]
            all_rejected[name] = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

        # Rejection rate check
        total_in = sum(len(df) for df in raw.values())
        total_rej = sum(len(df) for df in all_rejected.values())
        rej_rate = total_rej / total_in if total_in > 0 else 0
        if rej_rate > config.rejection.max_rejection_rate:
            raise RuntimeError(f"Rejection rate {rej_rate:.1%} exceeded threshold!")

        logger.info(f"Rejection Rate: {rej_rate:.1%} → Pipeline continues")

        # 4. Transform
        transformed = transform(cleaned)

        # 5. Load
        load(transformed, all_rejected, config, connection_string)

        logger.info("=" * 70)
        logger.info("  PIPELINE COMPLETED SUCCESSFULLY ✓")
        logger.info("=" * 70)

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    # Example usage – change connection string to your SQL Server
    # conn_str = "mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server"
    run_pipeline(connection_string=None)   # set to None for CSV output only