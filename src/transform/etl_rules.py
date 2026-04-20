"""
src/transform/etl_rules.py
==========================
Task 2 – Data Cleaning
Task 3 – Data Transformation
"""

import re

import pandas as pd
from datetime import datetime
from src.utils.logger import get_logger

logger = get_logger("transform.etl_rules")


# ============================================================
# CONSTANTS
# ============================================================
VALID_TRANSACTION_TYPES = {"Deposit", "Withdrawal", "Transfer", "Payment"}
VALID_ACCOUNT_TYPES     = {"Savings", "Checking", "Business"}
VALID_CARD_TYPES        = {"Credit", "Debit", "Prepaid"}
VALID_LOAN_TYPES        = {"Car", "Home", "Personal"}
VALID_RESOLVED_VALUES   = {"Yes", "No"}


# ============================================================
# HELPER
# ============================================================
def _report(label: str, before: int, after: int, reason: str) -> None:
    removed = before - after
    if removed:
        logger.warning(f"[{label}] Removed {removed} rows — {reason}")
    else:
        logger.info(f"[{label}] No issues found — {reason} ✓")


def _to_int_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")

# ---------------
# Global phone standardization function (used in customers cleaning)

def standardize_phone(phone: str) -> str:
    """Clean and standardize a phone number.
    - Keeps digits and the letter 'x' (for extension)
    - Removes all other characters (spaces, dashes, parentheses, dots, plus)
    - Converts multiple 'x' into a single 'x'
    - Returns None if the result is empty.
    """
    if pd.isna(phone):
        return None
    phone_str = str(phone).lower()
    cleaned = re.sub(r'[^0-9x]', '', phone_str)   # keep digits and x
    cleaned = re.sub(r'x+', 'x', cleaned)         # normalize multiple x
    return cleaned if cleaned else None

# ============================================================
# CLEAN CUSTOMERS
# ============================================================
# ============================================================
# CLEAN CUSTOMERS
# ============================================================
def clean_customers(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return df, pd.DataFrame()

    rejected = []
    original_len = len(df)

    # 1. Convert CustomerID to int
    df["CustomerID"] = _to_int_series(df["CustomerID"])

    # 2. Drop rows with null CustomerID
    before = len(df)
    null_id = df["CustomerID"].isna()
    if null_id.any():
        rejected.append(df[null_id].copy())
        df = df[~null_id]
        _report("customers", before, len(df), "null CustomerID")

    # 3. Remove duplicates on CustomerID
    before = len(df)
    dup = df[df.duplicated("CustomerID", keep="first")].copy()
    if not dup.empty:
        dup["rejection_reason"] = "Duplicate CustomerID"
        rejected.append(dup)
    df = df.drop_duplicates("CustomerID", keep="first")
    _report("customers", before, len(df), "duplicate CustomerID")

    # 4. Convert JoinDate (auto-detect seconds/milliseconds)
    if "JoinDate" in df.columns:
        before = len(df)
        # First convert to numeric (handles string epochs)
        df["JoinDate"] = pd.to_numeric(df["JoinDate"], errors="coerce")
        # Detect unit
        sample = df["JoinDate"].dropna().iloc[0] if not df["JoinDate"].dropna().empty else None
        if sample is not None:
            if sample > 1e12:
                df["JoinDate"] = pd.to_datetime(df["JoinDate"], unit="ms", errors="coerce")
            elif sample > 1e9:
                df["JoinDate"] = pd.to_datetime(df["JoinDate"], unit="s", errors="coerce")
            else:
                df["JoinDate"] = pd.to_datetime(df["JoinDate"], errors="coerce")
        else:
            df["JoinDate"] = pd.NaT

        null_date = df["JoinDate"].isna()
        if null_date.any():
            invalid = df[null_date].copy()
            invalid["rejection_reason"] = "Invalid JoinDate"
            rejected.append(invalid)
            df = df[~null_date]
        _report("customers", before, len(df), "JoinDate conversion")

    # 5. Clean phone numbers (using global standardize_phone)
    if "Phone" in df.columns:
        df["Phone"] = df["Phone"].apply(standardize_phone)

    # 6. Reject rows with missing FirstName or LastName
    before = len(df)
    missing_name = df["FirstName"].isna() | df["LastName"].isna()
    if missing_name.any():
        invalid = df[missing_name].copy()
        invalid["rejection_reason"] = "Missing FirstName or LastName"
        rejected.append(invalid)
        df = df[~missing_name]
    _report("customers", before, len(df), "missing name")

    # 7. (Optional) Reject invalid email format? Not required but can be added.

    rejected_df = pd.concat(rejected, ignore_index=True) if rejected else pd.DataFrame()
    logger.info(f"customers: kept {len(df)} / {original_len} (rejected {len(rejected_df)})")
    return df.reset_index(drop=True), rejected_df

# ============================================================
# CLEAN ACCOUNTS
# ============================================================
def clean_accounts(df: pd.DataFrame, valid_customer_ids: set):

    rejected = []

    df["CustomerID"] = _to_int_series(df["CustomerID"])
    df["AccountID"]  = _to_int_series(df["AccountID"])
    df["Balance"]    = pd.to_numeric(df["Balance"], errors="coerce")

    df = df.dropna(subset=["AccountID", "CustomerID"])

    # duplicates
    before = len(df)
    dup = df[df.duplicated("AccountID", keep="first")].copy()
    if not dup.empty:
        dup["rejection_reason"] = "Duplicate AccountID"
        rejected.append(dup)

    df = df.drop_duplicates("AccountID", keep="first")
    _report("accounts", before, len(df), "duplicates")

    # orphan check
    before = len(df)
    df = df[df["CustomerID"].isin(valid_customer_ids)]
    _report("accounts", before, len(df), "orphans")

    # valid type
    before = len(df)
    df = df[df["AccountType"].isin(VALID_ACCOUNT_TYPES)]
    _report("accounts", before, len(df), "invalid types")

    # balance check
    before = len(df)
    bad = df[df["Balance"].isna() | (df["Balance"] <= 0)].copy()
    if not bad.empty:
        bad["rejection_reason"] = "Invalid balance"
        rejected.append(bad)

    df = df[df["Balance"] > 0]
    _report("accounts", before, len(df), "invalid balance")

    return df.reset_index(drop=True), pd.concat(rejected, ignore_index=True) if rejected else pd.DataFrame()


# ============================================================
# CLEAN TRANSACTIONS
# ============================================================
def clean_transactions(df: pd.DataFrame, valid_account_ids: set):

    rejected = []
    today = pd.Timestamp(datetime.today().date())

    df["AccountID"] = _to_int_series(df["AccountID"])
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")
    df["TransactionDate"] = pd.to_datetime(df["TransactionDate"], errors="coerce")

    df = df.dropna(subset=["TransactionID", "AccountID"])

    # duplicates
    before = len(df)
    dup = df[df.duplicated("TransactionID", keep="first")].copy()
    if not dup.empty:
        dup["rejection_reason"] = "Duplicate TransactionID"
        rejected.append(dup)

    df = df.drop_duplicates("TransactionID", keep="first")
    _report("transactions", before, len(df), "duplicates")

    # amount
    before = len(df)
    bad = df[df["Amount"].isna() | (df["Amount"] <= 0)].copy()
    if not bad.empty:
        bad["rejection_reason"] = "Invalid amount"
        rejected.append(bad)

    df = df[df["Amount"] > 0]
    _report("transactions", before, len(df), "invalid amount")

    # type
    before = len(df)
    df = df[df["TransactionType"].isin(VALID_TRANSACTION_TYPES)]
    _report("transactions", before, len(df), "invalid type")

    # orphan
    before = len(df)
    df = df[df["AccountID"].isin(valid_account_ids)]
    _report("transactions", before, len(df), "orphans")

    # future dates
    before = len(df)
    df = df[df["TransactionDate"] <= today]
    _report("transactions", before, len(df), "future dates")

    return df.reset_index(drop=True), pd.concat(rejected, ignore_index=True) if rejected else pd.DataFrame()


# ============================================================
# CLEAN CARDS
# ============================================================
def clean_cards(df: pd.DataFrame, valid_customer_ids: set):

    rejected = []

    df["CustomerID"] = _to_int_series(df["CustomerID"])
    df["CardID"] = _to_int_series(df["CardID"])

    df = df.dropna(subset=["CardID", "CustomerID"])

    # duplicates
    for col in ["CardID", "CardNumber"]:
        before = len(df)
        dup = df[df.duplicated(col, keep="first")].copy()
        if not dup.empty:
            dup["rejection_reason"] = f"Duplicate {col}"
            rejected.append(dup)
        df = df.drop_duplicates(col, keep="first")
        _report("cards", before, len(df), col)

    df["IssuedDate"] = pd.to_datetime(df["IssuedDate"], errors="coerce")
    df["ExpirationDate"] = pd.to_datetime(df["ExpirationDate"], errors="coerce")

    df = df[df["CardType"].isin(VALID_CARD_TYPES)]
    df = df[df["ExpirationDate"] > df["IssuedDate"]]
    df = df[df["CustomerID"].isin(valid_customer_ids)]

    return df.reset_index(drop=True), pd.concat(rejected, ignore_index=True) if rejected else pd.DataFrame()


# ============================================================
# CLEAN LOANS
# ============================================================
def clean_loans(df: pd.DataFrame, valid_customer_ids: set):

    rejected = []

    df["CustomerID"] = _to_int_series(df["CustomerID"])
    df["LoanAmount"] = pd.to_numeric(df["LoanAmount"], errors="coerce")

    df = df.dropna(subset=["LoanID", "CustomerID"])

    df = df.drop_duplicates("LoanID", keep="first")

    df = df[df["LoanAmount"] > 0]
    df = df[df["LoanType"].isin(VALID_LOAN_TYPES)]

    df["LoanStartDate"] = pd.to_datetime(df["LoanStartDate"], errors="coerce")
    df["LoanEndDate"] = pd.to_datetime(df["LoanEndDate"], errors="coerce")

    df = df[df["LoanEndDate"] > df["LoanStartDate"]]
    df = df[df["CustomerID"].isin(valid_customer_ids)]

    return df.reset_index(drop=True), pd.concat(rejected, ignore_index=True) if rejected else pd.DataFrame()


# ============================================================
# CLEAN SUPPORT CALLS
# ============================================================
def clean_support_calls(df: pd.DataFrame, valid_customer_ids: set):

    rejected = []

    df["CustomerID"] = _to_int_series(df["CustomerID"])

    df = df.dropna(subset=["CallID", "CustomerID"])
    df = df.drop_duplicates("CallID", keep="first")

    df["Resolved"] = df["Resolved"].astype(str).str.strip().str.title()
    df = df[df["Resolved"].isin(VALID_RESOLVED_VALUES)]

    df = df[df["CustomerID"].isin(valid_customer_ids)]

    return df.reset_index(drop=True), pd.concat(rejected, ignore_index=True) if rejected else pd.DataFrame()


# ============================================================
# TRANSFORM
# ============================================================
def transform_transactions(transactions: pd.DataFrame, accounts: pd.DataFrame):

    df = transactions.copy()

    df["AccountID"] = pd.to_numeric(df["AccountID"], errors="coerce")
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")

    df = df.dropna(subset=["AccountID", "Amount"])

    deposits = df[df["TransactionType"] == "Deposit"]
    withdrawals = df[df["TransactionType"] == "Withdrawal"]

    total_deposits = deposits.groupby("AccountID")["Amount"].sum()
    total_withdrawals = withdrawals.groupby("AccountID")["Amount"].sum()

    account_summary = df.groupby("AccountID").agg(
        transaction_count=("TransactionID", "count")
    ).join(total_deposits, how="left").join(total_withdrawals, how="left").fillna(0).reset_index()

    account_summary["net_movement"] = account_summary["Amount_x"] - account_summary["Amount_y"]

    acc = accounts.copy()
    acc["AccountID"] = pd.to_numeric(acc["AccountID"], errors="coerce")
    acc["Balance"] = pd.to_numeric(acc["Balance"], errors="coerce")

    validation = acc.merge(account_summary, on="AccountID", how="left").fillna(0)

    validation["recalc"] = validation["Balance"] + validation["Amount_x"] - validation["Amount_y"]
    validation["diff"] = (validation["recalc"] - validation["Balance"]).abs()

    valid_accounts = validation[validation["diff"] <= 0.01]
    suspicious = validation[validation["diff"] > 0.01]

    tx = df.merge(acc[["AccountID", "CustomerID"]], on="AccountID", how="left")

    deposited = tx[tx["TransactionType"] == "Deposit"].groupby("CustomerID")["Amount"].sum()
    withdrawn = tx[tx["TransactionType"] == "Withdrawal"].groupby("CustomerID")["Amount"].sum()

    customer_metrics = pd.DataFrame({
        "deposited": deposited,
        "withdrawn": withdrawn
    }).fillna(0)

    df["TransactionDate"] = pd.to_datetime(df["TransactionDate"], errors="coerce")
    df = df.dropna(subset=["TransactionDate"])

    monthly = df.groupby([
        "AccountID",
        df["TransactionDate"].dt.to_period("M")
    ])["Amount"].agg(["count", "sum"]).reset_index()

    return {
        "deposits": deposits,
        "withdrawals": withdrawals,
        "account_summary": account_summary,
        "valid_accounts": valid_accounts,
        "suspicious_accounts": suspicious,
        "customer_metrics": customer_metrics,
        "monthly_activity": monthly
    }