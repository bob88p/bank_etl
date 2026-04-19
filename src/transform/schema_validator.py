"""
src/transform/schema_validator.py
==================================
Production-ready schema validation layer.
Ensures strict type enforcement + clean separation of valid/rejected rows.
"""

import pandas as pd
from src.utils.logger import get_logger

logger = get_logger("transform.schema_validator")

# ─────────────────────────────────────────────
# FIXED SCHEMAS (aligned with SQL + ETL rules)
# ─────────────────────────────────────────────
SCHEMAS = {
    "customers": {
        "CustomerID": ("int", False),
        "FirstName":  ("str", False),
        "LastName":   ("str", False),
        "Email":      ("str", True),
        "Phone":      ("str", True),
        "Address":    ("str", True),
        "JoinDate":   ("datetime", True),
    },
    "accounts": {
        "AccountID":   ("int", False),
        "CustomerID":  ("int", False),
        "AccountType": ("str", False),
        "Balance":     ("float", False),
        "CreatedDate": ("datetime", True),
    },
    "transactions": {
        "TransactionID":   ("int", False),
        "AccountID":       ("int", False),
        "TransactionType": ("str", False),
        "Amount":          ("float", False),
        "TransactionDate": ("datetime", False),
    },
    "cards": {
        "CardID":         ("int", False),
        "CustomerID":     ("int", False),
        "CardType":       ("str", False),
        "CardNumber":     ("str", False),
        "IssuedDate":     ("datetime", False),
        "ExpirationDate": ("datetime", False),
    },
    "loans": {
        "LoanID":        ("int", False),
        "CustomerID":    ("int", False),
        "LoanType":      ("str", False),
        "LoanAmount":    ("float", False),
        "InterestRate":  ("float", True),
        "LoanStartDate": ("datetime", False),
        "LoanEndDate":   ("datetime", False),
    },
    "support_calls": {
        "CallID":     ("int", False),
        "CustomerID": ("int", False),
        "CallDate":   ("datetime", True),
        "IssueType":  ("str", True),
        "Resolved":   ("str", False),
    },
}


# ─────────────────────────────────────────────
# MAIN FUNCTION (FIXED LOGIC)
# ─────────────────────────────────────────────
def validate_schema(df: pd.DataFrame, dataset_name: str):
    schema = SCHEMAS.get(dataset_name)

    if schema is None:
        logger.warning(f"[schema] No schema for {dataset_name}")
        return df.copy(), pd.DataFrame()

    df = df.copy()

    # ensure consistent rejection tracking
    rejected_mask = pd.Series(False, index=df.index)
    reasons = pd.Series("", index=df.index)

    # ─────────────────────────────────────────
    # STEP 1: enforce column existence
    # ─────────────────────────────────────────
    missing_cols = [c for c in schema if c not in df.columns]
    if missing_cols:
        raise ValueError(
            f"[{dataset_name}] Missing columns: {missing_cols}"
        )

    # ─────────────────────────────────────────
    # STEP 2: type coercion (FIXED + CLEAN)
    # ─────────────────────────────────────────
    for col, (dtype, nullable) in schema.items():

        if col not in df.columns:
            continue

        original = df[col]

        if dtype == "int":
            coerced = pd.to_numeric(original, errors="coerce").astype("Int64")

        elif dtype == "float":
            coerced = pd.to_numeric(original, errors="coerce")

        elif dtype == "datetime":
            coerced = pd.to_datetime(original, errors="coerce")

        else:
            coerced = original.astype(str)

        df[col] = coerced

        # ── invalid conversion detection ──
        invalid = original.notna() & df[col].isna()

        # ── NULL violation detection ──
        null_violation = df[col].isna() & (~nullable)

        # combine issues
        issue_mask = invalid | null_violation

        if issue_mask.any():
            if invalid.any():
                reasons[invalid] += f"Cannot convert {col} → {dtype}; "
            if null_violation.any():
                reasons[null_violation] += f"Null not allowed in {col}; "

            rejected_mask |= issue_mask

    # ─────────────────────────────────────────
    # STEP 3: split result
    # ─────────────────────────────────────────
    valid_df = df[~rejected_mask].copy()
    rejected_df = df[rejected_mask].copy()

    if not rejected_df.empty:
        rejected_df["rejection_reason"] = reasons[rejected_mask].values

    logger.info(
        f"[schema] {dataset_name}: "
        f"{len(valid_df)} valid | {len(rejected_df)} rejected"
    )

    return valid_df.reset_index(drop=True), rejected_df.reset_index(drop=True)