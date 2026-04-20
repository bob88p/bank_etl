-- ============================================================
-- TASK 4: DATA MODELING
-- Create: dim_customers, dim_accounts, fact_transactions
-- ============================================================

-- ============================================================
-- 1. DIM_CUSTOMERS (Dimension Table)
-- ============================================================

CREATE TABLE dim_customers (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(200) NOT NULL,
    Phone VARCHAR(50),
    Email VARCHAR(200),
    Address VARCHAR(500),
    CreatedDate DATE,
    UpdatedDate DATE,
    Status VARCHAR(20) DEFAULT 'Active'
);

-- ============================================================
-- 2. DIM_ACCOUNTS (Dimension Table)
-- ============================================================

CREATE TABLE dim_accounts (
    AccountID INT PRIMARY KEY,
    CustomerID INT NOT NULL,
    AccountNumber VARCHAR(50) UNIQUE,
    AccountType VARCHAR(50),
    AccountStatus VARCHAR(20),
    OpeningBalance DECIMAL(18,2) DEFAULT 0,
    CurrentBalance DECIMAL(18,2) DEFAULT 0,
    Currency VARCHAR(3) DEFAULT 'EGP',
    CreatedDate DATE,
    UpdatedDate DATE,
    FOREIGN KEY (CustomerID) REFERENCES dim_customers(CustomerID)
);

-- ============================================================
-- 3. FACT_TRANSACTIONS (Fact Table)
-- ============================================================

CREATE TABLE fact_transactions (
    TransactionID INT PRIMARY KEY,
    AccountID INT NOT NULL,
    CustomerID INT NOT NULL,
    TransactionDate DATE NOT NULL,
    TransactionType VARCHAR(50),
    Amount DECIMAL(18,2),
    NetChange DECIMAL(18,2),
    DepositAmount DECIMAL(18,2) DEFAULT 0,
    WithdrawalAmount DECIMAL(18,2) DEFAULT 0,
    PaymentAmount DECIMAL(18,2) DEFAULT 0,
    TransferAmount DECIMAL(18,2) DEFAULT 0,
    RunningBalance DECIMAL(18,2),
    Year INT,
    Month INT,
    YearMonth VARCHAR(7),
    Quarter INT,
    DayOfWeek INT,
    Hour INT,
    FOREIGN KEY (AccountID) REFERENCES dim_accounts(AccountID),
    FOREIGN KEY (CustomerID) REFERENCES dim_customers(CustomerID)
);