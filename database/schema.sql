-- ============================================================
-- sql/schema.sql
-- Banking ETL Pipeline – Data Warehouse Schema
-- SQL Server Compatible (Final Version)
-- ============================================================


-- ============================================================
-- DIMENSIONS
-- ============================================================

-- Customers
CREATE TABLE dim_customers (
    CustomerID   INT IDENTITY(1,1) NOT NULL,
    FirstName    NVARCHAR(100) NOT NULL,
    LastName     NVARCHAR(100) NOT NULL,
    Email        NVARCHAR(255) NULL,
    Phone        NVARCHAR(50)  NULL,
    Address      NVARCHAR(500) NULL,
    JoinDate     DATE NULL,

    CONSTRAINT PK_dim_customers PRIMARY KEY (CustomerID)
);


-- Accounts
CREATE TABLE dim_accounts (
    AccountID    INT IDENTITY(1,1) NOT NULL,
    CustomerID   INT NOT NULL,
    AccountType  NVARCHAR(50) NOT NULL,
    Balance      DECIMAL(18,2) NOT NULL,
    CreatedDate  DATE NULL,

    CONSTRAINT PK_dim_accounts PRIMARY KEY (AccountID),
    CONSTRAINT FK_accounts_customers
        FOREIGN KEY (CustomerID)
        REFERENCES dim_customers(CustomerID)
);


-- Cards
CREATE TABLE dim_cards (
    CardID         INT IDENTITY(1,1) NOT NULL,
    CustomerID     INT NOT NULL,
    CardType       NVARCHAR(50) NOT NULL,
    CardNumber     NVARCHAR(20) NOT NULL,
    IssuedDate     DATE NOT NULL,
    ExpirationDate DATE NOT NULL,

    CONSTRAINT PK_dim_cards PRIMARY KEY (CardID),
    CONSTRAINT UQ_card_number UNIQUE (CardNumber),
    CONSTRAINT FK_cards_customers
        FOREIGN KEY (CustomerID)
        REFERENCES dim_customers(CustomerID)
);


-- Loans
CREATE TABLE dim_loans (
    LoanID        INT IDENTITY(1,1) NOT NULL,
    CustomerID    INT NOT NULL,
    LoanType      NVARCHAR(50) NOT NULL,
    LoanAmount    DECIMAL(18,2) NOT NULL,
    InterestRate  DECIMAL(5,2) NULL,
    LoanStartDate DATE NOT NULL,
    LoanEndDate   DATE NOT NULL,

    CONSTRAINT PK_dim_loans PRIMARY KEY (LoanID),
    CONSTRAINT FK_loans_customers
        FOREIGN KEY (CustomerID)
        REFERENCES dim_customers(CustomerID)
);


-- ============================================================
-- FACT TABLES
-- ============================================================

-- Transactions
CREATE TABLE fact_transactions (
    TransactionID   INT IDENTITY(1,1) NOT NULL,
    AccountID       INT NOT NULL,
    TransactionType NVARCHAR(50) NOT NULL,
    Amount          DECIMAL(18,2) NOT NULL,
    TransactionDate DATETIME2 NOT NULL,

    CONSTRAINT PK_fact_transactions PRIMARY KEY (TransactionID),
    CONSTRAINT FK_transactions_accounts
        FOREIGN KEY (AccountID)
        REFERENCES dim_accounts(AccountID),

    CONSTRAINT CHK_amount_positive CHECK (Amount > 0),
    CONSTRAINT CHK_transaction_type CHECK (
        TransactionType IN ('Deposit','Withdrawal','Transfer','Payment')
    )
);


-- Support Calls (Fact Table)
CREATE TABLE fact_support_calls (
    CallID      INT IDENTITY(1,1) NOT NULL,
    CustomerID  INT NOT NULL,
    CallDate    DATETIME2 NULL,
    IssueType   NVARCHAR(100) NULL,
    Resolved    BIT NOT NULL,   -- 1 = Yes, 0 = No

    CONSTRAINT PK_fact_support_calls PRIMARY KEY (CallID),
    CONSTRAINT FK_calls_customers
        FOREIGN KEY (CustomerID)
        REFERENCES dim_customers(CustomerID),

    CONSTRAINT CHK_resolved CHECK (Resolved IN (0,1))
);

