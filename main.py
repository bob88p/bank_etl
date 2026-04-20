"""
main.py
=======
Entry point for the Banking ETL Pipeline.

Usage
-----
  python main.py                          # CSV output (no DB)
  python main.py --db "<conn_string>"     # Load into SQL Server

SQL Server connection string example:
  mssql+pyodbc://banks:123@localhost/Banking?driver=ODBC+Driver+17+for+SQL+Server
"""

import argparse
from src.pipeline import run_pipeline


def main():
    parser = argparse.ArgumentParser(description="Banking ETL Pipeline")
    parser.add_argument(
        "--config",
        default="configs/job_config.yaml",
        help="Path to job_config.yaml (default: configs/job_config.yaml)"
    )
    parser.add_argument(
        "--db",
        default=None,
        help="SQL Server connection string (optional). If omitted, outputs to CSV."
    )
    args = parser.parse_args()

    run_pipeline(
        config_path=args.config,
        connection_string=args.db,
    )


if __name__ == "__main__":
    main()