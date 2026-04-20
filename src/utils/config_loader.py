"""utils/config_loader.py – Parses job_config.yaml into typed dataclasses."""

import yaml
from dataclasses import dataclass, field
from typing import Dict, List, Optional


# ============================================================
# Config Data Classes
# ============================================================

@dataclass
class InputConfig:
    source_name: str
    input_type: str
    input_path: str
    input_suffix: str
    has_header: bool
    input_schema: Dict[str, str] = field(default_factory=dict)


@dataclass
class RejectionConfig:
    rejection_path: str
    rejection_type: str
    max_rejection_rate: float


@dataclass
class OutputConfig:
    target_table: str
    output_type: str
    output_path: str
    save_mode: str
    partition_cols: List[str] = field(default_factory=list)


@dataclass
class DatabaseConfig:
    server:   str
    database: str
    uid:      str
    pwd:      str

    def as_dict(self) -> dict:
        """Return as a plain dict — passed directly to pyodbc loader."""
        return {
            "server":   self.server,
            "database": self.database,
            "uid":      self.uid,
            "pwd":      self.pwd,
        }

    def connection_string(self) -> str:
        """Return a pyodbc connection string."""
        return (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.uid};"
            f"PWD={self.pwd};"
        )


@dataclass
class ETLConfig:
    rules: List[dict] = field(default_factory=list)


@dataclass
class QualityConfig:
    checks: List[str] = field(default_factory=list)


@dataclass
class JobConfig:
    name:        str
    version:     str
    description: str
    input:       InputConfig
    rejection:   RejectionConfig
    output:      OutputConfig
    database:    DatabaseConfig        # ← new
    etl:         ETLConfig
    quality:     QualityConfig


# ============================================================
# Loader
# ============================================================

def load_config(path: str) -> JobConfig:
    with open(path, "r") as f:
        raw = yaml.safe_load(f)

    job          = raw["job"]
    input_cfg    = raw["input"]
    rejection_cfg= raw["rejection"]
    output_cfg   = raw["output"]
    db_cfg       = raw["database"]          # ← new
    etl_cfg      = raw["etl"]
    quality_cfg  = raw["quality"]

    # ── Validation ────────────────────────────────────────────
    rate = rejection_cfg["max_rejection_rate"]
    if not (0 <= rate <= 1):
        raise ValueError("max_rejection_rate must be between 0 and 1")

    if not all(k in db_cfg for k in ("server", "database", "uid", "pwd")):
        raise ValueError("database config must include: server, database, uid, pwd")

    # ── Build and return ──────────────────────────────────────
    return JobConfig(
        name=job["name"],
        version=job["version"],
        description=job["description"],

        input=InputConfig(
            source_name=input_cfg["source_name"],
            input_type=input_cfg["input_type"],
            input_path=input_cfg["input_path"],
            input_suffix=input_cfg["input_suffix"],
            has_header=input_cfg["has_header"],
            input_schema=input_cfg.get("input_schema", {}),
        ),

        rejection=RejectionConfig(
            rejection_path=rejection_cfg["rejection_path"],
            rejection_type=rejection_cfg["rejection_type"],
            max_rejection_rate=rate,
        ),

        output=OutputConfig(
            target_table=output_cfg["target_table"],
            output_type=output_cfg["output_type"],
            output_path=output_cfg["output_path"],
            save_mode=output_cfg["save_mode"],
            partition_cols=output_cfg.get("partition_cols", []),
        ),

        database=DatabaseConfig(           # ← new
            server=str(db_cfg["server"]),
            database=str(db_cfg["database"]),
            uid=str(db_cfg["uid"]),
            pwd=str(db_cfg["pwd"]),
        ),

        etl=ETLConfig(
            rules=etl_cfg.get("rules", [])
        ),

        quality=QualityConfig(
            checks=quality_cfg.get("checks", [])
        ),
    )