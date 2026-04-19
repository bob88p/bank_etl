"""
src/utils/config_loader.py
Updated to support your advanced job_config.yaml
"""

import yaml
from dataclasses import dataclass, field
from typing import List, Dict, Any
from pathlib import Path


@dataclass
class InputSource:
    source_name: str
    input_type: str
    input_path: str
    input_suffix: str
    has_header: bool
    input_schema: Dict[str, str] = field(default_factory=dict)


@dataclass
class RejectionConfig:
    rejection_path: str
    rejection_type: str = "csv"
    max_rejection_rate: float = 0.20


@dataclass
class OutputConfig:
    output_path: str = "output"
    output_type: str = "sql"
    save_mode: str = "upsert"
    target_tables: List[str] = field(default_factory=list)
    partition_cols: List[str] = field(default_factory=list)


@dataclass
class JobConfig:
    name: str
    version: str
    description: str = ""
    inputs: List[InputSource] = field(default_factory=list)
    rejection: RejectionConfig = field(default_factory=RejectionConfig)
    output: OutputConfig = field(default_factory=OutputConfig)


def load_config(path: str = "configs/job_config.yaml") -> JobConfig:
    """Load and parse job_config.yaml safely"""
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    # Support both old and new structures
    if "job" in raw:
        job = raw["job"]
    else:
        job = raw

    # Extract inputs (support both 'inputs' list and old 'input')
    inputs_list = []
    if "inputs" in raw and isinstance(raw["inputs"], list):
        for item in raw["inputs"]:
            inputs_list.append(
                InputSource(
                    source_name=item.get("source_name", ""),
                    input_type=item.get("input_type", "csv"),
                    input_path=item.get("input_path", "data/raw"),
                    input_suffix=item.get("input_suffix", ""),
                    has_header=item.get("has_header", True),
                    input_schema=item.get("input_schema", {}),
                )
            )
    elif "input" in raw:
        # Fallback for old structure
        inp = raw["input"]
        inputs_list.append(
            InputSource(
                source_name=inp.get("source_name", "default"),
                input_type=inp.get("input_type", "csv"),
                input_path=inp.get("input_path", "data/raw"),
                input_suffix=inp.get("input_suffix", ""),
                has_header=inp.get("has_header", True),
                input_schema=inp.get("input_schema", {}),
            )
        )

    rejection = raw.get("rejection", {})
    output = raw.get("output", {})

    return JobConfig(
        name=job.get("name", "Banking ETL"),
        version=job.get("version", "1.0"),
        description=job.get("description", ""),
        inputs=inputs_list,
        rejection=RejectionConfig(
            rejection_path=rejection.get("rejection_path", "output/rejected"),
            rejection_type=rejection.get("rejection_type", "csv"),
            max_rejection_rate=rejection.get("max_rejection_rate", 0.20),
        ),
        output=OutputConfig(
            output_path=output.get("output_path", "output"),
            output_type=output.get("output_type", "sql"),
            save_mode=output.get("save_mode", "upsert"),
            target_tables=output.get("target_tables", []),
            partition_cols=output.get("partition_cols", []),
        ),
    )