"""
Shared utilities for Pipeline 2
app/pipelines/utils.py
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Optional


def clean_nan(value: Any) -> Any:
    """Convert NaN values to None for Pydantic compatibility."""
    if value is None:
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, 'isnull') and value.isnull():
        return None
    if str(value) in ('nan', 'NaN', 'NaT', ''):
        return None
    return value


@dataclass
class Company:
    """Company data container."""
    id: str
    name: str

    @classmethod
    def from_name(cls, name: str, index: int = 0) -> "Company":
        """Create a Company from a name, generating an ID."""
        return cls(id=f"company-{index}", name=name)

    @classmethod
    def from_names(cls, names: list[str]) -> list["Company"]:
        """Create a list of Companies from names."""
        return [cls.from_name(name, i) for i, name in enumerate(names)]

    def to_dict(self) -> dict[str, str]:
        """Convert to dictionary format for backward compatibility."""
        return {"id": self.id, "name": self.name}


def normalize_company_name(name: str) -> str:
    """Normalize company name for comparison."""
    if not name:
        return ""
    name = name.lower().strip()
    # Remove common suffixes
    for suffix in [", inc.", ", inc", " inc.", " inc", ", llc", " llc",
                   ", ltd", " ltd", " corporation", " corp.", " corp",
                   " technologies", " technology", " software", " systems"]:
        if name.endswith(suffix):
            name = name[:-len(suffix)]
    return name.strip()


def company_name_matches(job_company: str, target_company: str) -> bool:
    """
    Check if job's company name matches target company.

    Uses normalized comparison to handle variations like:
    - "Microsoft" vs "Microsoft Corporation"
    - "Google" vs "Google LLC"
    """
    if not job_company or not target_company:
        return False

    job_norm = normalize_company_name(job_company)
    target_norm = normalize_company_name(target_company)

    if not job_norm or not target_norm:
        return False

    # Exact match
    if job_norm == target_norm:
        return True

    # Target is contained in job company name
    if target_norm in job_norm:
        return True

    # Job company is contained in target
    if job_norm in target_norm:
        return True

    return False


def safe_filename(name: str) -> str:
    """Convert a string to a safe filename."""
    return "".join(c if c.isalnum() else "_" for c in name)
