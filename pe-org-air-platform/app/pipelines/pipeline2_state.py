"""
Pipeline 2 State - Job Scraping and Patent Collection
app/pipelines/pipeline2_state.py
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List


@dataclass
class Pipeline2State:
    """State container for Pipeline 2 job scraping and patent collection."""

    # Configuration
    request_delay: float = 6.0  # Rate limiting delay (seconds)
    output_dir: str = "data/signals/jobs"

    # Company data (can be from Snowflake or manual input)
    companies: List[Dict[str, Any]] = field(default_factory=list)

    # Collected job postings
    job_postings: List[Dict[str, Any]] = field(default_factory=list)

    # Collected patents
    patents: List[Dict[str, Any]] = field(default_factory=list)

    # Scores (company_id -> score)
    job_market_scores: Dict[str, float] = field(default_factory=dict)
    patent_scores: Dict[str, float] = field(default_factory=dict)
    techstack_scores: Dict[str, float] = field(default_factory=dict)

    # Techstack data (company_id -> list of unique keywords)
    company_techstacks: Dict[str, List[str]] = field(default_factory=dict)

    # Summary tracking
    summary: Dict[str, Any] = field(default_factory=lambda: {
        "companies_processed": 0,
        "job_postings_collected": 0,
        "ai_jobs_found": 0,
        "patents_collected": 0,
        "ai_patents_found": 0,
        "errors": [],
        "started_at": None,
        "completed_at": None,
    })

    def add_error(self, step: str, company_id: str, error: str) -> None:
        """Add an error to the summary."""
        self.summary["errors"].append({
            "step": step,
            "company_id": company_id,
            "error": error,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    def mark_started(self) -> None:
        """Mark pipeline as started."""
        self.summary["started_at"] = datetime.now(timezone.utc).isoformat()

    def mark_completed(self) -> None:
        """Mark pipeline as completed."""
        self.summary["completed_at"] = datetime.now(timezone.utc).isoformat()
        self.summary["companies_processed"] = len(self.companies)
        self.summary["ai_jobs_found"] = sum(
            1 for p in self.job_postings if p.get("is_ai_role")
        )
        self.summary["ai_patents_found"] = sum(
            1 for p in self.patents if p.get("is_ai_patent")
        )