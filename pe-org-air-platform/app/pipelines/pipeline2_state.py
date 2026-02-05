"""
Pipeline 2 State - Job Scraping and Patent Collection
app/pipelines/pipeline2_state.py
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class Pipeline2State:
    """State container for Pipeline 2 job scraping and patent collection."""

    # Configuration
    request_delay: float = 6.0  # Rate limiting delay (seconds)
    output_dir: str = "data/signals/jobs"
    results_per_company: int = 50  # Max results per company
    use_cloud_storage: bool = True  # Whether to use S3 + Snowflake
    mode: str = "jobs"  # "jobs", "patents", or "both"
    
    # Patents-specific config
    patents_years_back: int = 5
    patents_api_key: Optional[str] = None

    # Company data (can be from Snowflake or manual input)
    companies: List[Dict[str, Any]] = field(default_factory=list)

    # Collected job postings (by company)
    job_postings: List[Dict[str, Any]] = field(default_factory=list)
    
    # Collected patents (by company)
    patents: List[Dict[str, Any]] = field(default_factory=list)
    
    # Company-specific data storage
    company_job_data: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # company_id -> job data
    company_patent_data: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # company_id -> patent data

    # Scores (company_id -> score)
    job_market_scores: Dict[str, float] = field(default_factory=dict)
    patent_scores: Dict[str, float] = field(default_factory=dict)
    techstack_scores: Dict[str, float] = field(default_factory=dict)

    # Techstack data (company_id -> list of unique keywords)
    company_techstacks: Dict[str, List[str]] = field(default_factory=dict)

    # Techstack analyses (company_id -> analysis results)
    techstack_analyses: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # Job market analyses (company_id -> analysis results)
    job_market_analyses: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    # S3 loaded data (from step 2)
    loaded_s3_data: Dict[str, Any] = field(default_factory=lambda: {
        "jobs": {},
        "patents": {}
    })
    
    # Step tracking (similar to runner.py)
    steps_completed: List[str] = field(default_factory=list)
    step_history: List[Dict[str, Any]] = field(default_factory=list)

    # Summary tracking
    summary: Dict[str, Any] = field(default_factory=lambda: {
        "companies_processed": 0,
        "job_postings_collected": 0,
        "ai_jobs_found": 0,
        "patents_collected": 0,
        "ai_patents_found": 0,
        "s3_files_uploaded": 0,
        "s3_files_read": 0,
        "snowflake_records_inserted": 0,
        "snowflake_scores_updated": 0,
        "errors": [],
        "started_at": None,
        "completed_at": None,
    })
    
    # For compatibility with runner.py's step-based architecture
    stats: Dict[str, Any] = field(default_factory=lambda: {
        "downloaded": 0,
        "parsed": 0,
        "duplicates_skipped": 0,
        "unique_filings": 0,
        "total_chunks": 0,
        "items_extracted": 0,
        "errors": 0,
        "error_details": [],
    })

    def add_error(self, step: str, error: str, company_id: Optional[str] = None) -> None:
        """Add an error to the summary."""
        error_entry = {
            "step": step,
            "error": error,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if company_id:
            error_entry["company_id"] = company_id
        
        self.summary["errors"].append(error_entry)
        self.stats["errors"] += 1
        self.stats["error_details"].append(error_entry)

    def mark_step_complete(self, step_name: str) -> None:
        """Mark a step as completed."""
        if step_name not in self.steps_completed:
            self.steps_completed.append(step_name)
            
        self.step_history.append({
            "step": step_name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "stats": self.stats.copy(),
            "summary": self.summary.copy()
        })
        
        # Update step-specific counters
        if step_name == "extract":
            self.summary["companies_processed"] = len(self.companies)
        elif step_name == "read_s3":
            jobs_loaded = len(self.loaded_s3_data.get("jobs", {}))
            patents_loaded = len(self.loaded_s3_data.get("patents", {}))
            self.summary["s3_files_read"] = jobs_loaded + patents_loaded
        elif step_name == "snowflake_write":
            # This will be populated by the runner
            pass

    def is_step_complete(self, step_name: str) -> bool:
        """Check if a step is completed."""
        return step_name in self.steps_completed

    def reset(self) -> None:
        """Reset the state (similar to runner.py)."""
        self.steps_completed.clear()
        self.step_history.clear()
        self.stats = {
            "downloaded": 0,
            "parsed": 0,
            "duplicates_skipped": 0,
            "unique_filings": 0,
            "total_chunks": 0,
            "items_extracted": 0,
            "errors": 0,
            "error_details": [],
        }
        self.summary["errors"].clear()

    def mark_started(self) -> None:
        """Mark pipeline as started."""
        self.summary["started_at"] = datetime.now(timezone.utc).isoformat()

    def mark_completed(self) -> None:
        """Mark pipeline as completed."""
        self.summary["completed_at"] = datetime.now(timezone.utc).isoformat()
        self.summary["companies_processed"] = len(self.companies)
        
        # Count AI-related items
        ai_jobs = 0
        for job_data in self.company_job_data.values():
            for job in job_data.get("jobs", []):
                if job.get("is_ai_role"):
                    ai_jobs += 1
        self.summary["ai_jobs_found"] = ai_jobs
        
        ai_patents = 0
        for patent_data in self.company_patent_data.values():
            for patent in patent_data.get("patents", []):
                if patent.get("is_ai_patent"):
                    ai_patents += 1
        self.summary["ai_patents_found"] = ai_patents
        
        # Count total items
        total_jobs = sum(len(data.get("jobs", [])) for data in self.company_job_data.values())
        total_patents = sum(len(data.get("patents", [])) for data in self.company_patent_data.values())
        self.summary["job_postings_collected"] = total_jobs
        self.summary["patents_collected"] = total_patents
        
    def add_company_job_data(self, company_id: str, job_data: Dict[str, Any]) -> None:
        """Add job data for a specific company."""
        self.company_job_data[company_id] = job_data
        self.job_postings.extend(job_data.get("jobs", []))
        self.summary["job_postings_collected"] += len(job_data.get("jobs", []))
        
    def add_company_patent_data(self, company_id: str, patent_data: Dict[str, Any]) -> None:
        """Add patent data for a specific company."""
        self.company_patent_data[company_id] = patent_data
        self.patents.extend(patent_data.get("patents", []))
        self.summary["patents_collected"] += len(patent_data.get("patents", []))
        
    def get_company_name(self, company_id: str) -> str:
        """Get company name from ID."""
        for company in self.companies:
            if company.get("id") == company_id:
                return company.get("name", company_id)
        return company_id
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert state to dictionary for serialization."""
        return {
            "config": {
                "request_delay": self.request_delay,
                "output_dir": self.output_dir,
                "results_per_company": self.results_per_company,
                "use_cloud_storage": self.use_cloud_storage,
                "mode": self.mode,
                "patents_years_back": self.patents_years_back,
            },
            "companies": self.companies,
            "steps_completed": self.steps_completed,
            "scores": {
                "job_market": self.job_market_scores,
                "patent": self.patent_scores,
                "techstack": self.techstack_scores,
            },
            "summary": self.summary,
            "stats": self.stats,
        }