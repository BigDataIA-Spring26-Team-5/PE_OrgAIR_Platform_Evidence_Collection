"""
Signals Storage Service
app/services/signals_storage.py

Handles local storage and retrieval of collected signals data.
Storage structure:
    data/signals/
        {company_name}/
            summary.json          - Overall summary with scores
            job_postings.json     - All job postings
            patents.json          - All patents
            techstack.json        - Tech stack data
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.pipelines.utils import safe_filename


class SignalsStorage:
    """Service for storing and retrieving signals data from local filesystem."""

    BASE_DIR = Path("data/signals")

    def __init__(self):
        self.BASE_DIR.mkdir(parents=True, exist_ok=True)

    def _get_company_dir(self, company_name: str) -> Path:
        """Get the directory path for a company's signals data."""
        safe_name = safe_filename(company_name)
        return self.BASE_DIR / safe_name

    def save_signals(
        self,
        company_name: str,
        job_postings: List[Dict[str, Any]],
        patents: List[Dict[str, Any]],
        job_market_score: Optional[float],
        patent_score: Optional[float],
        techstack_score: Optional[float],
        techstack_keywords: List[str],
    ) -> str:
        """
        Save all collected signals for a company.

        Returns:
            Path to the company's signals directory
        """
        company_dir = self._get_company_dir(company_name)
        company_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(timezone.utc).isoformat()

        # Calculate counts
        total_jobs = len(job_postings)
        ai_jobs = sum(1 for j in job_postings if j.get("is_ai_role", False))
        total_patents = len(patents)
        ai_patents = sum(1 for p in patents if p.get("is_ai_patent", False))

        # Save summary
        summary = {
            "company_name": company_name,
            "collected_at": timestamp,
            "total_jobs": total_jobs,
            "ai_jobs": ai_jobs,
            "job_market_score": job_market_score,
            "total_patents": total_patents,
            "ai_patents": ai_patents,
            "patent_portfolio_score": patent_score,
            "techstack_score": techstack_score,
            "techstack_keywords": techstack_keywords,
        }
        self._save_json(company_dir / "summary.json", summary)

        # Save job postings
        self._save_json(company_dir / "job_postings.json", {
            "company_name": company_name,
            "collected_at": timestamp,
            "total_count": total_jobs,
            "ai_count": ai_jobs,
            "job_market_score": job_market_score,
            "job_postings": job_postings,
        })

        # Save patents
        self._save_json(company_dir / "patents.json", {
            "company_name": company_name,
            "collected_at": timestamp,
            "total_count": total_patents,
            "ai_count": ai_patents,
            "patent_portfolio_score": patent_score,
            "patents": patents,
        })

        # Save techstack
        self._save_json(company_dir / "techstack.json", {
            "company_name": company_name,
            "collected_at": timestamp,
            "techstack_score": techstack_score,
            "techstack_keywords": techstack_keywords,
        })

        return str(company_dir)

    def get_summary(self, company_name: str) -> Optional[Dict[str, Any]]:
        """Get the summary for a company's signals."""
        company_dir = self._get_company_dir(company_name)
        return self._load_json(company_dir / "summary.json")

    def get_job_postings(
        self,
        company_name: str,
        limit: int = 100,
        offset: int = 0,
    ) -> Optional[Dict[str, Any]]:
        """Get job postings for a company with pagination."""
        company_dir = self._get_company_dir(company_name)
        data = self._load_json(company_dir / "job_postings.json")

        if data is None:
            return None

        # Apply pagination
        all_postings = data.get("job_postings", [])
        paginated = all_postings[offset:offset + limit]

        return {
            "company_name": data.get("company_name"),
            "collected_at": data.get("collected_at"),
            "total_count": data.get("total_count", len(all_postings)),
            "ai_count": data.get("ai_count", 0),
            "job_market_score": data.get("job_market_score"),
            "job_postings": paginated,
        }

    def get_patents(
        self,
        company_name: str,
        limit: int = 100,
        offset: int = 0,
    ) -> Optional[Dict[str, Any]]:
        """Get patents for a company with pagination."""
        company_dir = self._get_company_dir(company_name)
        data = self._load_json(company_dir / "patents.json")

        if data is None:
            return None

        # Apply pagination
        all_patents = data.get("patents", [])
        paginated = all_patents[offset:offset + limit]

        return {
            "company_name": data.get("company_name"),
            "collected_at": data.get("collected_at"),
            "total_count": data.get("total_count", len(all_patents)),
            "ai_count": data.get("ai_count", 0),
            "patent_portfolio_score": data.get("patent_portfolio_score"),
            "patents": paginated,
        }

    def get_techstack(self, company_name: str) -> Optional[Dict[str, Any]]:
        """Get tech stack data for a company."""
        company_dir = self._get_company_dir(company_name)
        return self._load_json(company_dir / "techstack.json")

    def list_companies(self) -> List[Dict[str, Any]]:
        """List all companies with collected signals."""
        companies = []
        if not self.BASE_DIR.exists():
            return companies

        for company_dir in self.BASE_DIR.iterdir():
            if company_dir.is_dir():
                summary = self._load_json(company_dir / "summary.json")
                if summary:
                    companies.append(summary)

        # Sort by collection date (most recent first)
        companies.sort(key=lambda x: x.get("collected_at", ""), reverse=True)
        return companies

    def company_exists(self, company_name: str) -> bool:
        """Check if signals data exists for a company."""
        company_dir = self._get_company_dir(company_name)
        return (company_dir / "summary.json").exists()

    def _save_json(self, path: Path, data: Dict[str, Any]) -> None:
        """Save data to a JSON file."""
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)

    def _load_json(self, path: Path) -> Optional[Dict[str, Any]]:
        """Load data from a JSON file."""
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return None
