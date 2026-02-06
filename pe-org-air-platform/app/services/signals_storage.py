"""
Signals Storage Service
app/services/signals_storage.py

Handles local storage and retrieval of collected signals data.
Supports parallel upload to S3 (Option 1: Local + S3 parallel).

Storage structure:
    Local:
        data/signals/
            jobs/<ticker>/
                job_postings.json     - All job postings
                techstack.json        - Tech stack data
                summary.json          - Jobs summary with scores
            patents/<ticker>/
                patents.json          - All patents
                summary.json          - Patents summary with scores

    S3:
        signals/jobs/<ticker>/job_postings.json
        signals/jobs/<ticker>/techstack.json
        signals/jobs/<ticker>/summary.json
        signals/patents/<ticker>/patents.json
        signals/patents/<ticker>/summary.json
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from functools import lru_cache


class S3SignalsStorage:
    """S3 storage service for signals data."""

    def __init__(
        self,
        bucket_name: Optional[str] = None,
        region: Optional[str] = None,
    ):
        self.bucket = bucket_name or os.getenv("S3_BUCKET")
        self.region = region or os.getenv("AWS_REGION", "us-east-2")
        self._client = None
        self._enabled = bool(self.bucket)

    @property
    def client(self):
        """Lazy initialization of S3 client."""
        if self._client is None and self._enabled:
            try:
                self._client = boto3.client("s3", region_name=self.region)
            except (NoCredentialsError, Exception) as e:
                print(f"[S3SignalsStorage] Failed to initialize S3 client: {e}")
                self._enabled = False
        return self._client

    @property
    def is_enabled(self) -> bool:
        """Check if S3 storage is enabled and configured."""
        return self._enabled and self.client is not None

    def upload_json(self, data: Dict[str, Any], s3_key: str) -> Optional[str]:
        """
        Upload JSON data to S3.

        Args:
            data: Dictionary to upload as JSON
            s3_key: S3 object key (path)

        Returns:
            S3 key if successful, None otherwise
        """
        if not self.is_enabled:
            return None

        try:
            json_bytes = json.dumps(data, indent=2, default=str).encode("utf-8")
            self.client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=json_bytes,
                ContentType="application/json",
            )
            return s3_key
        except (ClientError, Exception) as e:
            print(f"[S3SignalsStorage] Failed to upload {s3_key}: {e}")
            return None

    def download_json(self, s3_key: str) -> Optional[Dict[str, Any]]:
        """
        Download JSON data from S3.

        Args:
            s3_key: S3 object key

        Returns:
            Dictionary if successful, None otherwise
        """
        if not self.is_enabled:
            return None

        try:
            response = self.client.get_object(Bucket=self.bucket, Key=s3_key)
            content = response["Body"].read().decode("utf-8")
            return json.loads(content)
        except (ClientError, Exception) as e:
            print(f"[S3SignalsStorage] Failed to download {s3_key}: {e}")
            return None


class SignalsStorage:
    """
    Service for storing and retrieving signals data.

    Supports Option 1: Local + S3 parallel storage.
    - Saves to local filesystem
    - Simultaneously uploads to S3 (if configured)
    """

    BASE_DIR = Path("data/signals")
    JOBS_DIR = BASE_DIR / "jobs"
    PATENTS_DIR = BASE_DIR / "patents"
    S3_PREFIX = "signals"

    def __init__(self, enable_s3: bool = True):
        """
        Initialize signals storage.

        Args:
            enable_s3: Whether to enable S3 parallel upload (default True)
        """
        self.JOBS_DIR.mkdir(parents=True, exist_ok=True)
        self.PATENTS_DIR.mkdir(parents=True, exist_ok=True)
        self._s3 = S3SignalsStorage() if enable_s3 else None

    @property
    def s3_enabled(self) -> bool:
        """Check if S3 storage is enabled."""
        return self._s3 is not None and self._s3.is_enabled

    def _get_jobs_dir(self, ticker: str) -> Path:
        """Get the directory path for a company's job signals data."""
        return self.JOBS_DIR / ticker.upper()

    def _get_patents_dir(self, ticker: str) -> Path:
        """Get the directory path for a company's patent signals data."""
        return self.PATENTS_DIR / ticker.upper()

    def save_job_signals(
        self,
        company_id: str,
        company_name: str,
        ticker: str,
        job_postings: List[Dict[str, Any]],
        job_market_score: Optional[float],
        techstack_score: Optional[float],
        techstack_keywords: List[str],
    ) -> Tuple[str, Optional[str]]:
        """
        Save job-related signals for a company (local + S3 parallel).

        Args:
            company_id: UUID of the company from Snowflake
            company_name: Company name
            ticker: Company ticker symbol (used for directory naming)

        Returns:
            Tuple of (local_path, s3_key or None)
        """
        ticker_upper = ticker.upper()
        jobs_dir = self._get_jobs_dir(ticker)
        jobs_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(timezone.utc).isoformat()

        # Calculate counts
        total_jobs = len(job_postings)
        ai_jobs = sum(1 for j in job_postings if j.get("is_ai_role", False))

        # Prepare data payloads
        job_postings_data = {
            "company_id": company_id,
            "company_name": company_name,
            "ticker": ticker_upper,
            "collected_at": timestamp,
            "total_count": total_jobs,
            "ai_count": ai_jobs,
            "job_market_score": job_market_score,
            "job_postings": job_postings,
        }

        techstack_data = {
            "company_id": company_id,
            "company_name": company_name,
            "ticker": ticker_upper,
            "collected_at": timestamp,
            "techstack_score": techstack_score,
            "techstack_keywords": techstack_keywords,
        }

        summary_data = {
            "company_id": company_id,
            "company_name": company_name,
            "ticker": ticker_upper,
            "collected_at": timestamp,
            "total_jobs": total_jobs,
            "ai_jobs": ai_jobs,
            "job_market_score": job_market_score,
            "techstack_score": techstack_score,
            "techstack_keywords": techstack_keywords,
        }

        # Save locally
        self._save_json(jobs_dir / "job_postings.json", job_postings_data)
        self._save_json(jobs_dir / "techstack.json", techstack_data)
        self._save_json(jobs_dir / "summary.json", summary_data)

        # Upload to S3 in parallel (if enabled)
        s3_key = None
        if self.s3_enabled:
            s3_base = f"{self.S3_PREFIX}/jobs/{ticker_upper}"
            self._s3.upload_json(job_postings_data, f"{s3_base}/job_postings.json")
            self._s3.upload_json(techstack_data, f"{s3_base}/techstack.json")
            s3_key = self._s3.upload_json(summary_data, f"{s3_base}/summary.json")
            if s3_key:
                print(f"[signals] S3 upload complete: {s3_base}/")

        return str(jobs_dir), s3_key

    def save_patent_signals(
        self,
        company_id: str,
        company_name: str,
        ticker: str,
        patents: List[Dict[str, Any]],
        patent_score: Optional[float],
    ) -> Tuple[str, Optional[str]]:
        """
        Save patent signals for a company (local + S3 parallel).

        Args:
            company_id: UUID of the company from Snowflake
            company_name: Company name
            ticker: Company ticker symbol (used for directory naming)

        Returns:
            Tuple of (local_path, s3_key or None)
        """
        ticker_upper = ticker.upper()
        patents_dir = self._get_patents_dir(ticker)
        patents_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(timezone.utc).isoformat()

        # Calculate counts
        total_patents = len(patents)
        ai_patents = sum(1 for p in patents if p.get("is_ai_patent", False))

        # Prepare data payloads
        patents_data = {
            "company_id": company_id,
            "company_name": company_name,
            "ticker": ticker_upper,
            "collected_at": timestamp,
            "total_count": total_patents,
            "ai_count": ai_patents,
            "patent_portfolio_score": patent_score,
            "patents": patents,
        }

        summary_data = {
            "company_id": company_id,
            "company_name": company_name,
            "ticker": ticker_upper,
            "collected_at": timestamp,
            "total_patents": total_patents,
            "ai_patents": ai_patents,
            "patent_portfolio_score": patent_score,
        }

        # Save locally
        self._save_json(patents_dir / "patents.json", patents_data)
        self._save_json(patents_dir / "summary.json", summary_data)

        # Upload to S3 in parallel (if enabled)
        s3_key = None
        if self.s3_enabled:
            s3_base = f"{self.S3_PREFIX}/patents/{ticker_upper}"
            self._s3.upload_json(patents_data, f"{s3_base}/patents.json")
            s3_key = self._s3.upload_json(summary_data, f"{s3_base}/summary.json")
            if s3_key:
                print(f"[signals] S3 upload complete: {s3_base}/")

        return str(patents_dir), s3_key

    def get_jobs_summary(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Get the jobs summary for a company."""
        jobs_dir = self._get_jobs_dir(ticker)
        return self._load_json(jobs_dir / "summary.json")

    def get_patents_summary(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Get the patents summary for a company."""
        patents_dir = self._get_patents_dir(ticker)
        return self._load_json(patents_dir / "summary.json")

    def get_combined_summary(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Get combined summary of all signals for a company."""
        jobs_summary = self.get_jobs_summary(ticker)
        patents_summary = self.get_patents_summary(ticker)

        if jobs_summary is None and patents_summary is None:
            return None

        # Merge summaries
        summary = {
            "company_id": None,
            "company_name": None,
            "ticker": ticker.upper(),
            "total_jobs": 0,
            "ai_jobs": 0,
            "job_market_score": None,
            "techstack_score": None,
            "techstack_keywords": [],
            "total_patents": 0,
            "ai_patents": 0,
            "patent_portfolio_score": None,
            "jobs_collected_at": None,
            "patents_collected_at": None,
        }

        if jobs_summary:
            summary["company_id"] = jobs_summary.get("company_id")
            summary["company_name"] = jobs_summary.get("company_name")
            summary["total_jobs"] = jobs_summary.get("total_jobs", 0)
            summary["ai_jobs"] = jobs_summary.get("ai_jobs", 0)
            summary["job_market_score"] = jobs_summary.get("job_market_score")
            summary["techstack_score"] = jobs_summary.get("techstack_score")
            summary["techstack_keywords"] = jobs_summary.get("techstack_keywords", [])
            summary["jobs_collected_at"] = jobs_summary.get("collected_at")

        if patents_summary:
            summary["company_id"] = summary["company_id"] or patents_summary.get("company_id")
            summary["company_name"] = summary["company_name"] or patents_summary.get("company_name")
            summary["total_patents"] = patents_summary.get("total_patents", 0)
            summary["ai_patents"] = patents_summary.get("ai_patents", 0)
            summary["patent_portfolio_score"] = patents_summary.get("patent_portfolio_score")
            summary["patents_collected_at"] = patents_summary.get("collected_at")

        return summary

    def get_job_postings(
        self,
        ticker: str,
        limit: int = 100,
        offset: int = 0,
    ) -> Optional[Dict[str, Any]]:
        """Get job postings for a company with pagination."""
        jobs_dir = self._get_jobs_dir(ticker)
        data = self._load_json(jobs_dir / "job_postings.json")

        if data is None:
            return None

        # Apply pagination
        all_postings = data.get("job_postings", [])
        paginated = all_postings[offset:offset + limit]

        return {
            "company_id": data.get("company_id"),
            "company_name": data.get("company_name"),
            "ticker": data.get("ticker"),
            "collected_at": data.get("collected_at"),
            "total_count": data.get("total_count", len(all_postings)),
            "ai_count": data.get("ai_count", 0),
            "job_market_score": data.get("job_market_score"),
            "job_postings": paginated,
        }

    def get_patents(
        self,
        ticker: str,
        limit: int = 100,
        offset: int = 0,
    ) -> Optional[Dict[str, Any]]:
        """Get patents for a company with pagination."""
        patents_dir = self._get_patents_dir(ticker)
        data = self._load_json(patents_dir / "patents.json")

        if data is None:
            return None

        # Apply pagination
        all_patents = data.get("patents", [])
        paginated = all_patents[offset:offset + limit]

        return {
            "company_id": data.get("company_id"),
            "company_name": data.get("company_name"),
            "ticker": data.get("ticker"),
            "collected_at": data.get("collected_at"),
            "total_count": data.get("total_count", len(all_patents)),
            "ai_count": data.get("ai_count", 0),
            "patent_portfolio_score": data.get("patent_portfolio_score"),
            "patents": paginated,
        }

    def get_techstack(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Get tech stack data for a company."""
        jobs_dir = self._get_jobs_dir(ticker)
        return self._load_json(jobs_dir / "techstack.json")

    def list_companies_with_jobs(self) -> List[Dict[str, Any]]:
        """List all companies with collected job signals."""
        companies = []
        if not self.JOBS_DIR.exists():
            return companies

        for company_dir in self.JOBS_DIR.iterdir():
            if company_dir.is_dir():
                summary = self._load_json(company_dir / "summary.json")
                if summary:
                    companies.append(summary)

        # Sort by collection date (most recent first)
        companies.sort(key=lambda x: x.get("collected_at", ""), reverse=True)
        return companies

    def list_companies_with_patents(self) -> List[Dict[str, Any]]:
        """List all companies with collected patent signals."""
        companies = []
        if not self.PATENTS_DIR.exists():
            return companies

        for company_dir in self.PATENTS_DIR.iterdir():
            if company_dir.is_dir():
                summary = self._load_json(company_dir / "summary.json")
                if summary:
                    companies.append(summary)

        # Sort by collection date (most recent first)
        companies.sort(key=lambda x: x.get("collected_at", ""), reverse=True)
        return companies

    def list_all_companies(self) -> List[Dict[str, Any]]:
        """List all companies with any collected signals."""
        # Get all unique tickers
        tickers = set()

        if self.JOBS_DIR.exists():
            for company_dir in self.JOBS_DIR.iterdir():
                if company_dir.is_dir():
                    tickers.add(company_dir.name)

        if self.PATENTS_DIR.exists():
            for company_dir in self.PATENTS_DIR.iterdir():
                if company_dir.is_dir():
                    tickers.add(company_dir.name)

        # Get combined summaries
        companies = []
        for ticker in tickers:
            summary = self.get_combined_summary(ticker)
            if summary:
                companies.append(summary)

        # Sort by most recent collection
        def get_latest_timestamp(s):
            timestamps = [s.get("jobs_collected_at", ""), s.get("patents_collected_at", "")]
            return max(t for t in timestamps if t) if any(timestamps) else ""

        companies.sort(key=get_latest_timestamp, reverse=True)
        return companies

    def jobs_exist(self, ticker: str) -> bool:
        """Check if job signals data exists for a company."""
        jobs_dir = self._get_jobs_dir(ticker)
        return (jobs_dir / "summary.json").exists()

    def patents_exist(self, ticker: str) -> bool:
        """Check if patent signals data exists for a company."""
        patents_dir = self._get_patents_dir(ticker)
        return (patents_dir / "summary.json").exists()

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

@lru_cache
def get_signals_storage_service() -> SignalsStorage:
    """
    FastAPI dependency provider for SignalsStorage.

    Uses singleton pattern so filesystem + S3 client
    are not recreated on every request.
    """
    return SignalsStorage(enable_s3=True)