import json
import logging
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any
from app.pipelines.job_signals import (
    step1_init_job_collection,
    step2_fetch_job_postings,
    step3_classify_ai_jobs,
    step4_score_job_market,
    step4b_score_techstack
)
from app.pipelines.pipeline2_state import Pipeline2State
from app.services.s3_storage import get_s3_service
from app.repositories.company_repository import CompanyRepository

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class JobDataService:
    """
    Service to collect and share raw job data between JobSignalService and TechSignalService.
    
    This service handles:
    1. Collecting raw job postings from job sites
    2. Storing raw job data in S3 and local cache
    3. Providing access to job data for analysis
    4. Managing data freshness and cache invalidation
    """
    
    def __init__(self):
        self.s3_service = get_s3_service()
        self.company_repo = CompanyRepository()
        self._cache: Dict[str, Dict] = {}  # Simple in-memory cache
        self._cache_ttl = timedelta(hours=1)  # Cache data for 1 hour
        
    async def collect_job_data(self, ticker: str, force_refresh: bool = False) -> Dict[str, Any]:
        """
        Collect job data for a company.
        
        Args:
            ticker: Company ticker symbol
            force_refresh: If True, ignore cache and fetch fresh data
            
        Returns:
            Dictionary containing raw job data and pipeline state
        """
        ticker = ticker.upper()
        
        # Check cache first (unless force refresh)
        cache_key = f"job_data_{ticker}"
        if not force_refresh and cache_key in self._cache:
            cached_data = self._cache[cache_key]
            cache_time = cached_data.get("collected_at")
            if cache_time and datetime.fromisoformat(cache_time) > datetime.now(timezone.utc) - self._cache_ttl:
                logger.info(f"📦 Using cached job data for {ticker} (collected at {cache_time})")
                return cached_data
        
        logger.info(f"🔄 Collecting fresh job data for {ticker}")
        
        # Get company from database
        company = self.company_repo.get_by_ticker(ticker)
        if not company:
            raise ValueError(f"Company not found: {ticker}")
        
        company_id = str(company['id'])
        company_name = company['name']
        
        # Create pipeline state
        state = Pipeline2State(
            companies=[{"id": company_id, "name": company_name, "ticker": ticker}],
            output_dir=f"data/signals/jobs/{ticker}"
        )
        
        try:
            # Run job collection pipeline steps
            state = step1_init_job_collection(state)
            state = await step2_fetch_job_postings(state)
            state = step3_classify_ai_jobs(state)
            
            # Store raw job data (without scoring)
            job_data = {
                "company_id": company_id,
                "company_name": company_name,
                "ticker": ticker,
                "job_postings": state.job_postings,
                "collected_at": datetime.now(timezone.utc).isoformat(),
                "total_jobs": len(state.job_postings),
                "ai_jobs": sum(1 for p in state.job_postings if p.get("is_ai_role")),
                "pipeline_state": {
                    "job_postings": state.job_postings,
                    "companies": state.companies,
                    "summary": state.summary
                }
            }
            
            # Store in S3 for persistence using common method
            self.s3_service.store_signal_data(
                signal_type="jobs",
                ticker=ticker,
                data=job_data
            )
            
            # Update cache
            self._cache[cache_key] = job_data
            
            logger.info(f"✅ Collected {len(state.job_postings)} job postings for {ticker}")
            return job_data
            
        except Exception as e:
            logger.error(f"❌ Error collecting job data for {ticker}: {e}")
            raise
    
    def get_job_data(self, ticker: str, max_age_hours: int = 24) -> Optional[Dict]:
        """
        Get job data for a company, fetching fresh data if cache is stale.
        
        Args:
            ticker: Company ticker symbol
            max_age_hours: Maximum age of data in hours before considering it stale
            
        Returns:
            Job data dictionary or None if not available
        """
        ticker = ticker.upper()
        cache_key = f"job_data_{ticker}"
        
        if cache_key in self._cache:
            cached_data = self._cache[cache_key]
            cache_time = cached_data.get("collected_at")
            
            if cache_time:
                cache_datetime = datetime.fromisoformat(cache_time)
                age = datetime.now(timezone.utc) - cache_datetime
                
                if age < timedelta(hours=max_age_hours):
                    logger.info(f"📦 Returning cached job data for {ticker} ({age.total_seconds()/3600:.1f} hours old)")
                    return cached_data
                else:
                    logger.info(f"🔄 Cache stale for {ticker} ({age.total_seconds()/3600:.1f} hours old)")
        
        # Try to load from S3 if not in cache or cache is stale
        try:
            # Look for most recent S3 file
            s3_files = self._find_recent_s3_file(ticker)
            if s3_files:
                latest_file = s3_files[0]
                content = self.s3_service.get_file(latest_file)
                if content:
                    job_data = json.loads(content.decode('utf-8'))
                    self._cache[cache_key] = job_data
                    logger.info(f"📦 Loaded job data from S3 for {ticker}")
                    return job_data
        except Exception as e:
            logger.warning(f"⚠️ Failed to load from S3: {e}")
        
        return None
    
    def _find_recent_s3_file(self, ticker: str) -> List[str]:
        """
        Find the most recent S3 file for a company.
        This is a simplified implementation - in production, you'd use S3 list operations.
        """
        # For now, return empty list - actual implementation would list S3 files
        # Example pattern: f"job_data/raw/{ticker}/"
        return []
    
    def analyze_job_market(self, job_data: Dict) -> Dict[str, Any]:
        """
        Analyze job market from raw job data.
        
        Returns:
            Dictionary with job market scores and metrics
        """
        if not job_data or "pipeline_state" not in job_data:
            raise ValueError("Invalid job data")
        
        # Recreate pipeline state from cached data
        state_data = job_data["pipeline_state"]
        state = Pipeline2State(
            companies=state_data["companies"],
            output_dir=f"data/signals/jobs/{job_data['ticker']}"
        )
        state.job_postings = state_data["job_postings"]
        state.summary = state_data["summary"]
        
        # Run job market scoring
        state = step4_score_job_market(state)
        
        return {
            "job_market_scores": state.job_market_scores,
            "total_jobs": len(state.job_postings),
            "ai_jobs": sum(1 for p in state.job_postings if p.get("is_ai_role")),
            "company_id": job_data["company_id"],
            "ticker": job_data["ticker"]
        }
    
    def analyze_tech_stack(self, job_data: Dict) -> Dict[str, Any]:
        """
        Analyze tech stack from raw job data.
        
        Returns:
            Dictionary with tech stack scores and keywords
        """
        if not job_data or "pipeline_state" not in job_data:
            raise ValueError("Invalid job data")
        
        # Recreate pipeline state from cached data
        state_data = job_data["pipeline_state"]
        state = Pipeline2State(
            companies=state_data["companies"],
            output_dir=f"data/signals/jobs/{job_data['ticker']}"
        )
        state.job_postings = state_data["job_postings"]
        state.summary = state_data["summary"]
        
        # Run tech stack scoring
        state = step4b_score_techstack(state)
        
        company_id = job_data["company_id"]
        
        return {
            "techstack_scores": state.techstack_scores,
            "company_techstacks": state.company_techstacks,
            "total_jobs": len(state.job_postings),
            "company_id": company_id,
            "ticker": job_data["ticker"],
            "techstack_keywords": state.company_techstacks.get(company_id, [])
        }
    
    def clear_cache(self, ticker: Optional[str] = None):
        """
        Clear cache for a specific company or all companies.
        
        Args:
            ticker: If provided, clear only this company's cache. Otherwise clear all.
        """
        if ticker:
            cache_key = f"job_data_{ticker.upper()}"
            if cache_key in self._cache:
                del self._cache[cache_key]
                logger.info(f"🧹 Cleared cache for {ticker}")
        else:
            self._cache.clear()
            logger.info("🧹 Cleared all job data cache")


# Singleton
_service: Optional[JobDataService] = None

def get_job_data_service() -> JobDataService:
    global _service
    if _service is None:
        _service = JobDataService()
    return _service