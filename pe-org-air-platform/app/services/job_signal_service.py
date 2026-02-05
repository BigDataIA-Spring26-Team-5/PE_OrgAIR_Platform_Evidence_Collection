import json
import logging
from typing import Dict, List, Optional
from datetime import datetime, timezone
from app.services.job_data_service import get_job_data_service
from app.services.s3_storage import get_s3_service
from app.repositories.company_repository import CompanyRepository
from app.repositories.signal_repository import get_signal_repository

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class JobSignalService:
    """Service to extract technology hiring signals from job postings."""
    
    def __init__(self):
        self.job_data_service = get_job_data_service()
        self.s3_service = get_s3_service()
        self.company_repo = CompanyRepository()
        self.signal_repo = get_signal_repository()
    
    async def analyze_company(self, ticker: str, force_refresh: bool = False) -> Dict:
        """
        Analyze job postings for a company and create technology_hiring signals.
        
        Args:
            ticker: Company ticker symbol
            force_refresh: If True, force fresh data collection
            
        Returns:
            Dictionary with analysis results
        """
        ticker = ticker.upper()
        logger.info("=" * 60)
        logger.info(f"🎯 ANALYZING TECHNOLOGY HIRING SIGNALS FOR: {ticker}")
        logger.info("=" * 60)
        
        # Get company from database
        company = self.company_repo.get_by_ticker(ticker)
        if not company:
            raise ValueError(f"Company not found: {ticker}")
        
        company_id = str(company['id'])
        company_name = company['name']
        logger.info(f"✅ Found company: {company_name} (ID: {company_id})")
        
        # Delete existing technology_hiring signals for fresh analysis
        deleted = self.signal_repo.delete_signals_by_category(company_id, "technology_hiring")
        if deleted:
            logger.info(f"  🗑️ Deleted {deleted} existing technology_hiring signals")
        
        try:
            # Get job data (collect fresh if needed)
            logger.info("📊 Getting job data for analysis...")
            job_data = await self.job_data_service.collect_job_data(ticker, force_refresh=force_refresh)
            
            if not job_data or "job_postings" not in job_data:
                raise ValueError(f"No job data available for {ticker}")
            
            # Analyze job market from the shared job data
            logger.info("📈 Analyzing job market...")
            analysis_result = self.job_data_service.analyze_job_market(job_data)
            
            job_market_score = analysis_result["job_market_scores"].get(company_id, 0.0)
            total_jobs = analysis_result["total_jobs"]
            ai_jobs = analysis_result["ai_jobs"]
            
            # Calculate confidence based on data quality
            confidence = self._calculate_confidence(total_jobs, ai_jobs)
            
            # Create signal record (focus only on job market, not tech stack)
            self.signal_repo.create_signal(
                company_id=company_id,
                category="technology_hiring",
                source="jobspy",
                signal_date=datetime.now(timezone.utc),
                raw_value=f"Job market analysis: {ai_jobs} AI jobs out of {total_jobs} total",
                normalized_score=job_market_score,
                confidence=confidence,
                metadata={
                    "job_market_score": job_market_score,
                    "ai_jobs_count": ai_jobs,
                    "total_jobs_count": total_jobs,
                    "job_postings_analyzed": total_jobs,
                    "data_collected_at": job_data.get("collected_at"),
                    "analysis_method": "job_market_scoring"
                }
            )
            
            # Update company signal summary
            logger.info("-" * 40)
            logger.info(f"📊 Updating company signal summary...")
            self.signal_repo.upsert_summary(
                company_id=company_id,
                ticker=ticker,
                hiring_score=job_market_score
            )
            
            # Summary
            logger.info("=" * 60)
            logger.info(f"📊 TECHNOLOGY HIRING ANALYSIS COMPLETE FOR: {ticker}")
            logger.info(f"   Total jobs analyzed: {total_jobs}")
            logger.info(f"   AI jobs found: {ai_jobs}")
            logger.info(f"   Job Market Score: {job_market_score:.1f}/100")
            logger.info(f"   Confidence: {confidence:.2f}")
            logger.info(f"   Data freshness: {job_data.get('collected_at', 'unknown')}")
            logger.info("=" * 60)
            
            return {
                "ticker": ticker,
                "company_id": company_id,
                "company_name": company_name,
                "normalized_score": round(job_market_score, 2),
                "confidence": round(confidence, 3),
                "breakdown": {
                    "job_market_score": round(job_market_score, 1)
                },
                "job_metrics": {
                    "total_jobs": total_jobs,
                    "ai_jobs": ai_jobs,
                    "ai_job_ratio": round(ai_jobs / total_jobs * 100, 1) if total_jobs > 0 else 0
                },
                "data_freshness": job_data.get("collected_at"),
                "job_postings_analyzed": total_jobs
            }
            
        except Exception as e:
            logger.error(f"❌ Error analyzing job signals for {ticker}: {e}")
            raise
    
    def _calculate_confidence(self, total_jobs: int, ai_jobs: int) -> float:
        """
        Calculate confidence score based on data quality.
        
        Factors:
        1. Total jobs analyzed (more data = higher confidence)
        2. Ratio of AI jobs (balanced data = higher confidence)
        3. Absolute number of AI jobs (more AI jobs = higher confidence)
        """
        if total_jobs == 0:
            return 0.3  # Low confidence with no data
        
        # Base confidence from total jobs (0.3 to 0.7)
        total_confidence = min(0.7, 0.3 + (total_jobs / 50.0))
        
        # Ratio confidence (balanced ratio is best)
        ai_ratio = ai_jobs / total_jobs
        ratio_confidence = 1.0 - abs(ai_ratio - 0.5) * 2.0  # 1.0 at 50%, decreases toward extremes
        
        # AI count confidence (more AI jobs = better)
        ai_count_confidence = min(0.5, ai_jobs / 10.0)
        
        # Combined confidence (weighted average)
        confidence = (total_confidence * 0.4) + (ratio_confidence * 0.3) + (ai_count_confidence * 0.3)
        
        return min(0.95, max(0.3, confidence))  # Clamp between 0.3 and 0.95
    
    async def analyze_all_companies(self, force_refresh: bool = False) -> Dict:
        """Analyze technology hiring signals for all target companies."""
        target_tickers = ["CAT", "DE", "UNH", "HCA", "ADP", "PAYX", "WMT", "TGT", "JPM", "GS"]
        
        logger.info("=" * 60)
        logger.info("🎯 ANALYZING TECHNOLOGY HIRING SIGNALS FOR ALL COMPANIES")
        logger.info(f"   Force refresh: {force_refresh}")
        logger.info("=" * 60)
        
        results = []
        success_count = 0
        failed_count = 0
        
        for ticker in target_tickers:
            try:
                result = await self.analyze_company(ticker, force_refresh=force_refresh)
                results.append({
                    "ticker": ticker,
                    "status": "success",
                    "score": result["normalized_score"],
                    "jobs_analyzed": result["job_postings_analyzed"],
                    "ai_jobs": result["job_metrics"]["ai_jobs"]
                })
                success_count += 1
            except Exception as e:
                logger.error(f"❌ Failed to analyze {ticker}: {e}")
                results.append({
                    "ticker": ticker,
                    "status": "failed",
                    "error": str(e)
                })
                failed_count += 1
        
        logger.info("=" * 60)
        logger.info("📊 ALL COMPANIES TECHNOLOGY HIRING ANALYSIS COMPLETE")
        logger.info(f"   Successful: {success_count}")
        logger.info(f"   Failed: {failed_count}")
        logger.info("=" * 60)
        
        return {
            "total_companies": len(target_tickers),
            "successful": success_count,
            "failed": failed_count,
            "force_refresh": force_refresh,
            "results": results
        }


# Singleton
_service: Optional[JobSignalService] = None

def get_job_signal_service() -> JobSignalService:
    global _service
    if _service is None:
        _service = JobSignalService()
    return _service