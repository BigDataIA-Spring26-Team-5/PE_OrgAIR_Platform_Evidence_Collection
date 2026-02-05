import json
import logging
from typing import Dict, List, Optional
from datetime import datetime, timezone
from app.pipelines.job_signals import run_job_signals
from app.pipelines.pipeline2_state import Pipeline2State
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
        self.s3_service = get_s3_service()
        self.company_repo = CompanyRepository()
        self.signal_repo = get_signal_repository()
    
    def analyze_company(self, ticker: str) -> Dict:
        """
        Analyze job postings for a company and create technology_hiring signals.
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
        
        # Create pipeline state for this company
        state = Pipeline2State(
            companies=[{"id": company_id, "name": company_name, "ticker": ticker}],
            output_dir=f"data/signals/jobs/{ticker}"
        )
        
        try:
            # Run job signals pipeline
            logger.info("📊 Running job signals pipeline...")
            state = run_job_signals(state, use_cloud_storage=True)
            
            # Get the job market score from the pipeline state
            job_market_score = state.job_market_scores.get(company_id, 0.0)
            techstack_score = state.techstack_scores.get(company_id, 0.0)
            
            # Calculate overall technology hiring score (weighted average)
            # 70% job market score, 30% techstack score
            overall_score = (job_market_score * 0.7) + (techstack_score * 0.3)
            
            # Get AI job count and total jobs
            ai_jobs = sum(1 for p in state.job_postings if p.get("is_ai_role"))
            total_jobs = len(state.job_postings)
            
            # Get unique techstack keywords
            techstack_keywords = state.company_techstacks.get(company_id, [])
            
            # Create signal record
            self.signal_repo.create_signal(
                company_id=company_id,
                category="technology_hiring",
                source="jobspy",
                signal_date=datetime.now(timezone.utc),
                raw_value=f"Job analysis: {ai_jobs} AI jobs out of {total_jobs} total",
                normalized_score=overall_score,
                confidence=0.8,  # High confidence for job data
                metadata={
                    "job_market_score": job_market_score,
                    "techstack_score": techstack_score,
                    "ai_jobs_count": ai_jobs,
                    "total_jobs_count": total_jobs,
                    "techstack_keywords": techstack_keywords,
                    "job_postings_analyzed": total_jobs
                }
            )
            
            # Update company signal summary
            logger.info("-" * 40)
            logger.info(f"📊 Updating company signal summary...")
            self.signal_repo.upsert_summary(
                company_id=company_id,
                ticker=ticker,
                hiring_score=overall_score
            )
            
            # Summary
            logger.info("=" * 60)
            logger.info(f"📊 TECHNOLOGY HIRING ANALYSIS COMPLETE FOR: {ticker}")
            logger.info(f"   Total jobs analyzed: {total_jobs}")
            logger.info(f"   AI jobs found: {ai_jobs}")
            logger.info(f"   Job Market Score: {job_market_score:.1f}/100")
            logger.info(f"   Techstack Score: {techstack_score:.1f}/100")
            logger.info(f"   Overall Score: {overall_score:.1f}/100")
            logger.info(f"   Techstack Keywords: {len(techstack_keywords)} unique")
            logger.info("=" * 60)
            
            return {
                "ticker": ticker,
                "company_id": company_id,
                "company_name": company_name,
                "normalized_score": round(overall_score, 2),
                "confidence": 0.8,
                "breakdown": {
                    "job_market_score": round(job_market_score, 1),
                    "techstack_score": round(techstack_score, 1),
                    "overall_score": round(overall_score, 1)
                },
                "job_metrics": {
                    "total_jobs": total_jobs,
                    "ai_jobs": ai_jobs,
                    "ai_job_ratio": round(ai_jobs / total_jobs * 100, 1) if total_jobs > 0 else 0
                },
                "techstack_keywords": techstack_keywords,
                "job_postings_analyzed": total_jobs
            }
            
        except Exception as e:
            logger.error(f"❌ Error analyzing job signals for {ticker}: {e}")
            raise
    
    def analyze_all_companies(self) -> Dict:
        """Analyze technology hiring signals for all target companies."""
        target_tickers = ["CAT", "DE", "UNH", "HCA", "ADP", "PAYX", "WMT", "TGT", "JPM", "GS"]
        
        logger.info("=" * 60)
        logger.info("🎯 ANALYZING TECHNOLOGY HIRING SIGNALS FOR ALL COMPANIES")
        logger.info("=" * 60)
        
        results = []
        success_count = 0
        failed_count = 0
        
        for ticker in target_tickers:
            try:
                result = self.analyze_company(ticker)
                results.append({
                    "ticker": ticker,
                    "status": "success",
                    "score": result["normalized_score"],
                    "jobs_analyzed": result["job_postings_analyzed"]
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
            "results": results
        }


# Singleton
_service: Optional[JobSignalService] = None

def get_job_signal_service() -> JobSignalService:
    global _service
    if _service is None:
        _service = JobSignalService()
    return _service