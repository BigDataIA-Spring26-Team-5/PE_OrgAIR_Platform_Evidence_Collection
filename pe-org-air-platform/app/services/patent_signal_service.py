import json
import logging
from typing import Dict, List, Optional
from datetime import datetime, timezone
from app.pipelines.patent_signals import run_patent_signals
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


class PatentSignalService:
    """Service to extract innovation activity signals from patent analysis."""
    
    def __init__(self):
        self.s3_service = get_s3_service()
        self.company_repo = CompanyRepository()
        self.signal_repo = get_signal_repository()
    
    async def analyze_company(self, ticker: str, years_back: int = 5) -> Dict:
        """
        Analyze patents for a company and create innovation_activity signals.
        """
        ticker = ticker.upper()
        logger.info("=" * 60)
        logger.info(f"🎯 ANALYZING INNOVATION ACTIVITY SIGNALS FOR: {ticker}")
        logger.info("=" * 60)
        
        # Get company from database
        company = self.company_repo.get_by_ticker(ticker)
        if not company:
            raise ValueError(f"Company not found: {ticker}")
        
        company_id = str(company['id'])
        company_name = company['name']
        logger.info(f"✅ Found company: {company_name} (ID: {company_id})")
        
        # Delete existing innovation_activity signals for fresh analysis
        deleted = self.signal_repo.delete_signals_by_category(company_id, "innovation_activity")
        if deleted:
            logger.info(f"  🗑️ Deleted {deleted} existing innovation_activity signals")
        
        # Create pipeline state for this company
        state = Pipeline2State(
            companies=[{"id": company_id, "name": company_name, "ticker": ticker}],
            output_dir=f"data/signals/patents/{ticker}"
        )
        
        try:
            # Run patent signals pipeline
            logger.info("📊 Running patent signals pipeline...")
            state = await run_patent_signals(
                state, 
                years_back=years_back,
                results_per_company=100
            )
            
            # Get the patent portfolio score from the pipeline state
            patent_score = state.patent_scores.get(company_id, 0.0)
            
            # Get AI patent count and total patents
            ai_patents = sum(1 for p in state.patents if p.get("is_ai_related"))
            total_patents = len(state.patents)

            # Get unique AI categories from patents
            all_ai_keywords = set()
            for patent in state.patents:
                if patent.get("is_ai_related"):
                    all_ai_keywords.update(patent.get("ai_categories", []))
            
            # Calculate confidence based on data quality
            confidence = self._calculate_confidence(total_patents, ai_patents)
            
            # Create signal record
            self.signal_repo.create_signal(
                company_id=company_id,
                category="innovation_activity",
                source="patentsview",
                signal_date=datetime.now(timezone.utc),
                raw_value=f"Patent analysis: {ai_patents} AI patents out of {total_patents} total",
                normalized_score=patent_score,
                confidence=confidence,
                metadata={
                    "patent_portfolio_score": patent_score,
                    "ai_patents_count": ai_patents,
                    "total_patents_count": total_patents,
                    "ai_keywords_found": list(all_ai_keywords),
                    "patents_analyzed": total_patents,
                    "years_back": years_back,
                    "analysis_date": datetime.now(timezone.utc).isoformat()
                }
            )
            
            # Update company signal summary
            logger.info("-" * 40)
            logger.info(f"📊 Updating company signal summary...")
            self.signal_repo.upsert_summary(
                company_id=company_id,
                ticker=ticker,
                innovation_score=patent_score
            )
            
            # Summary
            logger.info("=" * 60)
            logger.info(f"📊 INNOVATION ACTIVITY ANALYSIS COMPLETE FOR: {ticker}")
            logger.info(f"   Total patents analyzed: {total_patents}")
            logger.info(f"   AI patents found: {ai_patents}")
            logger.info(f"   Patent Portfolio Score: {patent_score:.1f}/100")
            logger.info(f"   Confidence: {confidence:.2f}")
            logger.info(f"   AI Keywords: {len(all_ai_keywords)} unique")
            logger.info(f"   Analysis period: Last {years_back} years")
            logger.info("=" * 60)
            
            return {
                "ticker": ticker,
                "company_id": company_id,
                "company_name": company_name,
                "normalized_score": round(patent_score, 2),
                "confidence": round(confidence, 3),
                "breakdown": {
                    "patent_portfolio_score": round(patent_score, 1)
                },
                "patent_metrics": {
                    "total_patents": total_patents,
                    "ai_patents": ai_patents,
                    "ai_patent_ratio": round(ai_patents / total_patents * 100, 1) if total_patents > 0 else 0,
                    "analysis_period_years": years_back
                },
                "ai_keywords_found": list(all_ai_keywords),
                "patents_analyzed": total_patents
            }
            
        except Exception as e:
            logger.error(f"❌ Error analyzing patent signals for {ticker}: {e}")
            raise
    
    def _calculate_confidence(self, total_patents: int, ai_patents: int) -> float:
        """
        Calculate confidence score based on data quality.
        
        Factors:
        1. Total patents analyzed (more data = higher confidence)
        2. Ratio of AI patents (balanced data = higher confidence)
        3. Absolute number of AI patents (more AI patents = higher confidence)
        """
        if total_patents == 0:
            return 0.3  # Low confidence with no data
        
        # Base confidence from total patents (0.3 to 0.7)
        total_confidence = min(0.7, 0.3 + (total_patents / 50.0))
        
        # Ratio confidence (balanced ratio is best)
        ai_ratio = ai_patents / total_patents
        ratio_confidence = 1.0 - abs(ai_ratio - 0.5) * 2.0  # 1.0 at 50%, decreases toward extremes
        
        # AI count confidence (more AI patents = better)
        ai_count_confidence = min(0.5, ai_patents / 10.0)
        
        # Combined confidence (weighted average)
        confidence = (total_confidence * 0.4) + (ratio_confidence * 0.3) + (ai_count_confidence * 0.3)
        
        return min(0.95, max(0.3, confidence))  # Clamp between 0.3 and 0.95
    
    async def analyze_all_companies(self, years_back: int = 5) -> Dict:
        """Analyze innovation activity signals for all target companies."""
        target_tickers = ["CAT", "DE", "UNH", "HCA", "ADP", "PAYX", "WMT", "TGT", "JPM", "GS"]
        
        logger.info("=" * 60)
        logger.info(f"🎯 ANALYZING INNOVATION ACTIVITY SIGNALS FOR ALL COMPANIES")
        logger.info(f"   Analysis period: Last {years_back} years")
        logger.info("=" * 60)
        
        results = []
        success_count = 0
        failed_count = 0
        
        for ticker in target_tickers:
            try:
                result = await self.analyze_company(ticker, years_back=years_back)
                results.append({
                    "ticker": ticker,
                    "status": "success",
                    "score": result["normalized_score"],
                    "patents_analyzed": result["patents_analyzed"],
                    "ai_patents": result["patent_metrics"]["ai_patents"]
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
        logger.info("📊 ALL COMPANIES INNOVATION ACTIVITY ANALYSIS COMPLETE")
        logger.info(f"   Successful: {success_count}")
        logger.info(f"   Failed: {failed_count}")
        logger.info(f"   Analysis period: Last {years_back} years")
        logger.info("=" * 60)
        
        return {
            "total_companies": len(target_tickers),
            "successful": success_count,
            "failed": failed_count,
            "analysis_period_years": years_back,
            "results": results
        }


# Singleton
_service: Optional[PatentSignalService] = None

def get_patent_signal_service() -> PatentSignalService:
    global _service
    if _service is None:
        _service = PatentSignalService()
    return _service