import json
import logging
from typing import Dict, List, Optional
from datetime import datetime, timezone
from app.pipelines.leadership_analyzer import get_leadership_analyzer, LeadershipScores
from app.services.s3_storage import get_s3_service
from app.repositories.document_repository import get_document_repository
from app.repositories.company_repository import CompanyRepository
from app.repositories.signal_repository import get_signal_repository

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class LeadershipSignalService:
    """Service to extract leadership signals from DEF 14A filings."""
    
    def __init__(self):
        self.analyzer = get_leadership_analyzer()
        self.s3_service = get_s3_service()
        self.doc_repo = get_document_repository()
        self.company_repo = CompanyRepository()
        self.signal_repo = get_signal_repository()
    
    def _get_parsed_s3_key(self, ticker: str, filing_type: str, filing_date: str) -> str:
        """Get S3 key for parsed DEF 14A content."""
        clean_filing_type = filing_type.replace(" ", "")
        return f"sec/parsed/{ticker}/{clean_filing_type}/{filing_date}_full.json"
    
    async def analyze_company(self, ticker: str) -> Dict:
        """
        Analyze all DEF 14A filings for a company and create leadership signals.
        """
        ticker = ticker.upper()
        logger.info("=" * 60)
        logger.info(f"🎯 ANALYZING LEADERSHIP SIGNALS FOR: {ticker}")
        logger.info("=" * 60)
        
        # Get company from database
        company = self.company_repo.get_by_ticker(ticker)
        if not company:
            raise ValueError(f"Company not found: {ticker}")
        
        company_id = str(company['id'])
        logger.info(f"✅ Found company: {company['name']} (ID: {company_id})")
        
        # Get all DEF 14A documents for this company
        all_docs = self.doc_repo.get_by_ticker(ticker)
        def14a_docs = [d for d in all_docs if d['filing_type'] in ['DEF 14A', 'DEF14A']]
        
        if not def14a_docs:
            logger.warning(f"❌ No DEF 14A filings found for: {ticker}")
            raise ValueError(f"No DEF 14A filings found for: {ticker}")
        
        logger.info(f"📚 Found {len(def14a_docs)} DEF 14A filings to analyze")
        
        # Delete existing leadership signals for fresh analysis
        deleted = self.signal_repo.delete_signals_by_category(company_id, "leadership_signals")
        if deleted:
            logger.info(f"  🗑️ Deleted {deleted} existing leadership signals")
        
        # Analyze each filing
        all_scores: List[LeadershipScores] = []
        filing_dates = []
        signals_created = 0
        
        for idx, doc in enumerate(def14a_docs, 1):
            filing_date = str(doc['filing_date'])
            logger.info("-" * 40)
            logger.info(f"📄 [{idx}/{len(def14a_docs)}] DEF 14A | {filing_date}")
            
            # Get parsed content from S3
            s3_key = self._get_parsed_s3_key(ticker, doc['filing_type'], filing_date)
            logger.info(f"  ⬇️ Loading parsed content: {s3_key}")
            
            try:
                content = self.s3_service.get_file(s3_key)
                if not content:
                    logger.warning(f"  ⚠️ Parsed content not found, skipping")
                    continue
                
                parsed_data = json.loads(content.decode('utf-8'))
                text_content = parsed_data.get('text_content', '')
                sections = parsed_data.get('sections', {})
                tables = parsed_data.get('tables', [])
                
                logger.info(f"  ✅ Loaded {len(text_content):,} chars, {len(sections)} sections, {len(tables)} tables")
                
                # Analyze the filing
                scores = self.analyzer.analyze(text_content, sections, tables)
                all_scores.append(scores)
                filing_dates.append(filing_date)
                
                # Calculate confidence
                confidence = self.analyzer.calculate_confidence(
                    len(text_content), len(sections), len(tables)
                )
                
                # Create signal record for this filing
                self.signal_repo.create_signal(
                    company_id=company_id,
                    category="leadership_signals",
                    source="sec_filing",
                    signal_date=datetime.strptime(filing_date, "%Y-%m-%d"),
                    raw_value=f"DEF 14A analysis: {scores.total_score:.1f}/100",
                    normalized_score=scores.total_score,
                    confidence=confidence,
                    metadata={
                        "filing_date": filing_date,
                        "tech_exec_score": scores.tech_exec_score,
                        "keyword_score": scores.keyword_score,
                        "performance_metric_score": scores.performance_metric_score,
                        "board_tech_score": scores.board_tech_score,
                        "tech_execs_found": scores.tech_execs_found,
                        "keyword_counts": scores.keyword_counts,
                        "tech_metrics_found": scores.tech_metrics_found,
                        "board_indicators": scores.board_indicators
                    }
                )
                signals_created += 1
                
            except Exception as e:
                logger.error(f"  ❌ Error analyzing filing: {e}")
                continue
        
        if not all_scores:
            raise ValueError(f"Could not analyze any DEF 14A filings for: {ticker}")
        
        # Calculate average score across all filings (with recency weighting)
        # More recent filings get higher weight
        weights = list(range(1, len(all_scores) + 1))  # [1, 2, 3, ...] - later = higher
        total_weight = sum(weights)
        
        weighted_score = sum(
            scores.total_score * weight 
            for scores, weight in zip(all_scores, weights)
        ) / total_weight
        
        avg_confidence = sum(
            self.analyzer.calculate_confidence(50000, 2, 10)  # Approximate
            for _ in all_scores
        ) / len(all_scores)
        
        # Aggregate all findings
        all_tech_execs = list(set(
            exec for scores in all_scores for exec in scores.tech_execs_found
        ))
        all_keyword_counts = {}
        for scores in all_scores:
            for kw, count in scores.keyword_counts.items():
                all_keyword_counts[kw] = all_keyword_counts.get(kw, 0) + count
        all_tech_metrics = list(set(
            m for scores in all_scores for m in scores.tech_metrics_found
        ))
        all_board_indicators = list(set(
            i for scores in all_scores for i in scores.board_indicators
        ))
        
        # Update company signal summary
        logger.info("-" * 40)
        logger.info(f"📊 Updating company signal summary...")
        self.signal_repo.upsert_summary(
            company_id=company_id,
            ticker=ticker,
            leadership_score=round(weighted_score, 2)
        )

        # Summary
        logger.info("=" * 60)
        logger.info(f"📊 LEADERSHIP ANALYSIS COMPLETE FOR: {ticker}")
        logger.info(f"   Filings analyzed: {len(all_scores)}")
        logger.info(f"   Signals created: {signals_created}")
        logger.info(f"   Average Score: {weighted_score:.1f}/100")
        logger.info(f"   Tech Execs Found: {all_tech_execs}")
        logger.info("=" * 60)

        return {
            "ticker": ticker,
            "company_id": company_id,
            "filing_count_analyzed": len(all_scores),
            "signals_created": signals_created,
            "normalized_score": round(weighted_score, 2),
            "confidence": round(avg_confidence, 3),
            "breakdown": {
                "tech_exec_score": round(sum(s.tech_exec_score for s in all_scores) / len(all_scores), 1),
                "keyword_score": round(sum(s.keyword_score for s in all_scores) / len(all_scores), 1),
                "performance_metric_score": round(sum(s.performance_metric_score for s in all_scores) / len(all_scores), 1),
                "board_tech_score": round(sum(s.board_tech_score for s in all_scores) / len(all_scores), 1),
                "total_score": round(weighted_score, 1)
            },
            "tech_execs_found": all_tech_execs,
            "keyword_counts": all_keyword_counts,
            "tech_linked_metrics_found": all_tech_metrics,
            "board_tech_indicators": all_board_indicators,
            "filing_dates": filing_dates
        }
    
    async def analyze_all_companies(self) -> Dict:
        """Analyze leadership signals for all 10 target companies."""
        target_tickers = ["CAT", "DE", "UNH", "HCA", "ADP", "PAYX", "WMT", "TGT", "JPM", "GS"]
        
        logger.info("=" * 60)
        logger.info("🎯 ANALYZING LEADERSHIP SIGNALS FOR ALL COMPANIES")
        logger.info("=" * 60)
        
        results = []
        success_count = 0
        failed_count = 0
        
        for ticker in target_tickers:
            try:
                result = await self.analyze_company(ticker)
                results.append({
                    "ticker": ticker,
                    "status": "success",
                    "score": result["normalized_score"],
                    "filings_analyzed": result["filing_count_analyzed"]
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
        logger.info("📊 ALL COMPANIES LEADERSHIP ANALYSIS COMPLETE")
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
_service: Optional[LeadershipSignalService] = None

def get_leadership_service() -> LeadershipSignalService:
    global _service
    if _service is None:
        _service = LeadershipSignalService()
    return _service