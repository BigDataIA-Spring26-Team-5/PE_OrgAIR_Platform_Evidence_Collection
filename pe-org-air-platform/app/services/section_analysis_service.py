import json
import logging
from typing import Dict, List, Optional
from datetime import datetime, timezone
from dataclasses import asdict
from app.pipelines.section_analyzer import get_section_analyzer, DocumentAnalysis
from app.services.s3_storage import get_s3_service
from app.repositories.document_repository import get_document_repository

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class SectionAnalysisService:
    """Service to analyze SEC filing sections"""
    
    TARGET_TICKERS = ["CAT", "DE", "UNH", "HCA", "ADP", "PAYX", "WMT", "TGT", "JPM", "GS"]
    
    def __init__(self):
        self.analyzer = get_section_analyzer()
        self.s3_service = get_s3_service()
        self.doc_repo = get_document_repository()
    
    def _get_parsed_s3_key(self, ticker: str, filing_type: str, filing_date: str) -> str:
        """Get S3 key for parsed content"""
        clean_filing_type = filing_type.replace(" ", "")
        return f"sec/parsed/{ticker}/{clean_filing_type}/{filing_date}_full.json"
    
    def analyze_document(self, document_id: str) -> Optional[Dict]:
        """Analyze a single document"""
        doc = self.doc_repo.get_by_id(document_id)
        if not doc:
            return None
        
        ticker = doc['ticker']
        filing_type = doc['filing_type']
        filing_date = str(doc['filing_date'])
        
        # Get parsed content from S3
        s3_key = self._get_parsed_s3_key(ticker, filing_type, filing_date)
        content = self.s3_service.get_file(s3_key)
        
        if not content:
            logger.warning(f"No parsed content found for {document_id}")
            return None
        
        parsed_data = json.loads(content.decode('utf-8'))
        sections = parsed_data.get('sections', {})
        total_word_count = parsed_data.get('word_count', 0)
        full_text = parsed_data.get('text_content', '')  # Get full document text
        
        # Analyze sections
        section_stats = []
        for section_name, section_content in sections.items():
            if not section_content:
                continue
            word_count = len(section_content.split())
            keywords = self.analyzer.count_keywords(section_content)
            section_stats.append({
                "section_name": section_name,
                "word_count": word_count,
                "keywords": keywords
            })
        
        # Count keywords from FULL document text (not just sections)
        total_keywords = self.analyzer.count_keywords(full_text)
        
        return {
            "document_id": document_id,
            "ticker": ticker,
            "filing_type": filing_type,
            "filing_date": filing_date,
            "total_word_count": total_word_count,
            "sections": section_stats,
            "total_keywords": total_keywords  # From full text
        }
    
    def analyze_by_ticker(self, ticker: str) -> Dict:
        """Analyze all documents for a company"""
        ticker = ticker.upper()
        logger.info(f"📊 Analyzing sections for: {ticker}")
        
        docs = self.doc_repo.get_by_ticker(ticker)
        parsed_docs = [d for d in docs if d.get('status') in ['parsed', 'chunked']]
        
        results = {
            "ticker": ticker,
            "filings": {
                "10-K": [],
                "10-Q": [],
                "8-K": [],
                "DEF 14A": []
            }
        }
        
        for doc in parsed_docs:
            analysis = self.analyze_document(doc['id'])
            if analysis:
                filing_type = analysis.get('filing_type', '')
                if filing_type in results["filings"]:
                    results["filings"][filing_type].append(analysis)
        
        return results
    
    def analyze_all_companies(self) -> Dict:
        """Analyze all documents for all companies"""
        logger.info("📊 Analyzing sections for ALL companies...")
        
        all_results = {}
        for ticker in self.TARGET_TICKERS:
            try:
                all_results[ticker] = self.analyze_by_ticker(ticker)
            except Exception as e:
                logger.error(f"Failed to analyze {ticker}: {e}")
                all_results[ticker] = {"error": str(e)}
        
        return all_results
    
    def generate_analysis_tables(self) -> Dict:
        """Generate analysis in table format (Option C)"""
        logger.info("📊 Generating section analysis tables...")
        
        all_data = self.analyze_all_companies()
        
        # Initialize tables for each filing type
        tables = {
            "10-K": self._create_filing_type_tables("10-K", all_data),
            "10-Q": self._create_filing_type_tables("10-Q", all_data),
            "8-K": self._create_filing_type_tables("8-K", all_data),
            "DEF 14A": self._create_filing_type_tables("DEF 14A", all_data),
        }
        
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "tables": tables
        }
    
    def _create_filing_type_tables(self, filing_type: str, all_data: Dict) -> Dict:
        """Create word count and keyword tables for a filing type"""
        
        # Define sections for each filing type (matching actual parsed section names)
        section_map = {
            "10-K": ["business", "risk_factors", "mda"],
            "10-Q": ["mda", "risk_factors"],
            "8-K": ["other_events"],
            "DEF 14A": ["executive_compensation", "director_compensation"],
        }
        
        # Display names for sections
        section_display_names = {
            "business": "Business",
            "risk_factors": "Risk Factors",
            "mda": "MD&A",
            "financial_statements": "Financial",
            "controls": "Controls",
            "other_events": "Other Events",
            "executive_compensation": "Exec Comp",
            "director_compensation": "Director Comp",
        }
        
        sections = section_map.get(filing_type, [])
        section_display = {s: section_display_names.get(s, s) for s in sections}
        
        # Build word count table
        word_count_headers = ["Ticker"] + [section_display[s] for s in sections]
        word_count_rows = []
        
        # Build keyword table
        keyword_headers = ["Ticker", "AI Total", "Tech Total", "AI", "ML", "Automation", "Digital", "Cloud"]
        keyword_rows = []
        
        for ticker in self.TARGET_TICKERS:
            ticker_data = all_data.get(ticker, {})
            filings = ticker_data.get("filings", {}).get(filing_type, [])
            
            if not filings:
                # No data for this ticker/filing type
                word_count_rows.append([ticker] + [0] * len(sections))
                keyword_rows.append([ticker, 0, 0, 0, 0, 0, 0, 0])
                continue
            
            # Aggregate across all filings of this type (average)
            section_words = {s: [] for s in sections}
            total_keywords = {
                "ai_total": 0,
                "tech_total": 0,
                "artificial intelligence": 0,
                "machine learning": 0,
                "automation": 0,
                "digital": 0,
                "cloud": 0,
            }
            
            for filing in filings:
                # Section word counts - match by section name
                for section_stat in filing.get("sections", []):
                    section_name = section_stat.get("section_name")
                    # Match section names
                    for s in sections:
                        if s == section_name or s in section_name or section_name in s:
                            section_words[s].append(section_stat.get("word_count", 0))
                            break
                
                # Keywords from FULL DOCUMENT TEXT (total_keywords)
                keywords = filing.get("total_keywords", {})
                for kw in self.analyzer.AI_KEYWORDS:
                    total_keywords["ai_total"] += keywords.get(kw, 0)
                for kw in self.analyzer.TECH_KEYWORDS:
                    total_keywords["tech_total"] += keywords.get(kw, 0)
                total_keywords["artificial intelligence"] += keywords.get("artificial intelligence", 0)
                total_keywords["machine learning"] += keywords.get("machine learning", 0)
                total_keywords["automation"] += keywords.get("automation", 0)
                total_keywords["digital"] += keywords.get("digital", 0)
                total_keywords["cloud"] += keywords.get("cloud", 0)
            
            # Calculate averages for word counts
            word_row = [ticker]
            for s in sections:
                counts = section_words[s]
                avg = int(sum(counts) / len(counts)) if counts else 0
                word_row.append(avg)
            word_count_rows.append(word_row)
            
            # Keyword row (totals across all filings)
            keyword_rows.append([
                ticker,
                total_keywords["ai_total"],
                total_keywords["tech_total"],
                total_keywords["artificial intelligence"],
                total_keywords["machine learning"],
                total_keywords["automation"],
                total_keywords["digital"],
                total_keywords["cloud"],
            ])
        
        return {
            "word_counts": {
                "headers": word_count_headers,
                "rows": word_count_rows
            },
            "keywords": {
                "headers": keyword_headers,
                "rows": keyword_rows
            }
        }
    
    def generate_markdown_report(self) -> str:
        """Generate markdown report"""
        logger.info("📊 Generating markdown analysis report...")
        
        tables = self.generate_analysis_tables()
        
        md = []
        md.append("# SEC Filing Section Analysis Report")
        md.append(f"**Generated:** {tables['generated_at']}")
        md.append("")
        
        # Individual filing type tables
        for filing_type in ["10-K", "10-Q", "8-K", "DEF 14A"]:
            ft_data = tables["tables"].get(filing_type, {})
            
            md.append(f"## {filing_type} Filings")
            md.append("")
            
            # Word Counts Table
            wc = ft_data.get("word_counts", {})
            if wc.get("headers"):
                md.append("### Section Word Counts")
                md.append("| " + " | ".join(wc["headers"]) + " |")
                md.append("| " + " | ".join(["---"] * len(wc["headers"])) + " |")
                for row in wc.get("rows", []):
                    formatted_row = [str(row[0])] + [f"{v:,}" if isinstance(v, int) else str(v) for v in row[1:]]
                    md.append("| " + " | ".join(formatted_row) + " |")
                md.append("")
            
            # Keywords Table
            kw = ft_data.get("keywords", {})
            if kw.get("headers"):
                md.append("### Keyword Mentions")
                md.append("| " + " | ".join(kw["headers"]) + " |")
                md.append("| " + " | ".join(["---"] * len(kw["headers"])) + " |")
                for row in kw.get("rows", []):
                    formatted_row = [str(v) for v in row]
                    md.append("| " + " | ".join(formatted_row) + " |")
                md.append("")
            
            md.append("---")
            md.append("")
        
        # ============================================================
        # TOTAL ACROSS ALL FILINGS
        # ============================================================
        md.append("## 📊 Total Across All Filing Types")
        md.append("")
        md.append("### Combined Keyword Mentions (All Filings)")
        md.append("| Ticker | AI Total | Tech Total | AI | ML | Automation | Digital | Cloud |")
        md.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
        
        # Calculate totals per ticker across all filing types
        for ticker in self.TARGET_TICKERS:
            ticker_totals = {
                "ai_total": 0,
                "tech_total": 0,
                "artificial intelligence": 0,
                "machine learning": 0,
                "automation": 0,
                "digital": 0,
                "cloud": 0,
            }
            
            for filing_type in ["10-K", "10-Q", "8-K", "DEF 14A"]:
                ft_data = tables["tables"].get(filing_type, {})
                kw_data = ft_data.get("keywords", {})
                rows = kw_data.get("rows", [])
                
                for row in rows:
                    if row[0] == ticker:
                        ticker_totals["ai_total"] += row[1]
                        ticker_totals["tech_total"] += row[2]
                        ticker_totals["artificial intelligence"] += row[3]
                        ticker_totals["machine learning"] += row[4]
                        ticker_totals["automation"] += row[5]
                        ticker_totals["digital"] += row[6]
                        ticker_totals["cloud"] += row[7]
                        break
            
            md.append(f"| {ticker} | {ticker_totals['ai_total']} | {ticker_totals['tech_total']} | {ticker_totals['artificial intelligence']} | {ticker_totals['machine learning']} | {ticker_totals['automation']} | {ticker_totals['digital']} | {ticker_totals['cloud']} |")
        
        # Grand total row
        grand_totals = {k: 0 for k in ["ai_total", "tech_total", "artificial intelligence", "machine learning", "automation", "digital", "cloud"]}
        for filing_type in ["10-K", "10-Q", "8-K", "DEF 14A"]:
            ft_data = tables["tables"].get(filing_type, {})
            kw_data = ft_data.get("keywords", {})
            for row in kw_data.get("rows", []):
                grand_totals["ai_total"] += row[1]
                grand_totals["tech_total"] += row[2]
                grand_totals["artificial intelligence"] += row[3]
                grand_totals["machine learning"] += row[4]
                grand_totals["automation"] += row[5]
                grand_totals["digital"] += row[6]
                grand_totals["cloud"] += row[7]
        
        md.append(f"| **TOTAL** | **{grand_totals['ai_total']}** | **{grand_totals['tech_total']}** | **{grand_totals['artificial intelligence']}** | **{grand_totals['machine learning']}** | **{grand_totals['automation']}** | **{grand_totals['digital']}** | **{grand_totals['cloud']}** |")
        md.append("")
        
        # Summary by filing type
        md.append("### Keyword Totals by Filing Type")
        md.append("| Filing Type | AI Total | Tech Total |")
        md.append("| --- | --- | --- |")
        
        for filing_type in ["10-K", "10-Q", "8-K", "DEF 14A"]:
            ft_data = tables["tables"].get(filing_type, {})
            kw_data = ft_data.get("keywords", {})
            ai_sum = sum(row[1] for row in kw_data.get("rows", []))
            tech_sum = sum(row[2] for row in kw_data.get("rows", []))
            md.append(f"| {filing_type} | {ai_sum} | {tech_sum} |")
        
        md.append("")
        md.append("---")
        
        return "\n".join(md)


# Singleton
_service: Optional[SectionAnalysisService] = None

def get_section_analysis_service() -> SectionAnalysisService:
    global _service
    if _service is None:
        _service = SectionAnalysisService()
    return _service