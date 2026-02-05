import re
import logging
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


@dataclass
class LeadershipScores:
    """Breakdown of leadership signal scores."""
    tech_exec_score: float  # Max 30
    keyword_score: float  # Max 30
    performance_metric_score: float  # Max 25
    board_tech_score: float  # Max 15
    total_score: float  # Max 100
    
    # Details
    tech_execs_found: List[str]
    keyword_counts: Dict[str, int]
    tech_metrics_found: List[str]
    board_indicators: List[str]


class LeadershipAnalyzer:
    """Analyze DEF 14A filings for leadership signals."""
    
    # Tech executive titles to search for
    TECH_EXEC_TITLES = {
        "chief technology officer": 10,
        "cto": 10,
        "chief digital officer": 10,
        "cdo": 8,
        "chief data officer": 8,
        "chief ai officer": 12,
        "chief artificial intelligence officer": 12,
        "chief information officer": 6,
        "cio": 6,
        "chief analytics officer": 8,
        "chief innovation officer": 8,
        "vp of artificial intelligence": 6,
        "vp of machine learning": 6,
        "vp of data science": 5,
        "svp of technology": 5,
        "head of ai": 7,
        "head of machine learning": 7,
    }
    
    # AI/Tech keywords for compensation discussion
    TECH_KEYWORDS = {
        "artificial intelligence": 3,
        "machine learning": 3,
        "ai": 2,
        "digital transformation": 2,
        "automation": 1.5,
        "data analytics": 1.5,
        "technology modernization": 1.5,
        "cloud": 1,
        "digital": 1,
        "innovation": 0.5,
        "technology": 0.5,
    }
    
    # Phrases indicating tech-linked performance metrics
    TECH_PERFORMANCE_PHRASES = [
        (r"technology\s+(?:metric|goal|objective|target|initiative)", 8),
        (r"digital\s+transformation\s+(?:bonus|incentive|award|metric)", 8),
        (r"(?:ai|artificial intelligence)\s+(?:initiative|deployment|implementation)", 10),
        (r"automation\s+(?:metric|goal|savings|efficiency)", 6),
        (r"innovation\s+(?:metric|performance|bonus)", 5),
        (r"(?:tech|technology|digital)\s+(?:investment|spend|budget)", 4),
        (r"data\s+(?:strategy|analytics|platform)\s+(?:goal|metric)", 5),
    ]
    
    # Board tech expertise indicators
    BOARD_TECH_INDICATORS = [
        (r"(?:google|microsoft|amazon|meta|apple|nvidia|intel|ibm|oracle|salesforce)", 5),
        (r"technology\s+(?:executive|leader|officer|expert)", 4),
        (r"(?:cto|cio|chief technology|chief digital)\s+(?:at|of|for)", 5),
        (r"(?:software|tech|digital)\s+(?:company|industry|sector)\s+(?:experience|background)", 3),
        (r"computer science|engineering degree|technical background", 2),
    ]
    
    def __init__(self):
        logger.info("🎯 Leadership Analyzer initialized")
    
    def analyze(
        self,
        text_content: str,
        sections: Dict[str, str],
        tables: List[Dict]
    ) -> LeadershipScores:
        """
        Analyze DEF 14A content for leadership signals.
        
        Returns scores out of 100:
        - Tech Exec Presence: 30 pts
        - AI/Tech Keywords: 30 pts
        - Tech-Linked Metrics: 25 pts
        - Board Tech Expertise: 15 pts
        """
        logger.info("  🔍 Analyzing leadership signals...")
        
        # Combine all text for analysis
        full_text = text_content.lower()
        exec_comp_text = sections.get("executive_compensation", "").lower()
        director_text = sections.get("director_compensation", "").lower()
        
        # 1. Tech Executive Presence (max 30 points)
        tech_exec_score, tech_execs = self._analyze_tech_execs(full_text, tables)
        tech_exec_score = min(tech_exec_score, 30)
        logger.info(f"    • Tech Exec Score: {tech_exec_score}/30 | Found: {tech_execs}")
        
        # 2. AI/Tech Keywords in Compensation (max 30 points)
        keyword_score, keyword_counts = self._analyze_keywords(exec_comp_text or full_text)
        keyword_score = min(keyword_score, 30)
        logger.info(f"    • Keyword Score: {keyword_score}/30 | Counts: {sum(keyword_counts.values())} mentions")
        
        # 3. Tech-Linked Performance Metrics (max 25 points)
        perf_score, tech_metrics = self._analyze_performance_metrics(exec_comp_text or full_text)
        perf_score = min(perf_score, 25)
        logger.info(f"    • Performance Metric Score: {perf_score}/25 | Found: {len(tech_metrics)} metrics")
        
        # 4. Board Tech Expertise (max 15 points)
        board_score, board_indicators = self._analyze_board_expertise(director_text or full_text)
        board_score = min(board_score, 15)
        logger.info(f"    • Board Tech Score: {board_score}/15 | Indicators: {len(board_indicators)}")
        
        # Calculate total
        total_score = tech_exec_score + keyword_score + perf_score + board_score
        logger.info(f"  ✅ Total Leadership Score: {total_score}/100")
        
        return LeadershipScores(
            tech_exec_score=tech_exec_score,
            keyword_score=keyword_score,
            performance_metric_score=perf_score,
            board_tech_score=board_score,
            total_score=total_score,
            tech_execs_found=tech_execs,
            keyword_counts=keyword_counts,
            tech_metrics_found=tech_metrics,
            board_indicators=board_indicators
        )
    
    def _analyze_tech_execs(self, text: str, tables: List[Dict]) -> Tuple[float, List[str]]:
        """Find tech executive titles in text and tables."""
        found_titles = []
        score = 0
        
        # Search in text
        for title, points in self.TECH_EXEC_TITLES.items():
            if title in text:
                found_titles.append(title.title())
                score += points
        
        # Search in tables (executive compensation tables)
        for table in tables:
            headers = [h.lower() if h else "" for h in table.get("headers", [])]
            if any("name" in h or "officer" in h or "executive" in h for h in headers):
                for row in table.get("rows", []):
                    row_text = " ".join(str(cell).lower() for cell in row if cell)
                    for title, points in self.TECH_EXEC_TITLES.items():
                        if title in row_text and title.title() not in found_titles:
                            found_titles.append(title.title())
                            score += points
        
        return score, list(set(found_titles))
    
    def _analyze_keywords(self, text: str) -> Tuple[float, Dict[str, int]]:
        """Count AI/tech keywords in compensation discussion."""
        keyword_counts = {}
        score = 0
        
        for keyword, points_per_mention in self.TECH_KEYWORDS.items():
            # Count occurrences (max 5 counted per keyword)
            count = len(re.findall(re.escape(keyword), text))
            if count > 0:
                keyword_counts[keyword] = count
                score += min(count, 5) * points_per_mention
        
        return score, keyword_counts
    
    def _analyze_performance_metrics(self, text: str) -> Tuple[float, List[str]]:
        """Find tech-linked performance metrics."""
        metrics_found = []
        score = 0
        
        for pattern, points in self.TECH_PERFORMANCE_PHRASES:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                metrics_found.extend(matches)
                score += points
        
        return score, list(set(metrics_found))
    
    def _analyze_board_expertise(self, text: str) -> Tuple[float, List[str]]:
        """Analyze board/director tech expertise."""
        indicators_found = []
        score = 0
        
        for pattern, points in self.BOARD_TECH_INDICATORS:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                indicators_found.extend(matches)
                score += points
        
        return score, list(set(indicators_found))
    
    def calculate_confidence(self, text_length: int, sections_found: int, tables_count: int) -> float:
        """Calculate confidence based on data quality."""
        # Base confidence for SEC filing (reliable source)
        base = 0.75
        
        # Bonus for more content
        if text_length > 50000:
            base += 0.05
        if text_length > 100000:
            base += 0.05
        
        # Bonus for sections found
        if sections_found >= 1:
            base += 0.03
        if sections_found >= 2:
            base += 0.02
        
        # Bonus for tables (compensation tables are valuable)
        if tables_count >= 5:
            base += 0.03
        if tables_count >= 10:
            base += 0.02
        
        return min(base, 0.95)


# Singleton
_analyzer: Optional[LeadershipAnalyzer] = None

def get_leadership_analyzer() -> LeadershipAnalyzer:
    global _analyzer
    if _analyzer is None:
        _analyzer = LeadershipAnalyzer()
    return _analyzer