"""
Patent Signals Pipeline - PatentsView API Integration
app/pipelines/patent_signals.py

Fetches patents from PatentsView PatentSearch API and classifies AI-related patents.
Outputs results to JSON files.

API Docs: https://search.patentsview.org/docs/
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional, Set
from dataclasses import dataclass, asdict
from uuid import uuid4

import httpx
from dotenv import load_dotenv

from app.pipelines.pipeline2_state import Pipeline2State
from app.pipelines.utils import clean_nan, safe_filename
from app.models.signal import SignalCategory, SignalSource, ExternalSignal

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)

# PatentsView PatentSearch API configuration
PATENTSVIEW_API_URL = os.getenv("PATENTSVIEW_API_URL", "https://search.patentsview.org/api/v1/patent/")
PATENTSVIEW_REQUEST_DELAY = 1.5  # Rate limiting (45 req/min = 1.33s minimum)
PATENTSVIEW_API_KEY = os.getenv("PATENTSVIEW_API_KEY")


@dataclass
class Patent:
    """A patent record."""
    patent_number: str
    title: str
    abstract: str
    filing_date: datetime
    grant_date: datetime | None
    inventors: list[str]
    assignee: str
    is_ai_related: bool = False
    ai_categories: list[str] = None
    
    def __post_init__(self):
        if self.ai_categories is None:
            self.ai_categories = []


class PatentSignalCollector:
    """Collect patent signals for AI innovation."""
    
    # AI Patent Keywords
    AI_PATENT_KEYWORDS = [
        "machine learning", "neural network", "deep learning",
        "artificial intelligence", "natural language processing",
        "computer vision", "reinforcement learning",
        "predictive model", "classification algorithm",
        "convolutional neural", "recurrent neural",
        "transformer", "attention mechanism",
        "generative adversarial", "large language model",
        "llm", "gpt", "bert", "transformer",
        "object detection", "semantic segmentation",
        "speech recognition", "sentiment analysis",
        "recommendation system", "anomaly detection"
    ]
    
    # AI Patent Categories
    AI_PATENT_CATEGORIES = {
        "deep_learning": ["neural network", "deep learning", "convolutional", "recurrent", "transformer"],
        "nlp": ["natural language", "language model", "llm", "gpt", "bert", "sentiment"],
        "computer_vision": ["computer vision", "image", "object detection", "segmentation"],
        "predictive_analytics": ["predictive", "forecast", "anomaly detection"],
        "reinforcement": ["reinforcement learning"],
        "generative": ["generative adversarial", "generative ai"]
    }
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or PATENTSVIEW_API_KEY
        self.headers = {"Content-Type": "application/json"}
        if self.api_key:
            self.headers["X-Api-Key"] = self.api_key
        
        # Pre-compile regex patterns for more accurate matching
        self.keyword_patterns = {
            keyword: re.compile(r'\b' + re.escape(keyword) + r'\b', re.IGNORECASE)
            for keyword in self.AI_PATENT_KEYWORDS
        }
        
        # Pre-compile category patterns
        self.category_patterns = {}
        for category, keywords in self.AI_PATENT_CATEGORIES.items():
            patterns = [re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE) for kw in keywords]
            self.category_patterns[category] = patterns
    
    async def fetch_patents(
        self,
        company_name: str,
        years_back: int = 5,
        max_results: int = 100
    ) -> List[Patent]:
        """
        Fetch patents for a company from PatentsView API.
        
        Args:
            company_name: Company name to search for
            years_back: How many years back to search
            max_results: Maximum number of patents to fetch
            
        Returns:
            List of Patent objects
        """
        # Calculate date range
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=years_back * 365)
        start_date_str = start_date.strftime("%Y-%m-%d")
        
        # Build query for PatentsView API
        query_obj = {
            "_and": [
                {"_contains": {"assignees.assignee_organization": company_name}},
                {"_gte": {"patent_date": start_date_str}}
            ]
        }
        
        # Fields to return
        fields = [
            "patent_id",
            "patent_title",
            "patent_abstract",
            "patent_date",
            "patent_type",
            "assignees.assignee_organization",
            "inventors.inventor_first_name",
            "inventors.inventor_last_name",
            "filing_date",  # Try to get filing date if available
            "grant_date"    # Try to get grant date if available
        ]
        
        # Build URL with query params
        params = {
            "q": json.dumps(query_obj),
            "f": json.dumps(fields),
            "s": json.dumps([{"patent_date": "desc"}]),
            "o": json.dumps({"size": min(max_results, 1000)})
        }
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            await asyncio.sleep(PATENTSVIEW_REQUEST_DELAY)
            
            try:
                response = await client.get(
                    PATENTSVIEW_API_URL,
                    params=params,
                    headers=self.headers
                )
                response.raise_for_status()
                data = response.json()
                
                patents_data = data.get("patents", []) or []
                patents = []
                
                for patent_data in patents_data:
                    # Extract assignee names
                    assignees = patent_data.get("assignees") or []
                    assignee_names = [
                        a.get("assignee_organization", "")
                        for a in assignees if a.get("assignee_organization")
                    ]
                    
                    # Extract inventor names
                    inventors = patent_data.get("inventors") or []
                    inventor_names = [
                        f"{inv.get('inventor_first_name', '')} {inv.get('inventor_last_name', '')}".strip()
                        for inv in inventors
                    ]
                    
                    # Parse dates
                    patent_date_str = clean_nan(patent_data.get("patent_date"))
                    filing_date_str = clean_nan(patent_data.get("filing_date"))
                    grant_date_str = clean_nan(patent_data.get("grant_date"))
                    
                    filing_date = None
                    grant_date = None
                    
                    try:
                        if filing_date_str:
                            filing_date = datetime.fromisoformat(filing_date_str.replace("Z", "+00:00"))
                        elif patent_date_str:
                            # Fallback to patent date if filing date not available
                            filing_date = datetime.fromisoformat(patent_date_str.replace("Z", "+00:00"))
                        
                        if grant_date_str:
                            grant_date = datetime.fromisoformat(grant_date_str.replace("Z", "+00:00"))
                    except ValueError:
                        # If date parsing fails, use current date as fallback
                        filing_date = filing_date or datetime.now(timezone.utc)
                    
                    # Use first assignee as primary assignee
                    primary_assignee = assignee_names[0] if assignee_names else company_name
                    
                    patent = Patent(
                        patent_number=str(patent_data.get("patent_id", "")),
                        title=str(patent_data.get("patent_title", "")),
                        abstract=str(patent_data.get("patent_abstract", "") or ""),
                        filing_date=filing_date,
                        grant_date=grant_date,
                        inventors=inventor_names,
                        assignee=primary_assignee,
                        is_ai_related=False,
                        ai_categories=[]
                    )
                    
                    patents.append(patent)
                
                logger.info(f"Fetched {len(patents)} patents for {company_name}")
                return patents
                
            except Exception as e:
                logger.error(f"Error fetching patents for {company_name}: {e}")
                return []
    
    def classify_patent(self, patent: Patent) -> Patent:
        """Classify a patent as AI-related."""
        text = f"{patent.title} {patent.abstract}".lower()
        
        # Check for AI keywords
        ai_keywords_found = []
        for keyword, pattern in self.keyword_patterns.items():
            if pattern.search(text):
                ai_keywords_found.append(keyword)
        
        # Determine AI categories
        ai_categories = []
        for category, patterns in self.category_patterns.items():
            for pattern in patterns:
                if pattern.search(text):
                    ai_categories.append(category)
                    break  # Found one keyword from this category
        
        patent.is_ai_related = len(ai_keywords_found) > 0 or len(ai_categories) > 0
        patent.ai_categories = list(set(ai_categories))  # Remove duplicates
        
        return patent
    
    def analyze_patents(
        self,
        company_id: str,
        company_name: str,
        patents: List[Patent],
        years: int = 5
    ) -> ExternalSignal:
        """Analyze patent portfolio for AI innovation."""
        
        cutoff = datetime.now(timezone.utc) - timedelta(days=years * 365)
        recent_patents = [p for p in patents if p.filing_date > cutoff]
        ai_patents = [p for p in recent_patents if p.is_ai_related]
        
        # Last year cutoff for recency bonus
        last_year = datetime.now(timezone.utc) - timedelta(days=365)
        recent_ai = [p for p in ai_patents if p.filing_date > last_year]
        
        # Collect all AI categories
        categories = set()
        for p in ai_patents:
            categories.update(p.ai_categories)
        
        # Scoring algorithm from pseudocode:
        # - AI patent count: 5 points each (max 50)
        # - Recency bonus: +2 per patent filed in last year (max 20)
        # - Category diversity: 10 points per category (max 30)
        
        score = (
            min(len(ai_patents) * 5, 50) +
            min(len(recent_ai) * 2, 20) +
            min(len(categories) * 10, 30)
        )
        
        # Ensure score is between 0 and 100
        normalized_score = min(100.0, max(0.0, float(score)))
        
        # Create ExternalSignal
        signal = ExternalSignal(
            company_id=company_id,
            company_name=company_name,
            category=SignalCategory.INNOVATION_ACTIVITY,
            source=SignalSource.USPTO,
            signal_date=datetime.now(timezone.utc),
            raw_value=f"{len(ai_patents)} AI patents in {years} years",
            normalized_score=round(normalized_score, 1),
            confidence=0.90,
            metadata={
                "total_patents": len(patents),
                "recent_patents": len(recent_patents),
                "ai_patents": len(ai_patents),
                "recent_ai_patents": len(recent_ai),
                "ai_categories": list(categories),
                "years_analyzed": years,
                "analysis_date": datetime.now(timezone.utc).isoformat()
            }
        )
        
        return signal


async def run_patent_signals(
    state: Pipeline2State,
    years_back: int = 5,
    results_per_company: int = 100,
    api_key: Optional[str] = None,
    skip_storage: bool = False,
) -> Pipeline2State:
    """
    Run the patent signals collection pipeline.
    
    Args:
        state: Pipeline state with companies loaded
        years_back: How many years back to search (default: 5)
        results_per_company: Max patents per company
        api_key: PatentsView API key (optional, or set PATENTSVIEW_API_KEY env var)
        skip_storage: If True, skip all storage steps
    
    Returns:
        Updated pipeline state with patents, classifications, and scores
    """
    logger.info("-" * 60)
    logger.info("📊 PATENT SIGNALS PIPELINE")
    logger.info("-" * 60)
    
    collector = PatentSignalCollector(api_key=api_key)
    
    all_patents = []
    patent_signals = {}
    
    for company in state.companies:
        company_id = company.get("id", "")
        company_name = company.get("name", "")
        
        if not company_name:
            continue
        
        logger.info(f"Processing {company_name}...")
        
        # Fetch patents
        patents = await collector.fetch_patents(
            company_name=company_name,
            years_back=years_back,
            max_results=results_per_company
        )
        
        if not patents:
            logger.warning(f"No patents found for {company_name}")
            continue
        
        # Classify each patent
        classified_patents = []
        for patent in patents:
            classified_patent = collector.classify_patent(patent)
            classified_patents.append(classified_patent)
        
        # Analyze patent portfolio
        signal = collector.analyze_patents(
            company_id=company_id,
            company_name=company_name,
            patents=classified_patents,
            years=years_back
        )
        
        # Store in state
        all_patents.extend([asdict(p) for p in classified_patents])
        patent_signals[company_id] = signal
        
        # Update state
        state.patents.extend([asdict(p) for p in classified_patents])
        state.patent_scores[company_id] = signal.normalized_score
        
        logger.info(f"  ✓ {company_name}: {signal.normalized_score}/100 "
                   f"({len([p for p in classified_patents if p.is_ai_related])} AI patents)")
    
    # Store aggregated results in state
    state.summary["patents_collected"] = len(all_patents)
    state.summary["ai_patents"] = sum(1 for p in all_patents if p.get("is_ai_related", False))
    state.summary["patent_signals"] = {
        company_id: {
            "score": signal.normalized_score,
            "ai_patents": signal.metadata.get("ai_patents", 0),
            "total_patents": signal.metadata.get("total_patents", 0)
        }
        for company_id, signal in patent_signals.items()
    }
    
    logger.info(f"\n✅ Patent pipeline complete:")
    logger.info(f"   • Total patents collected: {len(all_patents)}")
    logger.info(f"   • AI-related patents: {sum(1 for p in all_patents if p.get('is_ai_related', False))}")
    logger.info(f"   • Companies processed: {len(patent_signals)}")
    
    return state


# Legacy function for backward compatibility
async def run_patent_signals_legacy(
    state: Pipeline2State,
    years_back: int = 5,
    results_per_company: int = 100,
    api_key: Optional[str] = None,
    skip_storage: bool = False,
) -> Pipeline2State:
    """Legacy wrapper for backward compatibility."""
    return await run_patent_signals(
        state=state,
        years_back=years_back,
        results_per_company=results_per_company,
        api_key=api_key,
        skip_storage=skip_storage
    )