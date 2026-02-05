# File: app/pipelines/tech_signals.py
"""
Tech Stack Signal Analysis

Analyzes job postings to extract technology stack information
and score companies on their AI tool adoption.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Dict, Any, Set

logger = logging.getLogger(__name__)


@dataclass
class TechnologyDetection:
    """A detected technology."""
    name: str
    category: str
    is_ai_related: bool
    confidence: float


class TechStackCollector:
    """Analyze company technology stacks."""
    
    AI_TECHNOLOGIES = {
        # Cloud AI Services
        "aws sagemaker": "cloud_ml",
        "azure ml": "cloud_ml",
        "google vertex": "cloud_ml",
        "databricks": "cloud_ml",
        
        # ML Frameworks
        "tensorflow": "ml_framework",
        "pytorch": "ml_framework",
        "scikit-learn": "ml_framework",
        
        # Data Infrastructure
        "snowflake": "data_platform",
        "spark": "data_platform",
        
        # AI APIs
        "openai": "ai_api",
        "anthropic": "ai_api",
        "huggingface": "ai_api",
    }
    
    def analyze_tech_stack(
        self,
        company_id: str,
        technologies: List[TechnologyDetection]
    ) -> Dict[str, Any]:
        """Analyze technology stack for AI capabilities."""
        
        ai_techs = [t for t in technologies if t.is_ai_related]
        
        # Score by category
        categories_found = set(t.category for t in ai_techs)
        
        # Scoring:
        # - Each AI technology: 10 points (max 50)
        # - Each category covered: 12.5 points (max 50)
        tech_score = min(len(ai_techs) * 10, 50)
        category_score = min(len(categories_found) * 12.5, 50)
        
        score = tech_score + category_score
        
        return {
            "score": round(score, 1),
            "ai_technologies": [t.name for t in ai_techs],
            "categories": list(categories_found),
            "total_technologies": len(technologies),
            "confidence": 0.85
        }
    
    def detect_technologies_from_text(self, text: str) -> List[TechnologyDetection]:
        """Detect technologies from text (e.g., job descriptions)."""
        technologies = []
        text_lower = text.lower()
        
        for tech_name, category in self.AI_TECHNOLOGIES.items():
            if tech_name in text_lower:
                # Calculate confidence based on context
                confidence = self._calculate_confidence(tech_name, text_lower)
                technologies.append(
                    TechnologyDetection(
                        name=tech_name,
                        category=category,
                        is_ai_related=True,
                        confidence=confidence
                    )
                )
        
        return technologies
    
    def _calculate_confidence(self, tech_name: str, text: str) -> float:
        """Calculate confidence score for technology detection."""
        # Simple implementation - could be enhanced with NLP
        words = text.split()
        if tech_name in words:
            return 0.9  # Exact match
        elif any(word in tech_name for word in words):
            return 0.7  # Partial match
        else:
            return 0.5  # Substring match

def calculate_techstack_score(techstack_keywords: Set[str], tech_detections: List[TechnologyDetection]) -> Dict[str, Any]:
    """
    Calculate techstack score using pseudo code algorithm.
    
    Scoring (pseudo code):
    - Each AI technology: 10 points (max 50)
    - Each category covered: 12.5 points (max 50)
    """
    # Get AI technologies from detections
    ai_techs = [t for t in tech_detections if t.is_ai_related]
    
    # Score by category
    categories_found = set(t.category for t in ai_techs)
    
    # Pseudo code scoring
    tech_score = min(len(ai_techs) * 10, 50)
    category_score = min(len(categories_found) * 12.5, 50)
    
    score = tech_score + category_score
    
    return {
        "score": round(score, 1),
        "tech_score": tech_score,
        "category_score": category_score,
        "ai_technologies": [t.name for t in ai_techs],
        "categories": list(categories_found),
        "total_technologies": len(tech_detections),
        "confidence": 0.85
    }

def create_external_signal_from_techstack(
    company_id: str,
    company_name: str,
    techstack_analysis: Dict[str, Any],
    collector_analysis: Dict[str, Any],
    timestamp: str
) -> Dict[str, Any]:
    """Create external signal data from tech stack analysis."""
    
    return {
        "signal_id": f"{company_id}_tech_stack_{timestamp}",
        "company_id": company_id,
        "company_name": company_name,
        "category": "tech_stack",
        "source": "job_postings",
        "score": techstack_analysis.get("score", 0),
        "evidence_count": techstack_analysis.get("total_keywords", 0),
        "summary": f"Found {techstack_analysis.get('total_ai_tools', 0)} AI tools in tech stack",
        "raw_payload": {
            "collection_date": timestamp,
            "techstack_analysis": techstack_analysis,
            "collector_analysis": collector_analysis,
            "ai_tools": techstack_analysis.get("ai_tools_found", []),
            "total_keywords": techstack_analysis.get("total_keywords", 0)
        }
    }

def log_techstack_results(company_name: str, original_score: float, 
                         collector_score: float, final_score: float,
                         keywords_count: int, ai_tools_count: int):
    """Log tech stack analysis results."""
    logger.info(
        f"   • {company_name}: {final_score:.1f}/100 "
        f"(orig={original_score:.1f}, collector={collector_score:.1f}, "
        f"keywords={keywords_count}, ai_tools={ai_tools_count})"
    )