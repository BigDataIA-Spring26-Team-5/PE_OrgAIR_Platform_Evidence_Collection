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
    Calculate techstack score for Digital Presence signals.

    UPDATED Scoring (harder to max out):
    =====================================
    Component 1: AI-Specific Tools (max 40 points)
      - Explicit AI tools: 8 points each (need 5 to max)
      - AI tools: aws sagemaker, azure ml, databricks, tensorflow, pytorch,
                  huggingface, openai, mlflow, kubeflow, vertex ai

    Component 2: AI-Related Infrastructure (max 30 points)
      - Supporting tech: 3 points each (need 10 to max)
      - e.g., kubernetes, spark, kafka, airflow, docker (for ML pipelines)

    Component 3: General Tech Stack Breadth (max 30 points)
      - Any tech keyword: 1 point each (need 30 to max)
      - Shows overall technical sophistication

    Total: 100 points
    """
    # AI-SPECIFIC tools (highest weight)
    AI_SPECIFIC_TOOLS = {
        "aws sagemaker", "azure ml", "azure machine learning", "google vertex ai",
        "databricks", "tensorflow", "pytorch", "huggingface", "hugging face",
        "openai", "mlflow", "kubeflow", "ray", "langchain", "llamaindex",
        "anthropic", "bedrock", "sagemaker", "vertex ai"
    }

    # AI-related infrastructure (medium weight)
    AI_INFRASTRUCTURE = {
        "kubernetes", "k8s", "spark", "apache spark", "kafka", "apache kafka",
        "airflow", "apache airflow", "docker", "containerization",
        "snowflake", "bigquery", "redshift", "dbt", "prefect", "dagster",
        "argo", "argo workflows", "flink", "beam"
    }

    # Count AI-specific tools
    ai_tools_found = []
    for kw in techstack_keywords:
        kw_lower = kw.lower()
        if kw_lower in AI_SPECIFIC_TOOLS:
            ai_tools_found.append(kw)

    # Count AI infrastructure
    infra_found = []
    for kw in techstack_keywords:
        kw_lower = kw.lower()
        if kw_lower in AI_INFRASTRUCTURE and kw_lower not in [t.lower() for t in ai_tools_found]:
            infra_found.append(kw)

    # Calculate scores
    ai_tools_score = min(len(ai_tools_found) * 8, 40)      # 5 AI tools = max 40
    infra_score = min(len(infra_found) * 3, 30)            # 10 infra = max 30
    breadth_score = min(len(techstack_keywords) * 1, 30)   # 30 keywords = max 30

    total_score = ai_tools_score + infra_score + breadth_score

    # Calculate confidence
    if len(techstack_keywords) >= 20:
        confidence = 0.90
    elif len(techstack_keywords) >= 10:
        confidence = 0.75
    elif len(techstack_keywords) >= 5:
        confidence = 0.60
    else:
        confidence = 0.45

    return {
        "score": round(total_score, 1),
        "ai_tools_score": ai_tools_score,
        "infra_score": infra_score,
        "breadth_score": breadth_score,
        "ai_tools_found": ai_tools_found,
        "infra_found": infra_found,
        "total_keywords": len(techstack_keywords),
        "total_ai_tools": len(ai_tools_found),
        "score_breakdown": {
            "ai_tools_score": f"{ai_tools_score}/40 ({len(ai_tools_found)} AI tools)",
            "infra_score": f"{infra_score}/30 ({len(infra_found)} infra)",
            "breadth_score": f"{breadth_score}/30 ({len(techstack_keywords)} keywords)"
        },
        "confidence": confidence
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