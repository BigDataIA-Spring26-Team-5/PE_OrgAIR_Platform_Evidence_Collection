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
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import httpx
from dotenv import load_dotenv

from app.pipelines.pipeline2_state import Pipeline2State
from app.pipelines.keywords import PATENT_AI_KEYWORDS, AI_KEYWORDS
from app.pipelines.utils import clean_nan, safe_filename
from app.models.signal import Patent

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)

# PatentsView PatentSearch API configuration (new API as of 2024)
# Docs: https://search.patentsview.org/docs/docs/Search%20API/SearchAPIReference/
PATENTSVIEW_API_URL = os.getenv("PATENTSVIEW_API_URL", "https://search.patentsview.org/api/v1/patent/")
PATENTSVIEW_REQUEST_DELAY = 1.5  # Rate limiting (45 req/min = 1.33s minimum)
PATENTSVIEW_API_KEY = os.getenv("PATENTSVIEW_API_KEY")


def step1_init_patent_collection(state: Pipeline2State) -> Pipeline2State:
    """Initialize patent collection step."""
    # Create output directory
    Path(state.output_dir).mkdir(parents=True, exist_ok=True)

    logger.info("-" * 40)
    logger.info("📁 [1/5] INITIALIZING PATENT COLLECTION")
    logger.info(f"   Output directory: {state.output_dir}")
    return state


async def step2_fetch_patents(
    state: Pipeline2State,
    *,
    years_back: int = 5,
    results_per_company: int = 100,
    api_key: Optional[str] = None,
) -> Pipeline2State:
    """
    Fetch patents for each company using PatentsView PatentSearch API.

    Args:
        state: Pipeline state
        years_back: How many years back to search (default: 5)
        results_per_company: Max results per company (max 1000)
        api_key: PatentsView API key (or set PATENTSVIEW_API_KEY env var)
    """
    logger.info("-" * 40)
    logger.info("🔍 [2/5] FETCHING PATENTS FROM PATENTSVIEW")

    # Get API key from parameter, module constant, or environment
    api_key = api_key or PATENTSVIEW_API_KEY
    if api_key:
        logger.info(f"   API Key: {api_key[:8]}...")
    else:
        logger.warning("   ⚠️ No API key provided (may have rate limits)")
        logger.info("   Get free API key at: https://patentsview.org/apis/keyrequest")

    # Calculate date range (past N years)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=years_back * 365)
    start_date_str = start_date.strftime("%Y-%m-%d")

    logger.info(f"   Date range: {start_date_str} to {end_date.strftime('%Y-%m-%d')}")
    logger.info(f"   Max results per company: {results_per_company}")

    # Build headers
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-Api-Key"] = api_key

    async with httpx.AsyncClient(timeout=30.0) as client:
        for company in state.companies:
            company_id = company.get("id", "")
            company_name = company.get("name", "")

            if not company_name:
                continue

            # Rate limiting (45 req/min with key, stricter without)
            await asyncio.sleep(max(state.request_delay, PATENTSVIEW_REQUEST_DELAY))

            try:
                logger.info(f"   📥 Fetching: {company_name}...")

                # Build PatentSearch API query (new format)
                # Docs: https://search.patentsview.org/docs/docs/Search%20API/SearchAPIReference/
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
                    "cpc_current.cpc_group_id"
                ]

                # Build URL with query params
                params = {
                    "q": json.dumps(query_obj),
                    "f": json.dumps(fields),
                    "s": json.dumps([{"patent_date": "desc"}]),
                    "o": json.dumps({"size": min(results_per_company, 1000)})
                }

                response = await client.get(
                    PATENTSVIEW_API_URL,
                    params=params,
                    headers=headers
                )
                response.raise_for_status()
                data = response.json()

                patents_data = data.get("patents", []) or []
                postings = []

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

                    # Extract CPC codes
                    cpcs = patent_data.get("cpc_current") or []
                    cpc_codes = [c.get("cpc_group_id", "") for c in cpcs if c.get("cpc_group_id")]

                    # Parse date
                    patent_date_str = clean_nan(patent_data.get("patent_date"))
                    patent_date = None
                    if patent_date_str:
                        try:
                            patent_date = datetime.strptime(patent_date_str, "%Y-%m-%d")
                        except ValueError:
                            pass

                    patent = Patent(
                        company_id=company_id,
                        company_name=company_name,
                        patent_id=str(patent_data.get("patent_id", "")),
                        patent_number=str(patent_data.get("patent_id", "")),  # patent_id is the number in new API
                        title=str(patent_data.get("patent_title", "")),
                        abstract=str(patent_data.get("patent_abstract", "") or ""),
                        patent_date=patent_date,
                        patent_type=str(patent_data.get("patent_type", "")),
                        assignees=assignee_names,
                        inventors=inventor_names,
                        cpc_codes=cpc_codes,
                    )
                    postings.append(patent)

                state.patents.extend([p.model_dump() for p in postings])
                state.summary["patents_collected"] = state.summary.get("patents_collected", 0) + len(postings)
                logger.info(f"      • Patents found: {len(postings)}")

            except httpx.HTTPStatusError as e:
                error_msg = f"HTTP {e.response.status_code}: {e.response.text[:200]}"
                state.add_error("patent_fetch", company_id, error_msg)
                logger.error(f"      ❌ Error: {error_msg}")
            except Exception as e:
                state.add_error("patent_fetch", company_id, str(e))
                logger.error(f"      ❌ Error: {e}")

    logger.info(f"   ✅ Total collected: {len(state.patents)} patents")
    return state


def step3_classify_ai_patents(state: Pipeline2State) -> Pipeline2State:
    """Classify patents as AI-related using PATENT_AI_KEYWORDS and AI_KEYWORDS."""
    logger.info("-" * 40)
    logger.info("🤖 [3/5] CLASSIFYING AI-RELATED PATENTS")

    # Combine patent-specific and general AI keywords
    all_ai_keywords = PATENT_AI_KEYWORDS | AI_KEYWORDS

    # Pre-compile regex patterns for word boundary matching
    # This prevents false positives like "ai" matching "aisle" or "rag" matching "storage"
    keyword_patterns = {
        keyword: re.compile(r'\b' + re.escape(keyword) + r'\b', re.IGNORECASE)
        for keyword in all_ai_keywords
    }

    for patent in state.patents:
        text = f"{patent.get('title', '')} {patent.get('abstract', '')}"

        # Find matching AI keywords using word boundary matching
        ai_keywords_found = []
        for keyword, pattern in keyword_patterns.items():
            if pattern.search(text):
                ai_keywords_found.append(keyword)

        patent["ai_keywords_found"] = ai_keywords_found
        patent["is_ai_patent"] = len(ai_keywords_found) >= 1  # At least 1 keyword for patents

        # Calculate AI score (0-100)
        patent["ai_score"] = min(100.0, len(ai_keywords_found) * 20.0)

    ai_count = sum(1 for p in state.patents if p.get("is_ai_patent"))
    total_count = len(state.patents)

    logger.info(f"   • Total patents analyzed: {total_count}")
    logger.info(f"   • AI-related patents: {ai_count}")
    logger.info(f"   • AI patent ratio: {(ai_count/total_count*100):.1f}%" if total_count > 0 else "   • AI patent ratio: N/A")
    return state


def step4_score_patent_portfolio(state: Pipeline2State) -> Pipeline2State:
    """
    Calculate patent portfolio score for each company.

    Scoring algorithm:
    - Base: (AI patents / Total patents) * 40
    - Volume bonus: min(30, AI_patent_count * 2)
    - Recency bonus: (patents in last 2 years / total) * 20
    - Keyword diversity: (unique AI keywords / 10) * 10
    """
    logger.info("-" * 40)
    logger.info("📊 [4/5] SCORING PATENT PORTFOLIO")

    company_patents = defaultdict(list)
    for patent in state.patents:
        company_patents[patent["company_id"]].append(patent)

    two_years_ago = datetime.now() - timedelta(days=730)

    for company_id, patents in company_patents.items():
        if not patents:
            state.patent_scores[company_id] = 0.0
            continue

        ai_patents = [p for p in patents if p.get("is_ai_patent")]
        total_patents = len(patents)
        ai_count = len(ai_patents)

        # Ratio component (0-40 points)
        ratio_score = (ai_count / total_patents * 40) if total_patents > 0 else 0

        # Volume bonus (0-30 points)
        volume_bonus = min(30, ai_count * 2)

        # Recency bonus (0-20 points)
        recent_patents = 0
        for p in ai_patents:
            patent_date = p.get("patent_date")
            if patent_date:
                if isinstance(patent_date, str):
                    try:
                        patent_date = datetime.fromisoformat(patent_date.replace("Z", "+00:00"))
                    except ValueError:
                        continue
                if patent_date.replace(tzinfo=None) > two_years_ago:
                    recent_patents += 1
        recency_score = (recent_patents / ai_count * 20) if ai_count > 0 else 0

        # Keyword diversity (0-10 points)
        all_keywords = set()
        for patent in ai_patents:
            all_keywords.update(patent.get("ai_keywords_found", []))
        diversity_score = min(10, len(all_keywords))

        final_score = min(100.0, ratio_score + volume_bonus + recency_score + diversity_score)
        state.patent_scores[company_id] = round(final_score, 2)

        # Get company name for logging
        company_name = patents[0].get("company_name", company_id) if patents else company_id
        logger.info(f"   • {company_name}: {final_score:.1f}/100 (ratio={ratio_score:.1f}, volume={volume_bonus:.1f}, recency={recency_score:.1f}, diversity={diversity_score:.1f})")

    logger.info(f"   ✅ Scored {len(state.patent_scores)} companies")
    return state


def step5_save_patent_results(state: Pipeline2State) -> Pipeline2State:
    """Save patent results to local JSON files (legacy/debug mode)."""
    logger.info("-" * 40)
    logger.info("💾 [5/5] SAVING PATENT RESULTS TO JSON")

    output_dir = Path(state.output_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Save all patents
    all_patents_file = output_dir / f"all_patents_{timestamp}.json"
    with open(all_patents_file, "w", encoding="utf-8") as f:
        json.dump(state.patents, f, indent=2, default=str)
    logger.info(f"   📄 All patents: {all_patents_file}")

    # Save AI-related patents only
    ai_patents = [p for p in state.patents if p.get("is_ai_patent")]
    ai_patents_file = output_dir / f"ai_patents_{timestamp}.json"
    with open(ai_patents_file, "w", encoding="utf-8") as f:
        json.dump(ai_patents, f, indent=2, default=str)
    logger.info(f"   📄 AI patents: {ai_patents_file}")

    # Save per-company results
    company_patents = defaultdict(list)
    for patent in state.patents:
        company_patents[patent["company_id"]].append(patent)

    for company_id, patents in company_patents.items():
        # Get company name for filename
        company_name = patents[0].get("company_name", company_id) if patents else company_id
        safe_name = safe_filename(company_name)

        company_file = output_dir / f"{safe_name}_patents_{timestamp}.json"
        company_data = {
            "company_id": company_id,
            "company_name": company_name,
            "total_patents": len(patents),
            "ai_patents": sum(1 for p in patents if p.get("is_ai_patent")),
            "patent_portfolio_score": state.patent_scores.get(company_id, 0),
            "patents": patents
        }
        with open(company_file, "w", encoding="utf-8") as f:
            json.dump(company_data, f, indent=2, default=str)

    # Save summary
    summary_file = output_dir / f"patent_summary_{timestamp}.json"
    summary_data = {
        "patents_collected": len(state.patents),
        "ai_patents_found": len(ai_patents),
        "patent_scores": state.patent_scores,
        "companies": [c.get("name", c.get("id")) for c in state.companies],
        "errors": state.summary.get("errors", []),
    }
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, indent=2, default=str)
    logger.info(f"   📄 Summary: {summary_file}")

    logger.info(f"   ✅ Saved to {output_dir}")
    return state


def step5_store_to_s3_and_snowflake(state: Pipeline2State) -> Pipeline2State:
    """
    Store patent results in S3 (raw data) and Snowflake (aggregated signals).

    S3 Structure:
        signals/patents/{ticker}/{timestamp}.json - All patents for company

    Snowflake:
        external_signals table - One row per company with patent_portfolio score
    """
    from app.services.s3_storage import get_s3_service
    from app.services.snowflake import SnowflakeService

    logger.info("-" * 40)
    logger.info("☁️ [5/5] STORING PATENTS TO S3 & SNOWFLAKE")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Initialize services
    s3 = get_s3_service()
    db = SnowflakeService()

    try:
        # Group patents by company
        company_patents = defaultdict(list)
        for patent in state.patents:
            company_patents[patent["company_id"]].append(patent)

        for company_id, patents in company_patents.items():
            if not patents:
                continue

            company_name = patents[0].get("company_name", company_id)
            # Get ticker from company info in state
            ticker = None
            for company in state.companies:
                if company.get("id") == company_id:
                    ticker = company.get("ticker", "").upper()
                    break
            if not ticker:
                ticker = safe_filename(company_name).upper()

            ai_patents = [p for p in patents if p.get("is_ai_patent")]
            total_patents = len(patents)
            ai_count = len(ai_patents)
            score = state.patent_scores.get(company_id, 0.0)

            # -----------------------------------------
            # S3: Upload patent data
            # -----------------------------------------
            s3_key = f"signals/patents/{ticker}/{timestamp}.json"
            patent_data = {
                "company_id": company_id,
                "company_name": company_name,
                "ticker": ticker,
                "collection_date": timestamp,
                "total_patents": total_patents,
                "ai_patents": ai_count,
                "patent_portfolio_score": score,
                "patents": patents
            }
            s3.upload_json(patent_data, s3_key)
            logger.info(f"   📤 S3: {s3_key}")

            # -----------------------------------------
            # Snowflake: Insert external signal
            # -----------------------------------------
            # Collect all AI keywords found
            all_keywords = set()
            for patent in ai_patents:
                all_keywords.update(patent.get("ai_keywords_found", []))

            # Build summary text
            summary = f"Found {ai_count} AI patents out of {total_patents} total patents"

            # Build raw_payload with detailed metrics
            raw_payload = {
                "collection_date": timestamp,
                "s3_key": s3_key,
                "total_patents": total_patents,
                "ai_patents": ai_count,
                "ai_ratio": round(ai_count / total_patents * 100, 1) if total_patents > 0 else 0,
                "top_ai_keywords": list(all_keywords)[:20],
                "recent_patents": [
                    {
                        "patent_id": p.get("patent_id"),
                        "title": p.get("title"),
                        "date": str(p.get("patent_date", "")),
                        "is_ai": p.get("is_ai_patent", False)
                    }
                    for p in sorted(patents, key=lambda x: x.get("patent_date") or "", reverse=True)[:10]
                ]
            }

            # Insert into Snowflake
            signal_id = f"{company_id}_patent_portfolio_{timestamp}"
            db.insert_external_signal(
                signal_id=signal_id,
                company_id=company_id,
                category="patent_portfolio",
                source="patentsview",
                score=score,
                evidence_count=ai_count,
                summary=summary,
                raw_payload=raw_payload,
            )
            logger.info(f"   💾 Snowflake: {company_name} (score: {score})")

        logger.info(f"   ✅ Stored {len(company_patents)} companies in S3 + Snowflake")
        return state

    finally:
        db.close()


async def run_patent_signals(
    state: Pipeline2State,
    years_back: int = 5,
    results_per_company: int = 100,
    api_key: Optional[str] = None,
    skip_storage: bool = False,
    use_local_storage: bool = False,
) -> Pipeline2State:
    """
    Run the patent signals collection pipeline (extract, classify, score).

    Args:
        state: Pipeline state with companies loaded
        years_back: How many years back to search (default: 5)
        results_per_company: Max patents per company
        api_key: PatentsView API key (optional, or set PATENTSVIEW_API_KEY env var)
        skip_storage: If True, skip all storage steps (for pipeline2_runner integration)
        use_local_storage: If True and skip_storage=False, save to local JSON instead of S3/Snowflake

    Returns:
        Updated pipeline state with patents, classifications, and scores
    """
    # Step 1: Initialize
    state = step1_init_patent_collection(state)

    # Step 2: Fetch patents
    state = await step2_fetch_patents(
        state,
        years_back=years_back,
        results_per_company=results_per_company,
        api_key=api_key,
    )

    # Step 3: Classify AI patents
    state = step3_classify_ai_patents(state)

    # Step 4: Score patent portfolio
    state = step4_score_patent_portfolio(state)

    # Step 5: Storage (optional - pipeline2_runner handles this separately)
    if not skip_storage:
        if use_local_storage:
            state = step5_save_patent_results(state)
        else:
            state = step5_store_to_s3_and_snowflake(state)

    return state
