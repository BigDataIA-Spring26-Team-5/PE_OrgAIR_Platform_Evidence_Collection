from __future__ import annotations

import asyncio
import json
import logging
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from app.config import settings
from app.models.signal import JobPosting
from app.pipelines.keywords import AI_KEYWORDS, AI_TECHSTACK_KEYWORDS, TOP_AI_TOOLS
from app.pipelines.pipeline2_state import Pipeline2State
from app.pipelines.utils import clean_nan, company_name_matches, safe_filename

logger = logging.getLogger(__name__)


def step1_init_job_collection(state: Pipeline2State) -> Pipeline2State:
    """Initialize job collection step."""
    state.mark_started()

    # Create output directory
    Path(state.output_dir).mkdir(parents=True, exist_ok=True)

    logger.info("-" * 40)
    logger.info("📁 [1/5] INITIALIZING JOB COLLECTION")
    logger.info(f"   Output directory: {state.output_dir}")
    return state


async def step2_fetch_job_postings(
    state: Pipeline2State,
    *,
    sites: Optional[List[str]] = None,
    results_wanted: Optional[int] = None,
    hours_old: Optional[int] = None,
) -> Pipeline2State:
    """
    Fetch job postings for each company using python-jobspy.

    Strategy:
    1. Search using company name as search term
    2. Post-filter results by matching the 'company' column from JobSpy
    3. This filters out jobs that just mention the company (e.g., "Microsoft Office skills")

    Args:
        state: Pipeline state
        sites: Job sites to scrape (default: from config)
        results_wanted: Max results to fetch (before filtering, default: from config)
        hours_old: Max age of job postings in hours (default: from config)
    """
    logger.info("-" * 40)
    logger.info("🔍 [2/5] FETCHING JOB POSTINGS")

    # Use config defaults if not provided
    if sites is None:
        sites = settings.JOBSPY_DEFAULT_SITES
    if results_wanted is None:
        results_wanted = settings.JOBSPY_RESULTS_WANTED
    if hours_old is None:
        hours_old = settings.JOBSPY_HOURS_OLD

    logger.info(f"   Sites: {', '.join(sites)}")
    logger.info(f"   Max results: {results_wanted}")
    logger.info(f"   Hours old: {hours_old}")

    # Try to import jobspy
    try:
        from jobspy import scrape_jobs
    except ImportError as e:
        error_msg = "python-jobspy not installed. Run: pip install python-jobspy"
        logger.error(f"   ❌ {error_msg}")
        state.add_error("job_fetch", "import", error_msg)
        raise ImportError(error_msg) from e

    for company in state.companies:
        company_id = company.get("id", "")
        company_name = company.get("name", "")

        if not company_name:
            continue

        # Rate limiting
        await asyncio.sleep(max(state.request_delay, settings.JOBSPY_REQUEST_DELAY))

        try:
            logger.info(f"   📥 Scraping: {company_name}...")

            # Scrape jobs - search by company name
            # JobSpy will return jobs where the search term appears in title/description/company
            jobs_df = scrape_jobs(
                site_name=sites,
                search_term=company_name,  # Search for company name
                results_wanted=results_wanted,
                hours_old=hours_old,
                country_indeed="USA",
            )

            postings = []
            filtered_count = 0
            total_raw = 0

            if jobs_df is not None and not jobs_df.empty:
                total_raw = len(jobs_df)

                for _, row in jobs_df.iterrows():
                    # Get the ACTUAL company name from the job posting
                    job_company = str(row.get("company", "")) if clean_nan(row.get("company")) else ""
                    source = str(row.get("site", "unknown"))

                    # Post-filter: verify the job's company matches our target
                    # This filters out jobs that just mention "Microsoft Office" etc.
                    if not company_name_matches(job_company, company_name):
                        filtered_count += 1
                        continue

                    posting = JobPosting(
                        company_id=company_id,
                        company_name=job_company,  # Use actual company name from job
                        title=str(row.get("title", "")),
                        description=str(row.get("description", "")),
                        location=str(row.get("location", "")) if clean_nan(row.get("location")) else None,
                        posted_date=clean_nan(row.get("date_posted")),
                        source=source,
                        url=str(row.get("job_url", "")) if clean_nan(row.get("job_url")) else None,
                    )
                    postings.append(posting)

            state.job_postings.extend([p.model_dump() for p in postings])
            state.summary["job_postings_collected"] += len(postings)

            logger.info(f"      • Raw results: {total_raw}")
            logger.info(f"      • Matched jobs: {len(postings)} (filtered {filtered_count} unrelated)")

        except Exception as e:
            state.add_error("job_fetch", company_id, str(e))
            logger.error(f"      ❌ Error: {e}")

    logger.info(f"   ✅ Total collected: {len(state.job_postings)} job postings")
    return state


def _has_keyword(text: str, keyword: str) -> bool:
    """
    Check if keyword exists in text with word boundary awareness.
    Handles short keywords like 'ai', 'ml' that could match parts of words.
    """
    import re

    # For very short keywords (2-3 chars), use word boundary matching
    if len(keyword) <= 3:
        # Match as whole word or with common separators
        pattern = r'(?:^|[\s,\-_/\(\)])' + re.escape(keyword) + r'(?:$|[\s,\-_/\(\)])'
        return bool(re.search(pattern, text, re.IGNORECASE))
    else:
        # For longer keywords, simple substring match is fine
        return keyword in text


def step3_classify_ai_jobs(state: Pipeline2State) -> Pipeline2State:
    """
    Classify job postings as AI-related using AI_KEYWORDS.

    Classification logic:
    - If description is available: require 2+ AI keywords
    - If title-only (no description): require 1+ AI keyword (more lenient)
    """
    logger.info("-" * 40)
    logger.info("🤖 [3/5] CLASSIFYING AI-RELATED JOBS")

    for posting in state.job_postings:
        title = posting.get('title', '')
        description = posting.get('description', '') or ''

        # Check if we have a real description
        has_description = description and description.lower() not in ('none', 'nan', '')

        # Combine title and description for searching
        text = f"{title} {description}".lower()
        title_lower = title.lower()

        # Find matching AI keywords
        ai_keywords_found = []
        for keyword in AI_KEYWORDS:
            if _has_keyword(text, keyword):
                ai_keywords_found.append(keyword)

        # Find matching tech stack keywords
        techstack_found = []
        for keyword in AI_TECHSTACK_KEYWORDS:
            if _has_keyword(text, keyword):
                techstack_found.append(keyword)

        posting["ai_keywords_found"] = ai_keywords_found
        posting["techstack_keywords_found"] = techstack_found

        # Determine if AI role based on keyword count
        # Lower threshold for title-only jobs (LinkedIn often doesn't return descriptions)
        if has_description:
            # With description: require JOBSPY_AI_KEYWORDS_THRESHOLD_WITH_DESC keywords for confidence
            posting["is_ai_role"] = len(ai_keywords_found) >= settings.JOBSPY_AI_KEYWORDS_THRESHOLD_WITH_DESC
        else:
            # Title-only: require JOBSPY_AI_KEYWORDS_THRESHOLD_NO_DESC keyword (title keywords are more reliable)
            posting["is_ai_role"] = len(ai_keywords_found) >= settings.JOBSPY_AI_KEYWORDS_THRESHOLD_NO_DESC

        # Calculate AI score (0-JOBSPY_MAX_SCORE)
        posting["ai_score"] = min(settings.JOBSPY_MAX_SCORE, len(ai_keywords_found) * settings.JOBSPY_AI_SCORE_MULTIPLIER)

    ai_count = sum(1 for p in state.job_postings if p.get("is_ai_role"))
    total_count = len(state.job_postings)

    logger.info(f"   • Total jobs analyzed: {total_count}")
    logger.info(f"   • AI-related jobs: {ai_count}")
    logger.info(f"   • AI job ratio: {(ai_count/total_count*100):.1f}%" if total_count > 0 else "   • AI job ratio: N/A")
    return state


def step4_score_job_market(state: Pipeline2State) -> Pipeline2State:
    """
    Calculate job market score for each company.

    Scoring algorithm:
    - Base: (AI jobs / Total jobs) * JOBSPY_RATIO_SCORE_WEIGHT
    - Volume bonus: min(JOBSPY_VOLUME_BONUS_MAX, AI_job_count * JOBSPY_VOLUME_BONUS_MULTIPLIER)
    - Keyword diversity: (unique AI keywords / JOBSPY_DIVERSITY_DENOMINATOR) * JOBSPY_DIVERSITY_SCORE_MULTIPLIER
    """
    logger.info("-" * 40)
    logger.info("📊 [4/5] SCORING JOB MARKET")

    company_jobs = defaultdict(list)
    for posting in state.job_postings:
        company_jobs[posting["company_id"]].append(posting)

    for company_id, jobs in company_jobs.items():
        if not jobs:
            state.job_market_scores[company_id] = 0.0
            continue

        ai_jobs = [j for j in jobs if j.get("is_ai_role")]
        total_jobs = len(jobs)
        ai_count = len(ai_jobs)

        # Ratio component (0-JOBSPY_RATIO_SCORE_WEIGHT points)
        ratio_score = (ai_count / total_jobs * settings.JOBSPY_RATIO_SCORE_WEIGHT) if total_jobs > 0 else 0

        # Volume bonus (0-JOBSPY_VOLUME_BONUS_MAX points)
        volume_bonus = min(settings.JOBSPY_VOLUME_BONUS_MAX, ai_count * settings.JOBSPY_VOLUME_BONUS_MULTIPLIER)

        # Keyword diversity (0-JOBSPY_DIVERSITY_SCORE_MAX points)
        all_keywords = set()
        for job in ai_jobs:
            all_keywords.update(job.get("ai_keywords_found", []))
        diversity_score = min(settings.JOBSPY_DIVERSITY_SCORE_MAX, len(all_keywords) * settings.JOBSPY_DIVERSITY_SCORE_MULTIPLIER)

        final_score = min(settings.JOBSPY_MAX_SCORE, ratio_score + volume_bonus + diversity_score)
        state.job_market_scores[company_id] = round(final_score, 2)

        # Get company name for logging
        company_name = jobs[0].get("company_name", company_id) if jobs else company_id
        logger.info(f"   • {company_name}: {final_score:.1f}/100 (ratio={ratio_score:.1f}, volume={volume_bonus:.1f}, diversity={diversity_score:.1f})")

    logger.info(f"   ✅ Scored {len(state.job_market_scores)} companies")
    return state


def step4b_score_techstack(state: Pipeline2State) -> Pipeline2State:
    """
    Calculate techstack score for each company based on AI tools presence.

    Process:
    1. Aggregate all unique techstack keywords from ALL jobs (not just AI jobs)
    2. Score based on presence of specific AI tools (TOP_AI_TOOLS)

    Scoring algorithm:
    - Base: (AI tools found / Total AI tools) * 50
    - Volume bonus: min(30, total_techstack_keywords * 1)
    - Top tools bonus: 5 points per TOP_AI_TOOLS keyword (max 20)
    """
    logger.info("-" * 40)
    logger.info("🔧 [4b/5] SCORING TECH STACK")

    company_jobs = defaultdict(list)
    for posting in state.job_postings:
        company_jobs[posting["company_id"]].append(posting)

    for company_id, jobs in company_jobs.items():
        if not jobs:
            state.techstack_scores[company_id] = 0.0
            state.company_techstacks[company_id] = []
            continue

        # Aggregate ALL unique techstack keywords from all jobs
        all_techstack_keywords = set()
        for job in jobs:
            all_techstack_keywords.update(job.get("techstack_keywords_found", []))

        # Store unique techstack for this company
        state.company_techstacks[company_id] = sorted(list(all_techstack_keywords))

        # Find AI-specific tools (intersection with TOP_AI_TOOLS)
        ai_tools_found = all_techstack_keywords & TOP_AI_TOOLS
        total_ai_tools = len(TOP_AI_TOOLS)

        # Base score: ratio of AI tools found (0-50 points)
        base_score = (len(ai_tools_found) / total_ai_tools * 50) if total_ai_tools > 0 else 0

        # Volume bonus: reward having more techstack keywords (0-30 points)
        volume_bonus = min(30, len(all_techstack_keywords) * 1)

        # Top tools bonus: extra points for specific high-value AI tools (0-20 points)
        top_tools_bonus = min(20, len(ai_tools_found) * 5)

        final_score = min(100.0, base_score + volume_bonus + top_tools_bonus)
        state.techstack_scores[company_id] = round(final_score, 2)

        # Get company name for logging
        company_name = jobs[0].get("company_name", company_id) if jobs else company_id
        logger.info(f"   • {company_name}: {final_score:.1f}/100 (keywords={len(all_techstack_keywords)}, ai_tools={len(ai_tools_found)})")

    logger.info(f"   ✅ Scored techstack for {len(state.techstack_scores)} companies")
    return state


def step5_save_to_json(state: Pipeline2State) -> Pipeline2State:
    """Save results to local JSON files (legacy/debug mode)."""
    logger.info("-" * 40)
    logger.info("💾 [5/5] SAVING RESULTS TO JSON")

    output_dir = Path(state.output_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Save all job postings
    all_jobs_file = output_dir / f"all_jobs_{timestamp}.json"
    with open(all_jobs_file, "w", encoding="utf-8") as f:
        json.dump(state.job_postings, f, indent=2, default=str)
    logger.info(f"   📄 All jobs: {all_jobs_file}")

    # Save AI-related jobs only
    ai_jobs = [p for p in state.job_postings if p.get("is_ai_role")]
    ai_jobs_file = output_dir / f"ai_jobs_{timestamp}.json"
    with open(ai_jobs_file, "w", encoding="utf-8") as f:
        json.dump(ai_jobs, f, indent=2, default=str)
    logger.info(f"   📄 AI jobs: {ai_jobs_file}")

    # Save per-company results
    company_jobs = defaultdict(list)
    for posting in state.job_postings:
        company_jobs[posting["company_id"]].append(posting)

    for company_id, jobs in company_jobs.items():
        # Get company name for filename
        company_name = jobs[0].get("company_name", company_id) if jobs else company_id
        safe_name = safe_filename(company_name)

        company_file = output_dir / f"{safe_name}_{timestamp}.json"
        company_data = {
            "company_id": company_id,
            "company_name": company_name,
            "total_jobs": len(jobs),
            "ai_jobs": sum(1 for j in jobs if j.get("is_ai_role")),
            "job_market_score": state.job_market_scores.get(company_id, 0),
            "techstack_score": state.techstack_scores.get(company_id, 0),
            "techstack_keywords": state.company_techstacks.get(company_id, []),
            "jobs": jobs
        }
        with open(company_file, "w", encoding="utf-8") as f:
            json.dump(company_data, f, indent=2, default=str)

    # Save summary
    summary_file = output_dir / f"summary_{timestamp}.json"
    summary_data = {
        **state.summary,
        "job_market_scores": state.job_market_scores,
        "techstack_scores": state.techstack_scores,
        "company_techstacks": state.company_techstacks,
        "companies": [c.get("name", c.get("id")) for c in state.companies]
    }
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, indent=2, default=str)
    logger.info(f"   📄 Summary: {summary_file}")

    logger.info(f"   ✅ Saved to {output_dir}")
    return state


def step5_store_to_s3_and_snowflake(state: Pipeline2State) -> Pipeline2State:
    """
    Store results in S3 (raw data) and Snowflake (aggregated signals).

    S3 Structure:
        signals/jobs/{ticker}/{timestamp}.json - All job postings for company
        signals/techstack/{ticker}/{timestamp}.json - Tech stack analysis for company

    Snowflake:
        external_signals table - One row per company with job_market score
    """
    from app.services.s3_storage import get_s3_service
    from app.services.snowflake import SnowflakeService

    logger.info("-" * 40)
    logger.info("☁️ [5/5] STORING TO S3 & SNOWFLAKE")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Initialize services
    s3 = get_s3_service()
    db = SnowflakeService()

    try:
        # Group jobs by company
        company_jobs = defaultdict(list)
        for posting in state.job_postings:
            company_jobs[posting["company_id"]].append(posting)

        for company_id, jobs in company_jobs.items():
            if not jobs:
                continue

            company_name = jobs[0].get("company_name", company_id)
            # Get ticker from company info in state
            ticker = None
            for company in state.companies:
                if company.get("id") == company_id:
                    ticker = company.get("ticker", "").upper()
                    break
            if not ticker:
                ticker = safe_filename(company_name).upper()

            # -----------------------------------------
            # S3: Upload raw job postings
            # -----------------------------------------
            jobs_s3_key = f"signals/jobs/{ticker}/{timestamp}.json"
            jobs_data = {
                "company_id": company_id,
                "company_name": company_name,
                "ticker": ticker,
                "collection_date": timestamp,
                "total_jobs": len(jobs),
                "ai_jobs": sum(1 for j in jobs if j.get("is_ai_role")),
                "job_market_score": state.job_market_scores.get(company_id, 0),
                "jobs": jobs
            }
            s3.upload_json(jobs_data, jobs_s3_key)
            logger.info(f"   📤 S3 Jobs: {jobs_s3_key}")

            # -----------------------------------------
            # S3: Upload tech stack analysis
            # -----------------------------------------
            techstack_keywords = state.company_techstacks.get(company_id, [])
            techstack_score = state.techstack_scores.get(company_id, 0.0)
            ai_tools_found = list(set(techstack_keywords) & TOP_AI_TOOLS)

            techstack_s3_key = f"signals/techstack/{ticker}/{timestamp}.json"
            techstack_data = {
                "company_id": company_id,
                "company_name": company_name,
                "ticker": ticker,
                "collection_date": timestamp,
                "techstack_score": techstack_score,
                "all_keywords": techstack_keywords,
                "ai_tools_found": ai_tools_found,
                "total_keywords": len(techstack_keywords),
                "total_ai_tools": len(ai_tools_found)
            }
            s3.upload_json(techstack_data, techstack_s3_key)
            logger.info(f"   📤 S3 TechStack: {techstack_s3_key}")

            # -----------------------------------------
            # Snowflake: Insert external signal
            # -----------------------------------------
            ai_jobs = [j for j in jobs if j.get("is_ai_role")]
            score = state.job_market_scores.get(company_id, 0.0)
            total_jobs = len(jobs)
            ai_count = len(ai_jobs)

            # Calculate score breakdown for raw_payload
            ratio_score = (ai_count / total_jobs * settings.JOBSPY_RATIO_SCORE_WEIGHT) if total_jobs > 0 else 0
            volume_bonus = min(settings.JOBSPY_VOLUME_BONUS_MAX, ai_count * settings.JOBSPY_VOLUME_BONUS_MULTIPLIER)
            all_keywords = set()
            for job in ai_jobs:
                all_keywords.update(job.get("ai_keywords_found", []))
            diversity_score = min(settings.JOBSPY_DIVERSITY_SCORE_MAX, len(all_keywords) * settings.JOBSPY_DIVERSITY_SCORE_MULTIPLIER)

            # Build summary text
            summary = f"Found {ai_count} AI roles out of {total_jobs} total jobs"

            # Build raw_payload with detailed metrics
            raw_payload = {
                "collection_date": timestamp,
                "s3_jobs_key": jobs_s3_key,
                "s3_techstack_key": techstack_s3_key,
                "total_jobs": total_jobs,
                "ai_jobs": ai_count,
                "score_breakdown": {
                    "ratio_score": round(ratio_score, 2),
                    "volume_bonus": round(volume_bonus, 2),
                    "diversity_score": round(diversity_score, 2),
                },
                "top_ai_keywords": list(all_keywords)[:settings.JOBSPY_TOP_KEYWORDS_LIMIT],
                "sources": list(set(j.get("source", "unknown") for j in jobs)),
                "techstack": {
                    "score": techstack_score,
                    "all_keywords": techstack_keywords,
                    "ai_tools_found": ai_tools_found,
                },
            }

            # Determine primary source
            source_counts = defaultdict(int)
            for job in jobs:
                source_counts[job.get("source", "other")] += 1
            primary_source = max(source_counts, key=source_counts.get) if source_counts else "other"

            # Insert into Snowflake
            signal_id = f"{company_id}_job_market_{timestamp}"
            db.insert_external_signal(
                signal_id=signal_id,
                company_id=company_id,
                category="job_market",
                source=primary_source,
                score=score,
                evidence_count=ai_count,
                summary=summary,
                raw_payload=raw_payload,
            )
            logger.info(f"   💾 Snowflake: {company_name} (score: {score})")

        logger.info(f"   ✅ Stored {len(company_jobs)} companies in S3 + Snowflake")
        return state

    finally:
        db.close()


async def run_job_signals(
    state: Pipeline2State,
    *,
    use_cloud_storage: bool = True,
) -> Pipeline2State:
    """
    Run the complete job signals collection pipeline.

    Args:
        state: Pipeline state with companies loaded
        use_cloud_storage: If True, store in S3 + Snowflake. If False, save to local JSON only.

    Returns:
        Updated pipeline state with job postings and scores
    """
    state = step1_init_job_collection(state)
    state = await step2_fetch_job_postings(state)
    state = step3_classify_ai_jobs(state)
    state = step4_score_job_market(state)
    state = step4b_score_techstack(state)

    if use_cloud_storage:
        state = step5_store_to_s3_and_snowflake(state)
    else:
        state = step5_save_to_json(state)

    state.mark_completed()
    return state
