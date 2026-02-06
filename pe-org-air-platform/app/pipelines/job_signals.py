from __future__ import annotations
import asyncio
import json
import logging
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any
from app.config import (
    settings,
    get_company_search_name,
    get_company_aliases,
    get_search_name_by_official,
    get_aliases_by_official,
    COMPANY_NAME_MAPPINGS
)
from app.models.signal import JobPosting
from app.pipelines.keywords import AI_KEYWORDS, AI_TECHSTACK_KEYWORDS, TOP_AI_TOOLS
from app.pipelines.pipeline2_state import Pipeline2State
from app.pipelines.utils import clean_nan, safe_filename
from app.pipelines.tech_signals import (
    TechStackCollector, TechnologyDetection,
    calculate_techstack_score, create_external_signal_from_techstack,
    log_techstack_results
)

# RapidFuzz for fuzzy company name matching
from rapidfuzz import fuzz

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

def is_company_match_fuzzy(
    job_company: str,
    target_company: str,
    threshold: float = 75.0,
    ticker: Optional[str] = None
) -> bool:
    """
    Use fuzzy matching to determine if job company matches target company.

    Args:
        job_company: Company name from job posting
        target_company: Target company we're searching for
        threshold: Similarity score threshold (0-100)
        ticker: Optional ticker to use predefined aliases for matching

    Returns:
        True if companies match with sufficient similarity
    """
    if not job_company or not target_company:
        return False

    # Clean the job company string
    job_clean = str(job_company).strip().lower()

    # Build list of valid names to match against
    valid_names = [target_company]

    # Add aliases from config if ticker is provided
    if ticker:
        aliases = get_company_aliases(ticker)
        valid_names.extend(aliases)
    else:
        # Try to find aliases by official name
        aliases = get_aliases_by_official(target_company)
        if aliases:
            valid_names.extend(aliases)

    # Remove duplicates and clean
    valid_names_clean = list(set(name.strip().lower() for name in valid_names if name))

    # Check for exact match first
    if job_clean in valid_names_clean:
        return True

    # Fuzzy match against all valid names
    for valid_name in valid_names_clean:
        scores = [
            fuzz.token_sort_ratio(job_clean, valid_name),  # Handles word order
            fuzz.partial_ratio(job_clean, valid_name),     # Handles substrings
            fuzz.ratio(job_clean, valid_name),             # Simple ratio
        ]
        if max(scores) >= threshold:
            return True

    return False

async def step2_fetch_job_postings(
    state: Pipeline2State,
    *,
    sites: Optional[List[str]] = None,
    results_wanted: Optional[int] = None,
    hours_old: Optional[int] = None,
) -> Pipeline2State:
    
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
    logger.info(f"   Fuzzy match threshold: {settings.JOBSPY_FUZZY_MATCH_THRESHOLD}%")
    
    # Try to import jobspy
    try:
        from jobspy import scrape_jobs
    except ImportError as e:
        error_msg = "python-jobspy not installed. Run: pip install python-jobspy"
        logger.error(f"   ❌ {error_msg}")
        state.add_error("job_fetch", "import", error_msg)
        raise ImportError(error_msg) from e
    
    # Initialize tech stack collector
    tech_collector = TechStackCollector()
    
    for company in state.companies:
        company_id = company.get("id", "")
        company_name = company.get("name", "")
        ticker = company.get("ticker", "").upper()

        if not company_name:
            continue

        # Get search name from mappings (falls back to company_name if not mapped)
        search_name = None
        if ticker:
            search_name = get_company_search_name(ticker)
        if not search_name:
            search_name = get_search_name_by_official(company_name)
        if not search_name:
            search_name = company_name  # Fallback to original name

        # Rate limiting
        await asyncio.sleep(max(state.request_delay, settings.JOBSPY_REQUEST_DELAY))

        try:
            logger.info(f"   📥 Scraping: {company_name} (search: '{search_name}')...")

            # Scrape jobs - search by mapped search name
            jobs_df = scrape_jobs(
                site_name=sites,
                search_term=search_name,
                results_wanted=results_wanted,
                hours_old=hours_old,
                country_indeed="USA",
                linkedin_fetch_description=True,  # Get full descriptions
            )
            
            postings = []
            filtered_count = 0
            total_raw = 0
            
            if jobs_df is not None and not jobs_df.empty:
                total_raw = len(jobs_df)
                
                # Log a few samples for debugging
                sample_companies = jobs_df['company'].head(3).tolist()
                logger.debug(f"      Sample company names from scraped jobs: {sample_companies}")
                
                for _, row in jobs_df.iterrows():
                    # Get the ACTUAL company name from the job posting
                    job_company = str(row.get("company", "")) if clean_nan(row.get("company")) else ""
                    source = str(row.get("site", "unknown"))
                    
                    # Use fuzzy matching with aliases instead of strict matching
                    if not is_company_match_fuzzy(
                        job_company,
                        search_name,
                        threshold=settings.JOBSPY_FUZZY_MATCH_THRESHOLD,
                        ticker=ticker
                    ):
                        filtered_count += 1
                        
                        # Log first few filtered items for debugging
                        if filtered_count <= 3:
                            logger.debug(f"      Filtered: '{job_company}' vs '{company_name}'")
                        continue
                    
                    # Create JobPosting instance
                    posting = JobPosting(
                        company_id=company_id,
                        company_name=job_company,
                        title=str(row.get("title", "")),
                        description=str(row.get("description", "")),
                        location=str(row.get("location", "")) if clean_nan(row.get("location")) else None,
                        posted_date=clean_nan(row.get("date_posted")),
                        source=source,
                        url=str(row.get("job_url", "")) if clean_nan(row.get("job_url")) else None,
                    )
                    
                    # Detect technologies using TechStackCollector
                    description_text = posting.description or ""
                    tech_detections = tech_collector.detect_technologies_from_text(
                        f"{posting.title} {description_text}"
                    )
                    
                    # Convert to dict for storage
                    posting_dict = posting.model_dump()
                    posting_dict["tech_detections"] = [
                        {
                            "name": t.name,
                            "category": t.category,
                            "is_ai_related": t.is_ai_related,
                            "confidence": t.confidence
                        }
                        for t in tech_detections
                    ]
                    
                    postings.append(posting_dict)
            
            state.job_postings.extend(postings)
            state.summary["job_postings_collected"] += len(postings)
            
            logger.info(f"      • Raw results: {total_raw}")
            logger.info(f"      • Matched jobs: {len(postings)} (filtered {filtered_count} unrelated)")
            
            # If we filtered everything out, log warning
            if total_raw > 0 and len(postings) == 0:
                logger.warning(f"      ⚠️  No jobs matched after filtering. Try lowering JOBSPY_FUZZY_MATCH_THRESHOLD")
                
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
        if has_description:
            posting["is_ai_role"] = len(ai_keywords_found) >= settings.JOBSPY_AI_KEYWORDS_THRESHOLD_WITH_DESC
        else:
            posting["is_ai_role"] = len(ai_keywords_found) >= settings.JOBSPY_AI_KEYWORDS_THRESHOLD_NO_DESC
        
        # Calculate AI score (0-JOBSPY_MAX_SCORE)
        posting["ai_score"] = min(settings.JOBSPY_MAX_SCORE, len(ai_keywords_found) * settings.JOBSPY_AI_SCORE_MULTIPLIER)
    
    ai_count = sum(1 for p in state.job_postings if p.get("is_ai_role"))
    total_count = len(state.job_postings)
    
    logger.info(f"   • Total jobs analyzed: {total_count}")
    logger.info(f"   • AI-related jobs: {ai_count}")
    if total_count > 0:
        logger.info(f"   • AI job ratio: {(ai_count/total_count*100):.1f}%")
    
    return state


def calculate_job_score(jobs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calculate job market score for AI hiring signals.

    Uses pre-classified AI roles from step3 (is_ai_role flag) for consistency.

    Scoring Algorithm (0-100):
    - AI Ratio Score: (ai_jobs / total_jobs) * 40 (max 40 points)
    - AI Volume Score: ai_jobs * 2 (max 30 points, need 15 AI jobs)
    - Skill Diversity: (unique_ai_keywords / 15) * 30 (max 30 points)
    """
    total_jobs = len(jobs)

    # Use pre-classified AI roles from step3
    ai_jobs = sum(1 for job in jobs if job.get('is_ai_role', False))

    # Collect unique AI keywords found across all jobs
    all_ai_keywords = set()
    for job in jobs:
        all_ai_keywords.update(job.get('ai_keywords_found', []))

    # Calculate AI ratio
    ai_ratio = ai_jobs / total_jobs if total_jobs > 0 else 0

    # Calculate score components (updated - harder to max out)
    ratio_score = min(ai_ratio * 100, 40)  # 40% AI jobs = max 40 points
    volume_score = min(ai_jobs * 2, 30)     # 15 AI jobs = max 30 points
    diversity_score = min(len(all_ai_keywords) / 15, 1) * 30  # 15 keywords = max 30 points

    final_score = ratio_score + volume_score + diversity_score

    # Calculate confidence based on sample size
    if total_jobs >= 100:
        confidence = 0.95
    elif total_jobs >= 50:
        confidence = 0.85
    elif total_jobs >= 20:
        confidence = 0.70
    else:
        confidence = 0.50

    return {
        "score": round(final_score, 1),
        "ai_jobs": ai_jobs,
        "total_jobs": total_jobs,
        "ai_ratio": round(ai_ratio, 3),
        "ai_keywords": list(all_ai_keywords),
        "score_breakdown": {
            "ratio_score": round(ratio_score, 1),
            "volume_score": round(volume_score, 1),
            "diversity_score": round(diversity_score, 1)
        },
        "confidence": confidence
    }


def step4_score_job_market(state: Pipeline2State) -> Pipeline2State:
    """
    Calculate job market score for each company using the main scoring algorithm.
    """
    logger.info("-" * 40)
    logger.info("📊 [4/5] SCORING JOB MARKET")

    # Build lookup for target company names
    company_name_lookup = {c.get("id"): c.get("name", c.get("id")) for c in state.companies}

    # Group all jobs by TARGET company_id (from our database, not job site)
    company_jobs = defaultdict(list)
    for posting in state.job_postings:
        company_jobs[posting["company_id"]].append(posting)

    for company_id, jobs in company_jobs.items():
        if not jobs:
            state.job_market_scores[company_id] = 0.0
            continue

        # Use main scoring algorithm
        analysis = calculate_job_score(jobs)

        final_score = analysis["score"]
        state.job_market_scores[company_id] = round(final_score, 2)

        # Store analysis for later use
        state.job_market_analyses[company_id] = analysis

        # Use TARGET company name from state, not job posting company name
        company_name = company_name_lookup.get(company_id, company_id)
        breakdown = analysis["score_breakdown"]
        logger.info(
            f"   • {company_name}: {final_score:.1f}/100 "
            f"(ratio={breakdown['ratio_score']:.1f}, volume={breakdown['volume_score']:.1f}, diversity={breakdown['diversity_score']:.1f})"
        )

    logger.info(f"   ✅ Scored {len(state.job_market_scores)} companies")
    return state


def step4b_score_techstack(state: Pipeline2State) -> Pipeline2State:
    """
    Calculate techstack score for each company based on AI tools presence.

    Uses both original scoring and TechStackCollector analysis.
    """
    logger.info("-" * 40)
    logger.info("🔧 [4b/5] SCORING TECH STACK")

    # Build lookup for target company names
    company_name_lookup = {c.get("id"): c.get("name", c.get("id")) for c in state.companies}

    tech_collector = TechStackCollector()
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
        
        # Aggregate technology detections
        all_tech_detections = []
        for job in jobs:
            tech_detections = job.get("tech_detections", [])
            for tech in tech_detections:
                all_tech_detections.append(
                    TechnologyDetection(
                        name=tech["name"],
                        category=tech["category"],
                        is_ai_related=tech["is_ai_related"],
                        confidence=tech["confidence"]
                    )
                )
        
        # Store unique techstack for this company
        state.company_techstacks[company_id] = sorted(list(all_techstack_keywords))
        
        # Calculate original techstack score
        original_analysis = calculate_techstack_score(all_techstack_keywords, all_tech_detections)
        original_score = original_analysis["score"]
        
        # TechStackCollector scoring
        collector_analysis = tech_collector.analyze_tech_stack(
            company_id=company_id,
            technologies=all_tech_detections
        )
        collector_score = collector_analysis["score"]
        
        # Combine scores (weighted average)
        collector_weight = 0.3  # 30% weight to collector score
        original_weight = 0.7   # 70% weight to original score
        
        final_score = (
            original_score * original_weight +
            collector_score * collector_weight
        )
        
        state.techstack_scores[company_id] = round(final_score, 2)
        state.techstack_analyses[company_id] = {
            "original": original_analysis,
            "collector": collector_analysis,
            "combined_score": final_score
        }
        
        # Use TARGET company name from state, not job posting company name
        company_name = company_name_lookup.get(company_id, company_id)
        log_techstack_results(
            company_name=company_name,
            original_score=original_score,
            collector_score=collector_score,
            final_score=final_score,
            keywords_count=len(all_techstack_keywords),
            ai_tools_count=len(original_analysis.get("ai_tools_found", []))
        )

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
            "techstack_analysis": state.techstack_analyses.get(company_id, {}),
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
        "techstack_analyses": state.techstack_analyses,
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
            ticker = None
            for company in state.companies:
                if company.get("id") == company_id:
                    ticker = company.get("ticker", "").upper()
                    break
            if not ticker:
                ticker = safe_filename(company_name).upper()
            
            # Get tech stack analyses
            techstack_analysis = state.techstack_analyses.get(company_id, {})
            original_analysis = techstack_analysis.get("original", {})
            collector_analysis = techstack_analysis.get("collector", {})
            
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
                "techstack_score": state.techstack_scores.get(company_id, 0),
                "techstack_analysis": techstack_analysis,
                "jobs": jobs
            }
            s3.upload_json(jobs_data, jobs_s3_key)
            logger.info(f"   📤 S3 Jobs: {jobs_s3_key}")
            
            # -----------------------------------------
            # S3: Upload tech stack analysis
            # -----------------------------------------
            techstack_keywords = state.company_techstacks.get(company_id, [])
            techstack_score = state.techstack_scores.get(company_id, 0.0)
            
            techstack_s3_key = f"signals/techstack/{ticker}/{timestamp}.json"
            techstack_data = {
                "company_id": company_id,
                "company_name": company_name,
                "ticker": ticker,
                "collection_date": timestamp,
                "techstack_score": techstack_score,
                "all_keywords": techstack_keywords,
                "ai_tools_found": original_analysis.get("ai_tools_found", []),
                "total_keywords": original_analysis.get("total_keywords", 0),
                "total_ai_tools": original_analysis.get("total_ai_tools", 0),
                "techstack_analysis": techstack_analysis
            }
            s3.upload_json(techstack_data, techstack_s3_key)
            logger.info(f"   📤 S3 TechStack: {techstack_s3_key}")
            
            # -----------------------------------------
            # Snowflake: Insert external signal
            # -----------------------------------------
            job_market_analysis = state.job_market_analyses.get(company_id, {})
            score = state.job_market_scores.get(company_id, 0.0)

            ai_count = job_market_analysis.get("ai_jobs", 0)
            total_tech_jobs = job_market_analysis.get("total_tech_jobs", 0)

            # Build summary text
            summary = f"Found {ai_count} AI roles out of {total_tech_jobs} tech jobs"

            # Build raw_payload with detailed metrics from analysis
            raw_payload = {
                "collection_date": timestamp,
                "s3_jobs_key": jobs_s3_key,
                "s3_techstack_key": techstack_s3_key,
                "total_jobs": len(jobs),
                "total_tech_jobs": total_tech_jobs,
                "ai_jobs": ai_count,
                "ai_ratio": job_market_analysis.get("ai_ratio", 0),
                "score_breakdown": job_market_analysis.get("score_breakdown", {}),
                "all_skills": job_market_analysis.get("all_skills", []),
                "confidence": job_market_analysis.get("confidence", 0.5),
                "sources": list(set(j.get("source", "unknown") for j in jobs)),
                "techstack": {
                    "score": techstack_score,
                    "all_keywords": techstack_keywords,
                    "ai_tools_found": original_analysis.get("ai_tools_found", []),
                    "techstack_analysis": techstack_analysis,
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
    skip_storage: bool = False,
    use_local_storage: bool = False,
) -> Pipeline2State:
    """
    Run the job signals collection pipeline (extract, classify, score).

    Args:
        state: Pipeline state with companies loaded
        skip_storage: If True, skip all storage steps (for pipeline2_runner integration)
        use_local_storage: If True and skip_storage=False, save to local JSON instead of S3/Snowflake

    Returns:
        Updated pipeline state with job postings, classifications, and scores
    """
    # Step 1: Initialize
    state = step1_init_job_collection(state)
    # Step 2: Fetch job postings
    state = await step2_fetch_job_postings(state)
    # Step 3: Classify AI jobs
    state = step3_classify_ai_jobs(state)
    # Step 4: Score job market
    state = step4_score_job_market(state)
    # Step 4b: Score tech stack
    state = step4b_score_techstack(state)
    # Step 5: Storage (optional - pipeline2_runner handles this separately)
    if not skip_storage:
        if use_local_storage:
            state = step5_save_to_json(state)
        else:
            state = step5_store_to_s3_and_snowflake(state)
    return state