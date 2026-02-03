"""
Job Signals Pipeline - python-jobspy Integration
app/pipelines/job_signals.py

Scrapes job postings using python-jobspy and classifies AI-related roles.
Stores raw data in S3 and aggregated signals in Snowflake.
"""

from __future__ import annotations

import asyncio
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Optional


def _clean_nan(value: Any) -> Any:
    """Convert NaN values to None for Pydantic compatibility."""
    if value is None:
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
    except (TypeError, ValueError):
        pass
    # Handle pandas NaT (Not a Time)
    if hasattr(value, 'isnull') and value.isnull():
        return None
    if str(value) in ('nan', 'NaN', 'NaT'):
        return None
    return value

from app.pipelines.pipeline2_state import Pipeline2State
from app.pipelines.keywords import AI_KEYWORDS, AI_TECHSTACK_KEYWORDS
from app.models.job_signals import JobPosting

# Rate limiting delay for JobSpy (seconds)
JOBSPY_REQUEST_DELAY = 6.0


def _normalize_company_name(name: str) -> str:
    """Normalize company name for comparison."""
    if not name:
        return ""
    name = name.lower().strip()
    # Remove common suffixes
    for suffix in [", inc.", ", inc", " inc.", " inc", ", llc", " llc",
                   ", ltd", " ltd", " corporation", " corp.", " corp",
                   " technologies", " technology", " software", " systems"]:
        if name.endswith(suffix):
            name = name[:-len(suffix)]
    return name.strip()


def _company_name_matches(job_company: str, target_company: str) -> bool:
    """
    Check if job's company name matches target company.

    Uses normalized comparison to handle variations like:
    - "Microsoft" vs "Microsoft Corporation"
    - "Google" vs "Google LLC"
    """
    if not job_company or not target_company:
        return False

    job_norm = _normalize_company_name(job_company)
    target_norm = _normalize_company_name(target_company)

    # Empty after normalization
    if not job_norm or not target_norm:
        return False

    # Exact match
    if job_norm == target_norm:
        return True

    # Target is contained in job company name (e.g., "Microsoft" in "Microsoft Corporation")
    if target_norm in job_norm:
        return True

    # Job company is contained in target (e.g., "Google" in "Google LLC")
    if job_norm in target_norm:
        return True

    return False


def step1_init_job_collection(state: Pipeline2State) -> Pipeline2State:
    """Initialize job collection step."""
    state.mark_started()

    # Create output directory
    Path(state.output_dir).mkdir(parents=True, exist_ok=True)

    print("Step 1: Job collection initialized")
    print(f"  Output directory: {state.output_dir}")
    return state


async def step2_fetch_job_postings(
    state: Pipeline2State,
    *,
    sites: Optional[List[str]] = None,
    results_wanted: int = 100,
    hours_old: int = 72,  # 3 days - more recent = more relevant
) -> Pipeline2State:
    """
    Fetch job postings for each company using python-jobspy.

    Strategy:
    1. Search using company name as search term
    2. Post-filter results by matching the 'company' column from JobSpy
    3. This filters out jobs that just mention the company (e.g., "Microsoft Office skills")

    Args:
        state: Pipeline state
        sites: Job sites to scrape (default: linkedin only - more reliable company data)
        results_wanted: Max results to fetch (before filtering)
        hours_old: Max age of job postings in hours
    """
    # Default to LinkedIn only - it has better company name data
    if sites is None:
        sites = ["linkedin"]

    # Try to import jobspy
    try:
        from jobspy import scrape_jobs
    except ImportError:
        print("  [error] python-jobspy not installed. Run: pip install python-jobspy")
        return state

    for company in state.companies:
        company_id = company.get("id", "")
        company_name = company.get("name", "")

        if not company_name:
            continue

        # Rate limiting
        await asyncio.sleep(max(state.request_delay, JOBSPY_REQUEST_DELAY))

        try:
            print(f"  Scraping jobs for: {company_name}...")

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
                    job_company = str(row.get("company", "")) if _clean_nan(row.get("company")) else ""
                    source = str(row.get("site", "unknown"))

                    # Post-filter: verify the job's company matches our target
                    # This filters out jobs that just mention "Microsoft Office" etc.
                    if not _company_name_matches(job_company, company_name):
                        filtered_count += 1
                        continue

                    posting = JobPosting(
                        company_id=company_id,
                        company_name=job_company,  # Use actual company name from job
                        title=str(row.get("title", "")),
                        description=str(row.get("description", "")),
                        location=str(row.get("location", "")) if _clean_nan(row.get("location")) else None,
                        posted_date=_clean_nan(row.get("date_posted")),
                        source=source,
                        url=str(row.get("job_url", "")) if _clean_nan(row.get("job_url")) else None,
                    )
                    postings.append(posting)

            state.job_postings.extend([p.model_dump() for p in postings])
            state.summary["job_postings_collected"] += len(postings)

            print(f"    [raw] {total_raw} results from JobSpy")
            print(f"    [filtered] {len(postings)} jobs from {company_name} (removed {filtered_count} unrelated)")

        except Exception as e:
            state.add_error("job_fetch", company_id, str(e))
            print(f"  [error] {company_name}: {e}")

    print(f"Step 2: Collected {len(state.job_postings)} job postings total")
    return state


def step3_classify_ai_jobs(state: Pipeline2State) -> Pipeline2State:
    """Classify job postings as AI-related using AI_KEYWORDS."""

    for posting in state.job_postings:
        text = f"{posting.get('title', '')} {posting.get('description', '')}".lower()

        # Find matching AI keywords
        ai_keywords_found = []
        for keyword in AI_KEYWORDS:
            if keyword in text:
                ai_keywords_found.append(keyword)

        # Find matching tech stack keywords
        techstack_found = []
        for keyword in AI_TECHSTACK_KEYWORDS:
            if keyword in text:
                techstack_found.append(keyword)

        posting["ai_keywords_found"] = ai_keywords_found
        posting["techstack_keywords_found"] = techstack_found
        posting["is_ai_role"] = len(ai_keywords_found) >= 2  # At least 2 keywords

        # Calculate AI score (0-100)
        posting["ai_score"] = min(100.0, len(ai_keywords_found) * 15.0)

    ai_count = sum(1 for p in state.job_postings if p.get("is_ai_role"))
    print(f"Step 3: Classified {ai_count} AI-related jobs out of {len(state.job_postings)}")
    return state


def step4_score_job_market(state: Pipeline2State) -> Pipeline2State:
    """
    Calculate job market score for each company.

    Scoring algorithm:
    - Base: (AI jobs / Total jobs) * 50
    - Volume bonus: min(30, AI_job_count * 3)
    - Keyword diversity: (unique AI keywords / 10) * 20
    """

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

        # Ratio component (0-50 points)
        ratio_score = (ai_count / total_jobs * 50) if total_jobs > 0 else 0

        # Volume bonus (0-30 points)
        volume_bonus = min(30, ai_count * 3)

        # Keyword diversity (0-20 points)
        all_keywords = set()
        for job in ai_jobs:
            all_keywords.update(job.get("ai_keywords_found", []))
        diversity_score = min(20, len(all_keywords) * 2)

        final_score = min(100.0, ratio_score + volume_bonus + diversity_score)
        state.job_market_scores[company_id] = round(final_score, 2)

    print(f"Step 4: Scored job market for {len(state.job_market_scores)} companies")
    return state


def step5_save_to_json(state: Pipeline2State) -> Pipeline2State:
    """Save results to local JSON files (legacy/debug mode)."""

    output_dir = Path(state.output_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Save all job postings
    all_jobs_file = output_dir / f"all_jobs_{timestamp}.json"
    with open(all_jobs_file, "w", encoding="utf-8") as f:
        json.dump(state.job_postings, f, indent=2, default=str)
    print(f"  Saved all jobs to: {all_jobs_file}")

    # Save AI-related jobs only
    ai_jobs = [p for p in state.job_postings if p.get("is_ai_role")]
    ai_jobs_file = output_dir / f"ai_jobs_{timestamp}.json"
    with open(ai_jobs_file, "w", encoding="utf-8") as f:
        json.dump(ai_jobs, f, indent=2, default=str)
    print(f"  Saved AI jobs to: {ai_jobs_file}")

    # Save per-company results
    company_jobs = defaultdict(list)
    for posting in state.job_postings:
        company_jobs[posting["company_id"]].append(posting)

    for company_id, jobs in company_jobs.items():
        # Get company name for filename
        company_name = jobs[0].get("company_name", company_id) if jobs else company_id
        safe_name = "".join(c if c.isalnum() else "_" for c in company_name)

        company_file = output_dir / f"{safe_name}_{timestamp}.json"
        company_data = {
            "company_id": company_id,
            "company_name": company_name,
            "total_jobs": len(jobs),
            "ai_jobs": sum(1 for j in jobs if j.get("is_ai_role")),
            "job_market_score": state.job_market_scores.get(company_id, 0),
            "jobs": jobs
        }
        with open(company_file, "w", encoding="utf-8") as f:
            json.dump(company_data, f, indent=2, default=str)

    # Save summary
    summary_file = output_dir / f"summary_{timestamp}.json"
    summary_data = {
        **state.summary,
        "job_market_scores": state.job_market_scores,
        "companies": [c.get("name", c.get("id")) for c in state.companies]
    }
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, indent=2, default=str)
    print(f"  Saved summary to: {summary_file}")

    print(f"Step 5: Saved results to {output_dir}")
    return state


def step5_store_to_s3_and_snowflake(state: Pipeline2State) -> Pipeline2State:
    """
    Store results in S3 (raw data) and Snowflake (aggregated signals).

    S3 Structure:
        raw/jobs/{company_name}/{timestamp}.json - All job postings for company

    Snowflake:
        external_signals table - One row per company with job_market score
    """
    from app.services.s3_storage import S3Storage
    from app.services.snowflake import SnowflakeService

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(state.output_dir)

    # Initialize services
    s3 = S3Storage()
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
            safe_name = "".join(c if c.isalnum() else "_" for c in company_name)

            # -----------------------------------------
            # S3: Upload raw job postings
            # -----------------------------------------
            s3_key = f"raw/jobs/{safe_name}/{timestamp}.json"
            local_path = output_dir / f"{safe_name}_{timestamp}.json"

            # Save locally first
            with open(local_path, "w", encoding="utf-8") as f:
                json.dump(jobs, f, indent=2, default=str)

            # Upload to S3
            s3.upload_file(local_path, s3_key)
            print(f"  [S3] Uploaded: {s3_key}")

            # -----------------------------------------
            # Snowflake: Insert external signal
            # -----------------------------------------
            ai_jobs = [j for j in jobs if j.get("is_ai_role")]
            score = state.job_market_scores.get(company_id, 0.0)
            total_jobs = len(jobs)
            ai_count = len(ai_jobs)

            # Calculate score breakdown for raw_payload
            ratio_score = (ai_count / total_jobs * 50) if total_jobs > 0 else 0
            volume_bonus = min(30, ai_count * 3)
            all_keywords = set()
            for job in ai_jobs:
                all_keywords.update(job.get("ai_keywords_found", []))
            diversity_score = min(20, len(all_keywords) * 2)

            # Build summary text
            summary = f"Found {ai_count} AI roles out of {total_jobs} total jobs"

            # Build raw_payload with detailed metrics
            raw_payload = {
                "collection_date": timestamp,
                "s3_key": s3_key,
                "total_jobs": total_jobs,
                "ai_jobs": ai_count,
                "score_breakdown": {
                    "ratio_score": round(ratio_score, 2),
                    "volume_bonus": round(volume_bonus, 2),
                    "diversity_score": round(diversity_score, 2),
                },
                "top_ai_keywords": list(all_keywords)[:20],
                "sources": list(set(j.get("source", "unknown") for j in jobs)),
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
            print(f"  [Snowflake] Inserted signal: {signal_id} (score: {score})")

        print(f"Step 5: Stored results for {len(company_jobs)} companies in S3 + Snowflake")
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

    if use_cloud_storage:
        state = step5_store_to_s3_and_snowflake(state)
    else:
        state = step5_save_to_json(state)

    state.mark_completed()
    return state
