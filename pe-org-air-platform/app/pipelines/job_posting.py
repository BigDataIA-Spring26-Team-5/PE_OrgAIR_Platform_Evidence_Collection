"""
Job Signals Pipeline - python-jobspy Integration
app/pipelines/job_signals.py

Scrapes job postings using python-jobspy and classifies AI-related roles.
Outputs results to JSON files.
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
from app.models.pipeline2_models import JobPosting

# Rate limiting delay for JobSpy (seconds)
JOBSPY_REQUEST_DELAY = 6.0


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
    results_wanted: int = 50,
    hours_old: int = 168,  # 7 days
) -> Pipeline2State:
    """
    Fetch job postings for each company using python-jobspy.

    Args:
        state: Pipeline state
        sites: Job sites to scrape (default: linkedin, indeed)
        results_wanted: Max results per company
        hours_old: Max age of job postings in hours
    """
    if sites is None:
        sites = ["linkedin", "indeed"]

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

            # Scrape jobs using jobspy
            jobs_df = scrape_jobs(
                site_name=sites,
                search_term=f'"{company_name}"',
                results_wanted=results_wanted,
                hours_old=hours_old,
                country_indeed="USA",
            )

            postings = []
            if jobs_df is not None and not jobs_df.empty:
                for _, row in jobs_df.iterrows():
                    posting = JobPosting(
                        company_id=company_id,
                        company_name=company_name,
                        title=str(row.get("title", "")),
                        description=str(row.get("description", "")),
                        location=str(row.get("location", "")) if _clean_nan(row.get("location")) else None,
                        posted_date=_clean_nan(row.get("date_posted")),
                        source=str(row.get("site", "unknown")),
                        url=str(row.get("job_url", "")) if _clean_nan(row.get("job_url")) else None,
                    )
                    postings.append(posting)

            state.job_postings.extend([p.model_dump() for p in postings])
            state.summary["job_postings_collected"] += len(postings)
            print(f"  [fetched] {company_name}: {len(postings)} postings")

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
    """Save results to JSON files."""

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


async def run_job_signals(state: Pipeline2State) -> Pipeline2State:
    """
    Run the complete job signals collection pipeline.

    Args:
        state: Pipeline state with companies loaded

    Returns:
        Updated pipeline state with job postings and scores
    """
    state = step1_init_job_collection(state)
    state = await step2_fetch_job_postings(state)
    state = step3_classify_ai_jobs(state)
    state = step4_score_job_market(state)
    state = step5_save_to_json(state)
    state.mark_completed()
    return state
