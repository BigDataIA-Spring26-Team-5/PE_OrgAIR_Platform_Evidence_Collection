"""
Pipeline 2 Runner - Job and Patent Collection
app/pipelines/pipeline2_runner.py

Scrapes job postings and fetches patents for companies.
Stores data in S3 (raw) and Snowflake (aggregated signals).
Use --local-only flag to save to local JSON files instead.
"""

from __future__ import annotations

import argparse
import asyncio
from typing import List, Optional

from dotenv import load_dotenv

from app.pipelines.pipeline2_state import Pipeline2State
from app.pipelines.job_signals import run_job_signals
from app.pipelines.patent_signals import run_patent_signals
from app.pipelines.utils import Company

# Load environment variables from .env file
load_dotenv()


async def run_pipeline2(
    *,
    companies: Optional[List[str]] = None,
    mode: str = "jobs",  # "jobs", "patents", or "both"
    jobs_output_dir: str = "data/signals/jobs",
    patents_output_dir: str = "data/signals/patents",
    jobs_request_delay: float = 6.0,
    patents_request_delay: float = 1.5,
    jobs_results_per_company: int = 50,
    patents_results_per_company: int = 100,
    patents_years_back: int = 5,
    patents_api_key: Optional[str] = None,
    use_cloud_storage: bool = True,
) -> Pipeline2State:
    """
    Run Pipeline 2: Job and Patent Collection.

    Args:
        companies: List of company names to process
        mode: "jobs", "patents", or "both"
        jobs_output_dir: Directory to save job JSON output files
        patents_output_dir: Directory to save patent JSON output files
        jobs_request_delay: Delay between job API requests for rate limiting
        patents_request_delay: Delay between patent API requests for rate limiting
        jobs_results_per_company: Max job postings per company
        patents_results_per_company: Max patents per company
        patents_years_back: How many years back to search for patents
        patents_api_key: PatentsView API key (or set PATENTSVIEW_API_KEY env var)
        use_cloud_storage: If True, store in S3 + Snowflake. If False, local JSON only.

    Returns:
        Pipeline2State with all collected data and scores
    """
    mode_labels = {
        "jobs": "Job Scraping",
        "patents": "Patent Collection",
        "both": "Job and Patent Collection"
    }
    print("=" * 60)
    print(f"Pipeline 2: {mode_labels.get(mode, mode)}")
    print("=" * 60)

    if not companies:
        print("\n[error] No companies provided. Use --companies flag.")
        print("Example: python -m app.pipelines.pipeline2_runner --companies Microsoft Google")
        return Pipeline2State()

    # Create state with company list
    company_list = [Company.from_name(name, i).to_dict() for i, name in enumerate(companies)]
    state = Pipeline2State(companies=company_list)

    print(f"\nCompanies to process: {len(state.companies)}")
    for c in state.companies:
        print(f"  - {c['name']}")

    # Run job signals pipeline if requested
    if mode in ["jobs", "both"]:
        print("\n" + "-" * 60)
        print("Job Scraping Pipeline")
        print("-" * 60)
        print(f"Storage: {'S3 + Snowflake' if use_cloud_storage else 'Local JSON files only'}")

        state.output_dir = jobs_output_dir
        state.request_delay = jobs_request_delay
        state = await run_job_signals(state, use_cloud_storage=use_cloud_storage)

    # Run patent signals pipeline if requested
    if mode in ["patents", "both"]:
        print("\n" + "-" * 60)
        print("Patent Collection Pipeline")
        print("-" * 60)

        state.output_dir = patents_output_dir
        state.request_delay = patents_request_delay
        state = await run_patent_signals(
            state,
            years_back=patents_years_back,
            results_per_company=patents_results_per_company,
            api_key=patents_api_key,
        )

    _print_summary(state, mode, use_cloud_storage)
    return state


def _get_company_name(state: Pipeline2State, company_id: str) -> str:
    """Get company name from ID."""
    for c in state.companies:
        if c.get("id") == company_id:
            return c.get("name", company_id)
    return company_id


def _print_summary(state: Pipeline2State, mode: str, use_cloud_storage: bool) -> None:
    """Print pipeline execution summary."""
    print("\n" + "=" * 60)
    print("Pipeline Complete")
    print("=" * 60)

    if mode in ["jobs", "both"]:
        print(f"Companies processed: {state.summary.get('companies_processed', 0)}")
        print(f"Total job postings: {state.summary.get('job_postings_collected', 0)}")
        print(f"AI-related jobs: {state.summary.get('ai_jobs_found', 0)}")

        if state.job_market_scores:
            print("\nJob Market Scores:")
            for company_id, score in state.job_market_scores.items():
                print(f"  {_get_company_name(state, company_id)}: {score:.2f}/100")

    if mode in ["patents", "both"]:
        print(f"\nTotal patents: {state.summary.get('patents_collected', 0)}")
        print(f"AI-related patents: {state.summary.get('ai_patents_found', 0)}")

        if state.patent_scores:
            print("\nPatent Portfolio Scores:")
            for company_id, score in state.patent_scores.items():
                print(f"  {_get_company_name(state, company_id)}: {score:.2f}/100")

    print(f"\nErrors: {len(state.summary.get('errors', []))}")

    print("\nStorage:")
    if use_cloud_storage:
        print("  S3: raw/jobs/{company}/{timestamp}.json")
        print("  Snowflake: external_signals table")
    print("  Local: data/signals/jobs (working files)")

    errors = state.summary.get("errors", [])
    if errors:
        print(f"\nErrors ({len(errors)}):")
        for err in errors[:5]:
            print(f"  - [{err.get('step', 'unknown')}] {err.get('error', 'Unknown error')}")


async def main():
    """CLI entry point for Pipeline 2."""
    parser = argparse.ArgumentParser(
        description="Pipeline 2: Job and Patent Collection",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape jobs and store in S3 + Snowflake (default)
  python -m app.pipelines.pipeline2_runner --companies Microsoft Google Amazon

  # Scrape jobs and save to local JSON only (no cloud storage)
  python -m app.pipelines.pipeline2_runner --companies Microsoft --local-only

  # Fetch patents for specific companies
  python -m app.pipelines.pipeline2_runner --companies Microsoft --mode patents

  # Run both job scraping and patent collection
  python -m app.pipelines.pipeline2_runner --companies Microsoft --mode both

  # With PatentsView API key (recommended for patents)
  python -m app.pipelines.pipeline2_runner --companies Microsoft --mode patents --api-key YOUR_KEY

  # Or set environment variable
  set PATENTSVIEW_API_KEY=YOUR_KEY
  python -m app.pipelines.pipeline2_runner --companies Microsoft --mode patents

  # Custom output directories
  python -m app.pipelines.pipeline2_runner --companies Microsoft --jobs-output ./jobs --patents-output ./patents

  # Adjust years back for patents (default: 5)
  python -m app.pipelines.pipeline2_runner --companies Microsoft --mode patents --years 3

  # Limit results per company
  python -m app.pipelines.pipeline2_runner --companies Microsoft --mode both --jobs-results 20 --patents-results 50

Get PatentsView API key at: https://patentsview.org/apis/keyrequest
        """
    )
    parser.add_argument("--companies", nargs="+", required=True, help="Company names to process")
    parser.add_argument("--mode", choices=["jobs", "patents", "both"], default="jobs",
                        help="Pipeline mode: jobs (default), patents, or both")
    parser.add_argument("--jobs-output", default="data/signals/jobs", dest="jobs_output_dir",
                        help="Output directory for job JSON files")
    parser.add_argument("--patents-output", default="data/signals/patents", dest="patents_output_dir",
                        help="Output directory for patent JSON files")
    parser.add_argument("--jobs-delay", type=float, default=6.0, dest="jobs_request_delay",
                        help="Delay between job API requests in seconds")
    parser.add_argument("--patents-delay", type=float, default=1.5, dest="patents_request_delay",
                        help="Delay between patent API requests in seconds")
    parser.add_argument("--jobs-results", type=int, default=50, dest="jobs_results_per_company",
                        help="Max job postings per company")
    parser.add_argument("--patents-results", type=int, default=100, dest="patents_results_per_company",
                        help="Max patents per company (max: 1000)")
    parser.add_argument("--years", type=int, default=5, dest="patents_years_back",
                        help="Years back to search for patents")
    parser.add_argument("--api-key", default=None, dest="patents_api_key",
                        help="PatentsView API key (or set PATENTSVIEW_API_KEY env var)")
    parser.add_argument("--local-only", action="store_true", dest="local_only",
                        help="Save to local JSON files only (skip S3 and Snowflake)")

    args = parser.parse_args()

    await run_pipeline2(
        companies=args.companies,
        mode=args.mode,
        jobs_output_dir=args.jobs_output_dir,
        patents_output_dir=args.patents_output_dir,
        jobs_request_delay=args.jobs_request_delay,
        patents_request_delay=args.patents_request_delay,
        jobs_results_per_company=args.jobs_results_per_company,
        patents_results_per_company=args.patents_results_per_company,
        patents_years_back=args.patents_years_back,
        patents_api_key=args.patents_api_key,
        use_cloud_storage=not args.local_only,
    )


if __name__ == "__main__":
    asyncio.run(main())
