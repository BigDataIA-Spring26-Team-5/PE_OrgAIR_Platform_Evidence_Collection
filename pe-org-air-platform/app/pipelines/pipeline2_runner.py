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
    print("=" * 60)
    print(f"Pipeline 2: {'Job Scraping' if mode == 'jobs' else 'Patent Collection' if mode == 'patents' else 'Job and Patent Collection'}")
    print("=" * 60)

    # Create main state that will hold all results
    state = Pipeline2State(
        request_delay=jobs_request_delay if mode in ["jobs", "both"] else patents_request_delay,
        output_dir=jobs_output_dir if mode == "jobs" else patents_output_dir if mode == "patents" else jobs_output_dir
    )

    # Set companies
    if companies:
        state.companies = [
            {"id": f"company-{i}", "name": name}
            for i, name in enumerate(companies)
        ]
    else:
        print("\n[error] No companies provided. Use --companies flag.")
        print("Example: python -m app.pipelines.pipeline2_runner --companies Microsoft Google")
        return state

    print(f"\nCompanies to process: {len(state.companies)}")
    for c in state.companies:
        print(f"  - {c['name']}")

    # Run job signals pipeline if requested
    if mode in ["jobs", "both"]:
        print("\n" + "-" * 60)
        print("Job Scraping Pipeline")
        print("-" * 60)
        if use_cloud_storage:
            print("Storage: S3 + Snowflake")
        else:
            print("Storage: Local JSON files only")

        # Create a separate state for job scraping
        job_state = Pipeline2State(
            request_delay=jobs_request_delay,
            output_dir=jobs_output_dir
        )
        job_state.companies = state.companies

        job_state = await run_job_signals(job_state, use_cloud_storage=use_cloud_storage)

        # Copy job results to main state
        state.job_postings = job_state.job_postings
        state.job_market_scores = job_state.job_market_scores
        state.summary["companies_processed"] = job_state.summary.get("companies_processed", 0)
        state.summary["job_postings_collected"] = job_state.summary.get("job_postings_collected", 0)
        state.summary["ai_jobs_found"] = job_state.summary.get("ai_jobs_found", 0)
        state.summary["errors"].extend(job_state.summary.get("errors", []))

    # Run patent signals pipeline if requested
    if mode in ["patents", "both"]:
        print("\n" + "-" * 60)
        print("Patent Collection Pipeline")
        print("-" * 60)
        
        # Create a separate state for patent collection
        patent_state = Pipeline2State(
            request_delay=patents_request_delay,
            output_dir=patents_output_dir
        )
        patent_state.companies = state.companies
        
        patent_state = await run_patent_signals(
            patent_state,
            years_back=patents_years_back,
            results_per_company=patents_results_per_company,
            api_key=patents_api_key,
        )
        
        # Copy patent results to main state
        state.patents = patent_state.patents
        state.patent_scores = patent_state.patent_scores
        state.summary["patents_collected"] = patent_state.summary.get("patents_collected", 0)
        state.summary["ai_patents_found"] = sum(1 for p in patent_state.patents if p.get("is_ai_patent"))
        state.summary["errors"].extend(patent_state.summary.get("errors", []))

    # Print summary
    _print_summary(state, mode, use_cloud_storage)

    return state


def _print_summary(state: Pipeline2State, mode: str = "jobs", use_cloud_storage: bool = True) -> None:
    """Print pipeline execution summary."""
    print("\n" + "=" * 60)
    print("Pipeline Complete")
    print("=" * 60)

    if mode in ["jobs", "both"]:
        print(f"Companies processed: {state.summary.get('companies_processed', 0)}")
        print(f"Total job postings: {state.summary.get('job_postings_collected', 0)}")
        print(f"AI-related jobs: {state.summary.get('ai_jobs_found', 0)}")

        if state.job_market_scores:
            print(f"\nJob Market Scores:")
            for company_id, score in state.job_market_scores.items():
                # Get company name
                company_name = company_id
                for c in state.companies:
                    if c.get("id") == company_id:
                        company_name = c.get("name", company_id)
                        break
                print(f"  {company_name}: {score:.2f}/100")

    if mode in ["patents", "both"]:
        print(f"\nTotal patents: {state.summary.get('patents_collected', 0)}")
        print(f"AI-related patents: {state.summary.get('ai_patents_found', 0)}")

        if state.patent_scores:
            print(f"\nPatent Portfolio Scores:")
            for company_id, score in state.patent_scores.items():
                # Get company name
                company_name = company_id
                for c in state.companies:
                    if c.get("id") == company_id:
                        company_name = c.get("name", company_id)
                        break
                print(f"  {company_name}: {score:.2f}/100")

    print(f"\nErrors: {len(state.summary.get('errors', []))}")

    # Print storage locations
    print("\nStorage:")
    if use_cloud_storage:
        print("  S3: raw/jobs/{company}/{timestamp}.json")
        print("  Snowflake: external_signals table")
    print(f"  Local: data/signals/jobs (working files)")

    # Print errors if any
    if state.summary.get("errors"):
        print(f"\nErrors ({len(state.summary['errors'])}):")
        for err in state.summary["errors"][:5]:
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
    parser.add_argument(
        "--companies",
        nargs="+",
        required=True,
        help="Company names to process"
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["jobs", "patents", "both"],
        default="jobs",
        help="Pipeline mode: jobs (default), patents, or both"
    )
    parser.add_argument(
        "--jobs-output",
        type=str,
        default="data/signals/jobs",
        dest="jobs_output_dir",
        help="Output directory for job JSON files (default: data/signals/jobs)"
    )
    parser.add_argument(
        "--patents-output",
        type=str,
        default="data/signals/patents",
        dest="patents_output_dir",
        help="Output directory for patent JSON files (default: data/signals/patents)"
    )
    parser.add_argument(
        "--jobs-delay",
        type=float,
        default=6.0,
        dest="jobs_request_delay",
        help="Delay between job API requests in seconds (default: 6.0)"
    )
    parser.add_argument(
        "--patents-delay",
        type=float,
        default=1.5,
        dest="patents_request_delay",
        help="Delay between patent API requests in seconds (default: 1.5)"
    )
    parser.add_argument(
        "--jobs-results",
        type=int,
        default=50,
        dest="jobs_results_per_company",
        help="Max job postings per company (default: 50)"
    )
    parser.add_argument(
        "--patents-results",
        type=int,
        default=100,
        dest="patents_results_per_company",
        help="Max patents per company (default: 100, max: 1000)"
    )
    parser.add_argument(
        "--years",
        type=int,
        default=5,
        dest="patents_years_back",
        help="Years back to search for patents (default: 5)"
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        dest="patents_api_key",
        help="PatentsView API key (or set PATENTSVIEW_API_KEY env var)"
    )
    parser.add_argument(
        "--local-only",
        action="store_true",
        dest="local_only",
        help="Save to local JSON files only (skip S3 and Snowflake)"
    )

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