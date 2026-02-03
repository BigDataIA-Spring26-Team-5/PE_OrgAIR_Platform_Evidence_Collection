"""
Pipeline 2 Runner - Job Scraping Only
app/pipelines/pipeline2_runner.py

Scrapes job postings for companies and outputs JSON files.
No database dependencies required.
"""

from __future__ import annotations

import argparse
import asyncio
from typing import List, Optional

from app.pipelines.pipeline2_state import Pipeline2State
from app.pipelines.job_signals import run_job_signals


async def run_pipeline2(
    *,
    companies: Optional[List[str]] = None,
    output_dir: str = "data/signals/jobs",
    request_delay: float = 6.0,
    results_per_company: int = 50,
) -> Pipeline2State:
    """
    Run Pipeline 2: Job Scraping.

    Args:
        companies: List of company names to scrape jobs for
        output_dir: Directory to save JSON output files
        request_delay: Delay between API requests for rate limiting
        results_per_company: Max job postings per company

    Returns:
        Pipeline2State with all collected job postings and scores
    """
    print("=" * 60)
    print("Pipeline 2: Job Scraping")
    print("=" * 60)

    # Create state
    state = Pipeline2State(
        request_delay=request_delay,
        output_dir=output_dir
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

    # Run job signals pipeline
    print("\n" + "-" * 60)
    state = await run_job_signals(state)

    # Print summary
    _print_summary(state)

    return state


def _print_summary(state: Pipeline2State) -> None:
    """Print pipeline execution summary."""
    print("\n" + "=" * 60)
    print("Pipeline Complete")
    print("=" * 60)
    print(f"Companies processed: {state.summary.get('companies_processed', 0)}")
    print(f"Total job postings: {state.summary.get('job_postings_collected', 0)}")
    print(f"AI-related jobs: {state.summary.get('ai_jobs_found', 0)}")
    print(f"Errors: {len(state.summary.get('errors', []))}")

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

    print(f"\nOutput saved to: {state.output_dir}")

    # Print errors if any
    if state.summary.get("errors"):
        print(f"\nErrors ({len(state.summary['errors'])}):")
        for err in state.summary["errors"][:5]:
            print(f"  - [{err.get('step', 'unknown')}] {err.get('error', 'Unknown error')}")


async def main():
    """CLI entry point for Pipeline 2."""
    parser = argparse.ArgumentParser(
        description="Pipeline 2: Job Scraping",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape jobs for specific companies
  python -m app.pipelines.pipeline2_runner --companies Microsoft Google Amazon

  # Custom output directory
  python -m app.pipelines.pipeline2_runner --companies Microsoft --output-dir ./output

  # Adjust rate limiting (seconds between requests)
  python -m app.pipelines.pipeline2_runner --companies Microsoft --delay 10

  # Limit results per company
  python -m app.pipelines.pipeline2_runner --companies Microsoft --results 20
        """
    )
    parser.add_argument(
        "--companies",
        nargs="+",
        required=True,
        help="Company names to scrape jobs for"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/signals/jobs",
        help="Output directory for JSON files (default: data/signals/jobs)"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=6.0,
        help="Delay between requests in seconds (default: 6.0)"
    )
    parser.add_argument(
        "--results",
        type=int,
        default=50,
        help="Max results per company (default: 50)"
    )

    args = parser.parse_args()

    await run_pipeline2(
        companies=args.companies,
        output_dir=args.output_dir,
        request_delay=args.delay,
        results_per_company=args.results,
    )


if __name__ == "__main__":
    asyncio.run(main())
