"""
Patent Runner - PatentsView API Integration
app/pipelines/patent_runner.py

Fetches patents for companies and outputs JSON files.
No database dependencies required.
"""

from __future__ import annotations

import argparse
import asyncio
from typing import List, Optional

from dotenv import load_dotenv

from app.pipelines.pipeline2_state import Pipeline2State
from app.pipelines.patent_signals import run_patent_signals

# Load environment variables from .env file
load_dotenv()


async def run_patent_pipeline(
    *,
    companies: Optional[List[str]] = None,
    output_dir: str = "data/signals/patents",
    request_delay: float = 1.5,
    years_back: int = 5,
    results_per_company: int = 100,
    api_key: Optional[str] = None,
) -> Pipeline2State:
    """
    Run Patent Pipeline: PatentsView API.

    Args:
        companies: List of company names to fetch patents for
        output_dir: Directory to save JSON output files
        request_delay: Delay between API requests for rate limiting
        years_back: How many years back to search (default: 5)
        results_per_company: Max patents per company

    Returns:
        Pipeline2State with all collected patents and scores
    """
    print("=" * 60)
    print("Patent Pipeline: PatentsView API")
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
        print("Example: python -m app.pipelines.patent_runner --companies Microsoft Google")
        return state

    print(f"\nCompanies to process: {len(state.companies)}")
    for c in state.companies:
        print(f"  - {c['name']}")
    print(f"Years back: {years_back}")
    print(f"Max results per company: {results_per_company}")

    # Run patent signals pipeline
    print("\n" + "-" * 60)
    state = await run_patent_signals(
        state,
        years_back=years_back,
        results_per_company=results_per_company,
        api_key=api_key,
    )

    # Print summary
    _print_summary(state)

    return state


def _print_summary(state: Pipeline2State) -> None:
    """Print pipeline execution summary."""
    print("\n" + "=" * 60)
    print("Patent Pipeline Complete")
    print("=" * 60)
    print(f"Companies processed: {len(state.companies)}")
    print(f"Total patents: {len(state.patents)}")
    print(f"AI-related patents: {sum(1 for p in state.patents if p.get('is_ai_patent'))}")
    print(f"Errors: {len(state.summary.get('errors', []))}")

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

    print(f"\nOutput saved to: {state.output_dir}")

    # Print errors if any
    if state.summary.get("errors"):
        print(f"\nErrors ({len(state.summary['errors'])}):")
        for err in state.summary["errors"][:5]:
            print(f"  - [{err.get('step', 'unknown')}] {err.get('error', 'Unknown error')[:100]}")


async def main():
    """CLI entry point for Patent Pipeline."""
    parser = argparse.ArgumentParser(
        description="Patent Pipeline: PatentsView PatentSearch API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch patents for specific companies
  python -m app.pipelines.patent_runner --companies Microsoft Google Amazon

  # With API key (recommended)
  python -m app.pipelines.patent_runner --companies Microsoft --api-key YOUR_KEY

  # Or set environment variable
  set PATENTSVIEW_API_KEY=YOUR_KEY
  python -m app.pipelines.patent_runner --companies Microsoft

  # Custom output directory
  python -m app.pipelines.patent_runner --companies Microsoft --output-dir ./output

  # Adjust years back (default: 5)
  python -m app.pipelines.patent_runner --companies Microsoft --years 3

  # Limit results per company (max 1000)
  python -m app.pipelines.patent_runner --companies Microsoft --results 50

Get API key at: https://patentsview.org/apis/keyrequest
        """
    )
    parser.add_argument(
        "--companies",
        nargs="+",
        required=True,
        help="Company names to fetch patents for"
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        help="PatentsView API key (or set PATENTSVIEW_API_KEY env var)"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/signals/patents",
        help="Output directory for JSON files (default: data/signals/patents)"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.5,
        help="Delay between requests in seconds (default: 1.5)"
    )
    parser.add_argument(
        "--years",
        type=int,
        default=5,
        help="Years back to search (default: 5)"
    )
    parser.add_argument(
        "--results",
        type=int,
        default=100,
        help="Max results per company (default: 100, max: 1000)"
    )

    args = parser.parse_args()

    await run_patent_pipeline(
        companies=args.companies,
        output_dir=args.output_dir,
        request_delay=args.delay,
        years_back=args.years,
        results_per_company=args.results,
        api_key=args.api_key,
    )


if __name__ == "__main__":
    asyncio.run(main())
