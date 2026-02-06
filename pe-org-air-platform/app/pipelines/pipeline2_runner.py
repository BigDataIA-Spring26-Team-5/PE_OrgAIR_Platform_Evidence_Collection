"""
Pipeline 2 Runner - Job and Patent Collection
app/pipelines/pipeline2_runner.py

Scrapes job postings and fetches patents for companies.
Uses step-based architecture:
1. Extract data (jobs/patents) - scoring done by signal pipelines
2. Validate extracted data
3. Verify scores
4. Save to local directory (always runs - jobs/, patents/, techstack/, summaries/)
5. Write to Snowflake (aggregated scores) - skipped with --local-only

Examples:
  # Run complete pipeline (all steps)
  python -m app.pipelines.pipeline2_runner --companies Microsoft Google Amazon

  # Run specific step only (for debugging)
  python -m app.pipelines.pipeline2_runner --companies Microsoft --step extract
  python -m app.pipelines.pipeline2_runner --companies Microsoft --step validate
  python -m app.pipelines.pipeline2_runner --companies Microsoft --step score
  python -m app.pipelines.pipeline2_runner --companies Microsoft --step local
  python -m app.pipelines.pipeline2_runner --companies Microsoft --step snowflake

  # Skip cloud storage (local files only)
  python -m app.pipelines.pipeline2_runner --companies Microsoft --local-only

  # Custom output directory
  python -m app.pipelines.pipeline2_runner --companies Microsoft --output-dir ./output

  # Patent collection only
  python -m app.pipelines.pipeline2_runner --companies Microsoft --mode patents

  # Both jobs and patents
  python -m app.pipelines.pipeline2_runner --companies Microsoft --mode both

Get PatentsView API key at: https://patentsview.org/apis/keyrequest """

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict, Any

from dotenv import load_dotenv

from app.pipelines.pipeline2_state import Pipeline2State
from app.pipelines.job_signals import run_job_signals
from app.pipelines.patent_signals import run_patent_signals
from app.pipelines.utils import Company, safe_filename
from app.services.s3_storage import S3Storage
from app.services.snowflake import SnowflakeService

logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()


class Pipeline2Runner:
    """Pipeline 2 runner with step-based architecture similar to runner.py"""

    def __init__(self, output_dir: str = "data/signals"):
        self.state = Pipeline2State()
        self.state.output_dir = output_dir
        self.output_dir = Path(output_dir)
        self.s3 = S3Storage()
        self.snowflake = None

    def _init_snowflake(self):
        """Initialize Snowflake connection if needed"""
        if self.snowflake is None:
            self.snowflake = SnowflakeService()
    
    def _close_snowflake(self):
        """Close Snowflake connection"""
        if self.snowflake:
            self.snowflake.close()
            self.snowflake = None
    
    
    # STEP 1: EXTRACT DATA → Local + S3 (raw/)
    
    async def step_extract_data(
        self,
        *,
        companies: List[str],
        mode: str = "jobs",
        jobs_request_delay: float = 6.0,
        patents_request_delay: float = 1.5,
        jobs_results_per_company: int = 50,
        patents_results_per_company: int = 100,
        patents_years_back: int = 5,
        patents_api_key: Optional[str] = None,
        use_cloud_storage: bool = True,
    ) -> Dict[str, Any]:
        print("=" * 60)
        print("Step 1: Extract Data → S3 (raw/)")
        print("=" * 60)
        
        if not companies:
            return {"status": "error", "message": "No companies provided"}
        
        # Create state with company list
        company_list = [Company.from_name(name, i).to_dict() for i, name in enumerate(companies)]
        self.state.companies = company_list
        self.state.use_cloud_storage = use_cloud_storage
        
        print(f"\nCompanies to process: {len(self.state.companies)}")
        for c in self.state.companies:
            print(f"  - {c['name']}")
        
        s3_uploads = []
        
        # Run job signals pipeline if requested
        if mode in ["jobs", "both"]:
            print("\n" + "-" * 60)
            print("Extracting Job Postings")
            print("-" * 60)

            self.state.request_delay = jobs_request_delay
            self.state.results_per_company = jobs_results_per_company

            # Extract and score job data (skip storage - handled by pipeline2_runner)
            self.state = await run_job_signals(
                self.state,
                skip_storage=True  # Pipeline2Runner handles storage
            )

            # Track that we have job data
            if self.state.job_postings:
                s3_uploads.append(f"signals/jobs/ ({len(self.state.job_postings)} postings)")
        
        # Run patent signals pipeline if requested
        if mode in ["patents", "both"]:
            print("\n" + "-" * 60)
            print("Extracting Patent Data")
            print("-" * 60)

            self.state.request_delay = patents_request_delay
            self.state.results_per_company = patents_results_per_company

            # Extract and score patent data (skip storage - handled by pipeline2_runner)
            self.state = await run_patent_signals(
                self.state,
                years_back=patents_years_back,
                results_per_company=patents_results_per_company,
                api_key=patents_api_key,
                skip_storage=True  # Pipeline2Runner handles storage
            )

            # Track that we have patent data
            if self.state.patents:
                s3_uploads.append(f"signals/patents/ ({len(self.state.patents)} patents)")
        
        self.state.mark_step_complete("extract")
        
        return {
            "status": "success",
            "companies_processed": len(self.state.companies),
            "job_postings": self.state.summary.get('job_postings_collected', 0),
            "patents_collected": self.state.summary.get('patents_collected', 0),
            "s3_uploads": len(s3_uploads),
            "s3_files": s3_uploads,
            "mode": mode
        }
    
    
    # STEP 2: VALIDATE DATA (Previously: Read from S3)
    
    def step_validate_data(self) -> Dict[str, Any]:
        print("\n" + "=" * 60)
        print("Step 2: Validate Extracted Data")
        print("=" * 60)

        if not self.state.is_step_complete("extract"):
            return {"status": "error", "message": "Extract step not complete"}

        # Count data by company
        job_counts = {}
        patent_counts = {}

        for posting in self.state.job_postings:
            company_id = posting.get("company_id", "unknown")
            job_counts[company_id] = job_counts.get(company_id, 0) + 1

        for patent in self.state.patents:
            company_id = patent.get("company_id", "unknown")
            patent_counts[company_id] = patent_counts.get(company_id, 0) + 1

        print("\nData Summary:")
        for company in self.state.companies:
            company_id = company.get('id', '')
            company_name = company.get('name', company_id)
            jobs = job_counts.get(company_id, 0)
            patents = patent_counts.get(company_id, 0)
            print(f"  ✓ {company_name}: {jobs} jobs, {patents} patents")

        self.state.mark_step_complete("validate")

        return {
            "status": "success",
            "total_jobs": len(self.state.job_postings),
            "total_patents": len(self.state.patents),
            "companies_with_jobs": len(job_counts),
            "companies_with_patents": len(patent_counts)
        }
    
    
    # STEP 3: VERIFY SCORES
    
    def step_verify_scores(self) -> Dict[str, Any]:
        print("\n" + "=" * 60)
        print("Step 3: Verify Scores")
        print("=" * 60)

        if not self.state.is_step_complete("validate"):
            return {"status": "error", "message": "Validate step not complete"}

        print("\nJob Market Scores:")
        for company in self.state.companies:
            company_id = company.get('id', '')
            company_name = company.get('name', company_id)
            score = self.state.job_market_scores.get(company_id, 0)
            if score > 0:
                print(f"  ✓ {company_name}: {score:.1f}/100")

        print("\nPatent Portfolio Scores:")
        for company in self.state.companies:
            company_id = company.get('id', '')
            company_name = company.get('name', company_id)
            score = self.state.patent_scores.get(company_id, 0)
            if score > 0:
                print(f"  ✓ {company_name}: {score:.1f}/100")

        print("\nTech Stack Scores:")
        for company in self.state.companies:
            company_id = company.get('id', '')
            company_name = company.get('name', company_id)
            score = self.state.techstack_scores.get(company_id, 0)
            if score > 0:
                print(f"  ✓ {company_name}: {score:.1f}/100")

        self.state.mark_step_complete("score")

        return {
            "status": "success",
            "job_scores": len(self.state.job_market_scores),
            "patent_scores": len(self.state.patent_scores),
            "techstack_scores": len(self.state.techstack_scores)
        }

    
    # STEP 4: SAVE TO LOCAL DIRECTORY (Always runs)
    
    def step_save_to_local(self) -> Dict[str, Any]:
        """Save all collected data to local JSON files."""
        print("\n" + "=" * 60)
        print("Step 4: Save to Local Directory")
        print("=" * 60)

        if not self.state.is_step_complete("score"):
            return {"status": "error", "message": "Score step not complete"}

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        files_saved = []

        # Create output directories
        jobs_dir = self.output_dir / "jobs"
        patents_dir = self.output_dir / "patents"
        techstack_dir = self.output_dir / "techstack"
        summary_dir = self.output_dir / "summaries"

        for d in [jobs_dir, patents_dir, techstack_dir, summary_dir]:
            d.mkdir(parents=True, exist_ok=True)

        # Group data by company
        company_jobs = defaultdict(list)
        company_patents = defaultdict(list)

        for posting in self.state.job_postings:
            company_jobs[posting.get("company_id", "unknown")].append(posting)

        for patent in self.state.patents:
            company_patents[patent.get("company_id", "unknown")].append(patent)

        # Save per-company job data
        if self.state.job_postings:
            print("\nSaving job data...")
            for company_id, jobs in company_jobs.items():
                company_name = self._get_company_name(company_id)
                safe_name = safe_filename(company_name)

                job_data = {
                    "company_id": company_id,
                    "company_name": company_name,
                    "collection_date": timestamp,
                    "total_jobs": len(jobs),
                    "ai_jobs": sum(1 for j in jobs if j.get("is_ai_role")),
                    "job_market_score": self.state.job_market_scores.get(company_id, 0),
                    "job_market_analysis": self.state.job_market_analyses.get(company_id, {}),
                    "jobs": jobs
                }

                job_file = jobs_dir / f"{safe_name}_{timestamp}.json"
                with open(job_file, "w", encoding="utf-8") as f:
                    json.dump(job_data, f, indent=2, default=str)
                files_saved.append(str(job_file))
                print(f"  ✓ {job_file.name}")

        # Save per-company patent data
        if self.state.patents:
            print("\nSaving patent data...")
            for company_id, patents in company_patents.items():
                company_name = self._get_company_name(company_id)
                safe_name = safe_filename(company_name)

                patent_data = {
                    "company_id": company_id,
                    "company_name": company_name,
                    "collection_date": timestamp,
                    "total_patents": len(patents),
                    "ai_patents": sum(1 for p in patents if p.get("is_ai_patent")),
                    "patent_portfolio_score": self.state.patent_scores.get(company_id, 0),
                    "patents": patents
                }

                patent_file = patents_dir / f"{safe_name}_{timestamp}.json"
                with open(patent_file, "w", encoding="utf-8") as f:
                    json.dump(patent_data, f, indent=2, default=str)
                files_saved.append(str(patent_file))
                print(f"  ✓ {patent_file.name}")

        # Save per-company techstack data
        if self.state.techstack_scores:
            print("\nSaving techstack data...")
            for company_id, score in self.state.techstack_scores.items():
                company_name = self._get_company_name(company_id)
                safe_name = safe_filename(company_name)

                techstack_data = {
                    "company_id": company_id,
                    "company_name": company_name,
                    "collection_date": timestamp,
                    "techstack_score": score,
                    "techstack_keywords": self.state.company_techstacks.get(company_id, []),
                    "techstack_analysis": self.state.techstack_analyses.get(company_id, {})
                }

                techstack_file = techstack_dir / f"{safe_name}_{timestamp}.json"
                with open(techstack_file, "w", encoding="utf-8") as f:
                    json.dump(techstack_data, f, indent=2, default=str)
                files_saved.append(str(techstack_file))
                print(f"  ✓ {techstack_file.name}")

        # Save overall summary
        print("\nSaving summary...")
        summary_data = {
            "collection_date": timestamp,
            "companies_processed": len(self.state.companies),
            "companies": [c.get("name", c.get("id")) for c in self.state.companies],
            "total_jobs": len(self.state.job_postings),
            "total_patents": len(self.state.patents),
            "ai_jobs": sum(1 for j in self.state.job_postings if j.get("is_ai_role")),
            "ai_patents": sum(1 for p in self.state.patents if p.get("is_ai_patent")),
            "scores": {
                "job_market": self.state.job_market_scores,
                "patent_portfolio": self.state.patent_scores,
                "techstack": self.state.techstack_scores
            },
            "errors": self.state.summary.get("errors", [])
        }

        summary_file = summary_dir / f"pipeline2_summary_{timestamp}.json"
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump(summary_data, f, indent=2, default=str)
        files_saved.append(str(summary_file))
        print(f"  ✓ {summary_file.name}")

        self.state.mark_step_complete("local_save")

        print(f"\n✅ Saved {len(files_saved)} files to {self.output_dir}")

        return {
            "status": "success",
            "files_saved": len(files_saved),
            "output_dir": str(self.output_dir),
            "files": files_saved
        }

    
    # STEP 5: WRITE TO SNOWFLAKE
    
    def step_write_to_snowflake(self) -> Dict[str, Any]:

        print("\n" + "=" * 60)
        print("Step 5: Write to Snowflake")
        print("=" * 60)

        if not self.state.is_step_complete("local_save"):
            return {"status": "error", "message": "Local save step not complete"}

        if not self.state.use_cloud_storage:
            print("Cloud storage not enabled, skipping Snowflake write")
            return {"status": "skipped", "message": "Local mode enabled"}

        self._init_snowflake()

        try:
            snowflake_inserts = {
                "company_signal_summaries": 0
            }

            # Write aggregated scores to company_signal_summaries table
            print("\nWriting scores to company_signal_summaries table...")
            for company in self.state.companies:
                company_name = company['name']
                company_id = company.get('id', '')

                # Get scores for this company
                job_score = self.state.job_market_scores.get(company_id, 0)
                patent_score = self.state.patent_scores.get(company_id, 0)
                techstack_score = self.state.techstack_scores.get(company_id, 0)

                if job_score > 0 or patent_score > 0 or techstack_score > 0:
                    try:
                        # Calculate composite score
                        scores = [s for s in [job_score, patent_score, techstack_score] if s > 0]
                        total_score = sum(scores) / len(scores) if scores else 0

                        self.snowflake.insert_company_signal_summary(
                            company_id=company_id,
                            company_name=company_name,
                            job_market_score=job_score,
                            patent_portfolio_score=patent_score,
                            techstack_score=techstack_score,
                            total_score=total_score,
                            calculated_at=datetime.now(timezone.utc)
                        )
                        snowflake_inserts["company_signal_summaries"] += 1
                        print(f"  ✓ {company_name}: Job={job_score:.1f}, Patent={patent_score:.1f}, Tech={techstack_score:.1f}")
                    except Exception as e:
                        print(f"  ✗ Error inserting scores for {company_name}: {e}")

            self.state.mark_step_complete("snowflake_write")

            return {
                "status": "success",
                "snowflake_inserts": snowflake_inserts,
                "companies_processed": len(self.state.companies)
            }

        finally:
            self._close_snowflake()
    
    
    # COMPLETE PIPELINE RUN
    
    async def run_pipeline(
        self,
        *,
        companies: List[str],
        mode: str = "jobs",
        jobs_request_delay: float = 6.0,
        patents_request_delay: float = 1.5,
        jobs_results_per_company: int = 50,
        patents_results_per_company: int = 100,
        patents_years_back: int = 5,
        patents_api_key: Optional[str] = None,
        use_cloud_storage: bool = True,
    ) -> Dict[str, Any]:
        """
        Complete Pipeline 2 execution with all steps.
        """
        results = {}

        # Step 1: Extract and score data (jobs/patents)
        results["step1_extract"] = await self.step_extract_data(
            companies=companies,
            mode=mode,
            jobs_request_delay=jobs_request_delay,
            patents_request_delay=patents_request_delay,
            jobs_results_per_company=jobs_results_per_company,
            patents_results_per_company=patents_results_per_company,
            patents_years_back=patents_years_back,
            patents_api_key=patents_api_key,
            use_cloud_storage=use_cloud_storage
        )

        # Step 2: Validate extracted data
        results["step2_validate"] = self.step_validate_data()

        # Step 3: Verify scores
        results["step3_score"] = self.step_verify_scores()

        # Step 4: Save to local directory (always runs)
        results["step4_local"] = self.step_save_to_local()

        # Step 5: Write to Snowflake (only if cloud storage enabled)
        results["step5_snowflake"] = self.step_write_to_snowflake()

        # Print summary
        self._print_summary()

        return results
    
    def _print_summary(self) -> None:
        """Print pipeline execution summary."""
        print("\n" + "=" * 60)
        print("Pipeline 2 Complete - Summary")
        print("=" * 60)
        
        print(f"\nSteps completed: {', '.join(self.state.steps_completed)}")
        
        if self.state.job_market_scores:
            print("\nJob Market Scores:")
            for company_id, score in self.state.job_market_scores.items():
                company_name = self._get_company_name(company_id)
                print(f"  {company_name}: {score:.2f}/100")
        
        if self.state.patent_scores:
            print("\nPatent Portfolio Scores:")
            for company_id, score in self.state.patent_scores.items():
                company_name = self._get_company_name(company_id)
                print(f"  {company_name}: {score:.2f}/100")
        
        print(f"\nErrors: {len(self.state.summary.get('errors', []))}")

        print("\nLocal Storage:")
        print(f"  {self.output_dir}/jobs/{{company}}_{{timestamp}}.json")
        print(f"  {self.output_dir}/patents/{{company}}_{{timestamp}}.json")
        print(f"  {self.output_dir}/techstack/{{company}}_{{timestamp}}.json")
        print(f"  {self.output_dir}/summaries/pipeline2_summary_{{timestamp}}.json")

        if self.state.use_cloud_storage:
            print("\nCloud Storage:")
            print("  Snowflake: company_signal_summaries table (aggregated scores)")
    
    def _get_company_name(self, company_id: str) -> str:
        """Get company name from ID."""
        for c in self.state.companies:
            if c.get("id") == company_id:
                return c.get("name", company_id)
        return company_id


async def run_pipeline2(
    *,
    companies: Optional[List[str]] = None,
    mode: str = "jobs",
    output_dir: str = "data/signals",
    jobs_request_delay: float = 6.0,
    patents_request_delay: float = 1.5,
    jobs_results_per_company: int = 50,
    patents_results_per_company: int = 100,
    patents_years_back: int = 5,
    patents_api_key: Optional[str] = None,
    use_cloud_storage: bool = True,
) -> Pipeline2State:
    runner = Pipeline2Runner(output_dir=output_dir)

    await runner.run_pipeline(
        companies=companies or [],
        mode=mode,
        jobs_request_delay=jobs_request_delay,
        patents_request_delay=patents_request_delay,
        jobs_results_per_company=jobs_results_per_company,
        patents_results_per_company=patents_results_per_company,
        patents_years_back=patents_years_back,
        patents_api_key=patents_api_key,
        use_cloud_storage=use_cloud_storage
    )

    return runner.state


async def main():
    """CLI entry point for Pipeline 2 with step-based architecture."""
    parser = argparse.ArgumentParser(
        description="Pipeline 2: Job and Patent Collection (Step-based)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--companies", nargs="+", required=True, help="Company names to process")
    parser.add_argument("--mode", choices=["jobs", "patents", "both"], default="jobs",
                        help="Pipeline mode: jobs (default), patents, or both")
    parser.add_argument("--step", choices=["extract", "validate", "score", "local", "snowflake", "all"], default="all",
                        help="Run specific step only (default: all)")
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
                        help="Skip cloud storage (S3 and Snowflake), only save locally")
    parser.add_argument("--output-dir", default="data/signals", dest="output_dir",
                        help="Output directory for local JSON files (default: data/signals)")

    args = parser.parse_args()

    runner = Pipeline2Runner(output_dir=args.output_dir)

    if args.step == "all":
        # Run complete pipeline
        await runner.run_pipeline(
            companies=args.companies,
            mode=args.mode,
            jobs_request_delay=args.jobs_request_delay,
            patents_request_delay=args.patents_request_delay,
            jobs_results_per_company=args.jobs_results_per_company,
            patents_results_per_company=args.patents_results_per_company,
            patents_years_back=args.patents_years_back,
            patents_api_key=args.patents_api_key,
            use_cloud_storage=not args.local_only,
        )
    else:
        # Run specific step only
        if args.step == "extract":
            await runner.step_extract_data(
                companies=args.companies,
                mode=args.mode,
                jobs_request_delay=args.jobs_request_delay,
                patents_request_delay=args.patents_request_delay,
                jobs_results_per_company=args.jobs_results_per_company,
                patents_results_per_company=args.patents_results_per_company,
                patents_years_back=args.patents_years_back,
                patents_api_key=args.patents_api_key,
                use_cloud_storage=not args.local_only,
            )
        elif args.step == "validate":
            runner.step_validate_data()
        elif args.step == "score":
            runner.step_verify_scores()
        elif args.step == "local":
            runner.step_save_to_local()
        elif args.step == "snowflake":
            runner.step_write_to_snowflake()


if __name__ == "__main__":
    asyncio.run(main())