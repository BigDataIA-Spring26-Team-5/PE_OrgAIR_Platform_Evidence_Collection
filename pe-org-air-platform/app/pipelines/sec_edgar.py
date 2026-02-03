# from __future__ import annotations

# from pathlib import Path
# from typing import List, Optional
# import asyncio
# import os

# from sec_edgar_downloader import Downloader

# from app.pipelines.pipeline_state import PipelineState


# FILING_TYPES = ["10-K", "10-Q", "8-K", "DEF 14A"]


# def step1_initialize_pipeline(
#     *,
#     company_name: Optional[str] = None,
#     email_address: Optional[str] = None,
#     download_dir: str = "data/raw/sec"
# ) -> PipelineState:
#     """
#     If company_name/email aren't passed, try environment variables.
#     This makes it feel like you only need ticker to run.
#     """
#     company_name = company_name or os.getenv("SEC_DOWNLOADER_COMPANY_NAME") or "cs2-student"
#     email_address = email_address or os.getenv("SEC_DOWNLOADER_EMAIL") or "student@example.com"

#     state = PipelineState(
#         company_name=company_name,
#         email_address=email_address,
#         download_dir=Path(download_dir),
#     )
#     state.download_dir.mkdir(parents=True, exist_ok=True)
#     print(f"✓ Pipeline initialized (requester={state.company_name}, email={state.email_address})")
#     return state


# def step2_add_downloader(state: PipelineState) -> PipelineState:
#     state.downloader = Downloader(
#         company_name=state.company_name,
#         email_address=state.email_address,
#         download_folder=str(state.download_dir)
#     )
#     print("✓ SEC Downloader initialized")
#     return state


# def step3_configure_rate_limiting(state: PipelineState, request_delay: float = 0.1) -> PipelineState:
#     state.request_delay = request_delay
#     print(f"✓ Rate limiting set to {request_delay}s between requests")
#     return state


# async def step4_download_filings(
#     state: PipelineState,
#     *,
#     tickers: List[str],
#     filing_types: Optional[List[str]] = None,
#     after_date: Optional[str] = None,
#     limit: int = 2
# ) -> PipelineState:
#     """
#     Downloads filings for tickers and collects paths to full-submission.txt.
#     """
#     if not state.downloader:
#         raise ValueError("Downloader not initialized. Run step2_add_downloader first.")

#     filing_types = filing_types or FILING_TYPES
#     state.downloaded_filings = []

#     for ticker in tickers:
#         ticker = ticker.upper()
#         state.summary["attempted_downloads"] += len(filing_types) * limit

#         for filing_type in filing_types:
#             await asyncio.sleep(state.request_delay)

#             try:
#                 state.downloader.get(filing_type, ticker, after=after_date, limit=limit)

#                 # sec-edgar-downloader folder:
#                 # <download_dir>/sec-edgar-filings/<TICKER>/<FILING_TYPE>/<accession>/full-submission.txt
#                 filing_dir = state.download_dir / "sec-edgar-filings" / ticker / filing_type
#                 if filing_dir.exists():
#                     for submission in filing_dir.glob("**/full-submission.txt"):
#                         accession = submission.parent.name
#                         state.downloaded_filings.append({
#                             "ticker": ticker,
#                             "filing_type": filing_type,
#                             "accession_number": accession,
#                             "path": str(submission)
#                         })

#             except Exception as e:
#                 state.summary["details"].append({
#                     "ticker": ticker,
#                     "filing_type": filing_type,
#                     "status": "download_failed",
#                     "error": str(e),
#                 })

#     print(f"✓ Downloaded {len(state.downloaded_filings)} filings")
#     return state


from __future__ import annotations

import logging
from pathlib import Path
from typing import List

from sec_edgar_downloader import Downloader

logger = logging.getLogger(__name__)


class SECEdgarPipeline:
    """
    Pipeline for downloading SEC filings using sec-edgar-downloader.

    Downloads are stored under:
      {download_dir}/sec-edgar-filings/{ticker}/{filing_type}/.../full-submission.txt
    """

    SUPPORTED_FILING_TYPES = ["10-K", "10-Q", "8-K", "DEF 14A"]

    def __init__(
        self,
        company_name: str,
        email: str,
        download_dir: Path = Path("data/raw/sec"),
    ):
        self.download_dir = download_dir
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.dl = Downloader(company_name, email, self.download_dir)

    def download_filings(
        self,
        ticker: str,
        filing_types: List[str] | None = None,
        limit: int = 10,
        after: str = "2020-01-01",
    ) -> List[Path]:
        """
        Download filings for a company.

        Returns:
            list[Path]: paths to downloaded full-submission.txt files
        """
        if filing_types is None:
            filing_types = self.SUPPORTED_FILING_TYPES

        downloaded: List[Path] = []

        for filing_type in filing_types:
            try:
                self.dl.get(
                    filing_type,
                    ticker,
                    limit=limit,
                    after=after,
                )

                filing_dir = (
                    self.download_dir
                    / "sec-edgar-filings"
                    / ticker
                    / filing_type
                )

                if filing_dir.exists():
                    for filing_path in filing_dir.glob("**/full-submission.txt"):
                        downloaded.append(filing_path)
                        logger.info("Downloaded: %s", filing_path)

            except Exception as e:
                logger.exception(
                    "Error downloading %s for %s: %s",
                    filing_type,
                    ticker,
                    str(e),
                )

        return downloaded

