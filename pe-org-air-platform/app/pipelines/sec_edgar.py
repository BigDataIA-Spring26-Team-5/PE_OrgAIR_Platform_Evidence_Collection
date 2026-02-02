from __future__ import annotations

from sec_edgar_downloader import Downloader
from pathlib import Path
import logging
from typing import List

logger = logging.getLogger(__name__)


class SECEdgarPipeline:
    """Pipeline for downloading SEC filings."""

    def __init__(
        self,
        company_name: str,
        email: str,
        download_dir: Path | None = None,
    ):
        # Default: repo_root/data/raw/sec
        self.download_dir = download_dir or Path("data/raw/sec")
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
        Returns list of paths to downloaded filings.
        """
        filing_types = filing_types or ["10-K", "10-Q", "8-K"]
        downloaded: List[Path] = []

        for filing_type in filing_types:
            try:
                self.dl.get(filing_type, ticker, limit=limit, after=after)

                # sec-edgar-downloader writes into: <download_dir>/sec-edgar-filings/<ticker>/<filing_type>/
                filing_dir = self.download_dir / "sec-edgar-filings" / ticker / filing_type

                if filing_dir.exists():
                    for filing_path in filing_dir.glob("**/full-submission.txt"):
                        downloaded.append(filing_path)
                        logger.info("Downloaded: %s", filing_path)
                else:
                    logger.warning("Expected filing directory not found: %s", filing_dir)

            except Exception as e:
                logger.exception("Error downloading %s for %s: %s", filing_type, ticker, e)

        return downloaded
