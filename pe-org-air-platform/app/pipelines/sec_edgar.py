from __future__ import annotations

import logging
import re
import time
import requests
from pathlib import Path
from typing import List, Optional

from sec_edgar_downloader import Downloader

logger = logging.getLogger(__name__)


class SECEdgarPipeline:
    """
    Pipeline for downloading SEC filings using sec-edgar-downloader.

    Downloads:
    - full-submission.txt (main HTML filing)
    - Any PDF/Excel exhibits attached to the filing

    Downloads are stored under:
      {download_dir}/sec-edgar-filings/{ticker}/{filing_type}/{accession}/
        ├── full-submission.txt
        └── exhibits/
            ├── *.pdf
            └── *.xlsx
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
        self.company_name = company_name
        self.email = email
        self.headers = {
            "User-Agent": f"{company_name} ({email})",
            "Accept-Encoding": "gzip, deflate",
        }

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

                        # Download PDF/Excel exhibits
                        self._download_exhibits(filing_path, ticker)

            except Exception as e:
                logger.exception(
                    "Error downloading %s for %s: %s",
                    filing_type,
                    ticker,
                    str(e),
                )

        return downloaded

    def _download_exhibits(self, submission_path: Path, ticker: str) -> List[Path]:
        """
        Download PDF and Excel exhibits from a filing.
        
        Parses the full-submission.txt to find exhibit filenames and downloads them.
        """
        downloaded_exhibits: List[Path] = []
        
        try:
            content = submission_path.read_text(encoding="utf-8", errors="ignore")
            
            # Extract CIK from the submission
            cik = self._extract_cik(content)
            if not cik:
                logger.warning("Could not extract CIK from %s", submission_path)
                return downloaded_exhibits
            
            # Get accession number from folder name
            accession = submission_path.parent.name
            accession_clean = accession.replace("-", "")
            
            # Build base URL
            base_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_clean}/"
            
            # Find all filenames in the submission (PDFs, Excel, HTM)
            # Pattern matches <FILENAME>something.ext
            filename_pattern = r'<FILENAME>([^\n<]+)'
            matches = re.findall(filename_pattern, content)
            
            # Filter for PDFs and Excel files
            exhibit_files = [
                f.strip() for f in matches 
                if f.strip().lower().endswith(('.pdf', '.xlsx', '.xls'))
            ]
            
            if not exhibit_files:
                logger.info("No PDF/Excel exhibits found in %s", submission_path)
                return downloaded_exhibits
            
            # Create exhibits directory
            exhibits_dir = submission_path.parent / "exhibits"
            exhibits_dir.mkdir(parents=True, exist_ok=True)
            
            for filename in exhibit_files:
                local_path = exhibits_dir / filename
                
                # Skip if already downloaded
                if local_path.exists():
                    downloaded_exhibits.append(local_path)
                    continue
                
                try:
                    url = base_url + filename
                    logger.info("Downloading exhibit: %s", url)
                    
                    response = requests.get(url, headers=self.headers, timeout=60)
                    
                    if response.status_code == 200:
                        local_path.write_bytes(response.content)
                        downloaded_exhibits.append(local_path)
                        logger.info("Saved exhibit: %s", local_path)
                    else:
                        logger.warning("Failed to download %s: HTTP %d", url, response.status_code)
                    
                    # Rate limiting (SEC requires 10 requests/sec max)
                    time.sleep(0.15)
                    
                except Exception as e:
                    logger.warning("Error downloading exhibit %s: %s", filename, str(e))
            
        except Exception as e:
            logger.warning("Error processing exhibits for %s: %s", submission_path, str(e))
        
        return downloaded_exhibits

    def _extract_cik(self, content: str) -> Optional[str]:
        """Extract CIK (Central Index Key) from submission content."""
        patterns = [
            r'CENTRAL INDEX KEY:\s*(\d+)',
            r'<CIK>(\d+)',
            r'"cik":\s*"?(\d+)"?',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return None