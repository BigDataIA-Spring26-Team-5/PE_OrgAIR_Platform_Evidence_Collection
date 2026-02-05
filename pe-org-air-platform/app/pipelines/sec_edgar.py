# from __future__ import annotations

# import logging
# import re
# import time
# import requests
# from pathlib import Path
# from typing import List, Optional

# from sec_edgar_downloader import Downloader

# logger = logging.getLogger(__name__)


# class SECEdgarPipeline:
#     """
#     Pipeline for downloading SEC filings using sec-edgar-downloader.

#     Downloads:
#     - full-submission.txt (main HTML filing)
#     - Any PDF/Excel exhibits attached to the filing

#     Downloads are stored under:
#       {download_dir}/sec-edgar-filings/{ticker}/{filing_type}/{accession}/
#         ├── full-submission.txt
#         └── exhibits/
#             ├── *.pdf
#             └── *.xlsx
#     """

#     SUPPORTED_FILING_TYPES = ["10-K", "10-Q", "8-K", "DEF 14A"]

#     def __init__(
#         self,
#         company_name: str,
#         email: str,
#         download_dir: Path = Path("data/raw/sec"),
#     ):
#         self.download_dir = download_dir
#         self.download_dir.mkdir(parents=True, exist_ok=True)
#         self.dl = Downloader(company_name, email, self.download_dir)
#         self.company_name = company_name
#         self.email = email
#         self.headers = {
#             "User-Agent": f"{company_name} ({email})",
#             "Accept-Encoding": "gzip, deflate",
#         }

#     def download_filings(
#         self,
#         ticker: str,
#         filing_types: List[str] | None = None,
#         limit: int = 10,
#         after: str = "2020-01-01",
#     ) -> List[Path]:
#         """
#         Download filings for a company.

#         Returns:
#             list[Path]: paths to downloaded full-submission.txt files
#         """
#         if filing_types is None:
#             filing_types = self.SUPPORTED_FILING_TYPES

#         downloaded: List[Path] = []

#         for filing_type in filing_types:
#             try:
#                 self.dl.get(
#                     filing_type,
#                     ticker,
#                     limit=limit,
#                     after=after,
#                 )

#                 filing_dir = (
#                     self.download_dir
#                     / "sec-edgar-filings"
#                     / ticker
#                     / filing_type
#                 )

#                 if filing_dir.exists():
#                     for filing_path in filing_dir.glob("**/full-submission.txt"):
#                         downloaded.append(filing_path)
#                         logger.info("Downloaded: %s", filing_path)

#                         # Download PDF/Excel exhibits
#                         self._download_exhibits(filing_path, ticker)

#             except Exception as e:
#                 logger.exception(
#                     "Error downloading %s for %s: %s",
#                     filing_type,
#                     ticker,
#                     str(e),
#                 )

#         return downloaded

#     def _download_exhibits(self, submission_path: Path, ticker: str) -> List[Path]:
#         """
#         Download PDF and Excel exhibits from a filing.
        
#         Parses the full-submission.txt to find exhibit filenames and downloads them.
#         """
#         downloaded_exhibits: List[Path] = []
        
#         try:
#             content = submission_path.read_text(encoding="utf-8", errors="ignore")
            
#             # Extract CIK from the submission
#             cik = self._extract_cik(content)
#             if not cik:
#                 logger.warning("Could not extract CIK from %s", submission_path)
#                 return downloaded_exhibits
            
#             # Get accession number from folder name
#             accession = submission_path.parent.name
#             accession_clean = accession.replace("-", "")
            
#             # Build base URL
#             base_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_clean}/"
            
#             # Find all filenames in the submission (PDFs, Excel, HTM)
#             # Pattern matches <FILENAME>something.ext
#             filename_pattern = r'<FILENAME>([^\n<]+)'
#             matches = re.findall(filename_pattern, content)
            
#             # Filter for PDFs and Excel files
#             exhibit_files = [
#                 f.strip() for f in matches 
#                 if f.strip().lower().endswith(('.pdf', '.xlsx', '.xls'))
#             ]
            
#             if not exhibit_files:
#                 logger.info("No PDF/Excel exhibits found in %s", submission_path)
#                 return downloaded_exhibits
            
#             # Create exhibits directory
#             exhibits_dir = submission_path.parent / "exhibits"
#             exhibits_dir.mkdir(parents=True, exist_ok=True)
            
#             for filename in exhibit_files:
#                 local_path = exhibits_dir / filename
                
#                 # Skip if already downloaded
#                 if local_path.exists():
#                     downloaded_exhibits.append(local_path)
#                     continue
                
#                 try:
#                     url = base_url + filename
#                     logger.info("Downloading exhibit: %s", url)
                    
#                     response = requests.get(url, headers=self.headers, timeout=60)
                    
#                     if response.status_code == 200:
#                         local_path.write_bytes(response.content)
#                         downloaded_exhibits.append(local_path)
#                         logger.info("Saved exhibit: %s", local_path)
#                     else:
#                         logger.warning("Failed to download %s: HTTP %d", url, response.status_code)
                    
#                     # Rate limiting (SEC requires 10 requests/sec max)
#                     time.sleep(0.15)
                    
#                 except Exception as e:
#                     logger.warning("Error downloading exhibit %s: %s", filename, str(e))
            
#         except Exception as e:
#             logger.warning("Error processing exhibits for %s: %s", submission_path, str(e))
        
#         return downloaded_exhibits

#     def _extract_cik(self, content: str) -> Optional[str]:
#         """Extract CIK (Central Index Key) from submission content."""
#         patterns = [
#             r'CENTRAL INDEX KEY:\s*(\d+)',
#             r'<CIK>(\d+)',
#             r'"cik":\s*"?(\d+)"?',
#         ]
        
#         for pattern in patterns:
#             match = re.search(pattern, content, re.IGNORECASE)
#             if match:
#                 return match.group(1)
        
#         return None


import os
import time
import logging
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Generator
from dataclasses import dataclass
from app.config import settings

logger = logging.getLogger(__name__)

@dataclass
class SECFiling:
    accession_number: str
    filing_type: str
    filing_date: str
    primary_document: str
    primary_doc_url: str
    filing_url: str

class SECEdgarCollector:
    """SEC EDGAR filing collector with rate limiting"""
    
    BASE_URL = "https://www.sec.gov"
    SUBMISSIONS_URL = "https://data.sec.gov/submissions"
    ARCHIVES_URL = "https://www.sec.gov/Archives/edgar/data"
    
    # Mapping of ticker to CIK (Central Index Key)
    TICKER_TO_CIK = {
        "CAT": "0000018230",
        "DE": "0000315189",
        "UNH": "0000731766",
        "HCA": "0000860730",
        "ADP": "0000008670",
        "PAYX": "0000723531",
        "WMT": "0000104169",
        "TGT": "0000027419",
        "JPM": "0000019617",
        "GS": "0000886982",
    }
    
    # Only the primary filing types (no amendments or supplemental)
    FILING_TYPE_MAP = {
        "10-K": ["10-K"],       # Annual report
        "10-Q": ["10-Q"],       # Quarterly report
        "8-K": ["8-K"],         # Material events
        "DEF 14A": ["DEF 14A"], # Definitive proxy statement (annual)
    }

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": settings.SEC_USER_AGENT,
            "Accept-Encoding": "gzip, deflate",
        })
        self.rate_limit = settings.SEC_RATE_LIMIT
        self.last_request_time = 0
        logger.info(f"SEC Edgar Collector initialized (Rate limit: {self.rate_limit}/sec)")

    def _rate_limit_wait(self):
        """Enforce SEC rate limiting (10 requests per second)"""
        elapsed = time.time() - self.last_request_time
        min_interval = 1.0 / self.rate_limit
        if elapsed < min_interval:
            sleep_time = min_interval - elapsed
            logger.debug(f"  ⏳ Rate limiting: sleeping {sleep_time:.3f}s")
            time.sleep(sleep_time)
        self.last_request_time = time.time()

    def _make_request(self, url: str) -> Optional[requests.Response]:
        """Make rate-limited request to SEC"""
        self._rate_limit_wait()
        try:
            logger.debug(f"  🌐 Requesting: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logger.error(f"  ❌ Request failed: {url} - {e}")
            return None

    def get_cik(self, ticker: str) -> Optional[str]:
        """Get CIK for a ticker"""
        cik = self.TICKER_TO_CIK.get(ticker.upper())
        if cik:
            return cik
        
        # Try to look up CIK from SEC
        logger.info(f"  🔍 Looking up CIK for {ticker}")
        url = f"{self.SUBMISSIONS_URL}/CIK{ticker.upper()}.json"
        response = self._make_request(url)
        if response:
            data = response.json()
            return str(data.get("cik", "")).zfill(10)
        return None

    def get_company_filings(
        self,
        ticker: str,
        filing_types: List[str],
        years_back: int = 3
    ) -> Generator[SECFiling, None, None]:
        """
        Fetch filings for a company.
        Yields SECFiling objects for each matching filing.
        """
        cik = self.get_cik(ticker)
        if not cik:
            logger.error(f"❌ Could not find CIK for ticker: {ticker}")
            return

        cik_no_padding = cik.lstrip("0")
        logger.info(f"📋 Fetching filings for {ticker} (CIK: {cik})")
        
        # Get company submissions
        url = f"{self.SUBMISSIONS_URL}/CIK{cik}.json"
        response = self._make_request(url)
        if not response:
            return

        data = response.json()
        filings = data.get("filings", {}).get("recent", {})
        
        # Calculate date cutoff
        cutoff_date = datetime.now() - timedelta(days=years_back * 365)
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")
        logger.info(f"  📅 Looking for filings after {cutoff_str}")
        
        # Build list of acceptable form types
        acceptable_forms = []
        for ft in filing_types:
            acceptable_forms.extend(self.FILING_TYPE_MAP.get(ft, [ft]))
        
        # Process filings
        form_list = filings.get("form", [])
        date_list = filings.get("filingDate", [])
        accession_list = filings.get("accessionNumber", [])
        primary_doc_list = filings.get("primaryDocument", [])
        
        found_count = 0
        for i, form in enumerate(form_list):
            filing_date = date_list[i]
            
            # Check date
            if filing_date < cutoff_str:
                continue
                
            # Check form type
            if form not in acceptable_forms:
                continue
            
            accession = accession_list[i].replace("-", "")
            primary_doc = primary_doc_list[i]
            
            # Build URLs
            filing_url = f"{self.ARCHIVES_URL}/{cik_no_padding}/{accession}"
            primary_doc_url = f"{filing_url}/{primary_doc}"
            
            found_count += 1
            logger.info(f"  📄 Found: {form} filed {filing_date}")
            
            yield SECFiling(
                accession_number=accession_list[i],
                filing_type=form,
                filing_date=filing_date,
                primary_document=primary_doc,
                primary_doc_url=primary_doc_url,
                filing_url=filing_url
            )
        
        logger.info(f"  ✅ Found {found_count} filings for {ticker}")

    def download_filing(self, filing: SECFiling) -> Optional[bytes]:
        """Download the primary document of a filing"""
        logger.info(f"  ⬇️  Downloading: {filing.filing_type} ({filing.filing_date})")
        response = self._make_request(filing.primary_doc_url)
        if response:
            logger.info(f"  ✅ Downloaded {len(response.content):,} bytes")
            return response.content
        return None

    def download_filing_index(self, filing: SECFiling) -> Optional[Dict]:
        """Download the filing index to get all documents"""
        index_url = f"{filing.filing_url}/index.json"
        response = self._make_request(index_url)
        if response:
            return response.json()
        return None


# Singleton
_collector: Optional[SECEdgarCollector] = None

def get_sec_collector() -> SECEdgarCollector:
    global _collector
    if _collector is None:
        _collector = SECEdgarCollector()
    return _collector