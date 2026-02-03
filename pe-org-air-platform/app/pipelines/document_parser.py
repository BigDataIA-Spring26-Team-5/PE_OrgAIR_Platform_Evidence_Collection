# from __future__ import annotations

# from pathlib import Path
# from typing import Any, Dict, List, Tuple
# import re

# from bs4 import BeautifulSoup
# from app.pipelines.pipeline_state import PipelineState

# try:
#     import pdfplumber
# except Exception:
#     pdfplumber = None

# try:
#     import fitz  # PyMuPDF
# except Exception:
#     fitz = None


# class DocumentParser:
#     """Parses SEC filings to extract text and tables (HTML/TXT and PDF)."""

#     def _parse_html_like(self, file_path: Path) -> Tuple[str, List[Dict[str, Any]]]:
#         raw = file_path.read_text(encoding="utf-8", errors="ignore")

#         # Use lxml if available; otherwise fallback to built-in HTML parser
#         try:
#             soup = BeautifulSoup(raw, "lxml")
#         except Exception:
#             soup = BeautifulSoup(raw, "html.parser")

#         for element in soup(["script", "style"]):
#             element.extract()

#         text = soup.get_text(separator="\n")
#         text = re.sub(r"\n\s*\n", "\n", text).strip()

#         tables_data: List[Dict[str, Any]] = []
#         for table in soup.find_all("table"):
#             headers = [th.get_text(strip=True) for th in table.find_all("th")]
#             rows = []
#             for tr in table.find_all("tr"):
#                 if tr.find("th"):
#                     continue
#                 cells = [td.get_text(strip=True) for td in tr.find_all("td")]
#                 if cells:
#                     rows.append(cells)

#             if headers and rows:
#                 tables_data.append({"headers": headers, "rows": rows})
#             elif rows:
#                 tables_data.append({"rows": rows})

#         return text, tables_data

#     def _parse_pdf(self, file_path: Path) -> Tuple[str, List[Dict[str, Any]]]:
#         if pdfplumber is None:
#             raise RuntimeError("pdfplumber not installed but PDF parsing requested.")

#         full_text: List[str] = []
#         tables_data: List[Dict[str, Any]] = []

#         with pdfplumber.open(str(file_path)) as pdf:
#             for page_num, page in enumerate(pdf.pages):
#                 page_text = page.extract_text(x_tolerance=2, y_tolerance=2)
#                 if page_text:
#                     full_text.append(page_text)

#                 page_tables = page.extract_tables()
#                 if page_tables:
#                     for t in page_tables:
#                         if t and t[0]:
#                             tables_data.append({"page": page_num + 1, "headers": t[0], "rows": t[1:]})
#                         elif t:
#                             tables_data.append({"page": page_num + 1, "rows": t})

#         # Optional fallback: PyMuPDF text may be better
#         if fitz is not None:
#             try:
#                 doc = fitz.open(str(file_path))
#                 pymu = [page.get_text("text") for page in doc]
#                 doc.close()
#                 if len("".join(pymu)) > len("".join(full_text)):
#                     full_text = pymu
#             except Exception:
#                 pass

#         final_text = "\n".join(full_text).strip()
#         final_text = re.sub(r"\n\s*\n", "\n", final_text)
#         return final_text, tables_data

#     def parse_filing(self, file_path: str) -> Dict[str, Any]:
#         path = Path(file_path)

#         # Helpful proof/debug line
#         mode = "PDF" if path.suffix.lower() == ".pdf" else "HTML/TXT"
#         print(f"[parser] {path.name} -> mode={mode}")

#         if path.suffix.lower() in [".html", ".htm", ".txt"]:
#             text, tables = self._parse_html_like(path)
#         elif path.suffix.lower() == ".pdf":
#             text, tables = self._parse_pdf(path)
#         else:
#             text, tables = "", []

#         return {"text": text, "tables": tables}


# async def step5_parse_documents(state: PipelineState) -> PipelineState:
#     if not state.downloaded_filings:
#         raise ValueError("No downloaded filings. Run step4_download_filings first.")

#     state.parser = DocumentParser()
#     state.parsed_filings = []

#     for filing in state.downloaded_filings:
#         parsed = state.parser.parse_filing(filing["path"])
#         if not parsed["text"]:
#             state.summary["parsing_errors"] += 1
#             state.summary["details"].append({
#                 "ticker": filing.get("ticker"),
#                 "filing_type": filing.get("filing_type"),
#                 "accession_number": filing.get("accession_number"),
#                 "status": "parsing_failed",
#                 "error": "No text extracted",
#             })
#             continue

#         filing["parsed_text"] = parsed["text"]
#         filing["parsed_tables"] = parsed["tables"]
#         state.parsed_filings.append(filing)

#     print(f"✓ Parsed {len(state.parsed_filings)} documents")
#     return state


# def demo_pdf_parsing(pdf_path: str = "data/Sample_10k/10-k.pdf") -> None:
#     """
#     Demo that our parser can handle PDFs (requirement proof).
#     """
#     p = Path(pdf_path)
#     if not p.exists():
#         raise FileNotFoundError(f"PDF not found: {p.resolve()}")

#     parser = DocumentParser()
#     result = parser.parse_filing(str(p))

#     text = result.get("text", "") or ""
#     tables = result.get("tables", []) or []

#     print("\n=== PDF PARSING DEMO ===")
#     print("Input:", p.resolve())
#     print("Extracted chars:", len(text))
#     print("Extracted words:", len(text.split()))
#     print("Tables found:", len(tables))
#     print("Preview:", text[:400].replace("\n", " "), "..." if len(text) > 400 else "")
#     print("✅ PDF parsing capability confirmed.")


from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Tuple

import pdfplumber
from bs4 import BeautifulSoup


@dataclass
class ParsedDocument:
    company_ticker: str
    filing_type: str
    filing_date: datetime
    content: str
    sections: Dict[str, str]
    source_path: str
    content_hash: str
    word_count: int


class DocumentParser:
    """
    Parse SEC filings from PDF/HTML/TXT (sec-edgar-downloader stores full-submission.txt).
    Extracts key 10-K sections (Item 1, 1A, 7) where present.
    """

    SECTION_PATTERNS = {
        "item_1": r"(?:\bITEM\s*1\b[\.\s]*BUSINESS\b)",
        "item_1a": r"(?:\bITEM\s*1A\b[\.\s]*RISK\s*FACTORS\b)",
        "item_7": r"(?:\bITEM\s*7\b[\.\s]*MANAGEMENT(?:'S)?\s*DISCUSSION\b)",
        "item_7a": r"(?:\bITEM\s*7A\b[\.\s]*QUANTITATIVE\b)",
    }

    def parse_filing(self, file_path: Path, ticker: str) -> ParsedDocument:
        suffix = file_path.suffix.lower()

        if suffix == ".pdf":
            content = self._parse_pdf(file_path)
        elif suffix in [".htm", ".html", ".txt"]:
            content = self._parse_html(file_path)
        else:
            raise ValueError(f"Unsupported file type: {suffix}")

        content = self._normalize_text(content)

        sections = self._extract_sections(content)
        content_hash = hashlib.sha256(content.encode("utf-8", errors="ignore")).hexdigest()

        filing_type, filing_date = self._extract_metadata(file_path)

        return ParsedDocument(
            company_ticker=ticker,
            filing_type=filing_type,
            filing_date=filing_date,
            content=content,
            sections=sections,
            source_path=str(file_path),
            content_hash=content_hash,
            word_count=len(content.split()),
        )

    def _parse_pdf(self, file_path: Path) -> str:
        parts = []
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                txt = page.extract_text()
                if txt:
                    parts.append(txt)
        return "\n\n".join(parts)

    def _parse_html(self, file_path: Path) -> str:
        raw = file_path.read_text(encoding="utf-8", errors="ignore")

        # Many SEC "full-submission.txt" files are actually HTML-ish.
        soup = BeautifulSoup(raw, "html.parser")

        for element in soup(["script", "style"]):
            element.decompose()

        text = soup.get_text(separator="\n")
        return text

    def _normalize_text(self, text: str) -> str:
        # collapse excessive whitespace but keep newlines
        lines = (ln.strip() for ln in text.splitlines())
        cleaned = "\n".join(ln for ln in lines if ln)
        # remove repeated blank lines
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        return cleaned

    def _extract_sections(self, content: str) -> Dict[str, str]:
        sections: Dict[str, str] = {}
        upper = content.upper()

        for section_name, pattern in self.SECTION_PATTERNS.items():
            match = re.search(pattern, upper)
            if not match:
                continue

            start = match.start()

            # Find the next "ITEM <number>" after some offset to avoid matching the same header
            next_item = re.search(r"\bITEM\s*\d+\b", upper[start + 200 :])
            end = start + 200 + next_item.start() if next_item else len(content)

            sections[section_name] = content[start:end][:50000]  # cap size for DB/model safety

        return sections

    def _extract_metadata(self, file_path: Path) -> Tuple[str, datetime]:
        """
        Path structure from sec-edgar-downloader:
          .../sec-edgar-filings/{ticker}/{filing_type}/{accession}/full-submission.txt
        """
        parts = file_path.parts
        filing_type = parts[-3] if len(parts) >= 3 else "UNKNOWN"
        accession = parts[-2] if len(parts) >= 2 else ""

        # accession often: 0000xxxxx-YY-NNNNNN
        m = re.search(r"-(\d{2})-", accession)
        if m:
            yy = int(m.group(1))
            year = 2000 + yy if yy < 50 else 1900 + yy
            filing_date = datetime(year, 1, 1, tzinfo=timezone.utc)
        else:
            filing_date = datetime.now(timezone.utc)

        return filing_type, filing_date
