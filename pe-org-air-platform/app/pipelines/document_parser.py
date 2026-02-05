# from __future__ import annotations

# import hashlib
# import logging
# import re
# from dataclasses import dataclass
# from datetime import datetime, timezone
# from pathlib import Path
# from typing import Dict, Tuple, Optional, List, Any

# import pdfplumber
# from bs4 import BeautifulSoup

# logger = logging.getLogger(__name__)


# @dataclass
# class ParsedDocument:
#     company_ticker: str
#     filing_type: str
#     filing_date: datetime
#     content: str
#     sections: Dict[str, str]
#     tables: List[Dict[str, Any]]  # List of extracted tables as dicts
#     source_path: str
#     content_hash: str
#     word_count: int
#     table_count: int = 0


# class DocumentParser:
#     """
#     Parse SEC filings from PDF/HTML/TXT.
    
#     Extracts:
#     - Key 10-K sections (Item 1, 1A, 7) with FULL content
#     - ALL tables in the document (from both HTML and PDF)
#     """

#     SECTION_START_PATTERNS = {
#         "item_1": [
#             r"(?:^|\n)\s*ITEM\s+1\.?\s*[\.\-\—\–]?\s*BUSINESS\s*\n",
#             r"(?:^|\n)\s*ITEM\s+1\s*\n\s*BUSINESS\s*\n",
#         ],
#         "item_1a": [
#             r"(?:^|\n)\s*ITEM\s+1A\.?\s*[\.\-\—\–]?\s*RISK\s+FACTORS\s*\n",
#             r"(?:^|\n)\s*ITEM\s+1A\s*\n\s*RISK\s+FACTORS\s*\n",
#         ],
#         "item_7": [
#             r"(?:^|\n)\s*ITEM\s+7\.?\s*[\.\-\—\–]?\s*MANAGEMENT(?:'?S)?\s+DISCUSSION\s+AND\s+ANALYSIS",
#             r"(?:^|\n)\s*ITEM\s+7\s*\n\s*MANAGEMENT(?:'?S)?\s+DISCUSSION",
#         ],
#     }

#     SECTION_END_PATTERNS = {
#         "item_1": [
#             r"(?:^|\n)\s*ITEM\s+1A\.?\s*[\.\-\—\–]?\s*RISK\s+FACTORS",
#             r"(?:^|\n)\s*ITEM\s+1B",
#             r"(?:^|\n)\s*ITEM\s+2\.?\s*[\.\-\—\–]?\s*PROPERTIES",
#         ],
#         "item_1a": [
#             r"(?:^|\n)\s*ITEM\s+1B\.?\s*[\.\-\—\–]?\s*UNRESOLVED",
#             r"(?:^|\n)\s*ITEM\s+1C",
#             r"(?:^|\n)\s*ITEM\s+2\.?\s*[\.\-\—\–]?\s*PROPERTIES",
#         ],
#         "item_7": [
#             r"(?:^|\n)\s*ITEM\s+7A\.?\s*[\.\-\—\–]?\s*QUANTITATIVE",
#             r"(?:^|\n)\s*ITEM\s+8\.?\s*[\.\-\—\–]?\s*FINANCIAL\s+STATEMENTS",
#         ],
#     }

#     def parse_filing(self, file_path: Path, ticker: str) -> ParsedDocument:
#         """Parse a filing (HTML/TXT or PDF) and extract content + tables."""
#         suffix = file_path.suffix.lower()
        
#         logger.info(f"Parsing file: {file_path} (type: {suffix})")

#         if suffix == ".pdf":
#             content, tables = self._parse_pdf(file_path)
#             sections = {}  # PDFs typically don't have structured sections
#         elif suffix in [".htm", ".html", ".txt"]:
#             raw_content = file_path.read_text(encoding="utf-8", errors="ignore")
#             tables = self._extract_tables_from_html(raw_content)
#             content = self._parse_html_to_text(raw_content)
#             content = self._normalize_text(content)
#             sections = self._extract_sections(content)
#         else:
#             raise ValueError(f"Unsupported file type: {suffix}")

#         content_hash = hashlib.sha256(content.encode("utf-8", errors="ignore")).hexdigest()
#         filing_type, filing_date = self._extract_metadata(file_path)

#         return ParsedDocument(
#             company_ticker=ticker,
#             filing_type=filing_type,
#             filing_date=filing_date,
#             content=content,
#             sections=sections,
#             tables=tables,
#             source_path=str(file_path),
#             content_hash=content_hash,
#             word_count=len(content.split()),
#             table_count=len(tables),
#         )

#     # =========================================================================
#     # PDF PARSING
#     # =========================================================================
    
#     def _parse_pdf(self, file_path: Path) -> Tuple[str, List[Dict[str, Any]]]:
#         """Parse PDF file and extract text + tables."""
#         text_parts = []
#         tables: List[Dict[str, Any]] = []
#         table_idx = 0

#         try:
#             with pdfplumber.open(file_path) as pdf:
#                 for page_num, page in enumerate(pdf.pages):
#                     # Extract text
#                     page_text = page.extract_text()
#                     if page_text:
#                         text_parts.append(page_text)

#                     # Extract tables
#                     page_tables = page.extract_tables()
#                     for tbl in page_tables:
#                         if not tbl or len(tbl) < 2:
#                             continue

#                         # First row as headers
#                         headers = [str(cell).strip() if cell else "" for cell in tbl[0]]
                        
#                         # Remaining rows as data
#                         rows = []
#                         for row in tbl[1:]:
#                             cleaned_row = [str(cell).strip() if cell else "" for cell in row]
#                             if any(cell for cell in cleaned_row):
#                                 rows.append(cleaned_row)

#                         if rows:
#                             tables.append({
#                                 "table_index": table_idx,
#                                 "headers": headers,
#                                 "rows": rows,
#                                 "row_count": len(rows),
#                                 "col_count": len(headers),
#                                 "context": f"Page {page_num + 1}",
#                                 "source": f"PDF: {file_path.name}"
#                             })
#                             table_idx += 1

#             logger.info(f"PDF parsed: {len(text_parts)} pages, {len(tables)} tables")

#         except Exception as e:
#             logger.error(f"Error parsing PDF {file_path}: {e}")

#         return "\n\n".join(text_parts), tables

#     # =========================================================================
#     # HTML PARSING
#     # =========================================================================

#     def _parse_html_to_text(self, raw_content: str) -> str:
#         """Convert HTML to plain text."""
#         soup = BeautifulSoup(raw_content, "html.parser")

#         for element in soup(["script", "style"]):
#             element.decompose()

#         return soup.get_text(separator="\n")

#     def _extract_tables_from_html(self, html_content: str) -> List[Dict[str, Any]]:
#         """Extract all tables from HTML content."""
#         tables: List[Dict[str, Any]] = []
#         soup = BeautifulSoup(html_content, "html.parser")

#         for idx, table_tag in enumerate(soup.find_all("table")):
#             try:
#                 table_data = self._parse_html_table(table_tag, idx)
#                 if table_data and table_data["row_count"] > 0:
#                     tables.append(table_data)
#             except Exception as e:
#                 logger.warning(f"Error parsing table {idx}: {e}")
#                 continue

#         logger.info(f"HTML parsed: {len(tables)} tables extracted")
#         return tables

#     def _parse_html_table(self, table_tag, index: int) -> Optional[Dict[str, Any]]:
#         """Parse a single HTML table tag into a dictionary."""
#         headers: List[str] = []
#         rows_data: List[List[str]] = []

#         # Try to find headers in thead
#         thead = table_tag.find("thead")
#         if thead:
#             header_row = thead.find("tr")
#             if header_row:
#                 headers = [self._clean_cell_text(th.get_text()) for th in header_row.find_all(["th", "td"])]

#         # Extract all rows from tbody or table directly
#         tbody = table_tag.find("tbody") or table_tag
#         for tr in tbody.find_all("tr"):
#             cells = tr.find_all(["td", "th"])
#             if cells:
#                 row = [self._clean_cell_text(cell.get_text()) for cell in cells]
#                 if any(cell.strip() for cell in row):
#                     rows_data.append(row)

#         # If no headers found, use first row as headers
#         if not headers and rows_data:
#             if self._looks_like_header_row(rows_data[0]):
#                 headers = rows_data[0]
#                 rows_data = rows_data[1:]

#         if not rows_data:
#             return None

#         # Get context (text before the table)
#         context = ""
#         prev_sibling = table_tag.find_previous_sibling()
#         if prev_sibling:
#             context = self._clean_cell_text(prev_sibling.get_text())[:200]

#         col_count = max(len(row) for row in rows_data) if rows_data else len(headers)

#         return {
#             "table_index": index,
#             "headers": headers,
#             "rows": rows_data,
#             "row_count": len(rows_data),
#             "col_count": col_count,
#             "context": context,
#             "source": "HTML"
#         }

#     def _looks_like_header_row(self, row: List[str]) -> bool:
#         """Heuristic to determine if a row is likely a header row."""
#         if not row:
#             return False

#         numeric_cells = sum(1 for cell in row if re.match(r'^[\$\d\.\,\-\(\)%]+$', cell.strip()))
#         if numeric_cells > len(row) / 2:
#             return False

#         avg_len = sum(len(cell) for cell in row) / len(row)
#         if avg_len > 50:
#             return False

#         return True

#     def _clean_cell_text(self, text: str) -> str:
#         """Clean text from a table cell."""
#         text = re.sub(r'\s+', ' ', text)
#         return text.strip()

#     # =========================================================================
#     # TEXT PROCESSING
#     # =========================================================================

#     def _normalize_text(self, text: str) -> str:
#         """Normalize text by cleaning whitespace."""
#         lines = (ln.strip() for ln in text.splitlines())
#         cleaned = "\n".join(ln for ln in lines if ln)
#         cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
#         return cleaned

#     # =========================================================================
#     # SECTION EXTRACTION
#     # =========================================================================

#     def _find_section_start(self, content: str, section_name: str) -> Optional[int]:
#         """Find the actual start of a section (not the TOC entry)."""
#         upper_content = content.upper()
#         patterns = self.SECTION_START_PATTERNS.get(section_name, [])

#         all_matches: List[Tuple[int, int]] = []
#         for pattern in patterns:
#             for match in re.finditer(pattern, upper_content, re.IGNORECASE | re.MULTILINE):
#                 all_matches.append((match.start(), match.end()))

#         if not all_matches:
#             return None

#         all_matches.sort(key=lambda x: x[0])

#         for start_pos, end_pos in all_matches:
#             preview = content[end_pos:end_pos + 500]
#             lines = [l.strip() for l in preview.split('\n') if l.strip()]

#             if len(lines) < 3:
#                 continue

#             avg_line_len = sum(len(l) for l in lines[:5]) / min(5, len(lines))

#             if avg_line_len > 50:
#                 return start_pos

#             long_lines = sum(1 for l in lines[:10] if len(l) > 80)
#             if long_lines >= 3:
#                 return start_pos

#         return all_matches[-1][0] if all_matches else None

#     def _find_section_end(self, content: str, section_name: str, start_pos: int) -> int:
#         """Find where the section ends."""
#         upper_content = content.upper()
#         patterns = self.SECTION_END_PATTERNS.get(section_name, [])

#         search_start = start_pos + 100
#         earliest_end = len(content)

#         for pattern in patterns:
#             match = re.search(pattern, upper_content[search_start:], re.IGNORECASE | re.MULTILINE)
#             if match:
#                 end_pos = search_start + match.start()
#                 preview = content[end_pos:end_pos + 200]
#                 lines = [l.strip() for l in preview.split('\n') if l.strip()]
#                 if lines:
#                     avg_line_len = sum(len(l) for l in lines[:3]) / min(3, len(lines))
#                     if avg_line_len > 40:
#                         earliest_end = min(earliest_end, end_pos)

#         return earliest_end

#     def _extract_sections(self, content: str) -> Dict[str, str]:
#         """Extract full section content, skipping TOC entries."""
#         sections: Dict[str, str] = {}

#         for section_name in ["item_1", "item_1a", "item_7"]:
#             start_pos = self._find_section_start(content, section_name)

#             if start_pos is None:
#                 continue

#             end_pos = self._find_section_end(content, section_name, start_pos)
#             section_content = content[start_pos:end_pos].strip()

#             if len(section_content) > 500:
#                 sections[section_name] = section_content[:100000]

#         return sections

#     # =========================================================================
#     # METADATA EXTRACTION
#     # =========================================================================

#     def _extract_metadata(self, file_path: Path) -> Tuple[str, datetime]:
#         """Extract filing type and date from file path."""
#         parts = file_path.parts
        
#         # For main filing: .../sec-edgar-filings/{ticker}/{filing_type}/{accession}/full-submission.txt
#         # For PDF exhibit: .../sec-edgar-filings/{ticker}/{filing_type}/{accession}/exhibits/file.pdf
        
#         if "exhibits" in parts:
#             # PDF exhibit - go up one more level
#             filing_type = parts[-5] if len(parts) >= 5 else "UNKNOWN"
#             accession = parts[-3] if len(parts) >= 3 else ""
#         else:
#             filing_type = parts[-3] if len(parts) >= 3 else "UNKNOWN"
#             accession = parts[-2] if len(parts) >= 2 else ""

#         # Extract date from accession (format: 0000xxxxx-YY-NNNNNN)
#         m = re.search(r"-(\d{2})-", accession)
#         if m:
#             yy = int(m.group(1))
#             year = 2000 + yy if yy < 50 else 1900 + yy
#             filing_date = datetime(year, 1, 1, tzinfo=timezone.utc)
#         else:
#             filing_date = datetime.now(timezone.utc)

#         return filing_type, filing_date

import re
import json
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from bs4 import BeautifulSoup
import pdfplumber
import fitz  # PyMuPDF
from io import BytesIO

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


@dataclass
class ParsedTable:
    """Represents an extracted table"""
    table_index: int
    page_number: Optional[int]
    headers: List[str]
    rows: List[List[str]]
    row_count: int
    col_count: int


@dataclass 
class ParsedDocument:
    """Represents a fully parsed document"""
    document_id: str
    ticker: str
    filing_type: str
    filing_date: str
    source_format: str  # 'html' or 'pdf'
    text_content: str
    word_count: int
    tables: List[Dict]
    table_count: int
    sections: Dict[str, str]  # Extracted sections like MD&A, Risk Factors
    parse_errors: List[str]


class DocumentParser:
    """Universal document parser for SEC filings (HTML & PDF)"""
    
    # SEC filing sections to extract
    SECTION_PATTERNS = {
        "business": r"item\s*1[.\s]+business",
        "risk_factors": r"item\s*1a[.\s]+risk\s*factors",
        "mda": r"item\s*7[.\s]+management.{0,50}discussion",
        "financial_statements": r"item\s*8[.\s]+financial\s*statements",
        "controls": r"item\s*9a[.\s]+controls",
        "executive_compensation": r"executive\s*compensation",
        "other_events": r"item\s*8\.01[.\s]+other\s*events",
    }
    
    def __init__(self):
        logger.info("📄 Document Parser initialized")
    
    def detect_format(self, content: bytes, filename: str = "") -> str:
        """Detect if content is HTML or PDF"""
        # Check by filename extension first
        if filename.lower().endswith('.pdf'):
            return 'pdf'
        if filename.lower().endswith(('.html', '.htm')):
            return 'html'
        
        # Check by content magic bytes
        if content[:4] == b'%PDF':
            return 'pdf'
        
        # Check for HTML markers
        content_start = content[:1000].decode('utf-8', errors='ignore').lower()
        if '<html' in content_start or '<!doctype html' in content_start or '<sec-document' in content_start:
            return 'html'
        
        # Default to HTML (most SEC filings)
        return 'html'
    
    def parse(self, content: bytes, document_id: str, ticker: str, 
              filing_type: str, filing_date: str, filename: str = "") -> ParsedDocument:
        """Parse a document (auto-detect format)"""
        format_type = self.detect_format(content, filename)
        logger.info(f"  📋 Detected format: {format_type.upper()}")
        
        if format_type == 'pdf':
            return self._parse_pdf(content, document_id, ticker, filing_type, filing_date)
        else:
            return self._parse_html(content, document_id, ticker, filing_type, filing_date)
    
    def _parse_html(self, content: bytes, document_id: str, ticker: str,
                    filing_type: str, filing_date: str) -> ParsedDocument:
        """Parse HTML document"""
        logger.info(f"  🌐 Parsing HTML document...")
        errors = []
        
        try:
            html_text = content.decode('utf-8', errors='ignore')
            soup = BeautifulSoup(html_text, 'html.parser')
            
            # Remove script and style elements
            for element in soup(['script', 'style', 'meta', 'link']):
                element.decompose()
            
            # Extract text
            text = soup.get_text(separator=' ', strip=True)
            text = self._clean_text(text)
            word_count = len(text.split())
            logger.info(f"  ✅ Extracted {word_count:,} words")
            
            # Extract tables
            tables = self._extract_html_tables(soup)
            logger.info(f"  📊 Extracted {len(tables)} tables")
            
            # Extract sections
            sections = self._extract_sections(text)
            logger.info(f"  📑 Identified {len(sections)} sections")
            
        except Exception as e:
            logger.error(f"  ❌ HTML parsing error: {e}")
            errors.append(str(e))
            text = ""
            word_count = 0
            tables = []
            sections = {}
        
        return ParsedDocument(
            document_id=document_id,
            ticker=ticker,
            filing_type=filing_type,
            filing_date=filing_date,
            source_format='html',
            text_content=text,
            word_count=word_count,
            tables=[asdict(t) for t in tables],
            table_count=len(tables),
            sections=sections,
            parse_errors=errors
        )
    
    def _parse_pdf(self, content: bytes, document_id: str, ticker: str,
                   filing_type: str, filing_date: str) -> ParsedDocument:
        """Parse PDF document using pdfplumber and PyMuPDF"""
        logger.info(f"  📕 Parsing PDF document...")
        errors = []
        all_text = []
        tables = []
        
        try:
            # Use pdfplumber for tables
            with pdfplumber.open(BytesIO(content)) as pdf:
                logger.info(f"  📄 PDF has {len(pdf.pages)} pages")
                
                for page_num, page in enumerate(pdf.pages, 1):
                    # Extract text
                    page_text = page.extract_text() or ""
                    all_text.append(page_text)
                    
                    # Extract tables
                    page_tables = page.extract_tables()
                    for idx, table_data in enumerate(page_tables):
                        if table_data and len(table_data) > 1:
                            headers = [str(h) if h else "" for h in table_data[0]]
                            rows = [[str(c) if c else "" for c in row] for row in table_data[1:]]
                            tables.append(ParsedTable(
                                table_index=len(tables),
                                page_number=page_num,
                                headers=headers,
                                rows=rows,
                                row_count=len(rows),
                                col_count=len(headers)
                            ))
                    
                    if page_num % 10 == 0:
                        logger.info(f"  📖 Processed {page_num} pages...")
            
            text = self._clean_text(' '.join(all_text))
            word_count = len(text.split())
            logger.info(f"  ✅ Extracted {word_count:,} words from PDF")
            logger.info(f"  📊 Extracted {len(tables)} tables")
            
            # Extract sections
            sections = self._extract_sections(text)
            logger.info(f"  📑 Identified {len(sections)} sections")
            
        except Exception as e:
            logger.error(f"  ❌ PDF parsing error: {e}")
            errors.append(str(e))
            
            # Fallback to PyMuPDF
            try:
                logger.info(f"  🔄 Trying PyMuPDF fallback...")
                doc = fitz.open(stream=content, filetype="pdf")
                all_text = [page.get_text() for page in doc]
                text = self._clean_text(' '.join(all_text))
                word_count = len(text.split())
                sections = self._extract_sections(text)
                doc.close()
                logger.info(f"  ✅ PyMuPDF extracted {word_count:,} words")
            except Exception as e2:
                logger.error(f"  ❌ PyMuPDF fallback failed: {e2}")
                errors.append(str(e2))
                text = ""
                word_count = 0
                sections = {}
        
        return ParsedDocument(
            document_id=document_id,
            ticker=ticker,
            filing_type=filing_type,
            filing_date=filing_date,
            source_format='pdf',
            text_content=text,
            word_count=word_count,
            tables=[asdict(t) for t in tables],
            table_count=len(tables),
            sections=sections,
            parse_errors=errors
        )
    
    def _extract_html_tables(self, soup: BeautifulSoup) -> List[ParsedTable]:
        """Extract tables from HTML"""
        tables = []
        
        for idx, table in enumerate(soup.find_all('table')):
            try:
                rows_data = []
                for row in table.find_all('tr'):
                    cells = row.find_all(['td', 'th'])
                    row_data = [cell.get_text(strip=True) for cell in cells]
                    if any(row_data):  # Skip empty rows
                        rows_data.append(row_data)
                
                if len(rows_data) > 1:
                    headers = rows_data[0]
                    data_rows = rows_data[1:]
                    tables.append(ParsedTable(
                        table_index=idx,
                        page_number=None,
                        headers=headers,
                        rows=data_rows,
                        row_count=len(data_rows),
                        col_count=len(headers)
                    ))
            except Exception:
                continue
        
        return tables
    
    def _extract_sections(self, text: str) -> Dict[str, str]:
        """Extract key sections from filing text"""
        sections = {}
        text_lower = text.lower()
        
        for section_name, pattern in self.SECTION_PATTERNS.items():
            try:
                match = re.search(pattern, text_lower)
                if match:
                    start = match.start()
                    # Extract ~5000 chars after section header
                    end = min(start + 5000, len(text))
                    sections[section_name] = text[start:end].strip()
            except Exception:
                continue
        
        return sections
    
    def _clean_text(self, text: str) -> str:
        """Clean extracted text"""
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        # Remove special characters but keep punctuation
        text = re.sub(r'[^\w\s.,;:!?\'\"()\-$%]', '', text)
        # Remove repeated punctuation
        text = re.sub(r'([.,;:])\1+', r'\1', text)
        return text.strip()


# Singleton
_parser: Optional[DocumentParser] = None

def get_document_parser() -> DocumentParser:
    global _parser
    if _parser is None:
        _parser = DocumentParser()
    return _parser