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