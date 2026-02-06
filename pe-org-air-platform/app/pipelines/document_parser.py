import re
import json
import logging
import hashlib
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
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
    content_hash: str
    word_count: int
    tables: List[Dict]
    table_count: int
    sections: Dict[str, str]  # Extracted sections like MD&A, Risk Factors
    parse_errors: List[str]


class DocumentParser:
    """Universal document parser for SEC filings (HTML & PDF)"""
    
    def __init__(self):
        logger.info("📄 Document Parser initialized")
    
    def detect_format(self, content: bytes, filename: str = "") -> str:
        """Detect if content is HTML or PDF"""
        if filename.lower().endswith('.pdf'):
            return 'pdf'
        if filename.lower().endswith(('.html', '.htm')):
            return 'html'
        
        if content[:4] == b'%PDF':
            return 'pdf'
        
        content_start = content[:1000].decode('utf-8', errors='ignore').lower()
        if '<html' in content_start or '<!doctype html' in content_start or '<sec-document' in content_start:
            return 'html'
        
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
            
            # Get text with newline separator
            text = soup.get_text(separator='\n')
            
            # Clean up whitespace - line by line
            lines = (line.strip() for line in text.splitlines())
            text = '\n'.join(line for line in lines if line)
            
            # Final cleanup
            text = self._clean_text(text)
            word_count = len(text.split())
            
            # Generate content hash
            content_hash = hashlib.sha256(text.encode()).hexdigest()
            
            logger.info(f"  ✅ Extracted {word_count:,} words")
            
            # Extract tables
            tables = self._extract_html_tables(soup)
            logger.info(f"  📊 Extracted {len(tables)} tables")
            
            # Extract sections based on filing type
            sections = self._extract_sections(text, filing_type)
            logger.info(f"  📑 Identified {len(sections)} sections")
            for sec_name, sec_content in sections.items():
                sec_words = len(sec_content.split())
                logger.info(f"      • {sec_name}: {sec_words:,} words")
            
        except Exception as e:
            logger.error(f"  ❌ HTML parsing error: {e}")
            errors.append(str(e))
            text = ""
            content_hash = ""
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
            content_hash=content_hash,
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
            with pdfplumber.open(BytesIO(content)) as pdf:
                logger.info(f"  📄 PDF has {len(pdf.pages)} pages")
                
                for page_num, page in enumerate(pdf.pages, 1):
                    page_text = page.extract_text()
                    if page_text:
                        all_text.append(page_text)
                    
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
            
            text = '\n\n'.join(all_text)
            text = self._clean_text(text)
            word_count = len(text.split())
            content_hash = hashlib.sha256(text.encode()).hexdigest()
            
            logger.info(f"  ✅ Extracted {word_count:,} words from PDF")
            logger.info(f"  📊 Extracted {len(tables)} tables")
            
            sections = self._extract_sections(text, filing_type)
            logger.info(f"  📑 Identified {len(sections)} sections")
            for sec_name, sec_content in sections.items():
                sec_words = len(sec_content.split())
                logger.info(f"      • {sec_name}: {sec_words:,} words")
            
        except Exception as e:
            logger.error(f"  ❌ PDF parsing error: {e}")
            errors.append(str(e))
            
            try:
                logger.info(f"  🔄 Trying PyMuPDF fallback...")
                doc = fitz.open(stream=content, filetype="pdf")
                all_text = [page.get_text() for page in doc]
                text = '\n\n'.join(all_text)
                text = self._clean_text(text)
                word_count = len(text.split())
                content_hash = hashlib.sha256(text.encode()).hexdigest()
                sections = self._extract_sections(text, filing_type)
                doc.close()
                logger.info(f"  ✅ PyMuPDF extracted {word_count:,} words")
            except Exception as e2:
                logger.error(f"  ❌ PyMuPDF fallback failed: {e2}")
                errors.append(str(e2))
                text = ""
                content_hash = ""
                word_count = 0
                sections = {}
        
        return ParsedDocument(
            document_id=document_id,
            ticker=ticker,
            filing_type=filing_type,
            filing_date=filing_date,
            source_format='pdf',
            text_content=text,
            content_hash=content_hash,
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
                    if any(row_data):
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
    
    def _extract_sections(self, content: str, filing_type: str) -> Dict[str, str]:
        """Extract key sections from filing text based on filing type"""
        sections = {}
        content_upper = content.upper()
        
        # Define section patterns: (name, start_pattern, end_pattern)
        if filing_type == "10-K":
            section_patterns = [
                ("business", r"ITEM\s*1\.?\s*BUSINESS", r"ITEM\s*1A|ITEM\s*1B"),
                ("risk_factors", r"ITEM\s*1A\.?\s*RISK\s*FACTORS", r"ITEM\s*1B|ITEM\s*1C|ITEM\s*2"),
                ("mda", r"ITEM\s*7\.?\s*MANAGEMENT", r"ITEM\s*7A|ITEM\s*8"),
            ]
        elif filing_type == "10-Q":
            section_patterns = [
                ("mda", r"ITEM\s*2\.?\s*MANAGEMENT", r"ITEM\s*3|ITEM\s*4"),
                ("risk_factors", r"ITEM\s*1A\.?\s*RISK\s*FACTORS", r"ITEM\s*2|ITEM\s*3|ITEM\s*4"),
            ]
        elif filing_type == "8-K":
            section_patterns = [
                ("other_events", r"ITEM\s*8\.01\.?\s*OTHER\s*EVENTS", r"ITEM\s*9|SIGNATURE|EXHIBIT"),
            ]
        elif filing_type in ["DEF 14A", "DEF14A"]:
            section_patterns = [
                ("executive_compensation", r"EXECUTIVE\s*COMPENSATION", r"DIRECTOR\s*COMPENSATION|SECURITY\s*OWNERSHIP|CERTAIN\s*RELATIONSHIPS|EQUITY\s*COMPENSATION"),
                ("director_compensation", r"DIRECTOR\s*COMPENSATION", r"SECURITY\s*OWNERSHIP|CERTAIN\s*RELATIONSHIPS|EQUITY\s*COMPENSATION|AUDIT"),
            ]
        else:
            return sections
        
        for section_name, start_pattern, end_pattern in section_patterns:
            try:
                # Find section start
                start_match = re.search(start_pattern, content_upper)
                if not start_match:
                    continue
                
                start_pos = start_match.start()
                
                # Find section end (next section header)
                # Search from 500 chars after start to avoid matching within the header
                search_start = start_pos + 500
                end_match = re.search(end_pattern, content_upper[search_start:])
                
                if end_match:
                    end_pos = search_start + end_match.start()
                else:
                    # No next section found, take up to 150,000 chars (safe limit)
                    end_pos = min(start_pos + 150000, len(content))
                
                # Extract section text
                section_text = content[start_pos:end_pos].strip()
                
                # Only save if we got meaningful content (more than 100 words)
                word_count = len(section_text.split())
                if word_count > 100:
                    sections[section_name] = section_text
                    
            except Exception as e:
                logger.error(f"Error extracting {section_name}: {e}")
                continue
        
        return sections
    
    def _clean_text(self, text: str) -> str:
        """Clean extracted text"""
        # Remove excessive whitespace but preserve paragraph breaks
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        # Remove special characters but keep punctuation
        text = re.sub(r'[^\w\s.,;:!?\'\"()\-$%\n]', '', text)
        return text.strip()


# Singleton
_parser: Optional[DocumentParser] = None

def get_document_parser() -> DocumentParser:
    global _parser
    if _parser is None:
        _parser = DocumentParser()
    return _parser