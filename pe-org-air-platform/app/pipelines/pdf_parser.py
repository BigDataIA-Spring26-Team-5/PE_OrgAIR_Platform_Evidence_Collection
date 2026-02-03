"""
PDF Parser for SEC 10-K Filings

Usage:
    python pdf_parser.py <path_to_pdf> [--ticker TICKER]
    
Example:
    python pdf_parser.py data/raw/10-k.pdf --ticker AAPL
    python pdf_parser.py ./10-k.pdf
"""

import json
import hashlib
import logging
import re
import sys
import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional

try:
    import pdfplumber
except ImportError:
    print("Error: pdfplumber not installed. Run: pip install pdfplumber")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class ParsedPDFDocument:
    """Data class for parsed PDF document."""
    ticker: str
    filing_type: str
    filing_date: str
    content: str
    tables: List[Dict[str, Any]]
    source_path: str
    content_hash: str
    word_count: int
    page_count: int
    table_count: int


class PDFParser:
    """Parse SEC 10-K PDF filings and extract text + tables."""

    def __init__(self, output_dir: str = "data/parsed"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.tables_dir = Path("data/tables")
        self.tables_dir.mkdir(parents=True, exist_ok=True)

    def parse_pdf(self, pdf_path: str, ticker: str = "UNKNOWN") -> ParsedPDFDocument:
        """
        Parse a PDF file and extract text and tables.
        
        Args:
            pdf_path: Path to the PDF file
            ticker: Company ticker symbol (default: UNKNOWN)
            
        Returns:
            ParsedPDFDocument with extracted content
        """
        file_path = Path(pdf_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"PDF not found: {pdf_path}")
        
        if file_path.suffix.lower() != ".pdf":
            raise ValueError(f"File is not a PDF: {pdf_path}")
        
        logger.info(f"Parsing PDF: {file_path}")
        
        text_parts = []
        tables: List[Dict[str, Any]] = []
        table_idx = 0
        page_count = 0

        try:
            with pdfplumber.open(file_path) as pdf:
                page_count = len(pdf.pages)
                logger.info(f"PDF has {page_count} pages")
                
                for page_num, page in enumerate(pdf.pages):
                    if page_num % 10 == 0:
                        logger.info(f"Processing page {page_num + 1}/{page_count}")
                    
                    # Extract text
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(f"--- Page {page_num + 1} ---\n{page_text}")

                    # Extract tables
                    page_tables = page.extract_tables()
                    for tbl in page_tables:
                        if not tbl or len(tbl) < 2:
                            continue

                        headers = [str(cell).strip() if cell else "" for cell in tbl[0]]
                        
                        rows = []
                        for row in tbl[1:]:
                            cleaned = [str(cell).strip() if cell else "" for cell in row]
                            if any(cell for cell in cleaned):
                                rows.append(cleaned)

                        if rows:
                            tables.append({
                                "table_index": table_idx,
                                "headers": headers,
                                "rows": rows,
                                "row_count": len(rows),
                                "col_count": len(headers),
                                "page": page_num + 1,
                                "source": f"PDF: {file_path.name}"
                            })
                            table_idx += 1

        except Exception as e:
            logger.error(f"Error parsing PDF: {e}")
            raise

        content = "\n\n".join(text_parts)
        content_hash = hashlib.sha256(content.encode("utf-8", errors="ignore")).hexdigest()[:16]
        
        # Try to extract filing date from content
        filing_date = self._extract_filing_date(content)
        
        logger.info(f"Extraction complete: {len(text_parts)} pages, {len(tables)} tables, {len(content.split())} words")

        return ParsedPDFDocument(
            ticker=ticker.upper(),
            filing_type="10-K",
            filing_date=filing_date,
            content=content,
            tables=tables,
            source_path=str(file_path.absolute()),
            content_hash=content_hash,
            word_count=len(content.split()),
            page_count=page_count,
            table_count=len(tables)
        )

    def _extract_filing_date(self, content: str) -> str:
        """Try to extract filing date from document content."""
        patterns = [
            r"(?:filed|filing date)[:\s]+(\w+\s+\d{1,2},?\s+\d{4})",
            r"(?:for the fiscal year ended)[:\s]+(\w+\s+\d{1,2},?\s+\d{4})",
            r"(\d{1,2}/\d{1,2}/\d{4})",
            r"(\d{4}-\d{2}-\d{2})",
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content[:5000], re.IGNORECASE)
            if match:
                return match.group(1)
        
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def save_output(self, doc: ParsedPDFDocument) -> Dict[str, str]:
        """
        Save parsed document and tables to JSON files.
        
        Returns:
            Dictionary with paths to saved files
        """
        ticker_dir = self.output_dir / doc.ticker
        ticker_dir.mkdir(parents=True, exist_ok=True)
        
        self.tables_dir.mkdir(parents=True, exist_ok=True)

        # Prepare document data
        doc_data = {
            "document_id": doc.content_hash,
            "ticker": doc.ticker,
            "filing_type": doc.filing_type,
            "filing_date": doc.filing_date,
            "content_hash": doc.content_hash,
            "word_count": doc.word_count,
            "page_count": doc.page_count,
            "table_count": doc.table_count,
            "source_path": doc.source_path,
            "parsed_at": datetime.now(timezone.utc).isoformat(),
            "content_preview": doc.content[:2000] + "..." if len(doc.content) > 2000 else doc.content
        }

        # Save document metadata
        doc_path = ticker_dir / f"{doc.content_hash}.json"
        doc_path.write_text(json.dumps(doc_data, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info(f"Saved document metadata: {doc_path}")

        # Save full content separately
        content_path = ticker_dir / f"{doc.content_hash}_content.txt"
        content_path.write_text(doc.content, encoding="utf-8")
        logger.info(f"Saved full content: {content_path}")

        # Save ALL tables to data/tables/ directory
        tables_path = None
        individual_tables_dir = None
        
        if doc.tables:
            tables_data = {
                "document_id": doc.content_hash,
                "ticker": doc.ticker,
                "filing_type": doc.filing_type,
                "filing_date": doc.filing_date,
                "source_path": doc.source_path,
                "table_count": doc.table_count,
                "tables": doc.tables,
                "extracted_at": datetime.now(timezone.utc).isoformat()
            }
            tables_path = self.tables_dir / f"{doc.ticker}_{doc.content_hash}_tables.json"
            tables_path.write_text(json.dumps(tables_data, indent=2, ensure_ascii=False), encoding="utf-8")
            logger.info(f"Saved {doc.table_count} tables to: {tables_path}")
            
            # Also save individual tables
            individual_tables_dir = self.tables_dir / "individual" / doc.ticker
            individual_tables_dir.mkdir(parents=True, exist_ok=True)
            
            for table in doc.tables:
                table_file = individual_tables_dir / f"table_{table['table_index']:03d}_page_{table['page']}.json"
                table_file.write_text(json.dumps(table, indent=2, ensure_ascii=False), encoding="utf-8")
            
            logger.info(f"Saved {doc.table_count} individual table files to: {individual_tables_dir}")

        return {
            "document_json": str(doc_path),
            "content_txt": str(content_path),
            "tables_json": str(tables_path) if tables_path else None,
            "individual_tables_dir": str(individual_tables_dir) if individual_tables_dir else None
        }


def main():
    parser = argparse.ArgumentParser(description="Parse SEC 10-K PDF filings")
    parser.add_argument("pdf_path", nargs="?", default="data/sample_10k/10-k.pdf", help="Path to the PDF file")
    parser.add_argument("--ticker", "-t", default="UNKNOWN", help="Company ticker symbol")
    parser.add_argument("--output-dir", "-o", default="data/parsed", help="Output directory")
    
    args = parser.parse_args()
    
    # Auto-detect PDF in sample_10k folder if default path doesn't exist
    pdf_path = Path(args.pdf_path)
    if not pdf_path.exists():
        sample_dir = Path("data/sample_10k")
        if sample_dir.exists():
            pdfs = list(sample_dir.glob("*.pdf"))
            if pdfs:
                pdf_path = pdfs[0]
                print(f"Auto-detected PDF: {pdf_path}")
    
    args.pdf_path = str(pdf_path)

    print("\n" + "="*60)
    print("SEC 10-K PDF Parser")
    print("="*60)
    
    pdf_parser = PDFParser(output_dir=args.output_dir)
    
    try:
        doc = pdf_parser.parse_pdf(args.pdf_path, ticker=args.ticker)
        paths = pdf_parser.save_output(doc)
        
        print("\n" + "-"*60)
        print("PARSING COMPLETE")
        print("-"*60)
        print(f"  Ticker:      {doc.ticker}")
        print(f"  Filing Type: {doc.filing_type}")
        print(f"  Filing Date: {doc.filing_date}")
        print(f"  Pages:       {doc.page_count}")
        print(f"  Word Count:  {doc.word_count:,}")
        print(f"  Tables:      {doc.table_count}")
        print(f"  Content Hash:{doc.content_hash}")
        print("\nOutput Files:")
        print(f"  Document:    {paths['document_json']}")
        print(f"  Content:     {paths['content_txt']}")
        if paths['tables_json']:
            print(f"  Tables:      {paths['tables_json']}")
        if paths.get('individual_tables_dir'):
            print(f"  Individual:  {paths['individual_tables_dir']}")
        print("="*60 + "\n")
        
        return 0
        
    except FileNotFoundError as e:
        print(f"\nERROR: {e}")
        print("Make sure the PDF file exists at the specified path.")
        return 1
    except Exception as e:
        print(f"\nERROR: {e}")
        logger.exception("Parsing failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())