from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional



@dataclass
class PipelineState:
    company_name: str
    email_address: str
    download_dir: Path

    request_delay: float = 0.1

    # runtime components
    downloader: Any = None
    parser: Any = None
    registry: Any = None
    chunker: Any = None

    # artifacts
    downloaded_filings: List[Dict[str, Any]] = field(default_factory=list)
    parsed_filings: List[Dict[str, Any]] = field(default_factory=list)
    deduplicated_filings: List[Dict[str, Any]] = field(default_factory=list)
    chunked_filings: List[Dict[str, Any]] = field(default_factory=list)

    # summary
    summary: Dict[str, Any] = field(default_factory=lambda: {
        "attempted_downloads": 0,
        "unique_filings_processed": 0,
        "skipped_duplicates": 0,
        "parsing_errors": 0,
        "details": []
    })
