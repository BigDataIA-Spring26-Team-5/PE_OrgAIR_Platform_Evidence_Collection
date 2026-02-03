from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class PipelineState:
    """
    Tracks state across SEC pipeline steps.
    Persisted to disk between API calls for recovery and step continuity.
    """
    
    # Configuration (set during download step)
    company_id: str = ""
    ticker: str = ""
    filing_types: List[str] = field(default_factory=list)
    from_date: str = ""
    to_date: str = ""
    rate_limit: float = 0.1
    
    # Chunking parameters (set during chunk step)
    chunk_size: int = 1000
    chunk_overlap: int = 100
    
    # Artifacts from each pipeline step
    downloaded_filings: List[Dict[str, Any]] = field(default_factory=list)
    parsed_filings: List[Dict[str, Any]] = field(default_factory=list)
    deduplicated_filings: List[Dict[str, Any]] = field(default_factory=list)
    chunked_filings: List[Dict[str, Any]] = field(default_factory=list)
    extracted_items: List[Dict[str, Any]] = field(default_factory=list)
    
    # Pipeline statistics
    stats: Dict[str, Any] = field(default_factory=lambda: {
        "downloaded": 0,
        "parsed": 0,
        "unique_filings": 0,
        "duplicates_skipped": 0,
        "total_chunks": 0,
        "items_extracted": 0,
        "errors": 0,
        "error_details": []
    })
    
    # Step completion tracking
    steps_completed: Dict[str, bool] = field(default_factory=lambda: {
        "download": False,
        "parse": False,
        "deduplicate": False,
        "chunk": False,
        "extract_items": False
    })
    
    last_updated: str = ""

    def mark_step_complete(self, step: str) -> None:
        """Mark a pipeline step as complete."""
        self.steps_completed[step] = True
        self.last_updated = datetime.now(timezone.utc).isoformat()
    
    def is_step_complete(self, step: str) -> bool:
        """Check if a pipeline step has been completed."""
        return self.steps_completed.get(step, False)
    
    def reset(self) -> None:
        """Reset state for a new pipeline run."""
        self.downloaded_filings = []
        self.parsed_filings = []
        self.deduplicated_filings = []
        self.chunked_filings = []
        self.extracted_items = []
        self.stats = {
            "downloaded": 0,
            "parsed": 0,
            "unique_filings": 0,
            "duplicates_skipped": 0,
            "total_chunks": 0,
            "items_extracted": 0,
            "errors": 0,
            "error_details": []
        }
        self.steps_completed = {
            "download": False,
            "parse": False,
            "deduplicate": False,
            "chunk": False,
            "extract_items": False
        }


class PipelineStateManager:
    """
    Singleton manager for pipeline state persistence.
    State is saved to JSON file between API calls.
    """
    
    STATE_FILE = Path("data/.pipeline_state.json")
    _instance: Optional[PipelineState] = None
    
    @classmethod
    def get_state(cls) -> PipelineState:
        """Get the current pipeline state (loads from disk if needed)."""
        if cls._instance is None:
            cls._instance = cls._load_state()
        return cls._instance
    
    @classmethod
    def _load_state(cls) -> PipelineState:
        """Load state from JSON file."""
        if cls.STATE_FILE.exists():
            try:
                data = json.loads(cls.STATE_FILE.read_text())
                state = PipelineState()
                for key, value in data.items():
                    if hasattr(state, key):
                        setattr(state, key, value)
                return state
            except Exception:
                pass
        return PipelineState()
    
    @classmethod
    def save_state(cls) -> None:
        """Persist current state to JSON file."""
        if cls._instance is None:
            return
        cls.STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        data = asdict(cls._instance)
        cls.STATE_FILE.write_text(json.dumps(data, indent=2, default=str))
    
    @classmethod
    def reset_state(cls) -> PipelineState:
        """Reset state and delete state file."""
        cls._instance = PipelineState()
        if cls.STATE_FILE.exists():
            cls.STATE_FILE.unlink()
        return cls._instance