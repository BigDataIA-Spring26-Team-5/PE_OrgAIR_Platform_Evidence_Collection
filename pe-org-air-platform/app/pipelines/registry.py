from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Set


class DocumentRegistry:
    """
    Local file-based registry for document deduplication.
    Tracks processed document hashes to avoid reprocessing.
    
    Registry file location: data/processed/registry/document_registry.txt
    """

    def __init__(self, registry_file: str = "data/processed/registry/document_registry.txt"):
        self.registry_file = Path(registry_file)
        self.registry_file.parent.mkdir(parents=True, exist_ok=True)
        self.processed_hashes: Set[str] = set()
        self._load_registry()

    def _load_registry(self) -> None:
        """Load existing hashes from file."""
        if self.registry_file.exists():
            content = self.registry_file.read_text(encoding="utf-8")
            self.processed_hashes = {
                line.strip() 
                for line in content.splitlines() 
                if line.strip()
            }

    def _save_registry(self) -> None:
        """Persist hashes to file."""
        self.registry_file.write_text(
            "\n".join(sorted(self.processed_hashes)), 
            encoding="utf-8"
        )

    def compute_content_hash(self, content: str) -> str:
        """Generate SHA256 hash of content."""
        return hashlib.sha256(content.encode("utf-8", errors="ignore")).hexdigest()

    def is_processed(self, content_hash: str) -> bool:
        """Check if document has been processed."""
        return content_hash in self.processed_hashes

    def mark_as_processed(self, content_hash: str) -> None:
        """Mark document as processed and save to file."""
        if content_hash not in self.processed_hashes:
            self.processed_hashes.add(content_hash)
            self._save_registry()

    def get_count(self) -> int:
        """Return number of processed documents."""
        return len(self.processed_hashes)

    def clear(self) -> None:
        """Clear all processed hashes (use with caution)."""
        self.processed_hashes.clear()
        if self.registry_file.exists():
            self.registry_file.unlink()