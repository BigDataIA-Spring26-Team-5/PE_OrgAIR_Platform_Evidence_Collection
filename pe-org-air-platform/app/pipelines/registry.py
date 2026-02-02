from __future__ import annotations

from pathlib import Path
from typing import Set
import hashlib

from app.pipelines.pipeline_state import PipelineState


class DocumentRegistry:
    """Deduplicates based on SHA256 of parsed_text."""

    def __init__(self, registry_file: str = "data/processed/registry/document_registry.txt"):
        self.registry_file = Path(registry_file)
        self.registry_file.parent.mkdir(parents=True, exist_ok=True)
        self.processed_hashes: Set[str] = set()
        self._load_registry()

    def _load_registry(self) -> None:
        if self.registry_file.exists():
            self.processed_hashes = {line.strip() for line in self.registry_file.read_text().splitlines() if line.strip()}

    def _save_registry(self) -> None:
        self.registry_file.write_text("\n".join(sorted(self.processed_hashes)), encoding="utf-8")

    def compute_content_hash(self, content: str) -> str:
        return hashlib.sha256(content.encode("utf-8", errors="ignore")).hexdigest()

    def is_processed(self, content_hash: str) -> bool:
        return content_hash in self.processed_hashes

    def mark_as_processed(self, content_hash: str) -> None:
        if content_hash not in self.processed_hashes:
            self.processed_hashes.add(content_hash)
            self._save_registry()


def step6_deduplicate_documents(state: PipelineState, registry_file: str = "data/processed/registry/document_registry.txt") -> PipelineState:
    if not state.parsed_filings:
        raise ValueError("No parsed filings. Run step5_parse_documents first.")

    state.registry = DocumentRegistry(registry_file)
    state.deduplicated_filings = []
    skipped = 0

    for filing in state.parsed_filings:
        content_hash = state.registry.compute_content_hash(filing["parsed_text"])
        filing["content_hash"] = content_hash

        if state.registry.is_processed(content_hash):
            skipped += 1
            state.summary["skipped_duplicates"] += 1
            state.summary["details"].append({
                "ticker": filing["ticker"],
                "filing_type": filing["filing_type"],
                "accession_number": filing["accession_number"],
                "status": "duplicate_skipped",
                "content_hash": content_hash,
            })
        else:
            state.registry.mark_as_processed(content_hash)
            state.deduplicated_filings.append(filing)

    print(f"✓ Deduplicated: {len(state.deduplicated_filings)} unique ({skipped} skipped)")
    return state
