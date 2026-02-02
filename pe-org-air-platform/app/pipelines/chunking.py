from __future__ import annotations

from typing import List
from app.pipelines.pipeline_state import PipelineState


class DocumentChunker:
    """Splits parsed_text into overlapping word chunks."""

    def __init__(self, chunk_size: int = 750, chunk_overlap: int = 50):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

    def chunk_document(self, text: str) -> List[str]:
        if not text:
            return []
        words = text.split()
        chunks = []
        start = 0
        while start < len(words):
            end = min(start + self.chunk_size, len(words))
            chunks.append(" ".join(words[start:end]))
            start += max(1, self.chunk_size - self.chunk_overlap)
        return chunks


async def step7_chunk_text(state: PipelineState, chunk_size: int = 750, chunk_overlap: int = 50) -> PipelineState:
    if not state.deduplicated_filings:
        raise ValueError("No deduplicated filings. Run step6_deduplicate_documents first.")

    state.chunker = DocumentChunker(chunk_size, chunk_overlap)
    state.chunked_filings = []

    for filing in state.deduplicated_filings:
        chunks = state.chunker.chunk_document(filing["parsed_text"])
        filing["chunks"] = chunks
        filing["num_chunks"] = len(chunks)
        state.chunked_filings.append(filing)

        state.summary["unique_filings_processed"] += 1
        state.summary["details"].append({
            "ticker": filing["ticker"],
            "filing_type": filing["filing_type"],
            "accession_number": filing["accession_number"],
            "status": "success",
            "num_chunks": len(chunks),
            "num_tables": len(filing.get("parsed_tables", [])),
            "content_hash": filing["content_hash"],
        })

    print(f"✓ Chunked {len(state.chunked_filings)} documents")
    return state
