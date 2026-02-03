# from __future__ import annotations

# from typing import List
# from app.pipelines.pipeline_state import PipelineState


# class DocumentChunker:
#     """Splits parsed_text into overlapping word chunks."""

#     def __init__(self, chunk_size: int = 750, chunk_overlap: int = 50):
#         self.chunk_size = chunk_size
#         self.chunk_overlap = chunk_overlap

#     def chunk_document(self, text: str) -> List[str]:
#         if not text:
#             return []
#         words = text.split()
#         chunks = []
#         start = 0
#         while start < len(words):
#             end = min(start + self.chunk_size, len(words))
#             chunks.append(" ".join(words[start:end]))
#             start += max(1, self.chunk_size - self.chunk_overlap)
#         return chunks


# async def step7_chunk_text(state: PipelineState, chunk_size: int = 750, chunk_overlap: int = 50) -> PipelineState:
#     if not state.deduplicated_filings:
#         raise ValueError("No deduplicated filings. Run step6_deduplicate_documents first.")

#     state.chunker = DocumentChunker(chunk_size, chunk_overlap)
#     state.chunked_filings = []

#     for filing in state.deduplicated_filings:
#         chunks = state.chunker.chunk_document(filing["parsed_text"])
#         filing["chunks"] = chunks
#         filing["num_chunks"] = len(chunks)
#         state.chunked_filings.append(filing)

#         state.summary["unique_filings_processed"] += 1
#         state.summary["details"].append({
#             "ticker": filing["ticker"],
#             "filing_type": filing["filing_type"],
#             "accession_number": filing["accession_number"],
#             "status": "success",
#             "num_chunks": len(chunks),
#             "num_tables": len(filing.get("parsed_tables", [])),
#             "content_hash": filing["content_hash"],
#         })

#     print(f"✓ Chunked {len(state.chunked_filings)} documents")
#     return state


from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from app.pipelines.document_parser import ParsedDocument


@dataclass
class DocumentChunk:
    document_id: str
    chunk_index: int
    content: str
    section: Optional[str]
    start_char: int
    end_char: int
    word_count: int


class SemanticChunker:
    """
    Chunk documents with overlap (word-based).
    If doc.sections exists, chunk each section separately.
    """

    def __init__(
        self,
        chunk_size: int = 1000,
        chunk_overlap: int = 100,
        min_chunk_size: int = 200,
    ):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.min_chunk_size = min_chunk_size

    def chunk_document(self, doc: ParsedDocument) -> List[DocumentChunk]:
        chunks: List[DocumentChunk] = []

        if doc.sections:
            for section_name, section_content in doc.sections.items():
                chunks.extend(self._chunk_text(section_content, doc.content_hash, section_name))
        else:
            chunks.extend(self._chunk_text(doc.content, doc.content_hash, None))

        return chunks

    def _chunk_text(self, text: str, doc_id: str, section: Optional[str]) -> List[DocumentChunk]:
        words = text.split()
        chunks: List[DocumentChunk] = []

        start_idx = 0
        chunk_index = 0

        while start_idx < len(words):
            end_idx = min(start_idx + self.chunk_size, len(words))

            # avoid tiny tail
            if len(words) - end_idx < self.min_chunk_size:
                end_idx = len(words)

            chunk_words = words[start_idx:end_idx]
            chunk_content = " ".join(chunk_words)

            start_char = len(" ".join(words[:start_idx]))
            end_char = start_char + len(chunk_content)

            chunks.append(
                DocumentChunk(
                    document_id=doc_id,
                    chunk_index=chunk_index,
                    content=chunk_content,
                    section=section,
                    start_char=start_char,
                    end_char=end_char,
                    word_count=len(chunk_words),
                )
            )

            if end_idx >= len(words):
                break

            start_idx = max(end_idx - self.chunk_overlap, 0)
            chunk_index += 1

        return chunks
