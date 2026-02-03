import asyncio
import argparse
import json
from pathlib import Path

from app.pipelines.sec_edgar import (
    step1_initialize_pipeline,
    step2_add_downloader,
    step3_configure_rate_limiting,
    step4_download_filings,
)
from app.pipelines.document_parser import step5_parse_documents
from app.pipelines.registry import step6_deduplicate_documents
from app.pipelines.chunking import step7_chunk_text


def save_outputs(state, out_dir: str = "data/processed"):
    out = Path(out_dir)
    docs_dir = out / "documents"
    chunks_dir = out / "chunks"
    docs_dir.mkdir(parents=True, exist_ok=True)
    chunks_dir.mkdir(parents=True, exist_ok=True)

    # write one json per filing, plus one jsonl per filing
    for f in state.chunked_filings:
        ticker = f["ticker"]
        filing_type = f["filing_type"]
        acc = f["accession_number"]
        content_hash = f["content_hash"]

        doc_path = docs_dir / ticker / filing_type
        doc_path.mkdir(parents=True, exist_ok=True)
        (doc_path / f"{content_hash}.json").write_text(json.dumps({
            "ticker": ticker,
            "filing_type": filing_type,
            "accession_number": acc,
            "content_hash": content_hash,
            "parsed_text": f.get("parsed_text", ""),
            "parsed_tables": f.get("parsed_tables", []),
        }, indent=2), encoding="utf-8")

        chunk_path = chunks_dir / ticker / filing_type
        chunk_path.mkdir(parents=True, exist_ok=True)
        with (chunk_path / f"{content_hash}.jsonl").open("w", encoding="utf-8") as out_f:
            for i, c in enumerate(f.get("chunks", [])):
                out_f.write(json.dumps({
                    "chunk_index": i,
                    "content_hash": content_hash,
                    "text": c
                }) + "\n")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", required=True, help="e.g. AAPL, MSFT")
    ap.add_argument("--filings", nargs="+", default=["10-K", "10-Q"])
    ap.add_argument("--after", default="2023-01-01")
    ap.add_argument("--limit", type=int, default=1)
    ap.add_argument("--request_delay", type=float, default=0.1)
    ap.add_argument("--chunk_size", type=int, default=750)
    ap.add_argument("--chunk_overlap", type=int, default=50)
    args = ap.parse_args()

    # Step 1–3 (company_name/email auto from .env, with defaults)
    state = step1_initialize_pipeline(download_dir="data/raw/sec")
    state = step2_add_downloader(state)
    state = step3_configure_rate_limiting(state, request_delay=args.request_delay)

    # Step 4–7
    state = await step4_download_filings(
        state,
        tickers=[args.ticker],
        filing_types=args.filings,
        after_date=args.after,
        limit=args.limit,
    )
    state = await step5_parse_documents(state)
    state = step6_deduplicate_documents(state)
    state = await step7_chunk_text(state, chunk_size=args.chunk_size, chunk_overlap=args.chunk_overlap)

    save_outputs(state, out_dir="data/processed")

    print("\nDone. Outputs:")
    print(" - data/raw/sec/ (downloads)")
    print(" - data/processed/documents/ (parsed)")
    print(" - data/processed/chunks/ (chunks)")
    print(" - data/processed/registry/document_registry.txt (dedupe registry)")


if __name__ == "__main__":
    asyncio.run(main())
