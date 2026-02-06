# PE-OrgAIR Platform Evidence Collection (Case Study 2)

> **"SEC filings tell you what companies say. External signals tell you what they do."**

---

## Table of Contents

1. [Title](#pe-orgair-platform--evidence-collection-case-study-2)
2. [Codelab & Video Links](#codelab--video-links)
3. [Project Context](#project-context)
4. [Project Overview & Objectives](#project-overview--objectives)
5. [Architecture & Diagram](#architecture--diagram)
6. [Tech Stack](#tech-stack)
7. [Step-by-Step Setup Guide](#step-by-step-setup-guide)
8. [Directory Structure](#directory-structure)
9. [How Does the Flow Work](#how-does-the-flow-work)
10. [Summary & Key Takeaways](#summary--key-takeaways)
11. [Design Decisions & Trade-offs](#design-decisions--trade-offs)
12. [Known Limitations](#known-limitations)
13. [Team Member Contributions & AI Usage](#team-member-contributions--ai-usage)

---

## Codelab & Video Links

| Resource | Link |
|----------|------|
| Codelab Document | [Link to Codelab](https://codelabs-preview.appspot.com/?file_id=1zjg7GVe7g4RFy6kKCJcK1QNSoU7jn8RWnn62KT6tgjI#0) |
| Demo Video | [Link to Video](https://northeastern-my.sharepoint.com/personal/bukka_b_northeastern_edu/_layouts/15/stream.aspx?id=%2Fpersonal%2Fbukka_b_northeastern_edu%2FDocuments%2FRecordings%2FMeeting+with+Bhavya+Likhitha+Bukka-20260206_155007-Meeting+Recording.mp4&referrer=StreamWebApp.Web&referrerScenario=AddressBarCopied.view.418eceb9-0292-4bb4-a582-2dbfdcbc5e55&startedResponseCatch=true)|

---

## Project Context

This project is **Case Study 2** in the Big Data and Intelligent Analytics course (Spring 2026, QuantUniversity). It builds directly on **Case Study 1 (Platform Foundation)**, where API endpoints, data models, and a persistence layer were established. Case Study 2 populates that platform with **evidence** — the raw data that will eventually feed an AI-readiness scoring engine in Case Study 3.

### The Business Problem

Private equity partners need to answer: *"How do we know if a company is actually investing in AI, or just talking about it?"*

- **73%** of companies mention "AI" in 10-K filings (up from 12% in 2018)
- But only **23%** have deployed AI in production
- The gap between rhetoric and reality — the **Say-Do Gap** — is measurable

This case study builds pipelines to collect both types of evidence for **10 target companies** across 5 sectors (Industrials, Healthcare, Services, Consumer, Financial).

---

## Project Overview & Objectives

### Purpose

Build an evidence collection pipeline that ingests **SEC filings** (what companies *say*) and **external signals** (what companies *do*) to enable downstream AI-readiness scoring.

### Scope

| Dimension | Detail |
|-----------|--------|
| **Companies** | 10 targets — CAT, DE, UNH, HCA, ADP, PAYX, WMT, TGT, JPM, GS |
| **SEC Filings** | 10-K (annual), 10-Q (quarterly), 8-K (material events) and DEF 14A |
| **External Signals** | Job postings, technology stack, patents, leadership signals |
| **Storage** | Snowflake (metadata & structured data), S3 (raw documents), Redis (caching) |

### Objectives

1. **SEC EDGAR Pipeline** — Download, parse (PDF & HTML), extract key sections, and semantically chunk SEC filings for all 10 companies.
2. **External Signals Pipeline** — Collect and score job posting, technology stack, patent, and leadership signals with normalized scoring (0–100).
3. **Data Persistence** — Extend the database schema with `documents`, `document_chunks`, `external_signals`, and `company_signal_summaries` tables.
4. **API & Integration** — Expose collection and retrieval endpoints with background task processing, integrating with the CS1 platform foundation.

---

## Architecture & Diagram

![Architecture Diagram](case%20study2%20docs/Screenshots/Architecture.png)
---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Runtime** | Python 3.11+ | Core language |
| **API Framework** | FastAPI | REST endpoints, background tasks |
| **SEC Downloads** | `sec-edgar-downloader` | Automated SEC filing retrieval |
| **PDF Parsing** | `pdfplumber` | Extract text from PDF filings |
| **HTML Parsing** | `BeautifulSoup4` | Extract text from HTML filings |
| **HTTP Client** | `httpx` | External API calls (signals) |
| **Database** | Snowflake | Structured metadata & signal storage |
| **Object Storage** | AWS S3 | Raw document storage |
| **Caching** | Redis | Pipeline result caching |
| **Data Models** | Pydantic v2 | Request/response validation |
| **Logging** | `structlog` | Structured logging |
| **Containerization** | Docker / Docker Compose | Reproducible environments |
| **Testing** | `pytest` | Unit & integration tests |

---

## Step-by-Step Setup Guide

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- Snowflake account with credentials
- AWS S3 bucket access
- Redis instance (or use Docker Compose)

### 1. Clone the Repository

```bash
git clone https://github.com/BigDataIA-Spring26-Team-5/PE_OrgAIR_Platform_Evidence_Collection.git
cd pe-org-air-platform
```

### 2. Configure Environment Variables

```bash
cp .env.example .env
# Edit .env with your credentials:
#   SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
#   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET
#   REDIS_URL
#   SEC_EDGAR_EMAIL (required by SEC for rate limiting)
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Set Up the Database

Run the schema files against Snowflake in order:

```bash
# Core tables (from CS1)
# Then CS2 extensions:
  app/database/document_schema.sql
  app/database/document_chunks_schema.sql
  app/database/signals_schema.sql
```

### 5. Run with Docker (Recommended)

```bash
docker-compose up --build
docker ps
```

### 6. Run Locally (fast api)

```bash
uvicorn app.main:app --reload
```

### 7. Collect Evidence
 
- Test the api end points for evidence part (refer to Swagger UI Docs)

### 8. Run Tests

```bash
pytest tests/ -v
```

### Run Streamlit
```bash 
streamlit run ./streamlit/app/py
```
![Architecture Diagram](case%20study2%20docs/Screenshots/streamlit_sec.png)

![Architecture Diagram](case%20study2%20docs/Screenshots/streamlit_signals.png)

---

## Directory Structure

```
pe-org-air-platform/
├── pyproject.toml
├── requirements.txt
├── .env.example
├── app/
│   ├── config.py                  # App configuration & env vars
│   ├── main.py                    # FastAPI app entrypoint
│   ├── shutdown.py                # Graceful shutdown handling
│   ├── core/
│   │   ├── dependencies.py        # Dependency injection
│   │   └── exceptions.py          # Custom exception classes
│   ├── database/
│   │   ├── schema.sql             # Core schema (CS1)
│   │   ├── document_schema.sql    # Documents table
│   │   ├── document_chunks_schema.sql  # Chunks table
│   │   ├── signals_schema.sql     # Signals & summaries tables
│   │   └── seed-*.sql             # Seed data files
│   ├── models/                    # Pydantic data models
│   │   ├── assessment.py
│   │   ├── company.py
│   │   ├── dimension.py
│   │   ├── document.py            # DocumentRecord, DocumentStatus
│   │   ├── evidence.py            # Evidence aggregation models
│   │   ├── signal.py              # ExternalSignal, CompanySignalSummary
│   │   └── signal_responses.py
│   ├── pipelines/                 # Evidence collection pipelines
│   │   ├── sec_edgar.py           # SEC filing downloader
│   │   ├── document_parser.py     # PDF/HTML text extraction
│   │   ├── chunking.py            # Semantic chunking with overlap
│   │   ├── section_analyzer.py    # 10-K section extraction
│   │   ├── job_signals.py         # Job posting signal collector
│   │   ├── tech_signals.py        # Technology stack analyzer
│   │   ├── patent_signals.py      # Patent search & classification
│   │   ├── leadership_analyzer.py # Leadership signal analysis
│   │   ├── keywords.py            # AI keyword definitions
│   │   ├── runner.py              # Pipeline orchestration
│   │   ├── pipeline2_runner.py    # Pipeline 2 orchestration
│   │   ├── pipeline_state.py      # Pipeline state tracking
│   │   ├── pipeline2_state.py     # Pipeline 2 state tracking
│   │   ├── registry.py            # Document registry
│   │   ├── exporters.py           # Data export utilities
│   │   ├── pdf_parser.py          # PDF-specific parsing
│   │   └── utils.py               # Shared pipeline utilities
│   ├── repositories/              # Data access layer
│   │   ├── base.py                # Base repository pattern
│   │   ├── company_repository.py
│   │   ├── document_repository.py
│   │   ├── chunk_repository.py
│   │   ├── signal_repository.py
│   │   ├── signal_scores_repository.py
│   │   ├── assessment_repository.py
│   │   ├── dimension_score_repository.py
│   │   └── industry_repository.py
│   ├── routers/                   # API route handlers
│   │   ├── companies.py
│   │   ├── documents.py           # Document CRUD + collection trigger
│   │   ├── signals.py             # Signal CRUD + collection trigger
│   │   ├── evidence.py            # Unified evidence endpoints
│   │   ├── assessments.py
│   │   ├── dimensionScores.py
│   │   ├── health.py
│   │   ├── industries.py
│   │   ├── pdf_parser.py
│   │   └── sec_filings.py
│   ├── services/                  # Business logic layer
│   │   ├── snowflake.py           # Snowflake DB connection & queries
│   │   ├── s3_storage.py          # AWS S3 file operations
│   │   ├── redis_cache.py         # Redis caching
│   │   ├── cache.py               # Cache abstraction
│   │   ├── document_collector.py  # Document collection orchestration
│   │   ├── document_chunking_service.py
│   │   ├── document_parsing_service.py
│   │   ├── job_data_service.py
│   │   ├── job_signal_service.py
│   │   ├── tech_signal_service.py
│   │   ├── patent_signal_service.py
│   │   ├── leadership_service.py
│   │   └── signals_storage.py
│   └── Scripts/
│       ├── collect_evidence.py    # Main evidence collection script
│       ├── backfill_companies.py  # Backfill for all 10 companies
│       ├── query_snowflake.py     # Ad-hoc Snowflake queries
│       └── test_connections.py    # Connection verification
├── data/
│   ├── raw/                       # Downloaded SEC filings
│   ├── parsed/                    # Extracted JSON documents
│   ├── signals/                   # Cached signal data
│   └── tables/                    # Extracted table data
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
└── tests/
    ├── conftest.py                # Test fixtures
    ├── test_api.py                # API endpoint tests
    ├── test_models.py             # Model validation tests
    ├── test_redis_cache.py        # Cache tests
    ├── test_sec_edgar.py          # SEC pipeline tests
    └── test_signals.py            # Signal pipeline tests
```

---

## How Does the Flow Work

### 1. SEC Document Pipeline

<!-- ![Architecture Diagram](case%20study2%20docs/Screenshots/sec_edgar_flow.png) -->
<img src="case%20study2%20docs/Screenshots/sec_edgar_flow.png" alt="SEC EDGAR Flow" width="700" height="700">

### 2. External Signals Pipeline
<!-- ![Architecture Diagram](case%20study2%20docs/Screenshots/signals_flow.png) -->

<img src="case%20study2%20docs/Screenshots/signals_flow.png" alt="Signals Flow" width="700" and height = "700">

--- 

### 3. API Layer

The FastAPI application exposes endpoints for triggering collection and retrieving results:

| Method | Endpoint | Description |
|--------|----------|-------------|
| **Documents** | | |
| `POST` | `/api/v1/documents/collect` | Trigger document collection for a company |
| `GET` | `/api/v1/documents` | List documents (filterable by company, type) |
| `GET` | `/api/v1/documents/{id}` | Get document with metadata |
| `GET` | `/api/v1/documents/{id}/chunks` | Get document chunks |
| **Signals** | | |
| `POST` | `/api/v1/signals/collect` | Trigger signal collection for a company |
| `GET` | `/api/v1/signals` | List signals (filterable) |
| `GET` | `/api/v1/companies/{id}/signals` | Get signal summary for company |
| `GET` | `/api/v1/companies/{id}/signals/{category}` | Get signals by category |
| **Evidence** | | |
| `GET` | `/api/v1/companies/{id}/evidence` | Get all evidence for a company |
| `POST` | `/api/v1/evidence/backfill` | Backfill evidence for all 10 companies |
| `GET` | `/api/v1/evidence/stats` | Get evidence collection statistics |

---

## Summary & Key Takeaways

### The Say-Do Gap Framework

The core insight driving this project is that **what companies say about AI differs significantly from what they actually do**. We measure this through a normalized formula:

```
Say-Do Gap = (SAY - DO) / max(SAY, DO)
```

Where **SAY** = AI/ML keyword mentions in SEC filings and **DO** = Composite score from four external signal categories (Technology Hiring 30%, Innovation Activity 25%, Digital Presence 25%, Leadership Signals 20%).

### Key Findings Across 10 Companies

| Rank | Company | Gap Score | Interpretation |
|------|---------|-----------|----------------|
| 1 | Goldman Sachs (GS) | **+0.05** | Perfect alignment — communication matches execution across all dimensions |
| 2 | Walmart (WMT) | **+0.09** | Near-perfect alignment — conservative AI talk, strong balanced execution |
| 3 | Target (TGT) | **+0.13** | Nearly aligned — measured communication with solid multi-signal execution |
| 4 | UnitedHealth (UNH) | **+0.22** | Slight over-communication — patent-heavy, hiring-light strategy |
| 5 | JPMorgan Chase (JPM) | **-0.45** | Strong under-promiser — elite execution (highest composite: 68.66) with minimal hype |
| 6 | HCA Healthcare (HCA) | **+0.49** | Moderate over-promiser — implementation focus but zero innovation/patents |
| 7 | Deere & Company (DE) | **+0.54** | Moderate over-promiser — strong innovation (54.5) but imbalanced execution |
| 8 | Caterpillar (CAT) | **-0.62** | Strong under-promiser — only 12 AI mentions but second-highest composite (51.2) |
| 9 | Paychex (PAYX) | **+0.74** | Strong over-promiser — lowest composite (8.97) despite 60 AI/ML mentions |
| 10 | ADP | **+0.88** | Critical over-promiser — highest rhetoric (101 mentions) with minimal execution |

### Strategic Insights

1. **Under-promisers outperform.** JPMorgan (composite 68.66) and Caterpillar (51.2) say the least about AI but execute the most — they build competitive moats quietly.

2. **Alignment correlates with maturity.** Goldman Sachs (+0.05), Walmart (+0.09), and Target (+0.13) show disciplined governance where investor communications and technology teams are synchronized.

3. **Sector patterns emerge.** Retailers (WMT, TGT) describe "digital transformation" rather than using AI buzzwords. Financial services split between elite execution (JPM) and balanced alignment (GS). HR/payroll companies (ADP, PAYX) show the most severe over-promising.

4. **Patent activity is highly uneven.** Innovation scores range from 0.0 (HCA, WMT, TGT) to 87.5 (JPM), revealing fundamentally different AI strategies — some companies build IP, others apply existing tools.

5. **Cloud rhetoric ≠ cloud execution.** ADP has 148 cloud mentions (highest) but the weakest digital presence (2.1), suggesting vendor dependency rather than proprietary development.

6. **The composite score reveals more than any single signal.** Companies like UNH appear moderate overall (29.59) but have extreme imbalances (84.5 innovation vs. 4.0 hiring) that single metrics would miss.

### Execution Strength Rankings

| Rank | Company | Composite Score | Profile |
|------|---------|----------------|---------|
| 1 | JPMorgan Chase | 68.66 | Elite comprehensive execution |
| 2 | Caterpillar | 51.20 | Strong balanced execution |
| 3 | Target | 36.95 | Solid multi-signal execution |
| 4 | UnitedHealth | 29.59 | Innovation-heavy, hiring-light |
| 5 | Goldman Sachs | 28.14 | Perfectly balanced moderate execution |
| 6 | Deere & Company | 27.92 | Innovation-heavy, infrastructure-light |
| 7 | Walmart | 27.39 | Balanced moderate execution |
| 8 | HCA Healthcare | 20.31 | Hiring present, zero innovation |
| 9 | ADP | 11.78 | Weak across all signals |
| 10 | Paychex | 8.97 | Weakest execution in cohort |

### Detailed Signal Scores (from Snowflake `company_signal_summaries`)

Data collected as of February 5–6, 2026. Composite = 0.30×Hiring + 0.25×Innovation + 0.25×Digital + 0.20×Leadership.

| Ticker | Company | Hiring (30%) | Innovation (25%) | Digital (25%) | Leadership (20%) | **Composite** | Signals | Last Updated |
|--------|---------|:------------:|:-----------------:|:-------------:|:-----------------:|:-------------:|:-------:|:------------:|
| JPM | JPMorgan Chase | **74.60** | **87.50** | **83.20** | 18.00 | **68.66** | 4 | 2026-02-05 |
| CAT | Caterpillar | 70.10 | 30.00 | **71.75** | 23.67 | **51.20** | 5 | 2026-02-06 |
| TGT | Target | 53.50 | 0.00 | 54.00 | **37.00** | **36.95** | 6 | 2026-02-05 |
| UNH | UnitedHealth | 4.00 | 84.50 | 14.65 | 18.00 | **29.59** | 6 | 2026-02-06 |
| GS | Goldman Sachs | 30.10 | 28.50 | 27.15 | 26.00 | **28.14** | 4 | 2026-02-05 |
| DE | Deere & Company | 27.30 | 54.50 | 6.30 | 22.67 | **27.92** | 6 | 2026-02-05 |
| WMT | Walmart | 42.30 | 0.00 | 31.20 | 34.50 | **27.39** | 6 | 2026-02-05 |
| HCA | HCA Healthcare | 43.10 | 0.00 | 17.50 | 15.00 | **20.31** | 6 | 2026-02-05 |
| ADP | ADP | 2.00 | 27.00 | 2.10 | 19.50 | **11.78** | 6 | 2026-02-05 |
| PAYX | Paychex | 4.00 | 0.00 | 1.40 | **37.08** | **8.97** | 6 | 2026-02-05 |

> **Bold** values indicate the highest score in each column across the cohort.

---
## Design Decisions & Trade-offs

| Decision | Rationale | Trade-off |
|----------|-----------|-----------|
| **Word-based chunking** (vs. sentence/token) | Simpler implementation, predictable chunk sizes | Less precise semantic boundaries; a sentence may be split mid-thought |
| **Section-aware chunking** | Preserves filing structure; chunks don't cross section boundaries | Sections of varying lengths produce uneven chunk counts |
| **SHA-256 content hashing** | Reliable deduplication across re-runs | Doesn't detect near-duplicate filings with minor formatting differences |
| **Weighted composite scoring** | Reflects PE firm signal priorities (hiring weighted highest at 0.30) | Weights are static; different industries may warrant different weightings |
| **Background tasks (FastAPI)** | Simple async processing without a separate task queue | No retry mechanism, no persistent task state; tasks lost on server restart |
| **Snowflake for all structured data** | Single source of truth, strong SQL analytics support | Higher latency for simple lookups vs. a traditional RDBMS |
| **Redis for caching** | Fast pipeline result caching, reduces redundant API calls | Adds infrastructure complexity; cache invalidation must be managed |
| **Regex for section extraction** | No external dependencies, fast | Brittle — SEC filing formatting varies across companies and years |
| **Normalized 0–100 scoring** | Easy to interpret and compare | Absolute thresholds may not capture relative industry context |

---

## Known Limitations

1. **External signal data is simulated.** The job postings, tech stack, and patent collectors have the analysis logic implemented, but actual API integrations (Indeed, BuiltWith, USPTO) require API keys and are passed empty lists in the collection script. Real data must be sourced separately.

2. **SEC rate limiting.** While `sec-edgar-downloader` handles the 10 req/sec limit, bulk collection for all 10 companies with multiple filing types can be slow.

3. **Section extraction regex is fragile.** SEC filings do not follow a universal format. Edge cases in formatting (especially older filings) may cause section extraction to fail silently.

4. **No persistent task queue.** Background tasks in FastAPI are in-memory. If the server restarts during a collection run, the task is lost with no retry mechanism. A production system would use Celery or similar.

5. **No vector embeddings yet.** Document chunks are stored as text but are not yet embedded for semantic search. This is deferred to Case Study 4.

6. **Leadership signal collector** is included structurally but may have limited data sourcing compared to the other three signal categories.

7. **Single-threaded collection.** The evidence collection script processes companies sequentially. Parallel collection would significantly reduce total runtime.

---

## Team Member Contributions & AI Usage

### Team Members

| Member | Responsibilities |
|--------|-----------------|
| **Bhavya** | Snowflake database seeding & infrastructure setup · FastAPI endpoints (uvicorn) for SEC pipeline with S3 & Snowflake storage · Delete/reset endpoints for pipeline data management · Signal pipeline endpoints & quality testing · Evidence collection for AI readiness scoring · README and project documentation maintenance |
| **Deepika** | SEC EDGAR initial pipeline architecture & design · PDF parsing for SEC filings · Scaled pipeline to all 10 companies with S3 & Snowflake integration · Streamlit application for SEC and Signal pipelines · Key takeaways and Say-Do Gap analysis documentation |
| **Aqeel** | Signal pipeline for AI readiness scoring (job postings, patents, tech stack, leadership signals) · Composite scoring methodology across four signal categories · Evidence gathering and validation · Signal data integration with Snowflake |


### AI Tools Usage Disclosure

| Tool | Usage |
|------|-------|
| **ChatGPT / Claude** | code debugging, documentation draftin |
| **GitHub Copilot** | Code autocompletion for boilerplate repository methods |

> *All AI-generated code was reviewed, tested, and adapted to fit the project's architecture and requirements. AI was used as a productivity aid, not as a substitute for understanding the underlying concepts.*
