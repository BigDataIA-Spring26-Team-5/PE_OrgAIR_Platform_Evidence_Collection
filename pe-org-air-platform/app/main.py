from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError

# IMPORT ROUTERS
from app.routers.companies import router as companies_router
from app.routers.companies import validation_exception_handler
from app.routers.industries import router as industries_router
from app.routers.health import router as health_router
from app.routers.assessments import router as assessments_router
from app.routers.dimensionScores import router as dimension_scores_router
from app.routers.documents import router as documents_router
from app.routers.sec_filings import router as sec_filings_router  # NEW
from app.routers.pdf_parser import router as pdf_parser_router 

load_dotenv()

# FASTAPI APPLICATION CONFIGURATION
app = FastAPI(
    title="PE Org-AI-R Platform Foundation",
    description="""
# PE Organization AI Readiness Platform API

REST API for managing assessments, dimension scores, and portfolio companies.

---

## SEC Filings Pipeline (Step-by-Step)

Process SEC EDGAR filings in discrete, controllable steps:

| Step | Endpoint | Method | Description |
|------|----------|--------|-------------|
| 1 | `/api/v1/sec/download` | POST | Download filings (ticker, dates, types) |
| 2 | `/api/v1/sec/parse` | GET | Parse downloaded documents |
| 3 | `/api/v1/sec/deduplicate` | GET | Find unique documents |
| 4 | `/api/v1/sec/chunk` | POST | Chunk documents (set size, overlap) |
| 5 | `/api/v1/sec/extract-items` | GET | Extract Items 1, 1A, 7 |
| 6 | `/api/v1/sec/stats` | GET | View pipeline statistics |

**Reset:** `POST /api/v1/sec/reset` to start fresh.

### Data Storage

| Step | Local Path | Snowflake |
|------|------------|-----------|
| Download | `data/raw/sec/` | - |
| Parse | `data/parsed/{ticker}/` | - |
| Deduplicate | `data/processed/registry/` | Check only |
| Chunk | `data/chunks/{ticker}/` | documents, document_chunks |
| Extract | `data/output_items/{ticker}/` | - |

---
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

# REGISTER EXCEPTION HANDLERS
app.add_exception_handler(RequestValidationError, validation_exception_handler)
app.include_router(pdf_parser_router)  # ADD THIS

# ROOT ENDPOINT
@app.get("/", tags=["Root"], summary="Root endpoint")
async def root():
    """Root endpoint that returns API information."""
    return {
        "message": "Welcome to PE Org-AI-R Platform Foundation API",
        "version": "1.0.0",
        "documentation": {
            "swagger": "/docs",
            "redoc": "/redoc"
        },
        "sec_pipeline": {
            "description": "Step-by-step SEC filing processing pipeline",
            "steps": [
                {"step": 1, "method": "POST", "endpoint": "/api/v1/sec/download", "description": "Download filings"},
                {"step": 2, "method": "GET", "endpoint": "/api/v1/sec/parse", "description": "Parse documents"},
                {"step": 3, "method": "GET", "endpoint": "/api/v1/sec/deduplicate", "description": "Find unique docs"},
                {"step": 4, "method": "POST", "endpoint": "/api/v1/sec/chunk", "description": "Chunk documents"},
                {"step": 5, "method": "GET", "endpoint": "/api/v1/sec/extract-items", "description": "Extract Items 1, 1A, 7"},
                {"step": 6, "method": "GET", "endpoint": "/api/v1/sec/stats", "description": "View statistics"},
            ],
            "reset": {"method": "POST", "endpoint": "/api/v1/sec/reset"}
        }
    }


# REGISTER ROUTERS (order matters for docs display)
app.include_router(sec_filings_router)      # SEC Pipeline endpoints
app.include_router(documents_router)         # Legacy document endpoints
app.include_router(health_router)            # Health check
app.include_router(companies_router)         # Companies CRUD
app.include_router(industries_router)        # Industries CRUD
app.include_router(assessments_router)       # Assessments
app.include_router(dimension_scores_router)  # Dimension scores


# STARTUP & SHUTDOWN EVENTS
@app.on_event("startup")
async def startup_event():
    """Runs when the application starts."""
    print("\n" + "="*60)
    print("  PE Org-AI-R Platform Foundation API")
    print("="*60)
    print("\n📚 Documentation:")
    print("   Swagger UI: http://localhost:8000/docs")
    print("   ReDoc:      http://localhost:8000/redoc")
    print("\n📋 SEC Pipeline Endpoints:")
    print("   POST /api/v1/sec/download       - Download filings")
    print("   GET  /api/v1/sec/parse          - Parse documents")
    print("   GET  /api/v1/sec/deduplicate    - Find unique docs")
    print("   POST /api/v1/sec/chunk          - Chunk documents")
    print("   GET  /api/v1/sec/extract-items  - Extract Items 1, 1A, 7")
    print("   GET  /api/v1/sec/stats          - View statistics")
    print("   POST /api/v1/sec/reset          - Reset pipeline")
    print("\n" + "="*60 + "\n")


@app.on_event("shutdown")
async def shutdown_event():
    """Runs when the application shuts down."""
    print("\nShutting down PE Org-AI-R Platform Foundation API...")


# RUN WITH UVICORN
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )