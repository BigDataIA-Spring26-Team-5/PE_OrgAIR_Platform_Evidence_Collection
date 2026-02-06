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
from app.routers.pdf_parser import router as pdf_parser_router
from app.routers.signals import router as signals_router
from app.routers.evidence import router as evidence_router

load_dotenv()

# FASTAPI APPLICATION CONFIGURATION
app = FastAPI(
    title="PE Org-AI-R Platform Foundation API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

# REGISTER EXCEPTION HANDLERS
app.add_exception_handler(RequestValidationError, validation_exception_handler)

# REGISTER ROUTERS
app.include_router(signals_router)
app.include_router(health_router)
app.include_router(documents_router)
app.include_router(pdf_parser_router)
app.include_router(companies_router)
app.include_router(industries_router)
app.include_router(assessments_router)
app.include_router(dimension_scores_router)
app.include_router(evidence_router)

# ROOT ENDPOINT
@app.get("/", tags=["Root"], summary="Root endpoint")
async def root():
    return {
        "service": "PE Org-AI-R Platform Foundation API",
        "version": "1.0.0",
        "docs": {
            "swagger": "/docs",
            "redoc": "/redoc"
        },
        "status": "running"
    }

# STARTUP EVENT
@app.on_event("startup")
async def startup_event():
    print("Starting PE Org-AI-R Platform Foundation API...")
    print("Swagger UI available at: http://localhost:8000/docs")

# SHUTDOWN EVENT
@app.on_event("shutdown")
async def shutdown_event():
    print("Shutting down PE Org-AI-R Platform Foundation API...")

# RUN WITH UVICORN
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
