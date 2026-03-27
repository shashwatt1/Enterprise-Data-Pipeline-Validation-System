"""
Enterprise Data Pipeline & Validation System
Main FastAPI application entry point.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.utils.db import create_tables

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description=(
        "A production-grade data pipeline system demonstrating "
        "data ingestion, schema mapping, transformation, validation, "
        "and SQL-based querying via REST APIs."
    ),
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS — allow all origins for development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def on_startup():
    """Create database tables on application startup."""
    create_tables()


@app.get("/", tags=["Health"])
def root():
    return {
        "name": settings.app_name,
        "version": settings.app_version,
        "docs": "/docs",
        "status": "running",
    }


@app.get("/health", tags=["Health"])
def health_check():
    return {
        "status": "healthy",
        "version": settings.app_version,
        "database": settings.database_url.split("://")[0],
    }
