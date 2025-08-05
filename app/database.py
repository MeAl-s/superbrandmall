# app/database.py - Database connection setup for FastAPI
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from dotenv import load_dotenv
from typing import Generator

# Load environment variables from your existing .env file
load_dotenv(r"C:\Point Detection\.env")

# Database configuration for production load (4000+ receipts/day)
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in environment variables")

# Create engine with production-ready connection pooling
engine = create_engine(
    DATABASE_URL,
    # Connection pool settings for concurrent FastAPI requests
    poolclass=QueuePool,
    pool_size=20,              # Number of connections to maintain in pool
    max_overflow=30,           # Additional connections under high load
    pool_pre_ping=True,        # Verify connections before use
    pool_recycle=3600,         # Recycle connections after 1 hour
    echo=False,                # Set to True for SQL logging in development
    
    # Performance optimizations
    pool_timeout=30,           # Timeout for getting connection from pool
    pool_reset_on_return='commit',  # Reset connection state on return
)

# Create session factory
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

def get_db() -> Generator[Session, None, None]:
    """
    FastAPI dependency to get database session.
    
    Automatically handles session lifecycle:
    - Creates session for each request
    - Closes session after request completes
    - Rolls back on errors
    """
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()

def create_tables():
    """
    Create database tables if they don't exist.
    
    Call this during application startup.
    """
    from .models.receipt import Base
    Base.metadata.create_all(bind=engine)

def get_db_stats() -> dict:
    """Get database connection pool statistics for monitoring"""
    pool = engine.pool
    return {
        "pool_size": pool.size(),
        "checked_in": pool.checkedin(),
        "checked_out": pool.checkedout(),
        "overflow": pool.overflow(),
        "invalid": pool.invalid()
    }