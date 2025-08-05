# config/database.py
# Phase 1: Basic Database Configuration for Receipt Matcher API

from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base, sessionmaker
import psycopg2

# Load environment variables from your existing .env file
load_dotenv(r"C:\Point Detection\.env")

# Get DATABASE_URL from environment
DATABASE_URL = os.getenv("DATABASE_URL")
print("🔍 Original DATABASE_URL:", repr(DATABASE_URL))

# Fix for SQLAlchemy - convert postgres:// to postgresql://
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    SQLALCHEMY_DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    print("🔧 Fixed SQLAlchemy URL:", repr(SQLALCHEMY_DATABASE_URL))
else:
    SQLALCHEMY_DATABASE_URL = DATABASE_URL

def test_psycopg2_connection():
    """Test PostgreSQL connection using your existing format"""
    print("\n🧪 Testing psycopg2 connection...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        print("✅ Connected to Postgres with psycopg2!")
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        result = cur.fetchone()
        print("📊 Postgres replied:", result)
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print("❌ psycopg2 connection failed:", e)
        return False

def test_sqlalchemy_connection():
    """Test SQLAlchemy connection"""
    print("\n🧪 Testing SQLAlchemy connection...")
    try:
        # Create a test engine
        test_engine = create_engine(SQLALCHEMY_DATABASE_URL)
        
        # Test connection - use text() for raw SQL
        with test_engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            row = result.fetchone()
            print("✅ SQLAlchemy connected successfully!")
            print("📊 SQLAlchemy replied:", row)
        
        # Clean up
        test_engine.dispose()
        return True
    except Exception as e:
        print("❌ SQLAlchemy connection failed:", e)
        return False

# Create SQLAlchemy components
print("\n⚙️  Setting up SQLAlchemy components...")

# Create engine with connection pooling
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,  # Verify connections before use
    echo=False  # Set to True to see SQL queries
)

# Create SessionLocal class for database sessions
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create Base class for our models (fixed import)
Base = declarative_base()

def get_db():
    """Dependency to get database session for FastAPI"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_tables():
    """Create all database tables"""
    print("\n🗄️  Creating database tables...")
    try:
        Base.metadata.create_all(bind=engine)
        print("✅ Database tables created successfully!")
        return True
    except Exception as e:
        print(f"❌ Error creating tables: {e}")
        return False

def main():
    """Main function to test everything"""
    print("🚀 Phase 1: Database Configuration Test")
    print("=" * 50)
    
    # Test connections
    psycopg2_ok = test_psycopg2_connection()
    sqlalchemy_ok = test_sqlalchemy_connection()
    
    if psycopg2_ok and sqlalchemy_ok:
        print("\n✅ All database connections successful!")
        print("🎯 Ready for Phase 2: Creating database models")
        return True
    else:
        print("\n❌ Database connection issues detected!")
        return False

if __name__ == "__main__":
    main()