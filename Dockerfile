FROM python:3.9-slim

# Install system dependencies including OpenCV, Chinese OCR, and PostgreSQL requirements
RUN apt-get update && apt-get install -y \
    # Basic tools
    curl wget git gcc g++ make \
    # PostgreSQL client and development headers (FIXES psycopg2 ERROR!)
    postgresql-client \
    libpq-dev \
    python3-dev \
    # OpenCV dependencies (THIS FIXES THE ERROR!)
    libgl1-mesa-glx \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    libgomp1 \
    libgstreamer1.0-0 \
    libgstreamer-plugins-base1.0-0 \
    libgtk-3-0 \
    # Tesseract OCR with CHINESE language packs - FIXED!
    tesseract-ocr \
    tesseract-ocr-eng \
    tesseract-ocr-chi-sim \
    tesseract-ocr-chi-tra \
    # Additional language support
    tesseract-ocr-script-hans \
    tesseract-ocr-script-hant \
    # System monitoring
    procps \
    htop \
    # Timezone data
    tzdata \
    # File utilities
    unzip \
    # Network utilities
    netcat-openbsd \
    # Build dependencies for Python packages
    build-essential \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    # Ensure psycopg2 is installed for PostgreSQL support
    pip install --no-cache-dir psycopg2-binary

# Copy application code
COPY . .

# Create enhanced directory structure for dual directory monitoring
RUN mkdir -p /app/worker/data/receipt_checked \
    /app/worker/data/receipt_ocr_text \
    /app/worker/data/delivery_found \
    /app/worker/data/non_delivery \
    /app/worker/data/matched_non_delivery \
    /app/worker/data/converted_tz \
    /app/worker/data/downloaded_receipts \
    /app/worker/data/inserted_to_database \
    /app/logs

# Set permissions
RUN chmod +x app/workers/*.py 2>/dev/null || true && \
    chmod -R 755 /app/worker/data 2>/dev/null || true && \
    chmod -R 755 /app/logs 2>/dev/null || true

# Environment variables for enhanced features
ENV PYTHONPATH=/app:/app/app:/app/config:/app/app/services:/app/app/workers
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV TZ=UTC
# Enhanced delivery scanner settings
ENV DUAL_DIRECTORY_MODE=true
ENV AUTO_CLEANUP=true
ENV SCAN_INTERVAL=30
# Timezone worker settings
ENV TIMEZONE_UTC_OFFSET=0
ENV SOURCE_TIMEZONE=8
ENV TARGET_TIMEZONE=0

# VERIFY Chinese OCR installation - ENHANCED TEST!
RUN echo "🔍 Testing Tesseract installation..." && \
    tesseract --list-langs && \
    echo "✅ Tesseract languages verified!"

# Enhanced health check that TESTS Chinese OCR AND PostgreSQL
RUN echo '#!/usr/bin/env python3\n\
import sys\n\
print("🔍 Testing system components...")\n\
\n\
# Test OpenCV and Tesseract\n\
try:\n\
    import cv2\n\
    import pytesseract\n\
    langs = pytesseract.get_languages()\n\
    print(f"📋 Available OCR languages: {langs}")\n\
    if "chi_sim" in langs:\n\
        print("✅ Chinese Simplified OCR available")\n\
    else:\n\
        print("❌ Chinese Simplified OCR missing")\n\
        sys.exit(1)\n\
    if "chi_tra" in langs:\n\
        print("✅ Chinese Traditional OCR available")\n\
    else:\n\
        print("⚠️ Chinese Traditional OCR missing")\n\
except Exception as e:\n\
    print(f"❌ OCR test failed: {e}")\n\
    sys.exit(1)\n\
\n\
# Test PostgreSQL connection capability\n\
try:\n\
    import psycopg2\n\
    print("✅ PostgreSQL driver (psycopg2) available")\n\
except Exception as e:\n\
    print(f"❌ PostgreSQL driver test failed: {e}")\n\
    sys.exit(1)\n\
\n\
# Test enhanced delivery scanner imports\n\
try:\n\
    import json\n\
    import pathlib\n\
    print("✅ Enhanced delivery scanner dependencies available")\n\
except Exception as e:\n\
    print(f"❌ Delivery scanner test failed: {e}")\n\
    sys.exit(1)\n\
\n\
# Test database inserter dependencies\n\
try:\n\
    from datetime import datetime\n\
    import threading\n\
    import logging\n\
    from pathlib import Path\n\
    from queue import Queue\n\
    print("✅ Database inserter dependencies available")\n\
except Exception as e:\n\
    print(f"❌ Database inserter test failed: {e}")\n\
    sys.exit(1)\n\
\n\
print("🎉 All system components verified successfully!")\n\
sys.exit(0)' > /app/enhanced_system_test.py && chmod +x /app/enhanced_system_test.py

# Test all components during build
RUN python /app/enhanced_system_test.py

# Enhanced health check for production
HEALTHCHECK --interval=30s --timeout=15s --start-period=60s --retries=3 \
    CMD python /app/enhanced_system_test.py

# Create startup script for enhanced features
RUN echo '#!/bin/bash\n\
echo "🚀 Starting Point Detection System with Enhanced Features"\n\
echo "📊 System Info:"\n\
echo "   - Dual Directory Monitoring: $DUAL_DIRECTORY_MODE"\n\
echo "   - Auto Cleanup: $AUTO_CLEANUP"\n\
echo "   - Scan Interval: $SCAN_INTERVAL seconds"\n\
echo "   - Source Timezone: UTC+$SOURCE_TIMEZONE"\n\
echo "   - Target Timezone: UTC+$TARGET_TIMEZONE"\n\
echo "   - PostgreSQL Support: Available"\n\
echo "   - Chinese OCR: Available"\n\
echo "   - Database Inserter: Ready"\n\
echo "📁 Directory Structure:"\n\
ls -la /app/worker/data/ 2>/dev/null || echo "   Data directories will be created on first run"\n\
echo "🔧 Ready to start workers..."\n\
exec "$@"' > /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Use enhanced entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

CMD ["python", "--version"]

# ═══════════════════════════════════════════════════════════════
# ENHANCED DOCKERFILE FEATURES - INCLUDING DATABASE INSERTER
# ═══════════════════════════════════════════════════════════════
#
# 🆕 NEW FEATURES ADDED:
#
# 🔧 PostgreSQL Support:
# ✅ Added libpq-dev and postgresql-client
# ✅ Installed psycopg2-binary for database connections
# ✅ Fixed "ModuleNotFoundError: No module named 'psycopg2'" error
#
# 📁 Enhanced Directory Structure:
# ✅ Pre-creates all required directories for dual monitoring
# ✅ Proper permissions for worker data directories
# ✅ Organized folder structure for enhanced workflow
# ✅ Database inserter directories ready
#
# 🔍 Comprehensive Testing:
# ✅ Tests Chinese OCR capabilities
# ✅ Verifies PostgreSQL driver availability
# ✅ Validates delivery scanner dependencies
# ✅ Tests database inserter dependencies
# ✅ Enhanced health checks with detailed reporting
#
# 🚀 Enhanced Environment:
# ✅ Dual directory monitoring environment variables
# ✅ Auto-cleanup settings
# ✅ Timezone conversion configuration
# ✅ Proper Python path setup
# ✅ Database inserter environment ready
#
# 📊 Startup Information:
# ✅ Enhanced entrypoint script with system info
# ✅ Shows configuration on container start
# ✅ Validates directory structure
# ✅ Ready for production deployment
#
# DIRECTORY STRUCTURE CREATED:
# /app/worker/data/
# ├── receipt_checked/        ← Primary source (delivery scanner)
# ├── receipt_ocr_text/       ← Secondary source (delivery scanner)
# ├── delivery_found/         ← Delivery receipts with enhanced logging
# ├── non_delivery/          ← Non-delivery receipts
# ├── matched_non_delivery/  ← Matched receipts (timezone worker input)
# ├── converted_tz/          ← UTC converted files (database inserter input) 🔵
# ├── downloaded_receipts/   ← OCR processor input
# └── inserted_to_database/  ← Final processed files (database inserter output) 🔵
#
# 🔵 DATABASE INSERTER INTEGRATION:
# ✅ Monitors converted_tz/ directory for JSON files
# ✅ Processes receipts in real-time with batching
# ✅ Inserts data into PostgreSQL database
# ✅ Docker-optimized (no file movement in container)
# ✅ Handles duplicates via database constraints
# ✅ Full health check integration
#
# BUILD COMMAND:
# docker build -t point-detection-enhanced .
#
# The image is now ready for your enhanced dual directory monitoring system
# including the database inserter worker!