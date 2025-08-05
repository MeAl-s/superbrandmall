# realtime_ocr.py - Real-time OCR processor with smart batching
import os
import json
import cv2
import numpy as np
import time
import signal
import sys
from pathlib import Path
from PIL import Image
import pytesseract
from datetime import datetime
import logging
from threading import Thread
import queue

# ═══════════════════════════════════════════════════════════════
# DATA FOLDER SETUP
# ═══════════════════════════════════════════════════════════════

# Define paths
SOURCE_DIR = Path(__file__).parent / "data" / "downloaded_receipts"
OUTPUT_DIR = Path(__file__).parent / "data" / "receipt_checked"

def setup_ocr_folders():
    """Create OCR folders"""
    SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    print(f"📁 OCR folders ready:")
    print(f"   📂 Source: {SOURCE_DIR}")
    print(f"   📝 Output: {OUTPUT_DIR}")

# Setup folders on import
setup_ocr_folders()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# OCR PROCESSING CLASS
# ═══════════════════════════════════════════════════════════════

class RealtimeOCRProcessor:
    def __init__(self):
        self.processed_files = set()  # Track files we've processed (session only)
        self.processing_queue = queue.Queue()
        self.is_processing = False
        self.ocr_stats = {
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_processed": 0,
            "successful_ocr": 0,
            "failed_ocr": 0,
            "average_time_per_file": 0,
            "total_processing_time": 0
        }
        
        # OCR settings for better performance
        self.batch_size = 10  # Process 10 files at a time
        self.max_processing_time = 300  # 5 minutes max per batch
        
    def check_tesseract_installation(self):
        """Check if Tesseract is properly installed"""
        try:
            version = pytesseract.get_tesseract_version()
            logger.info(f"Tesseract version: {version}")
            
            languages = pytesseract.get_languages()
            logger.info(f"Available languages: {len(languages)} found")
            
            # Check for Chinese languages
            chinese_langs = [lang for lang in languages if 'chi' in lang]
            if chinese_langs:
                logger.info(f"Chinese language packs: {chinese_langs}")
            else:
                logger.warning("No Chinese language packs found")
            
            return True
            
        except Exception as e:
            logger.error(f"Tesseract check failed: {str(e)}")
            return False
    
    def preprocess_image(self, image_path):
        """Preprocess image for better OCR accuracy"""
        try:
            # Read image
            image = cv2.imread(str(image_path))
            if image is None:
                return None
            
            # Convert to grayscale
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            
            # Apply denoising (fast version for real-time)
            denoised = cv2.fastNlMeansDenoising(gray, h=10)
            
            # Apply adaptive thresholding
            thresh = cv2.adaptiveThreshold(
                denoised, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2
            )
            
            return thresh
            
        except Exception as e:
            logger.error(f"Error preprocessing {image_path}: {str(e)}")
            return None
    
    def perform_fast_ocr(self, image):
        """Perform fast OCR optimized for real-time processing"""
        try:
            # Convert to PIL Image
            pil_image = Image.fromarray(image)
            
            # Fast OCR configuration
            config = r'--oem 3 --psm 6'
            
            # Try languages in order of speed (fastest first)
            languages = ['chi_sim+eng']
            
            for lang in languages:
                try:
                    # Quick confidence check first
                    data = pytesseract.image_to_data(
                        pil_image, lang=lang, config=config, 
                        output_type=pytesseract.Output.DICT
                    )
                    
                    confidences = [int(conf) for conf in data['conf'] if int(conf) > 0]
                    avg_confidence = sum(confidences) / len(confidences) if confidences else 0
                    
                    # If confidence is good enough, use this result
                    if avg_confidence > 30:  # Lower threshold for speed
                        result = pytesseract.image_to_string(pil_image, lang=lang, config=config)
                        return result.strip(), avg_confidence, lang
                    
                except Exception:
                    continue
            
            # Fallback to basic English OCR
            result = pytesseract.image_to_string(pil_image, lang='eng')
            return result.strip(), 0, 'eng'
            
        except Exception as e:
            logger.error(f"OCR error: {str(e)}")
            return "", 0, "error"
    
    def clean_ocr_text(self, text):
        """Clean OCR text for better readability"""
        if not text:
            return ""
        
        lines = text.split('\n')
        cleaned_lines = [line.strip() for line in lines if line.strip()]
        return '\r\n'.join(cleaned_lines)
    
    def process_single_file(self, image_path):
        """Process a single image file"""
        start_time = time.time()
        
        try:
            logger.info(f"Processing: {image_path.name}")
            
            # Check if output already exists
            output_filename = image_path.stem + '.json'
            output_path = OUTPUT_DIR / output_filename
            
            if output_path.exists():
                logger.info(f"Output already exists: {output_filename}")
                return True, time.time() - start_time
            
            # Preprocess image
            preprocessed = self.preprocess_image(image_path)
            if preprocessed is None:
                logger.error(f"Failed to preprocess: {image_path.name}")
                return False, time.time() - start_time
            
            # Perform OCR
            ocr_text, confidence, language = self.perform_fast_ocr(preprocessed)
            
            # Clean text
            cleaned_text = self.clean_ocr_text(ocr_text)
            
            # Create output JSON
            output_data = {
                "data": cleaned_text,
                "success": True,
                "message": None,
                "fields": None,
                "total": None,
                "ocr_metadata": {
                    "confidence": confidence,
                    "language": language,
                    "processing_time": time.time() - start_time,
                    "processed_at": datetime.now().isoformat()
                }
            }
            
            # Save JSON file
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)
            
            processing_time = time.time() - start_time
            logger.info(f"✓ Completed {image_path.name} in {processing_time:.2f}s (confidence: {confidence:.1f}%)")
            
            return True, processing_time
            
        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Error processing {image_path.name}: {str(e)}")
            return False, processing_time
    
    def scan_for_new_files(self):
        """Scan for new image files"""
        if not SOURCE_DIR.exists():
            return []
        
        supported_formats = {'.bmp', '.png', '.jpg', '.jpeg', '.tiff', '.tif', '.webp', '.gif'}
        new_files = []
        
        for file_path in SOURCE_DIR.iterdir():
            if (file_path.is_file() and 
                file_path.suffix.lower() in supported_formats):
                
                file_key = f"{file_path.name}_{file_path.stat().st_mtime}"
                
                if file_key not in self.processed_files:
                    new_files.append(file_path)
                    self.processed_files.add(file_key)
        
        return new_files
    
    def process_batch(self, files):
        """Process a batch of files with time limit"""
        if not files:
            return
        
        batch_start = time.time()
        successful = 0
        failed = 0
        total_processing_time = 0
        
        print(f"\n🎯 Processing batch of {len(files)} files...")
        
        for i, file_path in enumerate(files, 1):
            # Check if we're running out of time
            elapsed = time.time() - batch_start
            if elapsed > self.max_processing_time:
                logger.warning(f"Batch timeout reached after {elapsed:.1f}s, stopping at file {i}")
                break
            
            print(f"📄 [{i}/{len(files)}] {file_path.name}")
            
            success, proc_time = self.process_single_file(file_path)
            total_processing_time += proc_time
            
            if success:
                successful += 1
            else:
                failed += 1
            
            # Update stats
            self.ocr_stats["total_processed"] += 1
            self.ocr_stats["total_processing_time"] += proc_time
            
            if self.ocr_stats["total_processed"] > 0:
                self.ocr_stats["average_time_per_file"] = (
                    self.ocr_stats["total_processing_time"] / 
                    self.ocr_stats["total_processed"]
                )
        
        batch_time = time.time() - batch_start
        
        # Update stats
        self.ocr_stats["successful_ocr"] += successful
        self.ocr_stats["failed_ocr"] += failed
        
        print(f"\n📊 Batch Summary:")
        print(f"    ✅ Successful: {successful}")
        print(f"    ❌ Failed: {failed}")
        print(f"    ⏱️ Batch time: {batch_time:.1f}s")
        print(f"    📈 Avg per file: {total_processing_time/len(files):.1f}s")
        
        # Performance warning
        if total_processing_time > 60:
            print(f"    ⚠️ Batch took {total_processing_time:.1f}s - may fall behind in heavy load")
    
    def run_realtime_monitor(self, check_interval=120):  # 2 minutes default
        """Main real-time monitoring loop"""
        print(f"\n🚀 Starting Real-time OCR Processor")
        print(f"⏱️  Check interval: {check_interval} seconds")
        print(f"📦 Batch size: {self.batch_size} files")
        print(f"⏰ Max batch time: {self.max_processing_time} seconds")
        print(f"📂 Monitoring: {SOURCE_DIR}")
        print(f"📝 Output to: {OUTPUT_DIR}")
        print(f"💡 Press Ctrl+C to stop")
        print("="*60)
        
        # Check Tesseract
        if not self.check_tesseract_installation():
            print("❌ Tesseract not properly installed. Exiting.")
            return
        
        def signal_handler(sig, frame):
            print(f"\n🛑 Stopping OCR processor...")
            print(f"📊 Final stats: {self.ocr_stats['total_processed']} files processed")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        
        try:
            while True:
                print(f"\n🔍 Scanning for new images... {datetime.now().strftime('%H:%M:%S')}")
                
                # Scan for new files
                new_files = self.scan_for_new_files()
                
                if new_files:
                    # Process in batches
                    for i in range(0, len(new_files), self.batch_size):
                        batch = new_files[i:i + self.batch_size]
                        self.process_batch(batch)
                        
                        # Small break between batches
                        if i + self.batch_size < len(new_files):
                            print("😴 Brief pause between batches...")
                            time.sleep(5)
                else:
                    print("📭 No new images to process")
                
                print(f"\n📊 Session Stats:")
                print(f"    🕐 Running since: {self.ocr_stats['start_time']}")
                print(f"    🔄 Total processed: {self.ocr_stats['total_processed']}")
                print(f"    ✅ Successful: {self.ocr_stats['successful_ocr']}")
                print(f"    ❌ Failed: {self.ocr_stats['failed_ocr']}")
                print(f"    ⏱️ Avg time/file: {self.ocr_stats['average_time_per_file']:.1f}s")
                print(f"    📚 Files tracked: {len(self.processed_files)}")
                
                # Performance analysis
                if self.ocr_stats["average_time_per_file"] > 0:
                    files_per_minute = 60 / self.ocr_stats["average_time_per_file"]
                    print(f"    🚀 Processing rate: {files_per_minute:.1f} files/minute")
                
                print(f"\n😴 Waiting {check_interval} seconds...")
                time.sleep(check_interval)
                
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            raise

# ═══════════════════════════════════════════════════════════════
# MANUAL PROCESSING FUNCTION
# ═══════════════════════════════════════════════════════════════

def process_existing_files():
    """Process all existing files in source directory"""
    print(f"\n🔄 Processing all existing images in {SOURCE_DIR}")
    
    processor = RealtimeOCRProcessor()
    
    if not processor.check_tesseract_installation():
        print("❌ Tesseract not properly installed. Exiting.")
        return
    
    if not SOURCE_DIR.exists():
        print(f"❌ Source directory {SOURCE_DIR} does not exist!")
        return
    
    # Get all image files
    supported_formats = {'.bmp', '.png', '.jpg', '.jpeg', '.tiff', '.tif', '.webp', '.gif'}
    files = [f for f in SOURCE_DIR.iterdir() 
             if f.is_file() and f.suffix.lower() in supported_formats]
    
    if not files:
        print("📭 No image files found to process")
        return
    
    print(f"📂 Found {len(files)} image files")
    
    # Process in batches
    for i in range(0, len(files), processor.batch_size):
        batch = files[i:i + processor.batch_size]
        batch_num = (i // processor.batch_size) + 1
        total_batches = (len(files) + processor.batch_size - 1) // processor.batch_size
        
        print(f"\n📦 Processing batch {batch_num}/{total_batches}")
        processor.process_batch(batch)
    
    print(f"\n✅ All files processed!")
    print(f"📊 Total: {processor.ocr_stats['successful_ocr']} successful, {processor.ocr_stats['failed_ocr']} failed")

# ═══════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Real-time OCR Processor")
    parser.add_argument("--interval", type=int, default=120,
                       help="Check interval in seconds (default: 120)")
    parser.add_argument("--batch-size", type=int, default=10,
                       help="Files to process per batch (default: 10)")
    parser.add_argument("--process-existing", action="store_true",
                       help="Process all existing files once and exit")
    
    args = parser.parse_args()
    
    if args.process_existing:
        # Process all existing files
        process_existing_files()
    else:
        # Start real-time monitoring
        processor = RealtimeOCRProcessor()
        processor.batch_size = args.batch_size
        processor.run_realtime_monitor(args.interval)

# ═══════════════════════════════════════════════════════════════
# USAGE EXAMPLES
# ═══════════════════════════════════════════════════════════════

"""
REAL-TIME OCR PROCESSOR WITH SMART BATCHING

USAGE:

1. Start real-time monitoring (default 2-minute intervals):
   python realtime_ocr.py

2. Custom settings:
   python realtime_ocr.py --interval 180 --batch-size 5

3. Process all existing files once:
   python realtime_ocr.py --process-existing

PERFORMANCE OPTIMIZATION:

Your Performance: 100 receipts = 10 minutes (6 seconds/receipt)

Recommended Settings:
- Check interval: 120 seconds (2 minutes)
- Batch size: 10 files
- Max batch time: 300 seconds (5 minutes)

This means:
✅ Light load (10 files): Processes in ~60s, ready for next batch
✅ Heavy load (20+ files): Processes in batches, may queue but won't crash
⚠️ Very heavy load (100+ files): Will fall behind but keep processing

RUNNING THE COMPLETE PIPELINE:

Terminal 1: python realtime_detector.py
Terminal 2: python ocr_processor.py  
Terminal 3: python realtime_classifier.py
Terminal 4: python realtime_downloader.py
Terminal 5: python realtime_ocr.py

FOLDER STRUCTURE:
data/
├── downloaded_receipts/     ← Input (images)
└── receipt_checked/         ← Output (JSON with OCR text)

FEATURES:
✅ Smart batching for performance
✅ Time limits to prevent hanging
✅ Performance monitoring
✅ Chinese + English OCR support
✅ Real-time processing stats
✅ Graceful handling of heavy loads
✅ Session-only tracking
"""