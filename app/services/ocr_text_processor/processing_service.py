# app/services/ocr_text_processor/processing_service.py - OCR text processing logic
import time
import logging
from datetime import datetime
from typing import Set, Dict, Any, List, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)

class ProcessingService:
    """Handles OCR text processing logic - extracted from your RealtimeOCRProcessor class"""
    
    def __init__(self):
        # Exact same tracking as your original class
        self.processed_files: Set[str] = set()  # Track files we've processed (session only)
        self.ocr_stats = {
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_processed": 0,
            "successful_ocr": 0,
            "failed_ocr": 0,
            "average_time_per_file": 0,
            "total_processing_time": 0
        }
        
        # OCR settings for better performance - exactly like your original
        self.batch_size = 10  # Process 10 files at a time
        self.max_processing_time = 300  # 5 minutes max per batch
    
    def process_single_file(self, image_path: Path, file_service, ocr_service) -> Tuple[bool, float]:
        """Process a single image file - exact logic from your process_single_file"""
        start_time = time.time()
        
        try:
            logger.info(f"Processing: {image_path.name}")
            
            # Check if output already exists
            if file_service.output_file_exists(image_path):
                logger.info(f"Output already exists: {image_path.stem}.json")
                return True, time.time() - start_time
            
            # Perform OCR
            ocr_text, confidence, language = ocr_service.process_image_ocr(image_path)
            
            if language in ["error", "preprocessing_error", "pipeline_error"]:
                # Save error result
                processing_time = time.time() - start_time
                file_service.save_ocr_error(image_path, f"OCR failed: {language}", processing_time)
                logger.error(f"Failed to process: {image_path.name} ({language})")
                return False, processing_time
            
            # Save successful result
            processing_time = time.time() - start_time
            success = file_service.save_ocr_result(image_path, ocr_text, confidence, language, processing_time)
            
            if success:
                logger.info(f"✓ Completed {image_path.name} in {processing_time:.2f}s (confidence: {confidence:.1f}%)")
                return True, processing_time
            else:
                return False, processing_time
            
        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Error processing {image_path.name}: {str(e)}")
            file_service.save_ocr_error(image_path, str(e), processing_time)
            return False, processing_time
    
    def process_batch(self, files: List[Path], file_service, ocr_service) -> None:
        """Process a batch of files with time limit - exact logic from your process_batch"""
        if not files:
            return
        
        batch_start = time.time()
        successful = 0
        failed = 0
        total_processing_time = 0
        
        print(f"\n🎯 Processing batch of {len(files)} files...")
        
        for i, file_path in enumerate(files, 1):
            # Check if we're running out of time - exactly like your original
            elapsed = time.time() - batch_start
            if elapsed > self.max_processing_time:
                logger.warning(f"Batch timeout reached after {elapsed:.1f}s, stopping at file {i}")
                break
            
            print(f"📄 [{i}/{len(files)}] {file_path.name}")
            
            success, proc_time = self.process_single_file(file_path, file_service, ocr_service)
            total_processing_time += proc_time
            
            if success:
                successful += 1
            else:
                failed += 1
            
            # Update stats - exactly like your original
            self.ocr_stats["total_processed"] += 1
            self.ocr_stats["total_processing_time"] += proc_time
            
            if self.ocr_stats["total_processed"] > 0:
                self.ocr_stats["average_time_per_file"] = (
                    self.ocr_stats["total_processing_time"] / 
                    self.ocr_stats["total_processed"]
                )
        
        batch_time = time.time() - batch_start
        
        # Update stats - exactly like your original
        self.ocr_stats["successful_ocr"] += successful
        self.ocr_stats["failed_ocr"] += failed
        
        self.print_batch_summary(successful, failed, batch_time, total_processing_time, len(files))
    
    def print_batch_summary(self, successful: int, failed: int, batch_time: float, 
                           total_processing_time: float, file_count: int):
        """Print batch summary - exact format from your original"""
        print(f"\n📊 Batch Summary:")
        print(f"    ✅ Successful: {successful}")
        print(f"    ❌ Failed: {failed}")
        print(f"    ⏱️ Batch time: {batch_time:.1f}s")
        print(f"    📈 Avg per file: {total_processing_time/file_count:.1f}s")
        
        # Performance warning - exactly like your original
        if total_processing_time > 60:
            print(f"    ⚠️ Batch took {total_processing_time:.1f}s - may fall behind in heavy load")
    
    def print_session_stats(self):
        """Print session statistics - exact format from your original"""
        print(f"\n📊 Session Stats:")
        print(f"    🕐 Running since: {self.ocr_stats['start_time']}")
        print(f"    🔄 Total processed: {self.ocr_stats['total_processed']}")
        print(f"    ✅ Successful: {self.ocr_stats['successful_ocr']}")
        print(f"    ❌ Failed: {self.ocr_stats['failed_ocr']}")
        print(f"    ⏱️ Avg time/file: {self.ocr_stats['average_time_per_file']:.1f}s")
        print(f"    📚 Files tracked: {len(self.processed_files)}")
        
        # Performance analysis - exactly like your original
        if self.ocr_stats["average_time_per_file"] > 0:
            files_per_minute = 60 / self.ocr_stats["average_time_per_file"]
            print(f"    🚀 Processing rate: {files_per_minute:.1f} files/minute")
    
    def process_existing_files(self, file_service, ocr_service) -> Dict[str, int]:
        """Process all existing files - exact logic from your process_existing_files"""
        source_dir = file_service.get_source_directory()
        
        print(f"\n🔄 Processing all existing images in {source_dir}")
        
        if not source_dir.exists():
            print(f"❌ Source directory {source_dir} does not exist!")
            return {"error": 1}
        
        # Get all image files
        files = file_service.get_all_image_files()
        
        if not files:
            print("📭 No image files found to process")
            return {"total": 0}
        
        print(f"📂 Found {len(files)} image files")
        
        # Process in batches - exactly like your original
        for i in range(0, len(files), self.batch_size):
            batch = files[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (len(files) + self.batch_size - 1) // self.batch_size
            
            print(f"\n📦 Processing batch {batch_num}/{total_batches}")
            self.process_batch(batch, file_service, ocr_service)
        
        print(f"\n✅ All files processed!")
        print(f"📊 Total: {self.ocr_stats['successful_ocr']} successful, {self.ocr_stats['failed_ocr']} failed")
        
        return {
            "successful_ocr": self.ocr_stats['successful_ocr'],
            "failed_ocr": self.ocr_stats['failed_ocr'],
            "total": self.ocr_stats['total_processed']
        }
    
    def get_processed_files(self) -> Set[str]:
        """Get processed files set for file service"""
        return self.processed_files
    
    def get_ocr_stats(self) -> Dict[str, Any]:
        """Get current OCR statistics"""
        return {
            **self.ocr_stats,
            "files_tracked": len(self.processed_files),
            "batch_size": self.batch_size,
            "max_processing_time": self.max_processing_time
        }