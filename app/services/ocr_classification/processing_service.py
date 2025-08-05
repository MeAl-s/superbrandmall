# app/services/ocr_classification/processing_service.py - OCR classification processing logic
from datetime import datetime
from typing import Set, Dict, Any, List
from pathlib import Path

class ProcessingService:
    """Handles OCR classification processing logic - extracted from your RealtimeFileClassifier class"""
    
    def __init__(self):
        # Exact same tracking as your original class
        self.processed_files: Set[str] = set()  # Track files we've seen (session only)
        self.classification_stats = {
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_processed": 0,
            "files_with_urls": 0,
            "files_without_urls": 0,
            "error_files": 0
        }
    
    def process_new_files(self, files: List[Path], file_service) -> None:
        """Process and classify new files - exact logic from your process_new_files"""
        if not files:
            return
        
        print(f"\n🎯 Processing {len(files)} new files for classification...")
        
        for i, file_path in enumerate(files, 1):
            print(f"\n📄 [{i}/{len(files)}] Processing: {file_path.name}")
            
            # Classify the file
            success, destination_type, error = file_service.classify_single_file(file_path)
            
            if success:
                if destination_type == "receipt_ocring":
                    print(f"    ✅ → receipt_ocring (contains hddc01 URL)")
                    self.classification_stats["files_with_urls"] += 1
                else:
                    print(f"    📋 → receipt_checked (no hddc01 URL)")
                    self.classification_stats["files_without_urls"] += 1
            else:
                print(f"    ❌ Error: {error}")
                self.classification_stats["error_files"] += 1
            
            self.classification_stats["total_processed"] += 1
        
        self.print_batch_summary()
    
    def print_batch_summary(self):
        """Print batch classification summary - exact format from your original"""
        print(f"\n📊 Batch Classification Summary:")
        print(f"    ✅ With URLs: {self.classification_stats['files_with_urls']}")
        print(f"    📋 Without URLs: {self.classification_stats['files_without_urls']}")
        print(f"    ❌ Errors: {self.classification_stats['error_files']}")
    
    def print_session_stats(self):
        """Print session statistics - exact format from your original"""
        print(f"\n📊 Session Stats:")
        print(f"    🕐 Running since: {self.classification_stats['start_time']}")
        print(f"    🔄 Total processed: {self.classification_stats['total_processed']}")
        print(f"    ✅ With URLs: {self.classification_stats['files_with_urls']}")
        print(f"    📋 Without URLs: {self.classification_stats['files_without_urls']}")
        print(f"    ❌ Errors: {self.classification_stats['error_files']}")
        print(f"    📚 Files tracked: {len(self.processed_files)}")
    
    def classify_existing_files(self, file_service) -> Dict[str, int]:
        """Manually classify all existing files - exact logic from your classify_existing_files"""
        source_dir = file_service.get_source_directory()
        
        print(f"\n🔄 Classifying all existing files in {source_dir}")
        
        if not source_dir.exists():
            print(f"❌ Source directory {source_dir} does not exist!")
            return {"error": 1}
        
        files_with_url = 0
        files_without_url = 0
        error_files = 0
        
        # Get all files
        files = file_service.get_all_files_in_source()
        
        if not files:
            print("📭 No files found to classify")
            return {"total": 0}
        
        print(f"📂 Found {len(files)} files to classify")
        print("-" * 60)
        
        for i, file_path in enumerate(files, 1):
            print(f"\n📄 [{i}/{len(files)}] Processing: {file_path.name}")
            
            success, destination_type, error = file_service.classify_single_file(file_path)
            
            if success:
                if destination_type == "receipt_ocring":
                    print(f"    ✅ → receipt_ocring (contains hddc01 URL)")
                    files_with_url += 1
                else:
                    print(f"    📋 → receipt_checked (no hddc01 URL)")
                    files_without_url += 1
            else:
                print(f"    ❌ Error: {error}")
                error_files += 1
        
        # Print summary - exact format from your original
        print("\n" + "=" * 60)
        print("CLASSIFICATION SUMMARY")
        print("=" * 60)
        print(f"✅ Files with hddc01 URLs → receipt_ocring: {files_with_url}")
        print(f"📋 Files without hddc01 URLs → receipt_checked: {files_without_url}")
        print(f"❌ Files with errors: {error_files}")
        print(f"📊 Total files processed: {files_with_url + files_without_url + error_files}")
        print("=" * 60)
        
        return {
            "files_with_urls": files_with_url,
            "files_without_urls": files_without_url,
            "error_files": error_files,
            "total": files_with_url + files_without_url + error_files
        }
    
    def get_processed_files(self) -> Set[str]:
        """Get processed files set for file service"""
        return self.processed_files
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get current processing statistics"""
        return {
            **self.classification_stats,
            "files_tracked": len(self.processed_files)
        }