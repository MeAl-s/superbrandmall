# app/services/ocr_downloader/processing_service.py - OCR downloader processing logic
from datetime import datetime
from typing import Set, Dict, Any, List, Tuple, Optional
from pathlib import Path

class ProcessingService:
    """Handles OCR downloader processing logic - extracted from your RealtimeFileDownloader class"""
    
    def __init__(self):
        # Exact same tracking as your original class
        self.processed_files: Set[str] = set()  # Track files we've processed (session only)
        self.download_stats = {
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_processed": 0,
            "successful_downloads": 0,
            "already_existed": 0,
            "failed_downloads": 0,
            "no_url_found": 0
        }
    
    def process_new_files(self, files: List[Path], file_service, api_service) -> None:
        """Process and download new files - exact logic from your process_new_files"""
        if not files:
            return
        
        print(f"\n🎯 Processing {len(files)} new files for download...")
        
        for i, file_path in enumerate(files, 1):
            print(f"\n📄 [{i}/{len(files)}] Processing: {file_path.name}")
            
            # Extract URL from file
            url = file_service.extract_url_from_file(file_path)
            
            if not url:
                print(f"    ❌ No URL found in {file_path.name}")
                self.download_stats["no_url_found"] += 1
            else:
                print(f"    🔗 Found URL: {url[:60]}...")
                
                # Download the file
                success, downloaded_path, error = self._download_single_file(
                    url, file_path.name, file_service, api_service
                )
                
                if success:
                    if error == "File already exists":
                        print(f"    ⏭️ File already exists: {Path(downloaded_path).name}")
                        self.download_stats["already_existed"] += 1
                    else:
                        print(f"    ✅ Downloaded: {Path(downloaded_path).name}")
                        self.download_stats["successful_downloads"] += 1
                else:
                    print(f"    ❌ Download failed: {error}")
                    self.download_stats["failed_downloads"] += 1
            
            self.download_stats["total_processed"] += 1
        
        self.print_batch_summary()
    
    def _download_single_file(self, url: str, source_filename: str, file_service, api_service) -> Tuple[bool, Optional[str], Optional[str]]:
        """Download a single file - combines your original download logic"""
        try:
            # Detect content type first
            content_type = api_service.detect_content_type(url)
            
            # Prepare filename and path
            filename, dest_path = file_service.prepare_download_filename(source_filename, content_type, url)
            
            # Check if file already exists
            if file_service.file_already_exists(dest_path):
                return True, str(dest_path), "File already exists"
            
            # Download file content
            response, actual_content_type = api_service.download_file_content(url)
            
            if not response:
                return False, None, "Failed to download content"
            
            # Update filename if content type changed
            if actual_content_type != content_type:
                filename, dest_path = file_service.prepare_download_filename(source_filename, actual_content_type, url)
                
                # Check again if file exists with new name
                if file_service.file_already_exists(dest_path):
                    return True, str(dest_path), "File already exists"
            
            # Save downloaded content
            if file_service.save_downloaded_content(dest_path, response):
                return True, str(dest_path), None
            else:
                return False, None, "Failed to save file"
                
        except Exception as e:
            return False, None, str(e)
    
    def print_batch_summary(self):
        """Print batch download summary - exact format from your original"""
        print(f"\n📊 Batch Download Summary:")
        print(f"    ✅ Downloaded: {self.download_stats['successful_downloads']}")
        print(f"    ⏭️ Already existed: {self.download_stats['already_existed']}")
        print(f"    ❌ Failed: {self.download_stats['failed_downloads']}")
        print(f"    🔗 No URL: {self.download_stats['no_url_found']}")
    
    def print_session_stats(self):
        """Print session statistics - exact format from your original"""
        print(f"\n📊 Session Stats:")
        print(f"    🕐 Running since: {self.download_stats['start_time']}")
        print(f"    🔄 Total processed: {self.download_stats['total_processed']}")
        print(f"    ✅ Downloaded: {self.download_stats['successful_downloads']}")
        print(f"    ⏭️ Already existed: {self.download_stats['already_existed']}")
        print(f"    ❌ Failed: {self.download_stats['failed_downloads']}")
        print(f"    🔗 No URL: {self.download_stats['no_url_found']}")
        print(f"    📚 Files tracked: {len(self.processed_files)}")
    
    def download_existing_files(self, file_service, api_service) -> Dict[str, int]:
        """Manually download all existing files - exact logic from your download_existing_files"""
        source_dir = file_service.get_source_directory()
        
        print(f"\n🔄 Downloading all existing files in {source_dir}")
        
        if not source_dir.exists():
            print(f"❌ Source directory {source_dir} does not exist!")
            return {"error": 1}
        
        success_count = 0
        already_existed = 0
        failed_count = 0
        no_url_count = 0
        total_count = 0
        
        # Get all files
        files = file_service.get_all_files_in_source()
        
        if not files:
            print("📭 No files found to download")
            return {"total": 0}
        
        print(f"📂 Found {len(files)} files to download")
        print("-" * 60)
        
        for i, file_path in enumerate(files, 1):
            total_count += 1
            print(f"\n📄 [{i}/{len(files)}] Processing: {file_path.name}")
            
            # Extract URL
            url = file_service.extract_url_from_file(file_path)
            
            if not url:
                print(f"    ❌ No URL found in {file_path.name}")
                no_url_count += 1
            else:
                print(f"    🔗 Found URL: {url[:60]}...")
                
                # Download file
                success, downloaded_path, error = self._download_single_file(
                    url, file_path.name, file_service, api_service
                )
                
                if success:
                    if error == "File already exists":
                        print(f"    ⏭️ File already exists: {Path(downloaded_path).name}")
                        already_existed += 1
                    else:
                        print(f"    ✅ Downloaded: {Path(downloaded_path).name}")
                        success_count += 1
                else:
                    print(f"    ❌ Download failed: {error}")
                    failed_count += 1
        
        # Print summary - exact format from your original
        print("\n" + "=" * 60)
        print("DOWNLOAD SUMMARY")
        print("=" * 60)
        print(f"✅ Successfully downloaded: {success_count}")
        print(f"⏭️ Already existed: {already_existed}")
        print(f"❌ Failed downloads: {failed_count}")
        print(f"🔗 No URL found: {no_url_count}")
        print(f"📊 Total files processed: {total_count}")
        print("=" * 60)
        
        return {
            "successful_downloads": success_count,
            "already_existed": already_existed,
            "failed_downloads": failed_count,
            "no_url_found": no_url_count,
            "total": total_count
        }
    
    def get_processed_files(self) -> Set[str]:
        """Get processed files set for file service"""
        return self.processed_files
    
    def get_download_stats(self) -> Dict[str, Any]:
        """Get current download statistics"""
        return {
            **self.download_stats,
            "files_tracked": len(self.processed_files)
        }