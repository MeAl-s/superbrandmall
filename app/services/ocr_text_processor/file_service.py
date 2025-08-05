# app/services/ocr_text_processor/file_service.py - Enhanced OCR text processor with date detection
import sys
from pathlib import Path
import json
from datetime import datetime
from typing import List, Optional, Set, Dict

# Add project root to path for imports
current_file = Path(__file__)  # file_service.py
services_dir = current_file.parent.parent  # services/
app_dir = services_dir.parent  # app/
project_root = app_dir.parent  # project root

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(app_dir))

from config.settings import settings

class FileService:
    """Enhanced OCR text processor file service with date folder detection"""
    
    def __init__(self):
        # Base directories - ALL inside worker/data/
        self.source_dir = settings.WORKER_DIR / "data" / "downloaded_receipts"  # worker/data/downloaded_receipts/
        self.output_dir = settings.WORKER_DIR / "data" / "receipt_ocr_text"     # worker/data/receipt_ocr_text/
        self.today = datetime.now().strftime("%Y-%m-%d")
        
        # Today's specific directories
        self.today_source_dir = self.source_dir / self.today  # worker/data/downloaded_receipts/2025-07-30/
        self.today_output_dir = self.output_dir / self.today  # worker/data/receipt_ocr_text/2025-07-30/
        
        # Supported image formats - exactly like your original
        self.supported_formats = {'.bmp', '.png', '.jpg', '.jpeg', '.tiff', '.tif', '.webp', '.gif'}
        
        # Track processed files by date folder
        self.file_date_map: Dict[str, str] = {}  # filename -> date_folder
        
        self._setup_folders()
    
    def _setup_folders(self):
        """Create OCR folders with enhanced date detection"""
        # Create base directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create today's directories
        self.today_output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"📁 Enhanced OCR folders ready:")
        print(f"   📂 Source: {self.today_source_dir}")
        print(f"   📝 Output: {self.today_output_dir}")
        
        # Check for other date folders
        if self.source_dir.exists():
            date_folders = self._get_date_folders()
            if date_folders:
                other_dates = [d.name for d in date_folders if d.name != self.today]
                if other_dates:
                    print(f"   📅 Other date folders found: {', '.join(other_dates)}")
    
    def _get_date_folders(self) -> List[Path]:
        """Get all date folders in source directory"""
        date_folders = []
        if not self.source_dir.exists():
            return date_folders
        
        for item in self.source_dir.iterdir():
            if item.is_dir() and self._is_date_folder(item.name):
                date_folders.append(item)
        
        return sorted(date_folders)
    
    def _is_date_folder(self, folder_name: str) -> bool:
        """Check if folder name matches YYYY-MM-DD pattern"""
        try:
            datetime.strptime(folder_name, "%Y-%m-%d")
            return True
        except ValueError:
            return False
    
    def scan_for_new_files(self, processed_files: Set[str]) -> List[Path]:
        """Enhanced scan with date folder detection"""
        new_files = []
        
        # Scan today's folder first (priority)
        if self.today_source_dir.exists():
            today_files = self._scan_single_directory(
                self.today_source_dir, processed_files, self.today
            )
            new_files.extend(today_files)
        
        # Scan other date folders for any missed files
        date_folders = self._get_date_folders()
        for date_folder in date_folders:
            if date_folder.name != self.today:  # Skip today's folder (already scanned)
                folder_files = self._scan_single_directory(
                    date_folder, processed_files, date_folder.name
                )
                if folder_files:
                    print(f"📅 Found {len(folder_files)} files in date folder: {date_folder.name}")
                    new_files.extend(folder_files)
        
        if new_files:
            print(f"📊 Total new files found: {len(new_files)}")
            # Group by date for summary
            date_counts = {}
            for file_path in new_files:
                date_folder = self.file_date_map.get(file_path.name, "unknown")
                date_counts[date_folder] = date_counts.get(date_folder, 0) + 1
            
            for date_folder, count in date_counts.items():
                print(f"   📅 {date_folder}: {count} files")
        
        return new_files
    
    def _scan_single_directory(self, directory: Path, processed_files: Set[str], date_folder: str) -> List[Path]:
        """Scan a single directory for new image files"""
        new_files = []
        
        for file_path in directory.iterdir():
            if (file_path.is_file() and 
                file_path.suffix.lower() in self.supported_formats):
                
                file_key = f"{file_path.name}_{file_path.stat().st_mtime}_{date_folder}"
                
                if file_key not in processed_files:
                    new_files.append(file_path)
                    processed_files.add(file_key)
                    # Track which date folder this file belongs to
                    self.file_date_map[file_path.name] = date_folder
        
        return new_files
    
    def output_file_exists(self, image_path: Path) -> bool:
        """Check if output JSON already exists with date-aware logic"""
        # Determine which date folder this file should go to
        date_folder = self.file_date_map.get(image_path.name, self.today)
        output_dir = self.output_dir / date_folder
        
        output_filename = image_path.stem + '.json'
        output_path = output_dir / output_filename
        return output_path.exists()
    
    def save_ocr_result(self, image_path: Path, ocr_text: str, confidence: float, 
                       language: str, processing_time: float) -> bool:
        """Save OCR result with date-aware organization"""
        try:
            # Determine output date folder
            date_folder = self.file_date_map.get(image_path.name, self.today)
            output_date_dir = self.output_dir / date_folder
            
            # Ensure output directory exists
            output_date_dir.mkdir(parents=True, exist_ok=True)
            
            # Create output filename
            output_filename = image_path.stem + '.json'
            output_path = output_date_dir / output_filename
            
            # Create enhanced output JSON
            output_data = {
                "data": ocr_text,
                "success": True,
                "message": None,
                "fields": None,
                "total": None,
                "ocr_metadata": {
                    "confidence": confidence,
                    "language": language,
                    "processing_time": processing_time,
                    "processed_at": datetime.now().isoformat(),
                    "source_file": image_path.name,
                    "source_date_folder": date_folder,
                    "output_date_folder": date_folder
                }
            }
            
            # Save JSON file
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)
            
            print(f"    ✅ Saved to: {date_folder}/{output_filename}")
            return True
            
        except Exception as e:
            print(f"❌ Error saving OCR result for {image_path.name}: {e}")
            return False
    
    def save_ocr_error(self, image_path: Path, error_message: str, processing_time: float) -> bool:
        """Save OCR error result with date-aware organization"""
        try:
            # Determine output date folder
            date_folder = self.file_date_map.get(image_path.name, self.today)
            output_date_dir = self.output_dir / date_folder
            
            # Ensure output directory exists
            output_date_dir.mkdir(parents=True, exist_ok=True)
            
            output_filename = image_path.stem + '.json'
            output_path = output_date_dir / output_filename
            
            output_data = {
                "data": "",
                "success": False,
                "message": error_message,
                "fields": None,
                "total": None,
                "ocr_metadata": {
                    "confidence": 0,
                    "language": "error",
                    "processing_time": processing_time,
                    "processed_at": datetime.now().isoformat(),
                    "source_file": image_path.name,
                    "source_date_folder": date_folder,
                    "output_date_folder": date_folder
                }
            }
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)
            
            print(f"    ❌ Error saved to: {date_folder}/{output_filename}")
            return True
            
        except Exception as e:
            print(f"❌ Error saving error result for {image_path.name}: {e}")
            return False
    
    def get_all_image_files(self) -> List[Path]:
        """Get all image files from all date folders"""
        all_files = []
        
        # Get files from today's folder
        if self.today_source_dir.exists():
            today_files = [f for f in self.today_source_dir.iterdir() 
                          if f.is_file() and f.suffix.lower() in self.supported_formats]
            all_files.extend(today_files)
            for f in today_files:
                self.file_date_map[f.name] = self.today
        
        # Get files from other date folders
        date_folders = self._get_date_folders()
        for date_folder in date_folders:
            if date_folder.name != self.today:
                date_files = [f for f in date_folder.iterdir() 
                             if f.is_file() and f.suffix.lower() in self.supported_formats]
                all_files.extend(date_files)
                for f in date_files:
                    self.file_date_map[f.name] = date_folder.name
        
        return all_files
    
    def get_ocr_summary(self) -> dict:
        """Get enhanced OCR summary with date folder breakdown"""
        # Count output files by date
        output_by_date = {}
        source_by_date = {}
        
        if self.output_dir.exists():
            for date_folder in self.output_dir.iterdir():
                if date_folder.is_dir() and self._is_date_folder(date_folder.name):
                    json_count = len(list(date_folder.glob("*.json")))
                    if json_count > 0:
                        output_by_date[date_folder.name] = json_count
        
        # Count source files by date
        if self.source_dir.exists():
            for date_folder in self.source_dir.iterdir():
                if date_folder.is_dir() and self._is_date_folder(date_folder.name):
                    image_files = [f for f in date_folder.iterdir() 
                                  if f.is_file() and f.suffix.lower() in self.supported_formats]
                    if image_files:
                        source_by_date[date_folder.name] = len(image_files)
        
        # Calculate totals
        total_output = sum(output_by_date.values())
        total_source = sum(source_by_date.values())
        
        return {
            "date": self.today,
            "images_available": total_source,
            "ocr_completed": total_output,
            "remaining": max(0, total_source - total_output),
            "output_dir": str(self.today_output_dir),
            "source_dir": str(self.today_source_dir),
            "by_date": {
                "source_files": source_by_date,
                "output_files": output_by_date
            },
            "date_folders_found": list(source_by_date.keys())
        }
    
    def get_source_directory(self) -> Path:
        """Get today's source directory"""
        return self.today_source_dir
    
    def get_output_directory(self) -> Path:
        """Get today's output directory"""
        return self.today_output_dir
    
    def cleanup_source_file(self, image_path: Path) -> bool:
        """Remove processed image file from source directory"""
        try:
            if image_path.exists():
                image_path.unlink()
                print(f"    🗑️  Removed source file: {image_path.name}")
                return True
            return False
        except Exception as e:
            print(f"    ⚠️  Could not remove source file {image_path.name}: {e}")
            return False