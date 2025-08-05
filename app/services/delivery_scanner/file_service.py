# app/services/delivery_scanner/file_service.py - Enhanced with dual directory monitoring
import sys
from pathlib import Path
import json
import shutil
from datetime import datetime
from typing import List, Optional, Set, Tuple, Dict

# Add project root to path for imports
current_file = Path(__file__)  # file_service.py
services_dir = current_file.parent.parent  # services/
app_dir = services_dir.parent  # app/
project_root = app_dir.parent  # project root

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(app_dir))

from config.settings import settings

class FileService:
    """Enhanced FileService with dual directory monitoring for delivery scanning"""
    
    def __init__(self):
        # Base directories - Monitor BOTH receipt_checked AND receipt_ocr_text
        self.primary_source_dir = settings.WORKER_DIR / "data" / "receipt_checked"
        self.secondary_source_dir = settings.WORKER_DIR / "data" / "receipt_ocr_text"  # NEW SOURCE
        self.delivery_dir = settings.WORKER_DIR / "data" / "delivery_found"
        self.non_delivery_dir = settings.WORKER_DIR / "data" / "non_delivery"
        self.today = datetime.now().strftime("%Y-%m-%d")
        
        # Today's specific directories
        self.today_primary_source = self.primary_source_dir / self.today     # receipt_checked/2025-07-30/
        self.today_secondary_source = self.secondary_source_dir / self.today  # receipt_ocr_text/2025-07-30/
        self.today_delivery_dir = self.delivery_dir / self.today
        self.today_non_delivery_dir = self.non_delivery_dir / self.today
        
        # Backward compatibility - keep old attributes
        self.source_dir = self.primary_source_dir  # For backward compatibility
        self.today_source_dir = self.today_primary_source  # For backward compatibility
        
        # Track which directory each file came from
        self.file_source_map: Dict[str, str] = {}  # filename -> source_type
        
        self._setup_folders()
    
    def _setup_folders(self):
        """Create delivery scanning folders with dual source support"""
        # Create base directories
        self.delivery_dir.mkdir(parents=True, exist_ok=True)
        self.non_delivery_dir.mkdir(parents=True, exist_ok=True)
        
        # Create today's directories
        self.today_delivery_dir.mkdir(parents=True, exist_ok=True)
        self.today_non_delivery_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"📁 Enhanced delivery scanning folders ready:")
        print(f"   📂 Primary Source: {self.today_primary_source}")
        print(f"   📂 Secondary Source: {self.today_secondary_source}")
        print(f"   🚚 Delivery: {self.today_delivery_dir}")
        print(f"   📋 Non-delivery: {self.today_non_delivery_dir}")
    
    def _is_date_folder(self, folder_name: str) -> bool:
        """Check if folder name matches YYYY-MM-DD pattern"""
        try:
            datetime.strptime(folder_name, "%Y-%m-%d")
            return True
        except ValueError:
            return False
    
    def scan_for_new_files(self, processed_files: Set[str]) -> List[Path]:
        """Enhanced scan for new files from BOTH directories with date detection"""
        new_files = []
        
        # Scan primary source (receipt_checked)
        primary_files = self._scan_directory_with_dates(
            self.primary_source_dir, processed_files, "primary"
        )
        new_files.extend(primary_files)
        
        # Scan secondary source (receipt_ocr_text)
        secondary_files = self._scan_directory_with_dates(
            self.secondary_source_dir, processed_files, "secondary"
        )
        new_files.extend(secondary_files)
        
        if new_files:
            print(f"📊 Found {len(new_files)} new files:")
            primary_count = len([f for f in new_files if self.file_source_map.get(f.name) == "primary"])
            secondary_count = len([f for f in new_files if self.file_source_map.get(f.name) == "secondary"])
            if primary_count > 0:
                print(f"   📂 Primary (receipt_checked): {primary_count} files")
            if secondary_count > 0:
                print(f"   📂 Secondary (receipt_ocr_text): {secondary_count} files")
        
        return new_files
    
    def _scan_directory_with_dates(self, base_dir: Path, processed_files: Set[str], source_type: str) -> List[Path]:
        """Scan directory and its date subdirectories for new files"""
        new_files = []
        
        if not base_dir.exists():
            return new_files
        
        # Check today's folder first (priority)
        today_dir = base_dir / self.today
        if today_dir.exists():
            files = self._scan_single_directory(today_dir, processed_files, source_type)
            new_files.extend(files)
        
        # Check for other date folders
        for item in base_dir.iterdir():
            if item.is_dir() and item.name != self.today and self._is_date_folder(item.name):
                files = self._scan_single_directory(item, processed_files, source_type)
                if files:
                    print(f"📅 Found {len(files)} files in {source_type} date folder: {item.name}")
                    new_files.extend(files)
        
        return new_files
    
    def _scan_single_directory(self, directory: Path, processed_files: Set[str], source_type: str) -> List[Path]:
        """Scan a single directory for new files"""
        new_files = []
        
        for file_path in directory.iterdir():
            if file_path.is_file():
                file_key = f"{file_path.name}_{file_path.stat().st_mtime}_{source_type}"
                
                if file_key not in processed_files:
                    new_files.append(file_path)
                    processed_files.add(file_key)
                    # Track which source this file came from
                    self.file_source_map[file_path.name] = source_type
        
        return new_files
    
    def read_file(self, filepath: Path) -> str:
        """Read file and extract text content - supports both JSON and text files"""
        try:
            # Read JSON files
            if filepath.suffix.lower() == '.json':
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, dict) and 'data' in data:
                        return data['data']
                    elif isinstance(data, dict) and 'text' in data:
                        return data['text']
                    elif isinstance(data, dict) and 'content' in data:
                        return data['content']
                    return str(data)
            
            # Read text files with UTF-8
            with open(filepath, 'r', encoding='utf-8') as f:
                return f.read()
                
        except Exception as e:
            print(f"    ❌ Error reading file {filepath.name}: {e}")
            return ""
    
    def move_to_delivery(self, file_path: Path) -> Tuple[bool, str]:
        """Move file to delivery folder and clean up source"""
        try:
            target_file = self.today_delivery_dir / file_path.name
            
            # Handle duplicates
            if target_file.exists():
                timestamp = datetime.now().strftime("%H%M%S%f")[:9]
                stem = file_path.stem
                suffix = file_path.suffix
                target_file = self.today_delivery_dir / f"{stem}_delivery_{timestamp}{suffix}"
            
            # Copy file first
            shutil.copy2(str(file_path), str(target_file))
            
            # Remove from original location and cleanup
            self._cleanup_source_file(file_path)
            
            return True, str(target_file)
        except Exception as e:
            return False, f"Error moving to delivery folder: {e}"
    
    def move_to_non_delivery(self, file_path: Path) -> Tuple[bool, str]:
        """Move file to non-delivery folder and clean up source"""
        try:
            target_file = self.today_non_delivery_dir / file_path.name
            
            # Handle duplicates
            if target_file.exists():
                timestamp = datetime.now().strftime("%H%M%S%f")[:9]
                stem = file_path.stem
                suffix = file_path.suffix
                target_file = self.today_non_delivery_dir / f"{stem}_nondelivery_{timestamp}{suffix}"
            
            # Copy file first
            shutil.copy2(str(file_path), str(target_file))
            
            # Remove from original location and cleanup
            self._cleanup_source_file(file_path)
            
            return True, str(target_file)
        except Exception as e:
            return False, f"Error moving to non-delivery folder: {e}"
    
    def _cleanup_source_file(self, file_path: Path):
        """Remove file from source directory after processing"""
        try:
            if file_path.exists():
                file_path.unlink()
                print(f"    🗑️  Removed from source: {file_path.name}")
                
                # Also check if there's a corresponding file in the other source
                source_type = self.file_source_map.get(file_path.name)
                if source_type == "primary":
                    # Check secondary source for same file
                    secondary_file = self.today_secondary_source / file_path.name
                    if secondary_file.exists():
                        secondary_file.unlink()
                        print(f"    🗑️  Also removed from secondary source: {file_path.name}")
                elif source_type == "secondary":
                    # Check primary source for same file
                    primary_file = self.today_primary_source / file_path.name
                    if primary_file.exists():
                        primary_file.unlink()
                        print(f"    🗑️  Also removed from primary source: {file_path.name}")
                        
        except Exception as e:
            print(f"    ⚠️  Could not remove source file {file_path.name}: {e}")
    
    def log_delivery_detection(self, filename: str, keywords: List[str], text_preview: str, source_type: str = None):
        """Log delivery detection details with enhanced source information"""
        log_file = self.today_delivery_dir / "delivery_detection_log.txt"
        
        try:
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(f"\n{'='*50}\n")
                f.write(f"Detection Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"File: {filename}\n")
                if source_type:
                    source_name = "receipt_checked" if source_type == "primary" else "receipt_ocr_text"
                    f.write(f"Source Directory: {source_name}\n")
                f.write(f"Keywords Found: {', '.join(keywords)}\n")
                f.write(f"Text Preview: {text_preview[:200]}...\n")
                f.write(f"{'='*50}\n")
                
        except Exception as e:
            print(f"    ⚠️ Could not write to log file: {e}")
    
    def get_all_files_in_sources(self) -> List[Path]:
        """Get all files from BOTH source directories"""
        all_files = []
        
        # Get files from primary source (receipt_checked)
        if self.today_primary_source.exists():
            primary_files = [f for f in self.today_primary_source.iterdir() if f.is_file()]
            all_files.extend(primary_files)
            for f in primary_files:
                self.file_source_map[f.name] = "primary"
        
        # Get files from secondary source (receipt_ocr_text)
        if self.today_secondary_source.exists():
            secondary_files = [f for f in self.today_secondary_source.iterdir() if f.is_file()]
            all_files.extend(secondary_files)
            for f in secondary_files:
                self.file_source_map[f.name] = "secondary"
        
        # Also check other date folders
        for base_dir, source_type in [(self.primary_source_dir, "primary"), (self.secondary_source_dir, "secondary")]:
            if base_dir.exists():
                for date_folder in base_dir.iterdir():
                    if date_folder.is_dir() and date_folder.name != self.today and self._is_date_folder(date_folder.name):
                        date_files = [f for f in date_folder.iterdir() if f.is_file()]
                        all_files.extend(date_files)
                        for f in date_files:
                            self.file_source_map[f.name] = source_type
        
        return all_files
    
    def get_all_files_in_source(self) -> List[Path]:
        """Backward compatibility method - returns files from both sources"""
        return self.get_all_files_in_sources()
    
    def get_delivery_summary(self) -> dict:
        """Get enhanced summary with dual source information"""
        delivery_count = len(list(self.today_delivery_dir.glob("*.json"))) if self.today_delivery_dir.exists() else 0
        non_delivery_count = len(list(self.today_non_delivery_dir.glob("*.json"))) if self.today_non_delivery_dir.exists() else 0
        
        # Count files in both sources
        primary_count = len([f for f in self.today_primary_source.iterdir() if f.is_file()]) if self.today_primary_source.exists() else 0
        secondary_count = len([f for f in self.today_secondary_source.iterdir() if f.is_file()]) if self.today_secondary_source.exists() else 0
        source_count = primary_count + secondary_count
        
        # Check for log file
        log_file = self.today_delivery_dir / "delivery_detection_log.txt"
        has_log = log_file.exists()
        
        return {
            "date": self.today,
            "primary_source_files": primary_count,
            "secondary_source_files": secondary_count,
            "source_files": source_count,  # Total for backward compatibility
            "delivery_found": delivery_count,
            "non_delivery": non_delivery_count,
            "total_processed": delivery_count + non_delivery_count,
            "remaining": max(0, source_count - (delivery_count + non_delivery_count)),
            "delivery_dir": str(self.today_delivery_dir),
            "non_delivery_dir": str(self.today_non_delivery_dir),
            "primary_source_dir": str(self.today_primary_source),
            "secondary_source_dir": str(self.today_secondary_source),
            "log_file": str(log_file) if has_log else None
        }
    
    def get_source_directory(self) -> Path:
        """Get primary source directory (for backward compatibility)"""
        return self.today_primary_source
    
    def get_delivery_directory(self) -> Path:
        """Get today's delivery directory"""
        return self.today_delivery_dir
    
    def get_non_delivery_directory(self) -> Path:
        """Get today's non_delivery directory"""
        return self.today_non_delivery_dir