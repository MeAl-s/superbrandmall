"""
app/services/timezone_converter/timezone_converter.py
Fixed Timezone Converter with proper date folder monitoring
"""

import json
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
import logging
from typing import Dict, Any, Tuple, Set

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
except ImportError:
    os.system("pip install watchdog")
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler

class DateFolderHandler(FileSystemEventHandler):
    """Specialized handler for monitoring date-specific folders"""
    
    def __init__(self, converter):
        self.converter = converter
        self.processed_files: Set[str] = set()
        self.logger = logging.getLogger(f"{__name__}.DateFolderHandler")
    
    def on_created(self, event):
        """Handle new file creation"""
        if not event.is_directory and event.src_path.endswith('.json'):
            self._process_file_safely(event.src_path)
    
    def on_modified(self, event):
        """Handle file modification"""
        if not event.is_directory and event.src_path.endswith('.json'):
            self._process_file_safely(event.src_path)
    
    def _process_file_safely(self, file_path: str):
        """Process file with duplicate prevention"""
        try:
            # Create unique key to prevent duplicate processing
            if os.path.exists(file_path):
                file_stat = os.stat(file_path)
                file_key = f"{file_path}_{file_stat.st_mtime}_{file_stat.st_size}"
                
                if file_key in self.processed_files:
                    return  # Already processed
                
                self.processed_files.add(file_key)
                
                # Small delay to ensure file is fully written
                time.sleep(0.2)
                
                if os.path.exists(file_path):
                    self.logger.info(f"🔍 Real-time processing: {Path(file_path).name}")
                    success = self.converter.convert_file(file_path)
                    if success:
                        # Remove original file after successful conversion
                        try:
                            os.remove(file_path)
                            self.logger.info(f"🗑️  Removed original: {Path(file_path).name}")
                        except Exception as e:
                            self.logger.warning(f"Could not remove original file: {e}")
                
        except Exception as e:
            self.logger.error(f"Error in real-time processing: {e}")


class TimezoneConverter:
    """
    Fixed Timezone Converter with proper date folder monitoring
    """
    
    def __init__(self, watch_dir: str = None, output_dir: str = None):
        # Setup directories
        if os.name == 'nt':  # Windows
            project_root = Path(__file__).parent.parent.parent.parent
            self.watch_dir = Path(watch_dir) if watch_dir else project_root / "worker" / "data" / "matched_non_delivery"
            self.output_dir = Path(output_dir) if output_dir else project_root / "worker" / "data" / "converted_tz"
        else:  # Docker/Linux
            self.watch_dir = Path(watch_dir) if watch_dir else Path("/app/worker/data/matched_non_delivery")
            self.output_dir = Path(output_dir) if output_dir else Path("/app/worker/data/converted_tz")
        
        # Stats
        self.processed_count = 0
        self.error_count = 0
        
        # Observer components
        self.observer = None
        self.running = False
        self.monitored_folders = set()
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Create directories
        self._setup_directories()
        
        self.logger.info(f"TimezoneConverter initialized")
        self.logger.info(f"Watch: {self.watch_dir}")
        self.logger.info(f"Output: {self.output_dir}")
    
    def _setup_directories(self):
        """Create directories"""
        self.watch_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_today_folder(self) -> Path:
        """Get today's date folder path"""
        today = datetime.now().strftime("%Y-%m-%d")
        return self.watch_dir / today
    
    def _get_date_folders(self) -> list:
        """Get all existing date folders"""
        date_folders = []
        if not self.watch_dir.exists():
            return date_folders
            
        for item in self.watch_dir.iterdir():
            if item.is_dir():
                try:
                    datetime.strptime(item.name, "%Y-%m-%d")
                    date_folders.append(item)
                except ValueError:
                    continue
        
        return sorted(date_folders)
    
    def convert_datetime(self, dt_string: str) -> Tuple[str, str]:
        """Convert datetime string from UTC+8 to UTC+0"""
        try:
            # Parse the datetime string
            dt = datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S")
            
            # Convert from UTC+8 to UTC+0 (subtract 8 hours)
            converted_dt = dt - timedelta(hours=8)
            
            converted_time = converted_dt.strftime("%Y-%m-%d %H:%M:%S")
            converted_date = converted_dt.strftime("%Y-%m-%d")
            
            return converted_time, converted_date
            
        except Exception as e:
            self.logger.error(f"Error converting datetime '{dt_string}': {e}")
            return dt_string, datetime.now().strftime("%Y-%m-%d")
    
    def convert_file(self, file_path: str) -> bool:
        """Convert a single JSON file"""
        try:
            file_path_obj = Path(file_path)
            
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if 'print_time' not in data or not data['print_time'] or str(data['print_time']).lower() == 'unknown':
                self.logger.warning(f"No valid 'print_time' in {file_path_obj.name}")
                self.error_count += 1
                return False
            
            original_time = data['print_time']
            converted_time, converted_date = self.convert_datetime(original_time)
            
            # Update the data
            data['print_time'] = converted_time
            data['original_print_time'] = original_time
            data['timezone_conversion'] = "UTC+8 -> UTC+0"
            
            # Create output date folder
            date_folder = self.output_dir / converted_date
            date_folder.mkdir(parents=True, exist_ok=True)
            
            # Handle duplicate filenames
            output_path = date_folder / file_path_obj.name
            if output_path.exists():
                timestamp = datetime.now().strftime("%H%M%S%f")[:9]
                stem = file_path_obj.stem
                suffix = file_path_obj.suffix
                output_path = date_folder / f"{stem}_tz_{timestamp}{suffix}"
            
            # Write converted file
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            self.processed_count += 1
            self.logger.info(f"✅ Converted: {file_path_obj.name} | {original_time} -> {converted_time} | Output: {converted_date}/")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error converting {Path(file_path).name}: {e}")
            self.error_count += 1
            return False
    
    def convert_directory(self, directory: str = None) -> Dict[str, int]:
        """Convert all JSON files in directory and subdirectories"""
        target_dir = Path(directory) if directory else self.watch_dir
        
        if not target_dir.exists():
            self.logger.error(f"Directory does not exist: {target_dir}")
            return {"total_files": 0, "processed": 0, "errors": 0}
        
        # Get all JSON files recursively
        json_files = list(target_dir.rglob("*.json"))
        
        self.logger.info(f"📊 Found {len(json_files)} JSON files in {target_dir}")
        
        # Show which date folders contain files
        date_folders_with_files = set()
        for file_path in json_files:
            parent_name = file_path.parent.name
            try:
                datetime.strptime(parent_name, "%Y-%m-%d")
                date_folders_with_files.add(parent_name)
            except ValueError:
                pass
        
        if date_folders_with_files:
            self.logger.info(f"📅 Date folders with files: {', '.join(sorted(date_folders_with_files))}")
        
        # Process files
        initial_processed = self.processed_count
        for file_path in json_files:
            success = self.convert_file(str(file_path))
            if success:
                # Remove original file
                try:
                    file_path.unlink()
                except Exception as e:
                    self.logger.warning(f"Could not remove original {file_path.name}: {e}")
        
        processed_this_run = self.processed_count - initial_processed
        
        return {
            "total_files": len(json_files),
            "processed": processed_this_run,
            "errors": self.error_count
        }
    
    def start_monitoring(self) -> None:
        """Start smart monitoring for date folders"""
        if self.running:
            self.logger.warning("⚠️  Already monitoring")
            return
        
        self.logger.info("🚀 Starting enhanced date folder monitoring...")
        
        # Check existing date folders
        date_folders = self._get_date_folders()
        today_folder = self._get_today_folder()
        
        if date_folders:
            self.logger.info(f"📅 Found existing date folders: {[f.name for f in date_folders]}")
        
        # Ensure today's folder exists
        if not today_folder.exists():
            self.logger.info(f"📁 Creating today's folder: {today_folder.name}")
            today_folder.mkdir(parents=True, exist_ok=True)
        
        # Setup observer with multiple watch points
        self.observer = Observer()
        handler = DateFolderHandler(self)
        
        # Watch the main directory (for new date folders)
        self.observer.schedule(handler, str(self.watch_dir), recursive=False)
        self.logger.info(f"👁️  Watching main directory: {self.watch_dir}")
        
        # Watch existing date folders
        for date_folder in date_folders:
            self.observer.schedule(handler, str(date_folder), recursive=False)
            self.monitored_folders.add(str(date_folder))
            self.logger.info(f"👁️  Watching date folder: {date_folder.name}")
        
        # Start observer
        try:
            self.observer.start()
            self.running = True
            self.logger.info("✅ Real-time monitoring active!")
            self.logger.info(f"🎯 Primary target: {today_folder.name}")
        except Exception as e:
            self.logger.error(f"❌ Failed to start monitoring: {e}")
            return
    
    def stop_monitoring(self) -> None:
        """Stop monitoring"""
        if self.observer and self.running:
            self.logger.info("🛑 Stopping monitoring...")
            self.observer.stop()
            self.observer.join(timeout=5)
            self.running = False
            self.monitored_folders.clear()
            self.logger.info("✅ Monitoring stopped")
    
    def check_monitoring_status(self) -> Dict[str, Any]:
        """Check what is currently being monitored"""
        today_folder = self._get_today_folder()
        
        status = {
            "monitoring": self.running,
            "today_folder": today_folder.name,
            "today_folder_exists": today_folder.exists(),
            "monitored_folders": list(self.monitored_folders),
            "files_in_today": len(list(today_folder.glob("*.json"))) if today_folder.exists() else 0
        }
        
        if today_folder.exists():
            status["today_files"] = [f.name for f in today_folder.glob("*.json")][:5]  # Show first 5
        
        return status
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive stats"""
        return {
            "processed_files": self.processed_count,
            "error_files": self.error_count,
            "monitoring": self.running,
            "watch_directory": str(self.watch_dir),
            "output_directory": str(self.output_dir),
            "monitored_folders": len(self.monitored_folders),
            "today_folder": self._get_today_folder().name
        }