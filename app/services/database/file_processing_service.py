# app/services/database/file_processing_service.py
"""
File processing service for handling file operations and monitoring
Located in database services directory for organizational purposes
"""
import shutil
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set
from queue import Queue
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class FileMonitor(FileSystemEventHandler):
    """File system event handler for monitoring JSON files"""
    
    def __init__(self, file_service: 'FileProcessingService'):
        self.file_service = file_service
        self.logger = logging.getLogger(f"{__name__}.FileMonitor")
        
    def on_created(self, event) -> None:
        """Handle new file creation"""
        if not event.is_directory and self._is_json_file(event.src_path):
            file_path = Path(event.src_path)
            self.logger.info(f"New file detected: {file_path.name}")
            self.file_service.queue_file(file_path)
    
    def on_modified(self, event) -> None:
        """Handle file modification"""
        if not event.is_directory and self._is_json_file(event.src_path):
            file_path = Path(event.src_path)
            self.logger.info(f"File modified: {file_path.name}")
            self.file_service.queue_file(file_path)
    
    def on_moved(self, event) -> None:
        """Handle file moves"""
        if hasattr(event, 'dest_path') and not event.is_directory and self._is_json_file(event.dest_path):
            file_path = Path(event.dest_path)
            self.logger.info(f"File moved in: {file_path.name}")
            self.file_service.queue_file(file_path)
    
    def _is_json_file(self, file_path: str) -> bool:
        """Check if file is JSON"""
        return Path(file_path).suffix.lower() == '.json'


class FileProcessingService:
    """Service for handling file operations, monitoring, and processing"""
    
    def __init__(self, watch_dir: Optional[str] = None, processed_dir: Optional[str] = None):
        self.logger = logging.getLogger(f"{__name__}.FileProcessingService")
        
        # Setup paths
        self._setup_paths(watch_dir, processed_dir)
        
        # File tracking
        self.file_queue: Queue = Queue()
        self.processed_files: Set[str] = set()
        
        # Monitoring components
        self.observer: Optional[Observer] = None
        self.is_monitoring = False
        
        # Docker mode flag (no file movement if processed_dir is None)
        self.docker_mode = processed_dir is None
        if self.docker_mode:
            self.logger.info("Running in Docker mode - files will not be moved after processing")
    
    def _setup_paths(self, watch_dir: Optional[str], processed_dir: Optional[str]) -> None:
        """Setup watch and processed directories"""
        import os
        
        # Default paths based on environment
        if os.name == 'nt':  # Windows
            default_watch = Path(r"C:\Point Detection\worker\data\converted_tz")
            default_processed = Path(r"C:\Point Detection\worker\data\inserted_to_database")
        else:  # Docker/Linux
            default_watch = Path("/app/worker/data/converted_tz")
            default_processed = Path("/app/worker/data/inserted_to_database")
        
        self.watch_path = Path(watch_dir) if watch_dir else default_watch
        
        # In Docker mode, we don't need a processed directory
        if processed_dir:
            self.processed_path = Path(processed_dir)
            # Create processed directory if it doesn't exist
            self.processed_path.mkdir(parents=True, exist_ok=True)
        else:
            self.processed_path = None
    
    def validate_paths(self) -> bool:
        """Validate that required paths exist"""
        if not self.watch_path.exists():
            self.logger.error(f"Watch directory does not exist: {self.watch_path}")
            return False
        
        # Only validate processed path if not in Docker mode
        if not self.docker_mode and self.processed_path:
            if not self.processed_path.exists():
                try:
                    self.processed_path.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    self.logger.error(f"Cannot create processed directory {self.processed_path}: {e}")
                    return False
        
        return True
    
    def scan_existing_files(self) -> Dict[str, List[Path]]:
        """Scan for existing files organized by date folders"""
        self.logger.info("Scanning for existing files...")
        
        date_files = {}
        
        if not self.watch_path.exists():
            return date_files
        
        # Look for date folders (YYYY-MM-DD format)
        for item in self.watch_path.iterdir():
            if item.is_dir():
                try:
                    # Validate date format
                    datetime.strptime(item.name, '%Y-%m-%d')
                    json_files = list(item.glob("*.json"))
                    if json_files:
                        date_files[item.name] = json_files
                        self.logger.info(f"Found {len(json_files)} files in {item.name}")
                except ValueError:
                    # Not a valid date folder, skip
                    continue
        
        total_files = sum(len(files) for files in date_files.values())
        self.logger.info(f"Found {len(date_files)} date folders with {total_files} total files")
        
        return date_files
    
    def queue_file(self, file_path: Path) -> bool:
        """Add file to processing queue"""
        file_str = str(file_path)
        
        if file_str in self.processed_files:
            self.logger.debug(f"File already processed: {file_path.name}")
            return False
        
        if not file_path.exists():
            self.logger.warning(f"File does not exist: {file_path}")
            return False
        
        self.file_queue.put(file_path)
        self.processed_files.add(file_str)
        self.logger.info(f"Queued file: {file_path.name}")
        return True
    
    def get_queued_file(self, timeout: float = None) -> Optional[Path]:
        """Get next file from queue"""
        try:
            if timeout is not None:
                return self.file_queue.get(timeout=timeout)
            else:
                return self.file_queue.get_nowait()
        except:
            return None
    
    def get_queue_size(self) -> int:
        """Get current queue size"""
        return self.file_queue.qsize()
    
    def move_to_processed(self, file_path: Path, date_str: str) -> bool:
        """Move file to processed directory (Docker mode: just mark as processed)"""
        if self.docker_mode:
            # In Docker mode, don't move files - just mark as processed
            self.logger.info(f"Docker mode: File {file_path.name} processed (no movement)")
            return True
        
        try:
            # Create date-specific processed directory
            processed_date_dir = self.processed_path / date_str
            processed_date_dir.mkdir(parents=True, exist_ok=True)
            
            dest_path = processed_date_dir / file_path.name
            
            # Handle filename conflicts
            if dest_path.exists():
                timestamp = datetime.now().strftime("%H%M%S%f")[:9]
                stem = file_path.stem
                suffix = file_path.suffix
                dest_path = processed_date_dir / f"{stem}_{timestamp}{suffix}"
            
            # Move file
            shutil.move(str(file_path), str(dest_path))
            self.logger.info(f"Moved {file_path.name} to {processed_date_dir}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to move file {file_path}: {e}")
            return False
    
    def start_monitoring(self) -> bool:
        """Start file system monitoring"""
        try:
            if self.is_monitoring:
                self.logger.warning("Monitoring already active")
                return True
            
            if not self.validate_paths():
                return False
            
            # Setup file observer
            handler = FileMonitor(self)
            self.observer = Observer()
            self.observer.schedule(handler, str(self.watch_path), recursive=True)
            
            self.observer.start()
            self.is_monitoring = True
            
            self.logger.info(f"Started monitoring: {self.watch_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start monitoring: {e}")
            return False
    
    def stop_monitoring(self) -> None:
        """Stop file system monitoring"""
        try:
            if self.observer and self.is_monitoring:
                self.observer.stop()
                self.observer.join(timeout=5)
                self.is_monitoring = False
                self.logger.info("Stopped file monitoring")
        except Exception as e:
            self.logger.error(f"Error stopping monitoring: {e}")
    
    def find_unprocessed_files(self) -> List[Path]:
        """Find files that haven't been processed yet"""
        unprocessed = []
        
        try:
            if not self.watch_path.exists():
                return unprocessed
            
            # Recursively find all JSON files
            json_files = list(self.watch_path.rglob("*.json"))
            
            for json_file in json_files:
                if (json_file.exists() and 
                    str(json_file) not in self.processed_files):
                    unprocessed.append(json_file)
            
            self.logger.info(f"Found {len(unprocessed)} unprocessed files")
            
        except Exception as e:
            self.logger.error(f"Error finding unprocessed files: {e}")
        
        return unprocessed
    
    def mark_file_processed(self, file_path: Path) -> None:
        """Mark a file as processed"""
        self.processed_files.add(str(file_path))
    
    def is_file_processed(self, file_path: Path) -> bool:
        """Check if file has been processed"""
        return str(file_path) in self.processed_files
    
    def get_file_stats(self) -> Dict[str, int]:
        """Get file processing statistics"""
        return {
            'processed_count': len(self.processed_files),
            'queue_size': self.get_queue_size(),
            'monitoring': self.is_monitoring
        }
    
    def cleanup(self) -> None:
        """Cleanup resources"""
        try:
            self.stop_monitoring()
            self.logger.info("File processing service cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.cleanup()