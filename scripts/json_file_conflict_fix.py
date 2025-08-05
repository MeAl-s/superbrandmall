#!/usr/bin/env python3
"""
OOP JSON Conflict Resolution System
Object-oriented solution for preventing JSON file conflicts between workers
"""

import json
import time
import os
import threading
import random
import shutil
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass
from abc import ABC, abstractmethod
import sys


@dataclass
class ConflictResolutionConfig:
    """Configuration for conflict resolution strategies"""
    strategy: str = "safe_shared_reading"  # or "copy_per_worker"
    retry_attempts: int = 5
    retry_delay: float = 0.1
    max_file_size_for_copy: int = 1024 * 1024  # 1MB
    auto_backup: bool = True
    backup_directory: str = "backup_original"


class FileLocker:
    """Cross-platform file locking implementation"""
    
    def __init__(self):
        self.is_windows = os.name == 'nt'
        
        if self.is_windows:
            try:
                import msvcrt
                self.msvcrt = msvcrt
            except ImportError:
                self.msvcrt = None
        else:
            try:
                import fcntl
                self.fcntl = fcntl
            except ImportError:
                self.fcntl = None
    
    def lock_file(self, file_handle) -> bool:
        """Lock file handle (returns True if successful)"""
        try:
            if self.is_windows and self.msvcrt:
                self.msvcrt.locking(file_handle.fileno(), self.msvcrt.LK_NBLCK, 1)
            elif not self.is_windows and self.fcntl:
                self.fcntl.flock(file_handle.fileno(), self.fcntl.LOCK_EX | self.fcntl.LOCK_NB)
            else:
                return True  # Fallback: no locking
            return True
        except (IOError, OSError):
            return False
    
    def unlock_file(self, file_handle) -> bool:
        """Unlock file handle"""
        try:
            if self.is_windows and self.msvcrt:
                self.msvcrt.locking(file_handle.fileno(), self.msvcrt.LK_UNLCK, 1)
            elif not self.is_windows and self.fcntl:
                self.fcntl.flock(file_handle.fileno(), self.fcntl.LOCK_UN)
            return True
        except (IOError, OSError):
            return False


class JSONConflictStrategy(ABC):
    """Abstract base class for JSON conflict resolution strategies"""
    
    @abstractmethod
    def read_json(self, json_file: Path, worker_name: str) -> Optional[Dict[Any, Any]]:
        """Read JSON data using this strategy"""
        pass
    
    @abstractmethod
    def setup(self, json_file: Path, worker_name: str) -> bool:
        """Setup this strategy (if needed)"""
        pass


class SafeSharedReadingStrategy(JSONConflictStrategy):
    """Strategy: Safe shared reading with file locking"""
    
    def __init__(self, config: ConflictResolutionConfig):
        self.config = config
        self.file_locker = FileLocker()
        self._lock = threading.Lock()
    
    def setup(self, json_file: Path, worker_name: str) -> bool:
        """No setup needed for shared reading"""
        return json_file.exists()
    
    def read_json(self, json_file: Path, worker_name: str) -> Optional[Dict[Any, Any]]:
        """Read JSON with file locking and retry logic"""
        
        for attempt in range(self.config.retry_attempts):
            try:
                with self._lock:
                    return self._read_with_lock(json_file, worker_name)
            
            except (FileNotFoundError, json.JSONDecodeError, PermissionError) as e:
                if attempt == self.config.retry_attempts - 1:
                    print(f"[{worker_name}] Failed to read JSON after {self.config.retry_attempts} attempts: {e}")
                    return None
                
                # Exponential backoff with jitter
                wait_time = self.config.retry_delay * (2 ** attempt) + random.uniform(0, 0.1)
                print(f"[{worker_name}] Attempt {attempt + 1} failed, retrying in {wait_time:.2f}s...")
                time.sleep(wait_time)
        
        return None
    
    def _read_with_lock(self, json_file: Path, worker_name: str) -> Dict[Any, Any]:
        """Read JSON file with proper locking"""
        
        if not json_file.exists():
            raise FileNotFoundError(f"JSON file not found: {json_file}")
        
        with open(json_file, 'r', encoding='utf-8') as f:
            if self.file_locker.lock_file(f):
                try:
                    data = json.load(f)
                    return data
                finally:
                    self.file_locker.unlock_file(f)
            else:
                raise PermissionError("Could not acquire file lock")


class CopyPerWorkerStrategy(JSONConflictStrategy):
    """Strategy: Each worker gets its own copy of the JSON file"""
    
    def __init__(self, config: ConflictResolutionConfig):
        self.config = config
        self.safe_reader = SafeSharedReadingStrategy(config)
    
    def setup(self, json_file: Path, worker_name: str) -> bool:
        """Create worker-specific copy if needed"""
        worker_json_file = self._get_worker_json_path(json_file, worker_name)
        
        # Create copy if it doesn't exist or base file is newer
        if not worker_json_file.exists() or self._is_base_file_newer(json_file, worker_json_file):
            return self._create_worker_copy(json_file, worker_json_file, worker_name)
        
        return worker_json_file.exists()
    
    def read_json(self, json_file: Path, worker_name: str) -> Optional[Dict[Any, Any]]:
        """Read from worker-specific JSON copy"""
        worker_json_file = self._get_worker_json_path(json_file, worker_name)
        
        # Ensure worker copy exists and is up to date
        if not self.setup(json_file, worker_name):
            return None
        
        try:
            with open(worker_json_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"[{worker_name}] Failed to read worker JSON copy: {e}")
            return None
    
    def _get_worker_json_path(self, json_file: Path, worker_name: str) -> Path:
        """Get path for worker-specific JSON file"""
        return json_file.parent / f"{json_file.stem}_{worker_name}.json"
    
    def _create_worker_copy(self, json_file: Path, worker_json_file: Path, worker_name: str) -> bool:
        """Create worker-specific copy of JSON file"""
        try:
            # Read from base file safely
            data = self.safe_reader.read_json(json_file, worker_name)
            if data is not None:
                # Write to worker-specific file
                with open(worker_json_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                print(f"[{worker_name}] Created worker-specific JSON: {worker_json_file}")
                return True
        except Exception as e:
            print(f"[{worker_name}] Failed to create worker JSON copy: {e}")
        
        return False
    
    def _is_base_file_newer(self, base_file: Path, worker_file: Path) -> bool:
        """Check if base file is newer than worker file"""
        if not worker_file.exists():
            return True
        
        try:
            base_mtime = base_file.stat().st_mtime
            worker_mtime = worker_file.stat().st_mtime
            return base_mtime > worker_mtime
        except OSError:
            return True


class JSONConflictResolver:
    """Main class for resolving JSON file conflicts between workers"""
    
    def __init__(self, config: ConflictResolutionConfig = None):
        self.config = config or ConflictResolutionConfig()
        self.strategy = self._create_strategy()
        self.json_managers = {}  # Cache for JSON managers
    
    def _create_strategy(self) -> JSONConflictStrategy:
        """Create the appropriate conflict resolution strategy"""
        if self.config.strategy == "copy_per_worker":
            return CopyPerWorkerStrategy(self.config)
        else:
            return SafeSharedReadingStrategy(self.config)
    
    def get_json_manager(self, json_file: Union[str, Path], worker_name: str) -> 'WorkerJSONManager':
        """Get or create a JSON manager for a specific worker and file"""
        json_file = Path(json_file)
        key = (str(json_file), worker_name)
        
        if key not in self.json_managers:
            self.json_managers[key] = WorkerJSONManager(
                json_file_path=json_file,
                worker_name=worker_name,
                strategy=self.strategy
            )
        
        return self.json_managers[key]
    
    def read_json_safe(self, json_file: Union[str, Path], worker_name: str) -> Optional[Dict[Any, Any]]:
        """Safely read JSON file without conflicts"""
        manager = self.get_json_manager(json_file, worker_name)
        return manager.get_json_data()
    
    def configure_strategy(self, strategy: str, **kwargs) -> 'JSONConflictResolver':
        """Configure the conflict resolution strategy (fluent interface)"""
        self.config.strategy = strategy
        
        # Update config with additional parameters
        for key, value in kwargs.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
        
        # Recreate strategy with new config
        self.strategy = self._create_strategy()
        
        # Clear cached managers to use new strategy
        self.json_managers.clear()
        
        return self
    
    def set_retry_config(self, attempts: int = 5, delay: float = 0.1) -> 'JSONConflictResolver':
        """Set retry configuration (fluent interface)"""
        self.config.retry_attempts = attempts
        self.config.retry_delay = delay
        return self
    
    def enable_auto_backup(self, backup_dir: str = "backup_original") -> 'JSONConflictResolver':
        """Enable automatic backup of original files (fluent interface)"""
        self.config.auto_backup = True
        self.config.backup_directory = backup_dir
        return self


class WorkerJSONManager:
    """JSON manager for individual workers"""
    
    def __init__(self, json_file_path: Path, worker_name: str, strategy: JSONConflictStrategy):
        self.json_file_path = Path(json_file_path)
        self.worker_name = worker_name
        self.strategy = strategy
        self._setup_complete = False
    
    def get_json_data(self) -> Optional[Dict[Any, Any]]:
        """Get JSON data using the configured strategy"""
        
        # Setup strategy if not done yet
        if not self._setup_complete:
            self._setup_complete = self.strategy.setup(self.json_file_path, self.worker_name)
            if not self._setup_complete:
                print(f"[{self.worker_name}] Failed to setup JSON strategy")
                return None
        
        return self.strategy.read_json(self.json_file_path, self.worker_name)
    
    def refresh_setup(self) -> bool:
        """Force refresh of strategy setup"""
        self._setup_complete = self.strategy.setup(self.json_file_path, self.worker_name)
        return self._setup_complete


class WorkerUpdater:
    """Handles updating existing worker files to use conflict resolution"""
    
    def __init__(self, conflict_resolver: JSONConflictResolver):
        self.conflict_resolver = conflict_resolver
    
    def update_workers(self, workers_dir: Union[str, Path], 
                      shared_json_file: Union[str, Path],
                      worker_files: List[str] = None) -> bool:
        """Update multiple worker files to use conflict resolution"""
        
        workers_dir = Path(workers_dir)
        shared_json_file = Path(shared_json_file)
        
        if worker_files is None:
            worker_files = [
                "realtime_detector.py",
                "ocr_processor.py",
                "ocr_classification.py",
                "ocr_downloader.py", 
                "ocr_text_processor.py",
                "delivery_scanner.py",
                "receipt_matcher.py",
                "timezone_worker.py"
            ]
        
        print(f"🔧 Updating workers in: {workers_dir}")
        print(f"📄 Shared JSON file: {shared_json_file}")
        print(f"🛡️ Strategy: {self.conflict_resolver.config.strategy}")
        
        # Create backup directory
        if self.conflict_resolver.config.auto_backup:
            backup_dir = workers_dir / self.conflict_resolver.config.backup_directory
            backup_dir.mkdir(exist_ok=True)
        
        updated_count = 0
        for worker_file in worker_files:
            worker_path = workers_dir / worker_file
            
            if worker_path.exists():
                if self._update_single_worker(worker_path, shared_json_file, backup_dir if self.conflict_resolver.config.auto_backup else None):
                    updated_count += 1
                    print(f"   ✅ Updated: {worker_file}")
                else:
                    print(f"   ❌ Failed to update: {worker_file}")
            else:
                print(f"   ⚠️ Not found: {worker_file}")
        
        print(f"\n✅ Updated {updated_count}/{len(worker_files)} workers")
        return updated_count == len(worker_files)
    
    def _update_single_worker(self, worker_path: Path, shared_json_file: Path, backup_dir: Optional[Path]) -> bool:
        """Update a single worker file"""
        
        try:
            # Backup original if requested
            if backup_dir:
                backup_path = backup_dir / worker_path.name
                if not backup_path.exists():
                    shutil.copy2(worker_path, backup_path)
            
            # Create updated worker content
            worker_name = worker_path.stem
            updated_content = self._create_updated_worker_content(worker_name, shared_json_file)
            
            # Write updated content
            with open(worker_path, 'w', encoding='utf-8') as f:
                f.write(updated_content)
            
            return True
            
        except Exception as e:
            print(f"Error updating {worker_path}: {e}")
            return False
    
    def _create_updated_worker_content(self, worker_name: str, shared_json_file: Path) -> str:
        """Create updated worker content with conflict resolution"""
        
        return f'''#!/usr/bin/env python3
"""
Conflict-Free {worker_name.title().replace('_', ' ')}
Updated to use JSON conflict resolution system
"""

import sys
import os
import time
import json
from pathlib import Path

# Import the conflict resolution system
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from json_file_conflict_fix import JSONConflictResolver, ConflictResolutionConfig
except ImportError:
    print("❌ Error: JSON conflict resolution system not found!")
    print("   Make sure json_file_conflict_fix.py is in the parent directory")
    sys.exit(1)

# Configuration
JSON_FILE_PATH = r"{shared_json_file}"
WORKER_NAME = "{worker_name}"

class {worker_name.title().replace('_', '')}Worker:
    """Main worker class with conflict-free JSON access"""
    
    def __init__(self):
        # Create conflict resolver with your preferred strategy
        config = ConflictResolutionConfig(
            strategy="safe_shared_reading",  # or "copy_per_worker"
            retry_attempts=5,
            retry_delay=0.1
        )
        
        self.conflict_resolver = JSONConflictResolver(config)
        self.worker_name = WORKER_NAME
        self.json_file = JSON_FILE_PATH
        
        print(f"[{{self.worker_name}}] Initialized with conflict resolution")
    
    def get_json_data(self):
        """Get JSON data safely without conflicts"""
        return self.conflict_resolver.read_json_safe(self.json_file, self.worker_name)
    
    def process_data(self, data):
        """
        Process the JSON data - REPLACE THIS WITH YOUR ACTUAL LOGIC
        """
        print(f"[{{self.worker_name}}] Processing {{len(data) if data else 0}} items...")
        
        if not data:
            print(f"[{{self.worker_name}}] No data to process")
            return
        
        # 🚨 TODO: Replace this section with your actual processing logic
        for i, item in enumerate(data):
            print(f"[{{self.worker_name}}] Processing item {{i+1}}: {{item}}")
            # Your processing code here
            time.sleep(0.1)  # Simulate processing time
        
        print(f"[{{self.worker_name}}] Processing complete")
    
    def run(self):
        """Main worker loop"""
        print(f"[{{self.worker_name}}] Starting main loop...")
        
        while True:
            try:
                # Get JSON data safely (no conflicts!)
                data = self.get_json_data()
                
                if data is None:
                    print(f"[{{self.worker_name}}] Failed to read JSON, retrying in 5 seconds...")
                    time.sleep(5)
                    continue
                
                # Process the data
                self.process_data(data)
                
                # Wait before next iteration
                time.sleep(10)  # Adjust as needed
                
            except KeyboardInterrupt:
                print(f"[{{self.worker_name}}] Shutting down...")
                break
            except Exception as e:
                print(f"[{{self.worker_name}}] Error: {{e}}")
                time.sleep(5)

def main():
    """Main function"""
    worker = {worker_name.title().replace('_', '')}Worker()
    worker.run()

if __name__ == "__main__":
    main()
'''


class ConflictResolutionSystem:
    """Main system class for JSON conflict resolution"""
    
    def __init__(self):
        self.conflict_resolver = None
        self.worker_updater = None
    
    def configure(self, strategy: str = "safe_shared_reading", 
                 retry_attempts: int = 5, retry_delay: float = 0.1,
                 auto_backup: bool = True) -> 'ConflictResolutionSystem':
        """Configure the conflict resolution system (fluent interface)"""
        
        config = ConflictResolutionConfig(
            strategy=strategy,
            retry_attempts=retry_attempts,
            retry_delay=retry_delay,
            auto_backup=auto_backup
        )
        
        self.conflict_resolver = JSONConflictResolver(config)
        self.worker_updater = WorkerUpdater(self.conflict_resolver)
        
        return self
    
    def fix_workers(self, workers_dir: Union[str, Path], 
                   json_file: Union[str, Path],
                   worker_files: List[str] = None) -> bool:
        """Fix existing workers to prevent JSON conflicts"""
        
        if not self.conflict_resolver:
            self.configure()  # Use default configuration
        
        return self.worker_updater.update_workers(workers_dir, json_file, worker_files)
    
    def get_json_reader(self, json_file: Union[str, Path], worker_name: str) -> WorkerJSONManager:
        """Get a JSON reader for a specific worker"""
        
        if not self.conflict_resolver:
            self.configure()  # Use default configuration
            
        return self.conflict_resolver.get_json_manager(json_file, worker_name)
    
    def read_json_safe(self, json_file: Union[str, Path], worker_name: str) -> Optional[Dict[Any, Any]]:
        """Quick method to safely read JSON"""
        
        if not self.conflict_resolver:
            self.configure()  # Use default configuration
            
        return self.conflict_resolver.read_json_safe(json_file, worker_name)


# Convenience functions for easy usage
def create_conflict_resolver(strategy: str = "safe_shared_reading") -> JSONConflictResolver:
    """Create a configured conflict resolver"""
    config = ConflictResolutionConfig(strategy=strategy)
    return JSONConflictResolver(config)


def quick_fix_json_conflicts(workers_dir: str, json_file: str, 
                           strategy: str = "safe_shared_reading") -> bool:
    """Quick fix function for JSON conflicts"""
    
    system = ConflictResolutionSystem()
    system.configure(strategy=strategy)
    
    return system.fix_workers(workers_dir, json_file)


# Example usage
if __name__ == "__main__":
    print("🔧 OOP JSON Conflict Resolution System")
    print("=" * 50)
    
    # Example 1: Quick fix
    print("\\nExample 1: Quick Fix")
    print("-" * 20)
    
    # quick_fix_json_conflicts(
    #     workers_dir=r"C:\\Point Detection\\workers",
    #     json_file=r"C:\\Point Detection\\data\\config.json",
    #     strategy="safe_shared_reading"
    # )
    
    # Example 2: Advanced usage
    print("\\nExample 2: Advanced Configuration")
    print("-" * 30)
    
    # system = (ConflictResolutionSystem()
    #          .configure(strategy="copy_per_worker", retry_attempts=3)
    #          .fix_workers("workers/", "data.json"))
    
    # Example 3: Direct usage in worker
    print("\\nExample 3: Direct Usage in Worker")
    print("-" * 35)
    
    # resolver = create_conflict_resolver("safe_shared_reading")
    # data = resolver.read_json_safe("config.json", "my_worker")
    # print(f"Read data: {len(data) if data else 0} items")
    
    print("\\n✅ Ready to use! Update the paths and uncomment examples above.")