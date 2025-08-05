# Add this to the top of each worker file to enable conflict resolution
import os
import json
import threading
from pathlib import Path

# Conflict resolution setup
if os.environ.get('CONFLICT_RESOLUTION') == '1':
    print(f"🛡️  [{os.environ.get('WORKER_NAME', 'worker')}] Conflict resolution enabled")
    
    # Create thread-safe JSON operations
    _json_locks = {}
    
    def get_json_lock(file_path):
        if file_path not in _json_locks:
            _json_locks[file_path] = threading.Lock()
        return _json_locks[file_path]
    
    # Override json.load to be thread-safe
    _original_json_load = json.load
    
    def safe_json_load(fp, *args, **kwargs):
        if hasattr(fp, 'name'):
            file_path = fp.name
            lock = get_json_lock(file_path)
            
            with lock:
                fp.seek(0)  # Reset file pointer
                return _original_json_load(fp, *args, **kwargs)
        else:
            return _original_json_load(fp, *args, **kwargs)
    
    json.load = safe_json_load
    
    print(f"🔒 [{os.environ.get('WORKER_NAME', 'worker')}] JSON operations are now thread-safe")
