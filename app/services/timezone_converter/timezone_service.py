# =================================================================
# FILE 2: app/services/timezone_converter/timezone_service.py
"""
Timezone Service - API interface for the converter
"""

import sys
from pathlib import Path
from typing import Dict, Any

# Add paths
current_file = Path(__file__)
app_dir = current_file.parent.parent.parent
sys.path.insert(0, str(app_dir))

from .timezone_converter import TimezoneConverter

class TimezoneService:
    """Service layer for timezone conversion"""
    
    def __init__(self):
        self.converter = None
        self.is_running = False
    
    def start_worker(self, watch_dir: str = None, output_dir: str = None) -> Dict[str, Any]:
        """Start the timezone conversion worker"""
        try:
            if self.is_running:
                return {"success": False, "message": "Worker already running"}
            
            self.converter = TimezoneConverter(watch_dir, output_dir)
            stats = self.converter.convert_directory()
            self.converter.start_monitoring()
            self.is_running = True
            
            return {
                "success": True,
                "message": "Worker started successfully",
                "stats": stats,
                "watch_directory": str(self.converter.watch_dir),
                "output_directory": str(self.converter.output_dir)
            }
            
        except Exception as e:
            return {"success": False, "message": str(e)}
    
    def stop_worker(self) -> Dict[str, Any]:
        """Stop the worker"""
        try:
            if not self.is_running or not self.converter:
                return {"success": False, "message": "Worker not running"}
            
            self.converter.stop_monitoring()
            self.is_running = False
            
            return {"success": True, "message": "Worker stopped successfully"}
            
        except Exception as e:
            return {"success": False, "message": str(e)}
    
    def get_status(self) -> Dict[str, Any]:
        """Get status"""
        if not self.converter:
            return {"running": False, "message": "Worker not initialized"}
        
        stats = self.converter.get_stats()
        stats["service_running"] = self.is_running
        return stats
    
    def convert_single_file(self, file_path: str) -> Dict[str, Any]:
        """Convert single file"""
        try:
            if not self.converter:
                self.converter = TimezoneConverter()
            
            success = self.converter.convert_file(file_path)
            
            return {
                "success": success,
                "message": f"File {'converted' if success else 'failed'}: {file_path}"
            }
            
        except Exception as e:
            return {"success": False, "message": str(e)}

# Global service instance
timezone_service = TimezoneService()