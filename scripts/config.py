#!/usr/bin/env python3
"""
config.py - Configuration Management for Worker Scheduler System
Handles all configuration-related functionality
"""

import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Union
from dataclasses import dataclass, field
import json


@dataclass
class WorkerConfig:
    """Configuration class for individual workers"""
    name: str
    script_path: str
    description: str = ""
    enabled: bool = True
    environment_vars: Dict[str, str] = field(default_factory=dict)
    restart_on_failure: bool = True
    max_retries: int = 3
    timeout: Optional[int] = None  # seconds
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return {
            'name': self.name,
            'script_path': self.script_path,
            'description': self.description,
            'enabled': self.enabled,
            'environment_vars': self.environment_vars,
            'restart_on_failure': self.restart_on_failure,
            'max_retries': self.max_retries,
            'timeout': self.timeout
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'WorkerConfig':
        """Create from dictionary"""
        return cls(**data)


@dataclass
class SchedulerConfig:
    """Main scheduler configuration"""
    app_dir: str
    python_path: str = field(default_factory=lambda: sys.executable)
    log_dir: Optional[str] = None
    user: Union[bool, str] = True
    log_level: str = "INFO"
    comment_prefix: str = "WorkerService"
    use_master_job: bool = True
    default_schedule: str = "0 6 * * *"  # Daily at 6 AM
    
    def __post_init__(self):
        """Post-initialization processing"""
        self.app_dir = Path(self.app_dir).resolve()
        if self.log_dir is None:
            self.log_dir = self.app_dir / "logs"
        else:
            self.log_dir = Path(self.log_dir).resolve()
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return {
            'app_dir': str(self.app_dir),
            'python_path': self.python_path,
            'log_dir': str(self.log_dir),
            'user': self.user,
            'log_level': self.log_level,
            'comment_prefix': self.comment_prefix,
            'use_master_job': self.use_master_job,
            'default_schedule': self.default_schedule
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'SchedulerConfig':
        """Create from dictionary"""
        return cls(**data)


class ConfigManager:
    """Manages configuration loading, saving, and validation"""
    
    def __init__(self, config_file: Optional[str] = None):
        self.config_file = Path(config_file) if config_file else None
        self.scheduler_config: Optional[SchedulerConfig] = None
        self.workers: List[WorkerConfig] = []
        self.global_environment: Dict[str, str] = {}
    
    def load_from_file(self, config_file: str) -> 'ConfigManager':
        """Load configuration from JSON file"""
        config_path = Path(config_file)
        
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_file}")
        
        try:
            with open(config_path, 'r') as f:
                data = json.load(f)
            
            # Load scheduler config
            if 'scheduler' in data:
                self.scheduler_config = SchedulerConfig.from_dict(data['scheduler'])
            
            # Load workers
            if 'workers' in data:
                self.workers = [WorkerConfig.from_dict(w) for w in data['workers']]
            
            # Load global environment
            self.global_environment = data.get('global_environment', {})
            
            self.config_file = config_path
            return self
            
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in config file: {e}")
        except Exception as e:
            raise ValueError(f"Error loading config file: {e}")
    
    def save_to_file(self, config_file: Optional[str] = None) -> bool:
        """Save configuration to JSON file"""
        target_file = Path(config_file) if config_file else self.config_file
        
        if target_file is None:
            raise ValueError("No config file specified")
        
        try:
            data = {
                'scheduler': self.scheduler_config.to_dict() if self.scheduler_config else {},
                'workers': [w.to_dict() for w in self.workers],
                'global_environment': self.global_environment
            }
            
            # Ensure directory exists
            target_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(target_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            self.config_file = target_file
            return True
            
        except Exception as e:
            print(f"Error saving config file: {e}")
            return False
    
    def create_default_config(self, app_dir: str, python_path: str = None) -> 'ConfigManager':
        """Create default configuration"""
        self.scheduler_config = SchedulerConfig(
            app_dir=app_dir,
            python_path=python_path or sys.executable
        )
        
        # Add default workers
        default_workers = [
            ("realtime_detector", "workers/realtime_detector.py", "Real-time detection worker"),
            ("ocr_processor", "workers/ocr_processor.py", "OCR processing worker"),
            ("ocr_classification", "workers/ocr_classification.py", "OCR classification worker"),
            ("ocr_downloader", "workers/ocr_downloader.py", "OCR download worker"),
            ("ocr_text_processor", "workers/ocr_text_processor.py", "OCR text processing worker"),
            ("delivery_scanner", "workers/delivery_scanner.py", "Delivery scanning worker"),
            ("receipt_matcher", "workers/receipt_matcher.py", "Receipt matching worker"),
            ("timezone_worker", "workers/timezone_worker.py", "Timezone processing worker")
        ]
        
        self.workers = []
        for name, script_path, description in default_workers:
            self.workers.append(WorkerConfig(
                name=name,
                script_path=script_path,
                description=description
            ))
        
        # Set default global environment
        self.global_environment = {
            'PYTHONPATH': str(self.scheduler_config.app_dir),
            'APP_ENV': 'development',
            'LOG_LEVEL': 'INFO'
        }
        
        return self
    
    def add_worker(self, worker_config: WorkerConfig) -> 'ConfigManager':
        """Add a worker configuration"""
        # Check for duplicate names
        existing_names = [w.name for w in self.workers]
        if worker_config.name in existing_names:
            raise ValueError(f"Worker with name '{worker_config.name}' already exists")
        
        self.workers.append(worker_config)
        return self
    
    def remove_worker(self, worker_name: str) -> bool:
        """Remove a worker by name"""
        for i, worker in enumerate(self.workers):
            if worker.name == worker_name:
                del self.workers[i]
                return True
        return False
    
    def get_worker(self, worker_name: str) -> Optional[WorkerConfig]:
        """Get a worker configuration by name"""
        for worker in self.workers:
            if worker.name == worker_name:
                return worker
        return None
    
    def update_worker(self, worker_name: str, **kwargs) -> bool:
        """Update a worker configuration"""
        worker = self.get_worker(worker_name)
        if worker is None:
            return False
        
        for key, value in kwargs.items():
            if hasattr(worker, key):
                setattr(worker, key, value)
        
        return True
    
    def validate_config(self) -> List[str]:
        """Validate configuration and return list of errors"""
        errors = []
        
        if self.scheduler_config is None:
            errors.append("No scheduler configuration found")
            return errors
        
        # Validate scheduler config
        if not Path(self.scheduler_config.app_dir).exists():
            errors.append(f"App directory does not exist: {self.scheduler_config.app_dir}")
        
        if not Path(self.scheduler_config.python_path).exists():
            errors.append(f"Python executable not found: {self.scheduler_config.python_path}")
        
        # Validate workers
        if not self.workers:
            errors.append("No workers configured")
        
        for worker in self.workers:
            script_path = self.scheduler_config.app_dir / worker.script_path
            if not script_path.exists():
                errors.append(f"Worker script not found: {script_path}")
        
        return errors
    
    def merge_environment_vars(self, worker_config: WorkerConfig) -> Dict[str, str]:
        """Merge global and worker-specific environment variables"""
        merged_env = self.global_environment.copy()
        merged_env.update(worker_config.environment_vars)
        return merged_env
    
    def get_enabled_workers(self) -> List[WorkerConfig]:
        """Get only enabled workers"""
        return [worker for worker in self.workers if worker.enabled]
    
    def print_config_summary(self):
        """Print configuration summary"""
        print("\n" + "="*50)
        print("CONFIGURATION SUMMARY")
        print("="*50)
        
        if self.scheduler_config:
            print(f"\nScheduler Configuration:")
            print(f"  App Directory: {self.scheduler_config.app_dir}")
            print(f"  Python Path: {self.scheduler_config.python_path}")
            print(f"  Log Directory: {self.scheduler_config.log_dir}")
            print(f"  User: {self.scheduler_config.user}")
            print(f"  Log Level: {self.scheduler_config.log_level}")
            print(f"  Use Master Job: {self.scheduler_config.use_master_job}")
            print(f"  Default Schedule: {self.scheduler_config.default_schedule}")
        
        print(f"\nWorkers ({len(self.workers)} total, {len(self.get_enabled_workers())} enabled):")
        for worker in self.workers:
            status = "ENABLED" if worker.enabled else "DISABLED"
            print(f"  {worker.name:20} {status:8} {worker.script_path}")
        
        if self.global_environment:
            print(f"\nGlobal Environment Variables:")
            for key, value in self.global_environment.items():
                print(f"  {key}: {value}")
        
        print("="*50)


# Predefined configuration templates
class ConfigTemplates:
    """Predefined configuration templates for common setups"""
    
    @staticmethod
    def development_config(app_dir: str) -> ConfigManager:
        """Development environment configuration"""
        config = ConfigManager().create_default_config(app_dir)
        config.scheduler_config.log_level = "DEBUG"
        config.scheduler_config.default_schedule = "*/5 * * * *"  # Every 5 minutes for testing
        config.global_environment.update({
            'APP_ENV': 'development',
            'DEBUG': '1',
            'LOG_LEVEL': 'DEBUG'
        })
        return config
    
    @staticmethod
    def production_config(app_dir: str, python_path: str, user: str = "production") -> ConfigManager:
        """Production environment configuration"""
        config = ConfigManager().create_default_config(app_dir, python_path)
        config.scheduler_config.user = user
        config.scheduler_config.log_level = "INFO"
        config.scheduler_config.default_schedule = "0 6 * * *"  # Daily at 6 AM
        config.global_environment.update({
            'APP_ENV': 'production',
            'LOG_LEVEL': 'INFO',
            'PYTHONUNBUFFERED': '1'
        })
        return config
    
    @staticmethod
    def testing_config(app_dir: str) -> ConfigManager:
        """Testing environment configuration"""
        config = ConfigManager().create_default_config(app_dir)
        config.scheduler_config.log_level = "DEBUG"
        config.scheduler_config.default_schedule = "@reboot"  # Only on reboot for testing
        
        # Disable some workers for testing
        for worker in config.workers:
            if worker.name in ['timezone_worker', 'delivery_scanner']:
                worker.enabled = False
        
        config.global_environment.update({
            'APP_ENV': 'testing',
            'LOG_LEVEL': 'DEBUG',
            'TEST_MODE': '1'
        })
        return config


# Example usage
if __name__ == "__main__":
    # Example 1: Create and save default config
    config = ConfigManager().create_default_config("/path/to/app")
    config.save_to_file("config.json")
    
    # Example 2: Load config from file
    config = ConfigManager().load_from_file("config.json")
    config.print_config_summary()
    
    # Example 3: Use templates
    prod_config = ConfigTemplates.production_config(
        app_dir="/opt/myapp",
        python_path="/opt/myapp/venv/bin/python",
        user="myapp"
    )
    prod_config.save_to_file("production_config.json")