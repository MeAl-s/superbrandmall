#!/usr/bin/env python3
"""
process_manager.py - Process Management for Worker Scheduler System
Handles process lifecycle, PID management, and status monitoring
"""

import os
import sys
import signal
import subprocess
import psutil
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from datetime import datetime
from enum import Enum
import json

from config import WorkerConfig


class ProcessStatus(Enum):
    """Process status enumeration"""
    RUNNING = "RUNNING"
    STOPPED = "STOPPED" 
    NOT_STARTED = "NOT_STARTED"
    ERROR = "ERROR"
    ZOMBIE = "ZOMBIE"
    UNKNOWN = "UNKNOWN"


class ProcessInfo:
    """Container for process information"""
    
    def __init__(self, worker_name: str, pid: Optional[int] = None, 
                 status: ProcessStatus = ProcessStatus.NOT_STARTED,
                 start_time: Optional[datetime] = None,
                 cpu_percent: float = 0.0, memory_mb: float = 0.0):
        self.worker_name = worker_name
        self.pid = pid
        self.status = status
        self.start_time = start_time
        self.cpu_percent = cpu_percent
        self.memory_mb = memory_mb
        self.last_updated = datetime.now()
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'worker_name': self.worker_name,
            'pid': self.pid,
            'status': self.status.value,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'cpu_percent': self.cpu_percent,
            'memory_mb': self.memory_mb,
            'last_updated': self.last_updated.isoformat()
        }
    
    def __str__(self) -> str:
        pid_str = f"PID:{self.pid}" if self.pid else "No PID"
        return f"{self.worker_name}: {self.status.value} ({pid_str})"


class PIDFileManager:
    """Manages PID files for worker processes"""
    
    def __init__(self, pid_dir: str):
        self.pid_dir = Path(pid_dir)
        self.pid_dir.mkdir(parents=True, exist_ok=True)
    
    def get_pid_file_path(self, worker_name: str) -> Path:
        """Get PID file path for worker"""
        return self.pid_dir / f"{worker_name}.pid"
    
    def save_pid(self, worker_name: str, pid: int) -> bool:
        """Save PID to file"""
        try:
            pid_file = self.get_pid_file_path(worker_name)
            with open(pid_file, 'w') as f:
                f.write(str(pid))
            return True
        except Exception as e:
            print(f"Error saving PID file for {worker_name}: {e}")
            return False
    
    def load_pid(self, worker_name: str) -> Optional[int]:
        """Load PID from file"""
        pid_file = self.get_pid_file_path(worker_name)
        if not pid_file.exists():
            return None
        
        try:
            with open(pid_file, 'r') as f:
                pid_str = f.read().strip()
                return int(pid_str)
        except (ValueError, FileNotFoundError, IOError):
            return None
    
    def remove_pid_file(self, worker_name: str) -> bool:
        """Remove PID file"""
        try:
            pid_file = self.get_pid_file_path(worker_name)
            if pid_file.exists():
                pid_file.unlink()
            return True
        except Exception:
            return False
    
    def cleanup_stale_pids(self) -> List[str]:
        """Remove PID files for non-running processes"""
        cleaned = []
        
        for pid_file in self.pid_dir.glob("*.pid"):
            worker_name = pid_file.stem
            pid = self.load_pid(worker_name)
            
            if pid and not self._is_process_running(pid):
                self.remove_pid_file(worker_name)
                cleaned.append(worker_name)
        
        return cleaned
    
    def _is_process_running(self, pid: int) -> bool:
        """Check if process is running"""
        try:
            return psutil.pid_exists(pid)
        except:
            # Fallback method
            try:
                os.kill(pid, 0)
                return True
            except OSError:
                return False


class ProcessMonitor:
    """Monitors process health and performance"""
    
    def __init__(self, pid_manager: PIDFileManager):
        self.pid_manager = pid_manager
        self._process_cache: Dict[int, psutil.Process] = {}
    
    def get_process_info(self, worker_name: str) -> ProcessInfo:
        """Get comprehensive process information"""
        pid = self.pid_manager.load_pid(worker_name)
        
        if pid is None:
            return ProcessInfo(worker_name, status=ProcessStatus.NOT_STARTED)
        
        try:
            # Use cached process object or create new one
            if pid not in self._process_cache:
                self._process_cache[pid] = psutil.Process(pid)
            
            process = self._process_cache[pid]
            
            # Check if process is still valid
            if not process.is_running():
                self.pid_manager.remove_pid_file(worker_name)
                if pid in self._process_cache:
                    del self._process_cache[pid]
                return ProcessInfo(worker_name, status=ProcessStatus.STOPPED)
            
            # Get process metrics
            status = self._map_psutil_status(process.status())
            start_time = datetime.fromtimestamp(process.create_time())
            
            try:
                cpu_percent = process.cpu_percent()
                memory_info = process.memory_info()
                memory_mb = memory_info.rss / 1024 / 1024  # Convert to MB
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                cpu_percent = 0.0
                memory_mb = 0.0
            
            return ProcessInfo(
                worker_name=worker_name,
                pid=pid,
                status=status,
                start_time=start_time,
                cpu_percent=cpu_percent,
                memory_mb=memory_mb
            )
            
        except psutil.NoSuchProcess:
            # Process no longer exists
            self.pid_manager.remove_pid_file(worker_name)
            if pid in self._process_cache:
                del self._process_cache[pid]
            return ProcessInfo(worker_name, status=ProcessStatus.STOPPED)
        
        except Exception as e:
            return ProcessInfo(worker_name, pid=pid, status=ProcessStatus.ERROR)
    
    def _map_psutil_status(self, psutil_status: str) -> ProcessStatus:
        """Map psutil status to our ProcessStatus enum"""
        status_mapping = {
            psutil.STATUS_RUNNING: ProcessStatus.RUNNING,
            psutil.STATUS_SLEEPING: ProcessStatus.RUNNING,
            psutil.STATUS_DISK_SLEEP: ProcessStatus.RUNNING,
            psutil.STATUS_STOPPED: ProcessStatus.STOPPED,
            psutil.STATUS_ZOMBIE: ProcessStatus.ZOMBIE,
        }
        return status_mapping.get(psutil_status, ProcessStatus.UNKNOWN)
    
    def get_all_process_info(self, worker_names: List[str]) -> Dict[str, ProcessInfo]:
        """Get process info for all workers"""
        return {name: self.get_process_info(name) for name in worker_names}
    
    def cleanup_cache(self):
        """Clean up cached process objects"""
        stale_pids = []
        for pid, process in self._process_cache.items():
            try:
                if not process.is_running():
                    stale_pids.append(pid)
            except:
                stale_pids.append(pid)
        
        for pid in stale_pids:
            del self._process_cache[pid]


class ProcessLauncher:
    """Launches and manages worker processes"""
    
    def __init__(self, pid_manager: PIDFileManager, log_dir: str):
        self.pid_manager = pid_manager
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
    
    def launch_process(self, worker_config: WorkerConfig, python_path: str, 
                      app_dir: str, environment_vars: Dict[str, str] = None) -> Tuple[bool, Optional[int]]:
        """Launch a single worker process"""
        script_path = Path(app_dir) / worker_config.script_path
        log_file = self.log_dir / f"{worker_config.name}.log"
        
        # Validate script exists
        if not script_path.exists():
            self._log_error(worker_config.name, f"Script not found: {script_path}")
            return False, None
        
        try:
            # Prepare environment
            env = os.environ.copy()
            if environment_vars:
                env.update(environment_vars)
            
            # Prepare command
            cmd = [python_path, str(script_path)]
            
            # Launch process
            process = subprocess.Popen(
                cmd,
                stdout=open(log_file, 'a'),
                stderr=subprocess.STDOUT,
                env=env,
                cwd=str(Path(app_dir)),
                preexec_fn=os.setsid  # Create new process group
            )
            
            # Save PID
            if self.pid_manager.save_pid(worker_config.name, process.pid):
                self._log_success(worker_config.name, f"Launched with PID {process.pid}")
                return True, process.pid
            else:
                self._log_error(worker_config.name, "Failed to save PID file")
                return False, None
                
        except Exception as e:
            self._log_error(worker_config.name, f"Launch failed: {str(e)}")
            return False, None
    
    def launch_multiple(self, worker_configs: List[WorkerConfig], python_path: str,
                       app_dir: str, global_env: Dict[str, str] = None,
                       launch_delay: float = 0.5) -> Dict[str, Tuple[bool, Optional[int]]]:
        """Launch multiple worker processes"""
        results = {}
        
        for worker_config in worker_configs:
            if not worker_config.enabled:
                results[worker_config.name] = (False, None)
                continue
            
            # Merge environment variables
            env = global_env.copy() if global_env else {}
            env.update(worker_config.environment_vars)
            
            success, pid = self.launch_process(worker_config, python_path, app_dir, env)
            results[worker_config.name] = (success, pid)
            
            # Small delay between launches
            if launch_delay > 0:
                time.sleep(launch_delay)
        
        return results
    
    def _log_success(self, worker_name: str, message: str):
        """Log success message"""
        self._write_log(worker_name, "SUCCESS", message)
    
    def _log_error(self, worker_name: str, message: str):
        """Log error message"""
        self._write_log(worker_name, "ERROR", message)
    
    def _write_log(self, worker_name: str, level: str, message: str):
        """Write log message"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] [{level}] {message}\n"
        
        # Write to worker-specific log
        worker_log = self.log_dir / f"{worker_name}.log"
        try:
            with open(worker_log, 'a') as f:
                f.write(log_entry)
        except:
            pass  # Ignore logging errors
        
        # Also print to stdout
        print(f"{worker_name}: {log_entry.strip()}")


class ProcessTerminator:
    """Handles process termination and cleanup"""
    
    def __init__(self, pid_manager: PIDFileManager):
        self.pid_manager = pid_manager
    
    def terminate_process(self, worker_name: str, force: bool = False, 
                         timeout: int = 10) -> bool:
        """Terminate a worker process"""
        pid = self.pid_manager.load_pid(worker_name)
        
        if pid is None:
            return True  # Already not running
        
        try:
            process = psutil.Process(pid)
            
            if not process.is_running():
                self.pid_manager.remove_pid_file(worker_name)
                return True
            
            if force:
                # Force kill
                process.kill()
            else:
                # Graceful termination
                process.terminate()
                
                # Wait for process to terminate
                try:
                    process.wait(timeout=timeout)
                except psutil.TimeoutExpired:
                    # Force kill if graceful termination failed
                    process.kill()
                    process.wait(timeout=5)
            
            self.pid_manager.remove_pid_file(worker_name)
            return True
            
        except psutil.NoSuchProcess:
            # Process already gone
            self.pid_manager.remove_pid_file(worker_name)
            return True
        except Exception as e:
            print(f"Error terminating {worker_name}: {e}")
            return False
    
    def terminate_multiple(self, worker_names: List[str], force: bool = False,
                          timeout: int = 10) -> Dict[str, bool]:
        """Terminate multiple worker processes"""
        results = {}
        
        for worker_name in worker_names:
            results[worker_name] = self.terminate_process(worker_name, force, timeout)
        
        return results
    
    def terminate_all_workers(self, worker_names: List[str], force: bool = False) -> bool:
        """Terminate all worker processes"""
        results = self.terminate_multiple(worker_names, force)
        return all(results.values())


class ProcessManager:
    """Main process management coordinator"""
    
    def __init__(self, log_dir: str):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize components
        self.pid_manager = PIDFileManager(str(self.log_dir))
        self.monitor = ProcessMonitor(self.pid_manager)
        self.launcher = ProcessLauncher(self.pid_manager, str(self.log_dir))
        self.terminator = ProcessTerminator(self.pid_manager)
    
    def launch_workers(self, worker_configs: List[WorkerConfig], python_path: str,
                      app_dir: str, global_env: Dict[str, str] = None) -> Dict[str, bool]:
        """Launch worker processes"""
        results = self.launcher.launch_multiple(worker_configs, python_path, app_dir, global_env)
        return {name: success for name, (success, pid) in results.items()}
    
    def get_worker_status(self, worker_name: str) -> ProcessInfo:
        """Get status of a single worker"""
        return self.monitor.get_process_info(worker_name)
    
    def get_all_worker_statuses(self, worker_names: List[str]) -> Dict[str, ProcessInfo]:
        """Get status of all workers"""
        return self.monitor.get_all_process_info(worker_names)
    
    def stop_worker(self, worker_name: str, force: bool = False) -> bool:
        """Stop a single worker"""
        return self.terminator.terminate_process(worker_name, force)
    
    def stop_all_workers(self, worker_names: List[str], force: bool = False) -> Dict[str, bool]:
        """Stop all workers"""
        return self.terminator.terminate_multiple(worker_names, force)
    
    def restart_worker(self, worker_config: WorkerConfig, python_path: str,
                      app_dir: str, environment_vars: Dict[str, str] = None) -> bool:
        """Restart a single worker"""
        # Stop the worker first
        self.stop_worker(worker_config.name)
        
        # Wait a moment
        time.sleep(1)
        
        # Launch it again
        success, pid = self.launcher.launch_process(worker_config, python_path, app_dir, environment_vars)
        return success
    
    def cleanup_stale_processes(self) -> List[str]:
        """Clean up stale PID files"""
        return self.pid_manager.cleanup_stale_pids()
    
    def get_system_stats(self, worker_names: List[str]) -> Dict:
        """Get overall system statistics"""
        statuses = self.get_all_worker_statuses(worker_names)
        
        running_count = sum(1 for info in statuses.values() if info.status == ProcessStatus.RUNNING)
        total_cpu = sum(info.cpu_percent for info in statuses.values())
        total_memory = sum(info.memory_mb for info in statuses.values())
        
        return {
            'total_workers': len(worker_names),
            'running_workers': running_count,
            'stopped_workers': len(worker_names) - running_count,
            'total_cpu_percent': total_cpu,
            'total_memory_mb': total_memory,
            'timestamp': datetime.now().isoformat()
        }
    
    def export_status_report(self, worker_names: List[str], output_file: str = None) -> Dict:
        """Export detailed status report"""
        statuses = self.get_all_worker_statuses(worker_names)
        system_stats = self.get_system_stats(worker_names)
        
        report = {
            'system_stats': system_stats,
            'worker_statuses': {name: info.to_dict() for name, info in statuses.items()},
            'generated_at': datetime.now().isoformat()
        }
        
        if output_file:
            try:
                with open(output_file, 'w') as f:
                    json.dump(report, f, indent=2)
            except Exception as e:
                print(f"Error saving report: {e}")
        
        return report
    
    def print_status_summary(self, worker_names: List[str]):
        """Print human-readable status summary"""
        statuses = self.get_all_worker_statuses(worker_names)
        system_stats = self.get_system_stats(worker_names)
        
        print("\n" + "="*60)
        print("PROCESS STATUS SUMMARY")
        print("="*60)
        
        print(f"\nOverall Statistics:")
        print(f"  Total Workers: {system_stats['total_workers']}")
        print(f"  Running: {system_stats['running_workers']}")
        print(f"  Stopped: {system_stats['stopped_workers']}")
        print(f"  Total CPU: {system_stats['total_cpu_percent']:.1f}%")
        print(f"  Total Memory: {system_stats['total_memory_mb']:.1f} MB")
        
        print(f"\nWorker Details:")
        for name, info in statuses.items():
            uptime = ""
            if info.start_time:
                uptime = str(datetime.now() - info.start_time).split('.')[0]
            
            print(f"  {name:20} {info.status.value:10} "
                  f"PID:{info.pid or 'N/A':>6} "
                  f"CPU:{info.cpu_percent:>5.1f}% "
                  f"MEM:{info.memory_mb:>6.1f}MB "
                  f"UP:{uptime}")
        
        print("="*60)


# Example usage and testing
if __name__ == "__main__":
    from config import WorkerConfig
    
    # Example usage
    process_manager = ProcessManager("/tmp/test_workers/logs")
    
    # Create test worker config
    test_worker = WorkerConfig(
        name="test_worker",
        script_path="test_script.py",
        description="Test worker"
    )
    
    # Test launching
    print("Testing process management...")
    
    # Get status (should be NOT_STARTED)
    status = process_manager.get_worker_status("test_worker")
    print(f"Initial status: {status}")
    
    # Clean up any stale PIDs
    cleaned = process_manager.cleanup_stale_processes()
    if cleaned:
        print(f"Cleaned up stale PIDs: {cleaned}")
    
    print("Process manager ready for use.")