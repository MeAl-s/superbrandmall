#!/usr/bin/env python3
"""
scheduler.py - Main Scheduler System
Orchestrates all components to provide a complete worker scheduling solution
"""

import os
import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple
from datetime import datetime

# Import our custom modules
from config import ConfigManager, WorkerConfig, SchedulerConfig
from process_manager import ProcessManager, ProcessInfo, ProcessStatus
from cron_manager import CronManager, CronJobInfo, CronPresets


class WorkerSchedulerSystem:
    """Main orchestrator class for the worker scheduling system"""
    
    def __init__(self, config_manager: ConfigManager = None, config_file: str = None):
        """
        Initialize the scheduler system
        
        Args:
            config_manager: Pre-configured ConfigManager instance
            config_file: Path to configuration file to load
        """
        # Initialize configuration
        if config_manager:
            self.config_manager = config_manager
        elif config_file:
            self.config_manager = ConfigManager().load_from_file(config_file)
        else:
            raise ValueError("Either config_manager or config_file must be provided")
        
        # Validate configuration
        config_errors = self.config_manager.validate_config()
        if config_errors:
            raise ValueError(f"Configuration errors: {config_errors}")
        
        self.scheduler_config = self.config_manager.scheduler_config
        
        # Setup logging
        self._setup_logging()
        
        # Initialize components
        self.process_manager = ProcessManager(str(self.scheduler_config.log_dir))
        self.cron_manager = CronManager(user=self.scheduler_config.user, logger=self.logger)
        
        self.logger.info(f"Initialized WorkerSchedulerSystem with {len(self.config_manager.workers)} workers")
    
    def _setup_logging(self):
        """Setup logging configuration"""
        log_dir = Path(self.scheduler_config.log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Configure logging
        logging.basicConfig(
            level=getattr(logging, self.scheduler_config.log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_dir / "scheduler_system.log"),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        self.logger = logging.getLogger("WorkerSchedulerSystem")
    
    @classmethod
    def from_config_file(cls, config_file: str) -> 'WorkerSchedulerSystem':
        """Create system from configuration file"""
        return cls(config_file=config_file)
    
    @classmethod
    def create_default(cls, app_dir: str, python_path: str = None, 
                      log_dir: str = None, user: Union[bool, str] = True) -> 'WorkerSchedulerSystem':
        """Create system with default configuration"""
        config_manager = ConfigManager().create_default_config(app_dir, python_path)
        
        if log_dir:
            config_manager.scheduler_config.log_dir = log_dir
        if user != True:
            config_manager.scheduler_config.user = user
            
        return cls(config_manager=config_manager)
    
    def add_worker(self, name: str, script_path: str, description: str = "",
                   enabled: bool = True, environment_vars: Dict[str, str] = None) -> 'WorkerSchedulerSystem':
        """Add a worker configuration (fluent interface)"""
        worker_config = WorkerConfig(
            name=name,
            script_path=script_path,
            description=description,
            enabled=enabled,
            environment_vars=environment_vars or {}
        )
        
        self.config_manager.add_worker(worker_config)
        self.logger.info(f"Added worker: {name}")
        return self
    
    def remove_worker(self, worker_name: str) -> bool:
        """Remove a worker configuration"""
        success = self.config_manager.remove_worker(worker_name)
        if success:
            self.logger.info(f"Removed worker: {worker_name}")
        else:
            self.logger.warning(f"Worker not found for removal: {worker_name}")
        return success
    
    def update_worker(self, worker_name: str, **kwargs) -> bool:
        """Update worker configuration"""
        success = self.config_manager.update_worker(worker_name, **kwargs)
        if success:
            self.logger.info(f"Updated worker: {worker_name}")
        else:
            self.logger.warning(f"Worker not found for update: {worker_name}")
        return success
    
    def set_global_environment(self, env_vars: Dict[str, str]) -> 'WorkerSchedulerSystem':
        """Set global environment variables"""
        self.config_manager.global_environment.update(env_vars)
        self.logger.info(f"Updated global environment with {len(env_vars)} variables")
        return self
    
    def setup_scheduler(self, schedule: str = None, use_master_job: bool = None) -> bool:
        """Setup the complete scheduler system"""
        schedule = schedule or self.scheduler_config.default_schedule
        use_master_job = use_master_job if use_master_job is not None else self.scheduler_config.use_master_job
        
        self.logger.info(f"Setting up scheduler (schedule='{schedule}', master_job={use_master_job})")
        
        # Remove existing jobs
        removed_count = self.cron_manager.remove_jobs_by_pattern(self.scheduler_config.comment_prefix)
        
        # Create new jobs
        if use_master_job:
            success = self._create_master_job(schedule)
        else:
            success = self._create_individual_jobs(schedule)
        
        # Write changes to crontab
        if success:
            success = self.cron_manager.write_changes()
        
        if success:
            self.logger.info("Scheduler setup completed successfully")
        else:
            self.logger.error("Scheduler setup failed")
        
        return success
    
    def _create_individual_jobs(self, schedule: str) -> bool:
        """Create individual cron jobs for each enabled worker"""
        success_count = 0
        enabled_workers = self.config_manager.get_enabled_workers()
        
        for worker in enabled_workers:
            # Merge environment variables
            env_vars = self.config_manager.merge_environment_vars(worker)
            
            # Build command
            script_path = Path(self.scheduler_config.app_dir) / worker.script_path
            command = f"{self.scheduler_config.python_path} {script_path}"
            comment = f"{self.scheduler_config.comment_prefix}_{worker.name}"
            
            # Create job
            if self.cron_manager.create_job(command, schedule, comment, env_vars):
                success_count += 1
                self.logger.info(f"Created individual job for {worker.name}")
            else:
                self.logger.error(f"Failed to create job for {worker.name}")
        
        success = success_count == len(enabled_workers)
        self.logger.info(f"Created {success_count}/{len(enabled_workers)} individual jobs")
        return success
    
    def _create_master_job(self, schedule: str) -> bool:
        """Create a single master job that launches all workers"""
        # Create master launcher script
        master_script_path = self._generate_master_launcher_script()
        
        if not master_script_path:
            return False
        
        # Create the cron job
        command = f"{self.scheduler_config.python_path} {master_script_path}"
        comment = f"{self.scheduler_config.comment_prefix}_Master"
        
        # Set global environment for master job
        env_vars = self.config_manager.global_environment.copy()
        
        success = self.cron_manager.create_job(command, schedule, comment, env_vars)
        
        if success:
            self.logger.info("Created master launcher job")
        else:
            self.logger.error("Failed to create master launcher job")
        
        return success
    
    def _generate_master_launcher_script(self) -> Optional[Path]:
        """Generate the master launcher script"""
        try:
            # Create scripts directory
            scripts_dir = Path(self.scheduler_config.app_dir) / "scripts"
            scripts_dir.mkdir(parents=True, exist_ok=True)
            
            script_path = scripts_dir / "master_launcher.py"
            
            # Generate script content
            script_content = self._build_master_launcher_content()
            
            # Write script file
            with open(script_path, 'w') as f:
                f.write(script_content)
            
            # Make executable
            script_path.chmod(0o755)
            
            self.logger.info(f"Generated master launcher script: {script_path}")
            return script_path
            
        except Exception as e:
            self.logger.error(f"Failed to generate master launcher script: {e}")
            return None
    
    def _build_master_launcher_content(self) -> str:
        """Build the content for the master launcher script"""
        enabled_workers = self.config_manager.get_enabled_workers()
        
        script_lines = [
            "#!/usr/bin/env python3",
            '"""Auto-generated master launcher script"""',
            "",
            "import os",
            "import sys",
            "import subprocess",
            "import time",
            "from datetime import datetime",
            "from pathlib import Path",
            "",
            "# Configuration",
            f'APP_DIR = "{self.scheduler_config.app_dir}"',
            f'PYTHON_PATH = "{self.scheduler_config.python_path}"',
            f'LOG_DIR = "{self.scheduler_config.log_dir}"',
            "",
            "# Worker definitions",
            "WORKERS = ["
        ]
        
        # Add worker definitions
        for worker in enabled_workers:
            env_vars = self.config_manager.merge_environment_vars(worker)
            script_lines.append(f'    {{')
            script_lines.append(f'        "name": "{worker.name}",')
            script_lines.append(f'        "script_path": "{worker.script_path}",')
            script_lines.append(f'        "env_vars": {repr(env_vars)},')
            script_lines.append(f'    }},')
        
        script_lines.extend([
            "]",
            "",
            "def log_message(message):",
            '    """Log message with timestamp"""',
            '    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")',
            '    log_entry = f"[{timestamp}] {message}"',
            '    print(log_entry)',
            '    ',
            '    # Also write to master log',
            '    master_log = Path(LOG_DIR) / "master_launcher.log"',
            '    try:',
            '        with open(master_log, "a") as f:',
            '            f.write(log_entry + "\\n")',
            '    except:',
            '        pass  # Ignore logging errors',
            "",
            "def launch_worker(worker_info):",
            '    """Launch a single worker"""',
            '    name = worker_info["name"]',
            '    script_path = Path(APP_DIR) / worker_info["script_path"]',
            '    env_vars = worker_info["env_vars"]',
            '    ',
            '    log_file = Path(LOG_DIR) / f"{name}.log"',
            '    pid_file = Path(LOG_DIR) / f"{name}.pid"',
            '    ',
            '    try:',
            '        # Check if script exists',
            '        if not script_path.exists():',
            '            log_message(f"ERROR: Script not found: {script_path}")',
            '            return False',
            '        ',
            '        # Prepare environment',
            '        env = os.environ.copy()',
            '        env.update(env_vars)',
            '        ',
            '        # Launch process',
            '        process = subprocess.Popen(',
            '            [PYTHON_PATH, str(script_path)],',
            '            stdout=open(log_file, "a"),',
            '            stderr=subprocess.STDOUT,',
            '            env=env,',
            '            cwd=APP_DIR,',
            '            preexec_fn=os.setsid',
            '        )',
            '        ',
            '        # Save PID',
            '        with open(pid_file, "w") as f:',
            '            f.write(str(process.pid))',
            '        ',
            '        log_message(f"SUCCESS: Launched {name} (PID: {process.pid})")',
            '        return True',
            '        ',
            '    except Exception as e:',
            '        log_message(f"ERROR: Failed to launch {name}: {str(e)}")',
            '        return False',
            "",
            "def main():",
            '    """Main launcher function"""',
            '    log_message("Starting master worker launcher...")',
            '    ',
            '    # Ensure log directory exists',
            '    Path(LOG_DIR).mkdir(parents=True, exist_ok=True)',
            '    ',
            '    successful_launches = 0',
            '    failed_launches = 0',
            '    ',
            '    for worker_info in WORKERS:',
            '        if launch_worker(worker_info):',
            '            successful_launches += 1',
            '        else:',
            '            failed_launches += 1',
            '        ',
            '        # Small delay between launches',
            '        time.sleep(0.5)',
            '    ',
            '    log_message(f"Launch completed: {successful_launches} successful, {failed_launches} failed")',
            '    ',
            '    # Exit with error code if any launches failed',
            '    if failed_launches > 0:',
            '        sys.exit(1)',
            "",
            'if __name__ == "__main__":',
            '    main()'
        ])
        
        return "\n".join(script_lines)
    
    def launch_workers_now(self) -> Dict[str, bool]:
        """Launch all workers immediately (bypass cron)"""
        self.logger.info("Launching all workers immediately")
        
        enabled_workers = self.config_manager.get_enabled_workers()
        results = {}
        
        for worker in enabled_workers:
            env_vars = self.config_manager.merge_environment_vars(worker)
            success = self.process_manager.launch_workers([worker], 
                                                        self.scheduler_config.python_path,
                                                        str(self.scheduler_config.app_dir),
                                                        env_vars)
            results[worker.name] = success.get(worker.name, False)
        
        successful = sum(1 for success in results.values() if success)
        self.logger.info(f"Immediate launch completed: {successful}/{len(enabled_workers)} successful")
        
        return results
    
    def get_worker_statuses(self) -> Dict[str, ProcessInfo]:
        """Get status of all workers"""
        worker_names = [w.name for w in self.config_manager.workers]
        return self.process_manager.get_all_worker_statuses(worker_names)
    
    def get_cron_jobs(self) -> List[CronJobInfo]:
        """Get all cron jobs for this system"""
        return self.cron_manager.list_jobs(self.scheduler_config.comment_prefix)
    
    def stop_worker(self, worker_name: str, force: bool = False) -> bool:
        """Stop a specific worker"""
        success = self.process_manager.stop_worker(worker_name, force)
        if success:
            self.logger.info(f"Stopped worker: {worker_name}")
        else:
            self.logger.error(f"Failed to stop worker: {worker_name}")
        return success
    
    def stop_all_workers(self, force: bool = False) -> Dict[str, bool]:
        """Stop all workers"""
        worker_names = [w.name for w in self.config_manager.workers]
        results = self.process_manager.stop_all_workers(worker_names, force)
        
        successful = sum(1 for success in results.values() if success)
        self.logger.info(f"Stopped {successful}/{len(worker_names)} workers")
        
        return results
    
    def restart_worker(self, worker_name: str) -> bool:
        """Restart a specific worker"""
        worker = self.config_manager.get_worker(worker_name)
        if not worker:
            self.logger.error(f"Worker not found: {worker_name}")
            return False
        
        env_vars = self.config_manager.merge_environment_vars(worker)
        success = self.process_manager.restart_worker(worker,
                                                    self.scheduler_config.python_path,
                                                    str(self.scheduler_config.app_dir),
                                                    env_vars)
        
        if success:
            self.logger.info(f"Restarted worker: {worker_name}")
        else:
            self.logger.error(f"Failed to restart worker: {worker_name}")
        
        return success
    
    def enable_all_jobs(self) -> bool:
        """Enable all cron jobs"""
        count = self.cron_manager.enable_jobs(self.scheduler_config.comment_prefix)
        success = self.cron_manager.write_changes()
        
        if success:
            self.logger.info(f"Enabled {count} cron jobs")
        else:
            self.logger.error("Failed to write cron changes")
        
        return success
    
    def disable_all_jobs(self) -> bool:
        """Disable all cron jobs"""
        count = self.cron_manager.disable_jobs(self.scheduler_config.comment_prefix)
        success = self.cron_manager.write_changes()
        
        if success:
            self.logger.info(f"Disabled {count} cron jobs")
        else:
            self.logger.error("Failed to write cron changes")
        
        return success
    
    def remove_all_jobs(self) -> bool:
        """Remove all cron jobs"""
        count = self.cron_manager.remove_jobs_by_pattern(self.scheduler_config.comment_prefix)
        success = self.cron_manager.write_changes()
        
        if success:
            self.logger.info(f"Removed {count} cron jobs")
        else:
            self.logger.error("Failed to write cron changes")
        
        return success
    
    def backup_crontab(self, backup_file: str) -> bool:
        """Backup current crontab"""
        return self.cron_manager.backup_crontab(backup_file)
    
    def save_config(self, config_file: str = None) -> bool:
        """Save current configuration to file"""
        return self.config_manager.save_to_file(config_file)
    
    def cleanup_stale_processes(self) -> List[str]:
        """Clean up stale PID files"""
        cleaned = self.process_manager.cleanup_stale_processes()
        if cleaned:
            self.logger.info(f"Cleaned up stale processes: {cleaned}")
        return cleaned
    
    def get_system_stats(self) -> Dict:
        """Get comprehensive system statistics"""
        worker_names = [w.name for w in self.config_manager.workers]
        process_stats = self.process_manager.get_system_stats(worker_names)
        
        cron_jobs = self.get_cron_jobs()
        enabled_jobs = sum(1 for job in cron_jobs if job.enabled)
        
        return {
            'process_stats': process_stats,
            'cron_stats': {
                'total_jobs': len(cron_jobs),
                'enabled_jobs': enabled_jobs,
                'disabled_jobs': len(cron_jobs) - enabled_jobs
            },
            'worker_stats': {
                'total_workers': len(self.config_manager.workers),
                'enabled_workers': len(self.config_manager.get_enabled_workers()),
                'disabled_workers': len(self.config_manager.workers) - len(self.config_manager.get_enabled_workers())
            },
            'system_info': {
                'app_dir': str(self.scheduler_config.app_dir),
                'log_dir': str(self.scheduler_config.log_dir),
                'python_path': self.scheduler_config.python_path,
                'user': self.scheduler_config.user
            }
        }
    
    def print_comprehensive_status(self):
        """Print comprehensive status report"""
        print("\n" + "="*80)
        print("WORKER SCHEDULER COMPREHENSIVE STATUS")
        print("="*80)
        
        # System configuration
        print(f"\nSystem Configuration:")
        print(f"  App Directory: {self.scheduler_config.app_dir}")
        print(f"  Python Path: {self.scheduler_config.python_path}")
        print(f"  Log Directory: {self.scheduler_config.log_dir}")
        print(f"  User: {self.scheduler_config.user}")
        print(f"  Comment Prefix: {self.scheduler_config.comment_prefix}")
        
        # Worker configurations
        print(f"\nWorker Configurations ({len(self.config_manager.workers)} total):")
        for worker in self.config_manager.workers:
            status = "ENABLED" if worker.enabled else "DISABLED"
            print(f"  {worker.name:20} {status:8} {worker.script_path}")
        
        # Process status
        print(f"\nProcess Status:")
        self.process_manager.print_status_summary([w.name for w in self.config_manager.workers])
        
        # Cron jobs
        print(f"\nScheduled Jobs:")
        self.cron_manager.print_jobs_summary(self.scheduler_config.comment_prefix)
        
        # System statistics
        stats = self.get_system_stats()
        print(f"\nSystem Statistics:")
        print(f"  Running Workers: {stats['process_stats']['running_workers']}")
        print(f"  Enabled Cron Jobs: {stats['cron_stats']['enabled_jobs']}")
        print(f"  Total CPU Usage: {stats['process_stats']['total_cpu_percent']:.1f}%")
        print(f"  Total Memory Usage: {stats['process_stats']['total_memory_mb']:.1f} MB")
        
        print("="*80)


# Convenience factory functions
def create_development_system(app_dir: str, python_path: str = None) -> WorkerSchedulerSystem:
    """Create a development-ready scheduler system"""
    from config import ConfigTemplates
    
    config_manager = ConfigTemplates.development_config(app_dir)
    if python_path:
        config_manager.scheduler_config.python_path = python_path
    
    return WorkerSchedulerSystem(config_manager=config_manager)


def create_production_system(app_dir: str, python_path: str, user: str = "production") -> WorkerSchedulerSystem:
    """Create a production-ready scheduler system"""
    from config import ConfigTemplates
    
    config_manager = ConfigTemplates.production_config(app_dir, python_path, user)
    return WorkerSchedulerSystem(config_manager=config_manager)


# Example usage and CLI interface
if __name__ == "__main__":
    import argparse
    
    def main():
        parser = argparse.ArgumentParser(description='Worker Scheduler System')
        parser.add_argument('action', choices=['setup', 'start', 'stop', 'restart', 'status', 'config'],
                           help='Action to perform')
        parser.add_argument('--config', '-c', help='Configuration file path')
        parser.add_argument('--app-dir', help='Application directory')
        parser.add_argument('--python-path', help='Python executable path')
        parser.add_argument('--schedule', default='0 6 * * *', help='Cron schedule')
        parser.add_argument('--worker', help='Specific worker name for targeted actions')
        parser.add_argument('--force', action='store_true', help='Force action (for stop/restart)')
        parser.add_argument('--master-job', action='store_true', help='Use master job approach')
        
        args = parser.parse_args()
        
        try:
            # Initialize system
            if args.config:
                system = WorkerSchedulerSystem.from_config_file(args.config)
            elif args.app_dir:
                system = WorkerSchedulerSystem.create_default(
                    args.app_dir, 
                    args.python_path,
                    user=True
                )
            else:
                print("Error: Must specify either --config or --app-dir")
                return 1
            
            # Execute action
            if args.action == 'setup':
                success = system.setup_scheduler(args.schedule, args.master_job)
                if success:
                    print("✓ Scheduler setup completed successfully")
                    system.print_comprehensive_status()
                else:
                    print("✗ Scheduler setup failed")
                    return 1
            
            elif args.action == 'start':
                if args.worker:
                    # Start specific worker
                    worker = system.config_manager.get_worker(args.worker)
                    if worker:
                        env_vars = system.config_manager.merge_environment_vars(worker)
                        success = system.process_manager.launch_workers([worker],
                                                                      system.scheduler_config.python_path,
                                                                      str(system.scheduler_config.app_dir),
                                                                      env_vars)
                        if success.get(args.worker):
                            print(f"✓ Started worker: {args.worker}")
                        else:
                            print(f"✗ Failed to start worker: {args.worker}")
                            return 1
                    else:
                        print(f"✗ Worker not found: {args.worker}")
                        return 1
                else:
                    # Start all workers
                    results = system.launch_workers_now()
                    successful = sum(1 for success in results.values() if success)
                    total = len(results)
                    print(f"Started {successful}/{total} workers")
                    if successful < total:
                        return 1
            
            elif args.action == 'stop':
                if args.worker:
                    success = system.stop_worker(args.worker, args.force)
                    if success:
                        print(f"✓ Stopped worker: {args.worker}")
                    else:
                        print(f"✗ Failed to stop worker: {args.worker}")
                        return 1
                else:
                    results = system.stop_all_workers(args.force)
                    successful = sum(1 for success in results.values() if success)
                    total = len(results)
                    print(f"Stopped {successful}/{total} workers")
            
            elif args.action == 'restart':
                if args.worker:
                    success = system.restart_worker(args.worker)
                    if success:
                        print(f"✓ Restarted worker: {args.worker}")
                    else:
                        print(f"✗ Failed to restart worker: {args.worker}")
                        return 1
                else:
                    # Restart all workers
                    system.stop_all_workers(args.force)
                    results = system.launch_workers_now()
                    successful = sum(1 for success in results.values() if success)
                    total = len(results)
                    print(f"Restarted {successful}/{total} workers")
            
            elif args.action == 'status':
                system.print_comprehensive_status()
            
            elif args.action == 'config':
                system.config_manager.print_config_summary()
                
        except Exception as e:
            print(f"Error: {e}")
            return 1
        
        return 0
    
    sys.exit(main())