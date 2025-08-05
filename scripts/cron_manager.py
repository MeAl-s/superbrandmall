#!/usr/bin/env python3
"""
cron_manager.py - Cron Job Management for Worker Scheduler System
Handles cron job creation, modification, and management using python-crontab
"""

import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple
from datetime import datetime
import logging

try:
    from crontab import CronTab, CronSlices
except ImportError:
    print("ERROR: python-crontab not installed. Run: pip install python-crontab")
    sys.exit(1)

from config import WorkerConfig, SchedulerConfig


class CronJobInfo:
    """Container for cron job information"""
    
    def __init__(self, comment: str, command: str, schedule: str, 
                 enabled: bool, valid: bool, env_vars: Dict[str, str] = None):
        self.comment = comment
        self.command = command
        self.schedule = schedule
        self.enabled = enabled
        self.valid = valid
        self.env_vars = env_vars or {}
        self.created_at = datetime.now()
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'comment': self.comment,
            'command': self.command,
            'schedule': self.schedule,
            'enabled': self.enabled,
            'valid': self.valid,
            'env_vars': self.env_vars,
            'created_at': self.created_at.isoformat()
        }
    
    def __str__(self) -> str:
        status = "ENABLED" if self.enabled else "DISABLED"
        return f"{self.comment}: {self.schedule} ({status})"


class ScheduleValidator:
    """Validates cron schedule expressions"""
    
    @staticmethod
    def is_valid_schedule(schedule: str) -> bool:
        """Check if schedule is valid"""
        try:
            return CronSlices.is_valid(schedule)
        except:
            return False
    
    @staticmethod
    def parse_schedule(schedule: str) -> Dict[str, str]:
        """Parse schedule into components"""
        if not ScheduleValidator.is_valid_schedule(schedule):
            raise ValueError(f"Invalid schedule: {schedule}")
        
        # Handle special cases
        special_schedules = {
            '@reboot': 'Every boot',
            '@hourly': 'Every hour (0 * * * *)',
            '@daily': 'Every day at midnight (0 0 * * *)',
            '@weekly': 'Every week on Sunday (0 0 * * 0)',
            '@monthly': 'Every month on 1st (0 0 1 * *)',
            '@yearly': 'Every year on Jan 1st (0 0 1 1 *)',
            '@annually': 'Every year on Jan 1st (0 0 1 1 *)',
            '@midnight': 'Every day at midnight (0 0 * * *)'
        }
        
        if schedule in special_schedules:
            return {
                'type': 'special',
                'description': special_schedules[schedule],
                'schedule': schedule
            }
        
        # Parse regular cron expression
        parts = schedule.split()
        if len(parts) != 5:
            raise ValueError("Regular cron expression must have 5 parts")
        
        field_names = ['minute', 'hour', 'day', 'month', 'weekday']
        parsed = dict(zip(field_names, parts))
        parsed['type'] = 'regular'
        parsed['schedule'] = schedule
        
        return parsed
    
    @staticmethod
    def get_human_readable(schedule: str) -> str:
        """Get human-readable description of schedule"""
        try:
            parsed = ScheduleValidator.parse_schedule(schedule)
            
            if parsed['type'] == 'special':
                return parsed['description']
            
            # Build human-readable description for regular expressions
            minute = parsed['minute']
            hour = parsed['hour']
            day = parsed['day']
            month = parsed['month']
            weekday = parsed['weekday']
            
            desc_parts = []
            
            # Minute part
            if minute == '*':
                desc_parts.append("every minute")
            elif '/' in minute:
                interval = minute.split('/')[1]
                desc_parts.append(f"every {interval} minutes")
            else:
                desc_parts.append(f"at minute {minute}")
            
            # Hour part
            if hour != '*':
                if '/' in hour:
                    interval = hour.split('/')[1]
                    desc_parts.append(f"every {interval} hours")
                else:
                    desc_parts.append(f"at hour {hour}")
            
            # Day/weekday parts
            if day != '*' and weekday != '*':
                desc_parts.append(f"on day {day} and weekday {weekday}")
            elif day != '*':
                desc_parts.append(f"on day {day}")
            elif weekday != '*':
                weekday_names = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
                if weekday.isdigit():
                    day_name = weekday_names[int(weekday)]
                    desc_parts.append(f"on {day_name}")
                else:
                    desc_parts.append(f"on weekday {weekday}")
            
            # Month part
            if month != '*':
                month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                              'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                if month.isdigit():
                    month_name = month_names[int(month) - 1]
                    desc_parts.append(f"in {month_name}")
                else:
                    desc_parts.append(f"in month {month}")
            
            return " ".join(desc_parts)
            
        except Exception:
            return f"Custom schedule: {schedule}"


class CronManager:
    """Main cron job management class"""
    
    def __init__(self, user: Union[bool, str] = True, logger: logging.Logger = None):
        self.user = user
        self.logger = logger or logging.getLogger(__name__)
        
        try:
            self.cron = CronTab(user=user)
            self.logger.info(f"Initialized CronTab for user: {user}")
        except Exception as e:
            self.logger.error(f"Failed to initialize CronTab: {e}")
            raise
    
    def create_job(self, command: str, schedule: str, comment: str,
                   environment_vars: Dict[str, str] = None,
                   enabled: bool = True) -> bool:
        """Create a new cron job"""
        try:
            # Validate schedule
            if not ScheduleValidator.is_valid_schedule(schedule):
                self.logger.error(f"Invalid schedule format: {schedule}")
                return False
            
            # Check if job with same comment already exists
            existing_job = self.find_job_by_comment(comment)
            if existing_job:
                self.logger.warning(f"Job with comment '{comment}' already exists")
                return False
            
            # Create new job
            job = self.cron.new(command=command, comment=comment)
            job.setall(schedule)
            
            # Set environment variables
            if environment_vars:
                for key, value in environment_vars.items():
                    job.env[key] = value
            
            # Set enabled state
            job.enable(enabled)
            
            # Validate job
            if not job.is_valid():
                self.logger.error(f"Created invalid job: {comment}")
                return False
            
            self.logger.info(f"Created cron job: {comment} ({schedule})")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create cron job '{comment}': {e}")
            return False
    
    def update_job(self, comment: str, command: str = None, schedule: str = None,
                   environment_vars: Dict[str, str] = None, enabled: bool = None) -> bool:
        """Update an existing cron job"""
        try:
            job = self.find_job_by_comment(comment)
            if not job:
                self.logger.error(f"Job not found: {comment}")
                return False
            
            # Update command
            if command is not None:
                job.set_command(command)
            
            # Update schedule
            if schedule is not None:
                if not ScheduleValidator.is_valid_schedule(schedule):
                    self.logger.error(f"Invalid schedule format: {schedule}")
                    return False
                job.setall(schedule)
            
            # Update environment variables
            if environment_vars is not None:
                # Clear existing env vars and set new ones
                job.clear_env()
                for key, value in environment_vars.items():
                    job.env[key] = value
            
            # Update enabled state
            if enabled is not None:
                job.enable(enabled)
            
            # Validate updated job
            if not job.is_valid():
                self.logger.error(f"Updated job is invalid: {comment}")
                return False
            
            self.logger.info(f"Updated cron job: {comment}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update cron job '{comment}': {e}")
            return False
    
    def remove_job(self, comment: str) -> bool:
        """Remove a single cron job by comment"""
        try:
            job = self.find_job_by_comment(comment)
            if job:
                self.cron.remove(job)
                self.logger.info(f"Removed cron job: {comment}")
                return True
            else:
                self.logger.warning(f"Job not found for removal: {comment}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to remove cron job '{comment}': {e}")
            return False
    
    def remove_jobs_by_pattern(self, comment_pattern: str) -> int:
        """Remove multiple jobs matching comment pattern"""
        try:
            jobs_to_remove = []
            
            for job in self.cron:
                if job.comment and comment_pattern in job.comment:
                    jobs_to_remove.append(job)
            
            for job in jobs_to_remove:
                self.cron.remove(job)
            
            count = len(jobs_to_remove)
            self.logger.info(f"Removed {count} jobs matching pattern '{comment_pattern}'")
            return count
            
        except Exception as e:
            self.logger.error(f"Failed to remove jobs with pattern '{comment_pattern}': {e}")
            return 0
    
    def find_job_by_comment(self, comment: str):
        """Find a job by exact comment match"""
        for job in self.cron:
            if job.comment == comment:
                return job
        return None
    
    def find_jobs_by_pattern(self, comment_pattern: str) -> List:
        """Find jobs by comment pattern"""
        matching_jobs = []
        for job in self.cron:
            if job.comment and comment_pattern in job.comment:
                matching_jobs.append(job)
        return matching_jobs
    
    def list_jobs(self, comment_pattern: str = None) -> List[CronJobInfo]:
        """List cron jobs, optionally filtered by comment pattern"""
        jobs = []
        
        for job in self.cron:
            if comment_pattern is None or (job.comment and comment_pattern in job.comment):
                job_info = CronJobInfo(
                    comment=job.comment or '',
                    command=job.command,
                    schedule=str(job),
                    enabled=job.is_enabled(),
                    valid=job.is_valid(),
                    env_vars=dict(job.env)
                )
                jobs.append(job_info)
        
        return jobs
    
    def enable_jobs(self, comment_pattern: str) -> int:
        """Enable jobs matching comment pattern"""
        count = 0
        try:
            for job in self.cron:
                if job.comment and comment_pattern in job.comment:
                    job.enable(True)
                    count += 1
            self.logger.info(f"Enabled {count} jobs matching '{comment_pattern}'")
        except Exception as e:
            self.logger.error(f"Error enabling jobs: {e}")
        
        return count
    
    def disable_jobs(self, comment_pattern: str) -> int:
        """Disable jobs matching comment pattern"""
        count = 0
        try:
            for job in self.cron:
                if job.comment and comment_pattern in job.comment:
                    job.enable(False)
                    count += 1
            self.logger.info(f"Disabled {count} jobs matching '{comment_pattern}'")
        except Exception as e:
            self.logger.error(f"Error disabling jobs: {e}")
        
        return count
    
    def toggle_job(self, comment: str) -> bool:
        """Toggle job enabled/disabled state"""
        try:
            job = self.find_job_by_comment(comment)
            if job:
                current_state = job.is_enabled()
                job.enable(not current_state)
                new_state = "enabled" if not current_state else "disabled"
                self.logger.info(f"Toggled job '{comment}' to {new_state}")
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error toggling job '{comment}': {e}")
            return False
    
    def set_global_environment(self, env_vars: Dict[str, str]) -> bool:
        """Set global environment variables for crontab"""
        try:
            for key, value in env_vars.items():
                self.cron.env[key] = value
            self.logger.info(f"Set {len(env_vars)} global environment variables")
            return True
        except Exception as e:
            self.logger.error(f"Error setting global environment: {e}")
            return False
    
    def get_global_environment(self) -> Dict[str, str]:
        """Get global environment variables"""
        return dict(self.cron.env)
    
    def write_changes(self) -> bool:
        """Write changes to crontab"""
        try:
            self.cron.write()
            self.logger.info("Successfully wrote changes to crontab")
            return True
        except Exception as e:
            self.logger.error(f"Failed to write changes to crontab: {e}")
            return False
    
    def backup_crontab(self, backup_file: str) -> bool:
        """Backup current crontab to file"""
        try:
            with open(backup_file, 'w') as f:
                for line in self.cron.render():
                    f.write(line + '\n')
            self.logger.info(f"Backed up crontab to {backup_file}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to backup crontab: {e}")
            return False
    
    def restore_crontab(self, backup_file: str) -> bool:
        """Restore crontab from backup file"""
        try:
            # Clear current crontab
            self.cron.remove_all()
            
            # Load from file
            with open(backup_file, 'r') as f:
                content = f.read()
            
            # Parse and recreate jobs
            temp_cron = CronTab(tab=content)
            for job in temp_cron:
                new_job = self.cron.new(command=job.command, comment=job.comment)
                new_job.setall(str(job))
                new_job.enable(job.is_enabled())
                
                # Copy environment variables
                for key, value in job.env.items():
                    new_job.env[key] = value
            
            self.logger.info(f"Restored crontab from {backup_file}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to restore crontab: {e}")
            return False
    
    def validate_all_jobs(self) -> Dict[str, bool]:
        """Validate all cron jobs"""
        validation_results = {}
        
        for job in self.cron:
            comment = job.comment or f"Job_{id(job)}"
            validation_results[comment] = job.is_valid()
        
        return validation_results
    
    def get_next_run_times(self, comment_pattern: str = None, limit: int = 5) -> Dict[str, List[str]]:
        """Get next run times for jobs"""
        # Note: python-crontab doesn't have built-in next run time calculation
        # This is a placeholder for potential future implementation
        next_runs = {}
        
        jobs = self.find_jobs_by_pattern(comment_pattern) if comment_pattern else list(self.cron)
        
        for job in jobs:
            comment = job.comment or f"Job_{id(job)}"
            # For now, just return the schedule
            next_runs[comment] = [f"Schedule: {str(job)}"]
        
        return next_runs
    
    def print_jobs_summary(self, comment_pattern: str = None):
        """Print human-readable summary of jobs"""
        jobs = self.list_jobs(comment_pattern)
        
        print("\n" + "="*80)
        print("CRON JOBS SUMMARY")
        print("="*80)
        
        if not jobs:
            print("No cron jobs found.")
            return
        
        print(f"Found {len(jobs)} jobs:")
        print()
        
        for job in jobs:
            status = "✓ ENABLED " if job.enabled else "✗ DISABLED"
            valid = "VALID" if job.valid else "INVALID"
            human_schedule = ScheduleValidator.get_human_readable(job.schedule)
            
            print(f"{status} | {valid:7} | {job.comment}")
            print(f"         Schedule: {job.schedule} ({human_schedule})")
            print(f"         Command:  {job.command}")
            
            if job.env_vars:
                print(f"         Env Vars: {job.env_vars}")
            
            print()
        
        print("="*80)


# Convenience functions for common operations
class CronPresets:
    """Predefined cron schedule presets"""
    
    EVERY_MINUTE = "* * * * *"
    EVERY_5_MINUTES = "*/5 * * * *"
    EVERY_15_MINUTES = "*/15 * * * *"
    EVERY_30_MINUTES = "*/30 * * * *"
    HOURLY = "@hourly"
    EVERY_2_HOURS = "0 */2 * * *"
    EVERY_6_HOURS = "0 */6 * * *"
    EVERY_12_HOURS = "0 */12 * * *"
    DAILY = "@daily"
    DAILY_6AM = "0 6 * * *"
    DAILY_NOON = "0 12 * * *"
    DAILY_6PM = "0 18 * * *"
    TWICE_DAILY = "0 6,18 * * *"
    WEEKDAYS_8AM = "0 8 * * 1-5"
    WEEKLY = "@weekly"
    MONTHLY = "@monthly"
    YEARLY = "@yearly"
    ON_REBOOT = "@reboot"
    
    @classmethod
    def get_all_presets(cls) -> Dict[str, str]:
        """Get all predefined presets"""
        return {
            name: value for name, value in cls.__dict__.items()
            if not name.startswith('_') and isinstance(value, str)
        }


# Example usage
if __name__ == "__main__":
    # Example usage
    cron_manager = CronManager(user=True)
    
    # Create a test job
    success = cron_manager.create_job(
        command="echo 'Hello World'",
        schedule=CronPresets.EVERY_5_MINUTES,
        comment="TestJob",
        environment_vars={'TEST_VAR': 'test_value'}
    )
    
    if success:
        print("Test job created successfully")
        
        # List all jobs
        cron_manager.print_jobs_summary()
        
        # Write changes (in real usage, you'd want to do this)
        # cron_manager.write_changes()
        
        # Clean up test job
        cron_manager.remove_job("TestJob")
        cron_manager.write_changes()
    else:
        print("Failed to create test job")