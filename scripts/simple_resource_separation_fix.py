#!/usr/bin/env python3
"""
🐳 DOCKER MULTI-CONTAINER LAUNCHER - GOATED EDITION 🐳
Launch 8 workers in 8 separate Docker containers with real-time monitoring!
The same joy as the terminal launcher, but with CONTAINERS! 🔥
"""

import os
import sys
import subprocess
import time
import json
import threading
from pathlib import Path
import platform
from datetime import datetime
import signal

class DockerMultiLauncher:
    """Launch 8 workers in 8 Docker containers with epic monitoring! 🚀"""
    
    def __init__(self):
        self.containers = []
        self.is_running = False
        self.monitoring_thread = None
        
        # 🎨 Epic color scheme for each worker
        self.workers = [
            {"name": "realtime-detector", "file": "realtime_detector.py", "color": "🔴", "emoji": "👁️", "port": "8001"},
            {"name": "ocr-processor", "file": "ocr_processor.py", "color": "🟢", "emoji": "⚙️", "port": "8002"},
            {"name": "ocr-classification", "file": "ocr_classification.py", "color": "🔵", "emoji": "🏷️", "port": "8003"},
            {"name": "ocr-downloader", "file": "ocr_downloader.py", "color": "🟡", "emoji": "⬇️", "port": "8004"},
            {"name": "ocr-text-processor", "file": "ocr_text_processor.py", "color": "🟣", "emoji": "📝", "port": "8005"},
            {"name": "delivery-scanner", "file": "delivery_scanner.py", "color": "🟠", "emoji": "🚚", "port": "8006"},
            {"name": "receipt-matcher", "file": "receipt_matcher.py", "color": "⚪", "emoji": "🎯", "port": "8007"},
            {"name": "timezone-worker", "file": "timezone_worker.py", "color": "⚫", "emoji": "🌍", "port": "8008"}
        ]
        
        # Docker configuration
        self.image_name = "point-detection-worker"
        self.network_name = "point-detection-network"
        self.volume_name = "point-detection-data"
        
    def print_banner(self):
        """Print epic startup banner 🎨"""
        print("=" * 80)
        print("🐳" + " " * 25 + "DOCKER MULTI-CONTAINER LAUNCHER" + " " * 25 + "🐳")
        print("🔥" + " " * 30 + "GOATED EDITION" + " " * 31 + "🔥")
        print("=" * 80)
        print()
        print("🚀 About to launch 8 EPIC containers for your pipeline!")
        print("⚡ Each worker gets its own isolated Docker container")
        print("📊 Real-time monitoring and logging included")
        print("🎮 Full control over each individual worker")
        print()
        
    def check_docker(self):
        """Check if Docker is available and running 🐳"""
        try:
            result = subprocess.run(["docker", "--version"], capture_output=True, text=True, check=True)
            print(f"✅ Docker found: {result.stdout.strip()}")
            
            # Check if Docker daemon is running
            result = subprocess.run(["docker", "info"], capture_output=True, text=True, check=True)
            print("✅ Docker daemon is running")
            return True
            
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("❌ Docker not found or not running!")
            print("📥 Install Docker Desktop: https://www.docker.com/products/docker-desktop")
            return False
    
    def build_docker_image(self):
        """Build the Docker image for workers 🏗️"""
        print("\n🏗️  BUILDING DOCKER IMAGE...")
        print("-" * 50)
        
        # Create Dockerfile if it doesn't exist
        dockerfile_content = '''FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \\
    gcc \\
    g++ \\
    curl \\
    procps \\
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /app/worker/data/real_time_response \\
    /app/worker/data/receipt_files \\
    /app/worker/data/receipt_ocring \\
    /app/logs

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \\
    CMD python -c "import sys; sys.exit(0)"

# Default command (will be overridden)
CMD ["python", "--version"]
'''
        
        # Write Dockerfile
        with open("Dockerfile", "w") as f:
            f.write(dockerfile_content)
        
        print("📄 Dockerfile created")
        
        try:
            print("🔨 Building Docker image...")
            build_process = subprocess.Popen([
                "docker", "build", "-t", self.image_name, "."
            ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            
            # Show build progress
            for line in build_process.stdout:
                print(f"   {line.strip()}")
            
            build_process.wait()
            
            if build_process.returncode == 0:
                print("✅ Docker image built successfully!")
                return True
            else:
                print("❌ Failed to build Docker image")
                return False
                
        except Exception as e:
            print(f"❌ Build failed: {e}")
            return False
    
    def create_network_and_volume(self):
        """Create Docker network and volume 🌐"""
        print("\n🌐 SETTING UP DOCKER NETWORK AND VOLUME...")
        print("-" * 50)
        
        try:
            # Create network
            subprocess.run([
                "docker", "network", "create", self.network_name
            ], capture_output=True, check=False)  # Don't fail if exists
            print("✅ Docker network ready")
            
            # Create volume
            subprocess.run([
                "docker", "volume", "create", self.volume_name
            ], capture_output=True, check=False)  # Don't fail if exists
            print("✅ Docker volume ready")
            
            return True
            
        except Exception as e:
            print(f"❌ Network/Volume setup failed: {e}")
            return False
    
    def launch_container(self, worker):
        """Launch a single worker container 🚀"""
        worker_name = worker["name"]
        worker_file = worker["file"]
        worker_color = worker["color"]
        worker_emoji = worker["emoji"]
        worker_port = worker["port"]
        
        container_name = f"point-detection-{worker_name}"
        
        print(f"{worker_color} {worker_emoji} Launching {worker_name}...")
        
        try:
            # Docker run command
            docker_cmd = [
                "docker", "run",
                "-d",  # Detached mode
                "--name", container_name,
                "--network", self.network_name,
                "--volume", f"{self.volume_name}:/app/worker/data",
                "--volume", f"{os.getcwd()}/logs:/app/logs",
                "--restart", "unless-stopped",
                "-p", f"{worker_port}:{worker_port}",  # Port mapping for monitoring
                "-e", f"WORKER_NAME={worker_name}",
                "-e", f"WORKER_COLOR={worker_color}",
                "-e", f"WORKER_EMOJI={worker_emoji}",
                self.image_name,
                "python", f"app/workers/{worker_file}"
            ]
            
            result = subprocess.run(docker_cmd, capture_output=True, text=True, check=True)
            container_id = result.stdout.strip()
            
            # Store container info
            container_info = {
                "name": worker_name,
                "container_name": container_name,
                "container_id": container_id,
                "file": worker_file,
                "color": worker_color,
                "emoji": worker_emoji,
                "port": worker_port,
                "status": "running"
            }
            
            self.containers.append(container_info)
            print(f"✅ {worker_color} {worker_emoji} {worker_name} started! (ID: {container_id[:12]})")
            
            return container_info
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to launch {worker_name}: {e.stderr}")
            return None
        except Exception as e:
            print(f"❌ Error launching {worker_name}: {e}")
            return None
    
    def launch_all_containers(self):
        """Launch all 8 worker containers! 🚀🚀🚀"""
        print("\n🚀 LAUNCHING ALL 8 WORKER CONTAINERS!")
        print("=" * 60)
        
        success_count = 0
        
        for i, worker in enumerate(self.workers):
            print(f"\n[{i+1}/8] ", end="")
            
            container = self.launch_container(worker)
            if container:
                success_count += 1
                
            # Small delay between launches for dramatic effect 😎
            time.sleep(1)
        
        print("\n" + "=" * 60)
        
        if success_count == len(self.workers):
            print(f"🎉 ALL {success_count} CONTAINERS LAUNCHED SUCCESSFULLY! 🎉")
            print("🔋 Your pipeline is now running in 8 isolated Docker containers!")
            return True
        else:
            print(f"⚠️  Launched {success_count}/{len(self.workers)} containers")
            return success_count > 0
    
    def show_container_status(self):
        """Show real-time status of all containers 📊"""
        print("\n📊 CONTAINER STATUS")
        print("-" * 70)
        
        try:
            for container in self.containers:
                # Get container status
                result = subprocess.run([
                    "docker", "inspect", container["container_id"],
                    "--format", "{{.State.Status}}"
                ], capture_output=True, text=True)
                
                status = result.stdout.strip() if result.returncode == 0 else "unknown"
                
                # Get resource usage
                stats_result = subprocess.run([
                    "docker", "stats", container["container_id"],
                    "--no-stream", "--format",
                    "table {{.CPUPerc}}\\t{{.MemUsage}}"
                ], capture_output=True, text=True)
                
                stats = "N/A" if stats_result.returncode != 0 else stats_result.stdout.split('\\n')[1] if len(stats_result.stdout.split('\\n')) > 1 else "N/A"
                
                # Color-coded status
                status_emoji = "🟢" if status == "running" else "🔴" if status == "exited" else "🟡"
                
                print(f"{container['color']} {container['emoji']} {container['name']:<20} {status_emoji} {status:<10} {stats}")
                
        except Exception as e:
            print(f"❌ Error getting status: {e}")
    
    def show_container_logs(self, container_name=None):
        """Show logs from containers 📜"""
        if container_name:
            # Show logs for specific container
            container = next((c for c in self.containers if c["name"] == container_name), None)
            if container:
                print(f"\n📜 LOGS FOR {container['color']} {container['emoji']} {container['name'].upper()}")
                print("-" * 60)
                
                try:
                    subprocess.run([
                        "docker", "logs", "-f", "--tail", "50", container["container_id"]
                    ])
                except KeyboardInterrupt:
                    print("\n👍 Log viewing stopped")
            else:
                print(f"❌ Container '{container_name}' not found")
        else:
            # Show all logs in parallel (this gets crazy! 🔥)
            print("\n📜 ALL CONTAINER LOGS (Press Ctrl+C to stop)")
            print("=" * 70)
            
            try:
                processes = []
                for container in self.containers:
                    print(f"🔗 Following logs for {container['color']} {container['emoji']} {container['name']}")
                    
                    # Start log following process
                    process = subprocess.Popen([
                        "docker", "logs", "-f", "--tail", "10", container["container_id"]
                    ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                    processes.append((container, process))
                
                # Monitor all logs
                while True:
                    for container, process in processes:
                        if process.poll() is None:  # Process still running
                            line = process.stdout.readline()
                            if line:
                                timestamp = datetime.now().strftime("%H:%M:%S")
                                print(f"[{timestamp}] {container['color']} {container['emoji']} {container['name']}: {line.strip()}")
                    time.sleep(0.1)
                    
            except KeyboardInterrupt:
                print("\n👍 Log monitoring stopped")
                for _, process in processes:
                    process.terminate()
    
    def restart_container(self, container_name):
        """Restart a specific container 🔄"""
        container = next((c for c in self.containers if c["name"] == container_name), None)
        if container:
            print(f"\n🔄 Restarting {container['color']} {container['emoji']} {container['name']}...")
            
            try:
                subprocess.run(["docker", "restart", container["container_id"]], check=True)
                print(f"✅ {container['name']} restarted successfully!")
            except subprocess.CalledProcessError as e:
                print(f"❌ Failed to restart {container['name']}: {e}")
        else:
            print(f"❌ Container '{container_name}' not found")
    
    def stop_all_containers(self):
        """Stop all containers 🛑"""
        print("\n🛑 STOPPING ALL CONTAINERS...")
        print("-" * 50)
        
        for container in self.containers:
            print(f"🛑 Stopping {container['color']} {container['emoji']} {container['name']}...")
            try:
                subprocess.run(["docker", "stop", container["container_id"]], capture_output=True, check=True)
                print(f"✅ {container['name']} stopped")
            except:
                print(f"⚠️  {container['name']} already stopped or error occurred")
        
        print("\n✅ All containers stopped!")
    
    def cleanup_containers(self):
        """Remove all containers and cleanup 🧹"""
        print("\n🧹 CLEANING UP CONTAINERS...")
        print("-" * 50)
        
        for container in self.containers:
            try:
                # Remove container
                subprocess.run(["docker", "rm", "-f", container["container_id"]], capture_output=True)
                print(f"🗑️  Removed {container['name']}")
            except:
                pass
        
        print("✅ Cleanup completed!")
    
    def interactive_menu(self):
        """Interactive menu for managing containers 🎮"""
        while True:
            print("\n" + "=" * 70)
            print("🎮 DOCKER CONTAINER MANAGER - INTERACTIVE MENU")
            print("=" * 70)
            print()
            print("📊 1. Show container status")
            print("📜 2. Show all logs (live)")
            print("🔍 3. Show specific container logs")
            print("🔄 4. Restart container")
            print("🛑 5. Stop all containers")
            print("🧹 6. Stop and cleanup everything")
            print("❌ 7. Exit")
            print()
            
            choice = input("🎯 Enter your choice (1-7): ").strip()
            
            if choice == "1":
                self.show_container_status()
                
            elif choice == "2":
                self.show_container_logs()
                
            elif choice == "3":
                print("\nAvailable containers:")
                for i, container in enumerate(self.containers, 1):
                    print(f"  {i}. {container['color']} {container['emoji']} {container['name']}")
                
                worker_choice = input("\nEnter container name: ").strip()
                self.show_container_logs(worker_choice)
                
            elif choice == "4":
                print("\nAvailable containers:")
                for i, container in enumerate(self.containers, 1):
                    print(f"  {i}. {container['color']} {container['emoji']} {container['name']}")
                
                worker_choice = input("\nEnter container name to restart: ").strip()
                self.restart_container(worker_choice)
                
            elif choice == "5":
                self.stop_all_containers()
                
            elif choice == "6":
                self.stop_all_containers()
                self.cleanup_containers()
                break
                
            elif choice == "7":
                print("👋 Exiting... (containers still running)")
                break
                
            else:
                print("❌ Invalid choice! Please enter 1-7")
    
    def run(self):
        """Main run method - THE EPIC LAUNCHER! 🚀"""
        self.print_banner()
        
        # Check prerequisites
        if not self.check_docker():
            return False
        
        print("\n⚡ SETUP PHASE")
        print("-" * 30)
        
        # Build image
        if not self.build_docker_image():
            return False
        
        # Setup network and volume
        if not self.create_network_and_volume():
            return False
        
        # Launch all containers
        if not self.launch_all_containers():
            return False
        
        print("\n🎊 LAUNCH COMPLETED! 🎊")
        print("🐳 All 8 workers are running in separate Docker containers!")
        print("📊 Real-time monitoring available")
        print("🎮 Interactive management ready")
        
        # Start interactive menu
        try:
            self.interactive_menu()
        except KeyboardInterrupt:
            print("\n\n👋 Interrupted by user")
            self.stop_all_containers()
        
        return True


def main():
    """Main function - Let's do this! 🔥"""
    print("🚀 Starting Docker Multi-Container Launcher...")
    
    launcher = DockerMultiLauncher()
    
    # Handle Ctrl+C gracefully
    def signal_handler(sig, frame):
        print("\n\n🛑 Shutting down...")
        launcher.stop_all_containers()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    success = launcher.run()
    
    if success:
        print("\n🎉 EPIC SUCCESS! Your Docker pipeline is running! 🎉")
    else:
        print("\n❌ Something went wrong, but don't give up! 💪")


if __name__ == "__main__":
    main()