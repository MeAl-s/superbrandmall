#!/usr/bin/env python3
# docker_diagnostic.py - Debug Docker environment
import sys
import os
from pathlib import Path

def diagnose_docker_environment():
    """Diagnose Docker environment for Point Detection services"""
    
    print("🐳 DOCKER ENVIRONMENT DIAGNOSTIC")
    print("=" * 60)
    
    # Current working directory and paths
    cwd = Path.cwd()
    print(f"📁 Current working directory: {cwd}")
    print(f"📁 __file__ location: {Path(__file__).parent if '__file__' in globals() else 'Not available'}")
    
    # Python path
    print(f"\n🐍 Python path:")
    for i, path in enumerate(sys.path):
        print(f"   {i}: {path}")
    
    # Environment variables
    print(f"\n🌍 Key environment variables:")
    env_vars = [
        'PYTHONPATH', 'WORKER_NAME', 'WATCH_DIRECTORY', 
        'DATABASE_URL', 'DEBUG', 'PYTHONUNBUFFERED'
    ]
    for var in env_vars:
        value = os.getenv(var, 'Not set')
        # Mask sensitive data
        if 'PASSWORD' in var or 'DATABASE_URL' in var:
            if value != 'Not set' and len(value) > 10:
                value = value[:10] + "***"
        print(f"   {var}: {value}")
    
    # Directory structure analysis
    print(f"\n📂 Directory structure analysis:")
    
    # Check /app directory
    app_dir = Path("/app")
    if app_dir.exists():
        print(f"✅ /app directory exists")
        print(f"📁 Contents of /app:")
        for item in sorted(app_dir.iterdir()):
            if item.is_dir():
                print(f"   📁 {item.name}/")
            else:
                print(f"   📄 {item.name}")
    else:
        print(f"❌ /app directory not found")
    
    # Check /app/app directory
    app_app_dir = Path("/app/app")
    if app_app_dir.exists():
        print(f"\n✅ /app/app directory exists")
        print(f"📁 Contents of /app/app:")
        for item in sorted(app_app_dir.iterdir()):
            if item.is_dir():
                print(f"   📁 {item.name}/")
            else:
                print(f"   📄 {item.name}")
    else:
        print(f"\n❌ /app/app directory not found")
    
    # Check services directory
    services_paths = [
        Path("/app/services"),
        Path("/app/app/services"),
        Path("./services"),
        Path("./app/services")
    ]
    
    print(f"\n🔍 Checking services directories:")
    services_found = False
    for services_path in services_paths:
        if services_path.exists():
            services_found = True
            print(f"✅ Found services at: {services_path}")
            print(f"📁 Contents of {services_path}:")
            for item in sorted(services_path.iterdir()):
                if item.is_dir():
                    print(f"   📁 {item.name}/")
                    # Check database subdirectory
                    if item.name == "database":
                        db_dir = item
                        print(f"   📁 Contents of {db_dir}:")
                        for db_item in sorted(db_dir.iterdir()):
                            print(f"      📄 {db_item.name}")
                else:
                    print(f"   📄 {item.name}")
        else:
            print(f"❌ Not found: {services_path}")
    
    if not services_found:
        print(f"🚨 No services directory found in any expected location!")
    
    # Test imports
    print(f"\n🔧 Testing imports:")
    
    # Add potential paths to sys.path for testing
    test_paths = [
        "/app",
        "/app/app", 
        "/app/app/services",
        "/app/services"
    ]
    
    original_path = sys.path.copy()
    
    for test_path in test_paths:
        if Path(test_path).exists() and test_path not in sys.path:
            sys.path.insert(0, test_path)
    
    # Test different import strategies
    import_strategies = [
        ("app.services.database.receipt_service", "ReceiptService"),
        ("services.database.receipt_service", "ReceiptService"),
        ("database.receipt_service", "ReceiptService"),
        ("app.config.settings", "settings"),
        ("config.settings", "settings"),
    ]
    
    successful_imports = []
    for module_path, class_name in import_strategies:
        try:
            module = __import__(module_path, fromlist=[class_name])
            if hasattr(module, class_name):
                successful_imports.append(f"{module_path}.{class_name}")
                print(f"✅ {module_path}.{class_name}")
            else:
                print(f"❌ {module_path} (no {class_name})")
        except ImportError as e:
            print(f"❌ {module_path}: {e}")
    
    # Restore original path
    sys.path = original_path
    
    # Summary
    print(f"\n📊 SUMMARY:")
    print(f"   Services directory found: {'✅' if services_found else '❌'}")
    print(f"   Successful imports: {len(successful_imports)}")
    if successful_imports:
        print(f"   Working imports:")
        for imp in successful_imports:
            print(f"      ✅ {imp}")
    
    # Recommendations
    print(f"\n💡 RECOMMENDATIONS:")
    if not services_found:
        print(f"   1. Ensure services directory is properly copied to Docker image")
        print(f"   2. Check Dockerfile COPY commands")
        print(f"   3. Verify directory structure in container")
    elif len(successful_imports) == 0:
        print(f"   1. Check __init__.py files in services directories")
        print(f"   2. Verify Python syntax in service files")
        print(f"   3. Check for missing dependencies")
    else:
        print(f"   ✅ Some imports working - update worker to use successful import paths")
    
    return services_found, len(successful_imports) > 0

if __name__ == "__main__":
    services_found, imports_working = diagnose_docker_environment()
    
    if services_found and imports_working:
        print(f"\n🎉 Docker environment looks good!")
        sys.exit(0)
    else:
        print(f"\n🚨 Docker environment has issues!")
        sys.exit(1)