# app/services/delivery_scanner/processing_service.py - Backward compatible processing service
import sys
from pathlib import Path
from typing import List, Set, Dict, Any
from datetime import datetime

# Add project root to path for imports
current_file = Path(__file__)
services_dir = current_file.parent.parent
app_dir = services_dir.parent
project_root = app_dir.parent

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(app_dir))

class ProcessingService:
    """Backward compatible processing service that works with both single and dual directory setups"""
    
    def __init__(self):
        # Session tracking (no persistent files)
        self.processed_files: Set[str] = set()
        self.session_stats = {
            'total_processed': 0,
            'delivery_found': 0,
            'non_delivery_found': 0,
            'primary_source_processed': 0,
            'secondary_source_processed': 0,
            'session_start': datetime.now()
        }
    
    def get_processed_files(self) -> Set[str]:
        """Get set of processed files"""
        return self.processed_files
    
    def process_new_files(self, new_files: List[Path], file_service, detection_service):
        """Process new files with backward compatibility"""
        if not new_files:
            return
        
        print(f"📊 Processing {len(new_files)} new files...")
        
        # Process in batches of 20 for better performance
        batch_size = 20
        for i in range(0, len(new_files), batch_size):
            batch = new_files[i:i + batch_size]
            print(f"📦 Processing batch {i//batch_size + 1}: {len(batch)} files")
            
            for file_path in batch:
                self._process_single_file(file_path, file_service, detection_service)
    
    def _process_single_file(self, file_path: Path, file_service, detection_service):
        """Process a single file with backward compatibility"""
        try:
            print(f"    🔍 Scanning: {file_path.name}")
            
            # Check if file_service has dual directory support
            has_dual_support = hasattr(file_service, 'file_source_map')
            
            if has_dual_support:
                # Enhanced mode with dual directory support
                source_type = file_service.file_source_map.get(file_path.name, "unknown")
                print(f"       📍 Source: {source_type} directory")
            else:
                # Single directory mode
                source_type = "single"
            
            # Read file content
            text = file_service.read_file(file_path)
            if not text.strip():
                print(f"    ⚠️  Empty or unreadable file: {file_path.name}")
                return
            
            # Check for delivery keywords using the correct method name
            is_delivery, found_keywords = detection_service.check_delivery_keywords(text)
            
            if is_delivery:
                # This is a delivery receipt
                print(f"    🚚 DELIVERY DETECTED: {file_path.name}")
                if has_dual_support:
                    print(f"       📍 Source: {source_type} directory")
                print(f"       🔑 Keywords: {', '.join(found_keywords)}")
                
                # Move to delivery folder
                success, target_path = file_service.move_to_delivery(file_path)
                if success:
                    print(f"       ✅ Moved to: delivery_found/")
                    
                    # Log the detection (check if enhanced logging is available)
                    text_preview = text[:100] if len(text) > 100 else text
                    if hasattr(file_service, 'log_delivery_detection'):
                        # Check if the method accepts source_type parameter
                        try:
                            file_service.log_delivery_detection(
                                file_path.name, 
                                found_keywords, 
                                text_preview,
                                source_type
                            )
                        except TypeError:
                            # Fallback to original method signature
                            file_service.log_delivery_detection(
                                file_path.name, 
                                found_keywords, 
                                text_preview
                            )
                    
                    # Update stats
                    self.session_stats['delivery_found'] += 1
                else:
                    print(f"       ❌ Move failed: {target_path}")
            else:
                # Regular receipt (non-delivery)
                print(f"    📋 Regular receipt: {file_path.name}")
                if has_dual_support:
                    print(f"       📍 Source: {source_type} directory")
                
                # Move to non-delivery folder
                success, target_path = file_service.move_to_non_delivery(file_path)
                if success:
                    print(f"       ✅ Moved to: non_delivery/")
                    self.session_stats['non_delivery_found'] += 1
                else:
                    print(f"       ❌ Move failed: {target_path}")
            
            # Update source-specific stats (only if dual directory support exists)
            if has_dual_support:
                if source_type == "primary":
                    self.session_stats['primary_source_processed'] += 1
                elif source_type == "secondary":
                    self.session_stats['secondary_source_processed'] += 1
            else:
                # In single directory mode, count everything as primary
                self.session_stats['primary_source_processed'] += 1
            
            # Update total stats
            self.session_stats['total_processed'] += 1
            
        except Exception as e:
            print(f"    ❌ Error processing {file_path.name}: {e}")
    
    def scan_existing_files(self, file_service, detection_service) -> Dict[str, Any]:
        """Scan all existing files with backward compatibility"""
        print("🔍 Scanning all existing files...")
        
        # Check if file_service has dual directory support
        has_dual_support = hasattr(file_service, 'get_all_files_in_sources')
        
        if has_dual_support:
            all_files = file_service.get_all_files_in_sources()
            print("   (Using enhanced dual directory scanning)")
        else:
            # Use single directory method
            if hasattr(file_service, 'get_all_files_in_source'):
                all_files = file_service.get_all_files_in_source()
            else:
                # Fallback to basic scanning
                source_dir = file_service.get_source_directory()
                all_files = [f for f in source_dir.iterdir() if f.is_file()] if source_dir.exists() else []
            print("   (Using single directory scanning)")
        
        if not all_files:
            print("📭 No files found in source directory/directories")
            return {
                'total_files': 0,
                'delivery_found': 0,
                'non_delivery': 0,
                'primary_source': 0,
                'secondary_source': 0
            }
        
        print(f"📊 Found {len(all_files)} files total")
        
        # Count by source (only if dual directory support exists)
        if has_dual_support:
            primary_files = [f for f in all_files if file_service.file_source_map.get(f.name) == "primary"]
            secondary_files = [f for f in all_files if file_service.file_source_map.get(f.name) == "secondary"]
            print(f"   📂 Primary source: {len(primary_files)} files")
            print(f"   📂 Secondary source: {len(secondary_files)} files")
        else:
            primary_files = all_files
            secondary_files = []
            print(f"   📂 Source files: {len(primary_files)} files")
        
        # Process all files
        delivery_count = 0
        non_delivery_count = 0
        
        for i, file_path in enumerate(all_files, 1):
            if has_dual_support:
                source_type = file_service.file_source_map.get(file_path.name, "unknown")
                print(f"🔍 [{i}/{len(all_files)}] Scanning: {file_path.name} ({source_type})")
            else:
                source_type = "single"
                print(f"🔍 [{i}/{len(all_files)}] Scanning: {file_path.name}")
            
            # Read and analyze file
            text = file_service.read_file(file_path)
            if not text.strip():
                print(f"    ⚠️  Empty or unreadable file")
                continue
            
            # Check for delivery keywords using the correct method name
            is_delivery, found_keywords = detection_service.check_delivery_keywords(text)
            
            if is_delivery:
                # Delivery receipt
                print(f"    🚚 DELIVERY DETECTED!")
                if has_dual_support:
                    print(f"       📍 Source: {source_type} directory")
                print(f"       🔑 Keywords: {', '.join(found_keywords)}")
                
                success, target_path = file_service.move_to_delivery(file_path)
                if success:
                    delivery_count += 1
                    print(f"       ✅ Moved to delivery folder")
                    
                    # Log detection with backward compatibility
                    text_preview = text[:100] if len(text) > 100 else text
                    if hasattr(file_service, 'log_delivery_detection'):
                        try:
                            file_service.log_delivery_detection(
                                file_path.name, 
                                found_keywords, 
                                text_preview,
                                source_type
                            )
                        except TypeError:
                            file_service.log_delivery_detection(
                                file_path.name, 
                                found_keywords, 
                                text_preview
                            )
            else:
                # Regular receipt
                print(f"    📋 Regular receipt")
                if has_dual_support:
                    print(f"       📍 Source: {source_type} directory")
                
                success, target_path = file_service.move_to_non_delivery(file_path)
                if success:
                    non_delivery_count += 1
                    print(f"       ✅ Moved to non-delivery folder")
        
        # Update session stats
        self.session_stats.update({
            'total_processed': len(all_files),
            'delivery_found': delivery_count,
            'non_delivery_found': non_delivery_count,
            'primary_source_processed': len(primary_files),
            'secondary_source_processed': len(secondary_files) if has_dual_support else 0
        })
        
        # Print final summary
        print(f"\n📊 SCANNING COMPLETE:")
        print(f"   📁 Total files processed: {len(all_files)}")
        print(f"   🚚 Delivery receipts found: {delivery_count}")
        print(f"   📋 Non-delivery receipts: {non_delivery_count}")
        if has_dual_support:
            print(f"   📂 From primary source: {len(primary_files)}")
            print(f"   📂 From secondary source: {len(secondary_files)}")
        else:
            print(f"   📂 From source: {len(primary_files)}")
        
        if delivery_count > 0:
            delivery_rate = (delivery_count / len(all_files)) * 100
            print(f"   📈 Delivery rate: {delivery_rate:.1f}%")
        
        return {
            'total_files': len(all_files),
            'delivery_found': delivery_count,
            'non_delivery': non_delivery_count,
            'primary_source': len(primary_files),
            'secondary_source': len(secondary_files) if has_dual_support else 0,
            'delivery_rate': (delivery_count / len(all_files) * 100) if len(all_files) > 0 else 0
        }
    
    def print_session_stats(self):
        """Print session statistics with backward compatibility"""
        elapsed_time = datetime.now() - self.session_stats['session_start']
        elapsed_minutes = elapsed_time.total_seconds() / 60
        
        print(f"\n📊 SESSION STATS:")
        print(f"   📁 Total processed: {self.session_stats['total_processed']}")
        print(f"   🚚 Delivery found: {self.session_stats['delivery_found']}")
        print(f"   📋 Non-delivery: {self.session_stats['non_delivery_found']}")
        
        # Only show dual directory stats if there are secondary files processed
        if self.session_stats['secondary_source_processed'] > 0:
            print(f"   📂 Primary source: {self.session_stats['primary_source_processed']}")
            print(f"   📂 Secondary source: {self.session_stats['secondary_source_processed']}")
        else:
            print(f"   📂 Files processed: {self.session_stats['primary_source_processed']}")
        
        print(f"   ⏱️  Session time: {elapsed_minutes:.1f} minutes")
        
        if self.session_stats['total_processed'] > 0:
            delivery_rate = (self.session_stats['delivery_found'] / self.session_stats['total_processed']) * 100
            print(f"   📈 Delivery rate: {delivery_rate:.1f}%")
    
    def get_scanning_stats(self) -> Dict[str, Any]:
        """Get current scanning statistics"""
        return self.session_stats.copy()