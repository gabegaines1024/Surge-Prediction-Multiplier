#!/usr/bin/env python3
"""
Memory and Progress Monitor for Dask Processing
Run this in a separate terminal while retrieval.py is running
"""

import psutil
import time
import os
from datetime import datetime

def get_memory_usage():
    """Get current memory usage in GB"""
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss / (1024 ** 3)  # Convert to GB

def get_system_memory():
    """Get system memory stats"""
    mem = psutil.virtual_memory()
    return {
        'total': mem.total / (1024 ** 3),
        'available': mem.available / (1024 ** 3),
        'percent': mem.percent,
        'used': mem.used / (1024 ** 3)
    }

def check_processed_files():
    """Check for processed output files"""
    output_dir = './processed_data/'
    files = {
        'aggregated_temp.parquet': False,
        'train_data.parquet': False,
        'test_data.parquet': False
    }
    
    if os.path.exists(output_dir):
        # Use list() to create a copy of keys to avoid modifying dict during iteration
        for filename in list(files.keys()):
            filepath = os.path.join(output_dir, filename)
            if os.path.exists(filepath):
                files[filename] = True
                try:
                    # Handle both file and directory (partitioned parquet)
                    if os.path.isdir(filepath):
                        size_mb = sum(
                            os.path.getsize(os.path.join(filepath, f)) 
                            for f in os.listdir(filepath) if f.endswith('.parquet')
                        ) / (1024 ** 2)
                    else:
                        size_mb = os.path.getsize(filepath) / (1024 ** 2)
                    files[f"{filename}_size"] = f"{size_mb:.2f} MB"
                except OSError:
                    files[f"{filename}_size"] = "? MB"
    
    return files

def monitor(interval=5):
    """Monitor system resources"""
    print("=" * 70)
    print("  DASK PROCESSING MONITOR")
    print("=" * 70)
    print("\nPress Ctrl+C to stop monitoring\n")
    
    start_time = datetime.now()
    max_memory = 0
    
    try:
        while True:
            # Get current stats
            sys_mem = get_system_memory()
            max_memory = max(max_memory, sys_mem['used'])
            elapsed = (datetime.now() - start_time).total_seconds()
            
            # Check files
            files = check_processed_files()
            
            # Clear screen (works on Unix-like systems)
            os.system('clear' if os.name == 'posix' else 'cls')
            
            # Display
            print("=" * 70)
            print(f"  DASK PROCESSING MONITOR  |  Elapsed: {elapsed//60:.0f}m {elapsed%60:.0f}s")
            print("=" * 70)
            
            # Memory stats
            print(f"\n📊 MEMORY USAGE:")
            print(f"   Current:  {sys_mem['used']:.2f} GB / {sys_mem['total']:.2f} GB ({sys_mem['percent']:.1f}%)")
            print(f"   Peak:     {max_memory:.2f} GB")
            print(f"   Available: {sys_mem['available']:.2f} GB")
            
            # Memory bar
            bar_length = 40
            filled = int(bar_length * sys_mem['percent'] / 100)
            bar = '█' * filled + '░' * (bar_length - filled)
            print(f"   [{bar}]")
            
            # File status
            print(f"\n📁 OUTPUT FILES:")
            status_map = {True: "✓", False: "⏳"}
            print(f"   {status_map[files['aggregated_temp.parquet']]} aggregated_temp.parquet", end="")
            if files.get('aggregated_temp.parquet_size'):
                print(f" ({files['aggregated_temp.parquet_size']})")
            else:
                print()
                
            print(f"   {status_map[files['train_data.parquet']]} train_data.parquet", end="")
            if files.get('train_data.parquet_size'):
                print(f" ({files['train_data.parquet_size']})")
            else:
                print()
                
            print(f"   {status_map[files['test_data.parquet']]} test_data.parquet", end="")
            if files.get('test_data.parquet_size'):
                print(f" ({files['test_data.parquet_size']})")
            else:
                print()
            
            # Status message
            if all([files['aggregated_temp.parquet'], files['train_data.parquet'], files['test_data.parquet']]):
                print(f"\n✅ PROCESSING COMPLETE!")
                print(f"   Total time: {elapsed//60:.0f}m {elapsed%60:.0f}s")
                print(f"   Peak memory: {max_memory:.2f} GB")
                break
            else:
                print(f"\n⏳ Processing... (refresh every {interval}s)")
            
            print("\n" + "=" * 70)
            
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped by user.")
        print(f"Session duration: {elapsed//60:.0f}m {elapsed%60:.0f}s")
        print(f"Peak memory usage: {max_memory:.2f} GB")

if __name__ == "__main__":
    import sys
    
    # Check if psutil is installed
    try:
        import psutil
    except ImportError:
        print("Error: psutil not installed")
        print("Install with: pip install psutil")
        sys.exit(1)
    
    # Get interval from command line
    interval = 5
    if len(sys.argv) > 1:
        try:
            interval = int(sys.argv[1])
        except ValueError:
            print(f"Invalid interval: {sys.argv[1]}, using default: 5 seconds")
    
    monitor(interval)

