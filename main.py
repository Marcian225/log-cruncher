import os
import csv
import json
import psutil
import time
from functools import wraps
import log_cruncher

def benchmark(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        process = psutil.Process(os.getpid())
        mem_before = process.memory_info().rss / (1024 * 1024) 
        start_time = time.perf_counter()
        
        count = func(*args, **kwargs)
        
        duration = time.perf_counter() - start_time
        mem_after = process.memory_info().rss / (1024 * 1024)
        mem_used = max(0, mem_after - mem_before)
        throughput = count / duration if duration > 0 else 0
        
        print(f" Time: {duration:.4f}s | RAM used: {mem_used:.2f} MB | {throughput:,.0f} rows/s")
        return count
    return wrapper

def _detect_format(file_path: str) -> str:
    """Helper to validate file existence and format."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    if file_path.endswith('.csv'):
        return 'csv'
    elif file_path.endswith('.jsonl'):
        return 'jsonl'
    else:
        raise ValueError("Unsupported format. Please provide a .csv or .jsonl file.")

# ---------------------------------------------------------
# RUST PIPELINES
# ---------------------------------------------------------

@benchmark
def rust_filter_eager(file_path: str, target_level: str):
    """Loads the entire file into RAM at once and filters it using Rust."""
    file_type = _detect_format(file_path)
    if file_type == 'csv':
        data = log_cruncher.filter_csv_by_level(file_path, target_level)
    else:
        data = log_cruncher.filter_json_by_level(file_path, target_level)
    return len(data)

@benchmark
def rust_filter_lazy(file_path: str, target_level: str, chunk_size: int = 100_000):
    """Processes the file in memory-safe chunks using Rust Rayon."""
    _detect_format(file_path)
    processor = log_cruncher.BatchLogProcessor(file_path, target_level, chunk_size)
    count = 0
    for chunk in processor:
        count += len(chunk)
    return count

@benchmark
def rust_process_all_lazy(file_path: str, chunk_size: int = 100_000):
    """Processes the entire file in chunks without filtering using Rust."""
    _detect_format(file_path)
    processor = log_cruncher.BatchLogProcessor(file_path, None, chunk_size)
    count = 0
    for chunk in processor:
        count += len(chunk)
    return count

@benchmark
def rust_process_all_eager(file_path: str):
    """Loads the entire file into RAM at once without filtering using Rust."""
    file_type = _detect_format(file_path)
    if file_type == 'csv':
        data = log_cruncher.process_logs_csv(file_path)
    else:
        data = log_cruncher.process_logs_json(file_path)
    return len(data)

# ---------------------------------------------------------
# PYTHON PIPELINES (Control Group)
# ---------------------------------------------------------

@benchmark
def python_eager(file_path: str, target_level: str = None):
    """Pure Python baseline: Loads everything into RAM using fast csv.reader."""
    file_type = _detect_format(file_path)
    data = []
    
    with open(file_path, 'r', newline='', encoding='utf-8') as f:
        if file_type == 'csv':
            reader = csv.reader(f)
            next(reader, None) # Skip the header row
            if target_level:
                # row[1] assumes 'level' is the second column
                data = [row for row in reader if row[1] == target_level]
            else:
                data = list(reader)
        else:
            for line in f:
                record = json.loads(line)
                if not target_level or record.get('level') == target_level:
                    data.append(record)
    return len(data)

@benchmark
def python_lazy(file_path: str, target_level: str = None, chunk_size: int = 100_000):
    """Pure Python baseline: Processes data in chunks using fast csv.reader."""
    file_type = _detect_format(file_path)
    total_count = 0
    
    with open(file_path, 'r', newline='', encoding='utf-8') as f:
        if file_type == 'csv':
            reader = csv.reader(f)
            next(reader, None) # Skip the header row
            
            chunk_count = 0
            for row in reader:
                if not target_level or row[1] == target_level:
                    chunk_count += 1
                    
                if chunk_count == chunk_size:
                    total_count += chunk_count
                    chunk_count = 0
        else:
            chunk_count = 0
            for line in f:
                record = json.loads(line)
                if not target_level or record.get('level') == target_level:
                    chunk_count += 1
                    
                if chunk_count == chunk_size:
                    total_count += chunk_count
                    chunk_count = 0
                    
        if chunk_count > 0:
            total_count += chunk_count
            
    return total_count


def main():
    print("--- Log Cruncher Benchmark Showcase ---\n")
    
    test_file = "massive_logs.csv"  # change to desired file (CSV or JSONL)
    target = "ERROR" # change to desired log level for filtering ['INFO', 'ERROR', 'WARN', 'DEBUG', 'FATAL']
    
    try:
        # 1. Filter Eager Comparison
        print(f"1. Filter Eager (Target: {target})")
        print("  [Python]")
        py_eager_count = python_eager(test_file, target)
        print(f"  Total occurrences verified (python): {py_eager_count}")
        print("  [Rust]")
        rust_eager_count = rust_filter_eager(test_file, target)
        print(f"  Total occurrences verified (rust): {rust_eager_count}\n")
            
        # 2. Process All Eager Comparison
        print("2. Process All Eager (No Filter)")
        print("  [Python]")
        py_all_eager_count = python_eager(test_file, None)
        print(f"  Total occurrences verified (python): {py_all_eager_count}")
        print("  [Rust]")
        rust_all_eager_count = rust_process_all_eager(test_file)
        print(f"  Total occurrences verified (rust): {rust_all_eager_count}\n")

        # 3. Filter Lazy Comparison
        print(f"3. Filter Lazy (Target: {target})")
        print("  [Python]")
        py_lazy_count = python_lazy(test_file, target)
        print(f"  Total occurrences verified (python): {py_lazy_count}")
        print("  [Rust]")
        rust_lazy_count = rust_filter_lazy(test_file, target)
        print(f"  Total occurrences verified (rust): {rust_lazy_count}\n")

        # 4. Process All Lazy Comparison
        print("4. Process All Lazy (No Filter)")
        print("  [Python]")
        py_all_lazy_count = python_lazy(test_file, None)
        print(f"  Total occurrences verified (python): {py_all_lazy_count}")
        print("  [Rust]")
        rust_all_lazy_count = rust_process_all_lazy(test_file)
        print(f"  Total occurrences verified (rust): {rust_all_lazy_count}\n")


    except Exception as e:
        print(f"Error during execution: {e}")

if __name__ == "__main__":
    main()