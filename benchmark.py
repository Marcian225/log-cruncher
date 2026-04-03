import os
import csv
import json
import psutil
import time
from functools import wraps
from dataclasses import dataclass
import log_cruncher

@dataclass
class LogEntry:
    """Python equivalent of the Rust LogEntry struct."""
    timestamp: str
    level: str
    method: str
    endpoint: str
    status_code: int
    response_time_ms: int
    user_id: int
    message: str

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
        
        print(f"  Time: {duration:.4f}s | RAM spike: {mem_used:.2f} MB | {throughput:,.0f} rows/s")
        return count
    return wrapper

def _detect_format(file_path: str) -> str:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    if file_path.endswith('.csv'): return 'csv'
    if file_path.endswith('.jsonl'): return 'jsonl'
    raise ValueError("Unsupported format. Please provide a .csv or .jsonl file.")


# ---------------------------------------------------------
# RUST PIPELINES
# ---------------------------------------------------------

@benchmark
def rust_aggregate(file_path: str):
    """Rust Compute Engine: Aggregates counts purely in Rust and returns a dict."""
    file_type = _detect_format(file_path)
    if file_type == 'csv':
        results = log_cruncher.process_csv_aggregate(file_path)
    else:
        results = log_cruncher.process_json_aggregate(file_path)
    return sum(results.values())

@benchmark
def rust_stream(file_path: str, target_level: str = None, chunk_size: int = 100_000):
    """Rust Data Pipeline: Streams typed Python objects across the FFI boundary."""
    processor = log_cruncher.BatchLogProcessor(file_path, target_level, chunk_size)
    count = 0
    for chunk in processor:
        count += len(chunk)
    return count


# ---------------------------------------------------------
# PYTHON PIPELINES (Strict Typing Control Group)
# ---------------------------------------------------------

@benchmark
def python_aggregate(file_path: str):
    """Pure Python Compute Engine: Uses dataclasses to match Rust's strict typing."""
    file_type = _detect_format(file_path)
    counts = {}
    
    with open(file_path, 'r', newline='', encoding='utf-8') as f:
        if file_type == 'csv':
            reader = csv.reader(f)
            next(reader, None)  # Skip header
            for row in reader:
                # Force Python to do the type conversion work Rust does via serde
                entry = LogEntry(
                    timestamp=row[0], level=row[1], method=row[2], endpoint=row[3],
                    status_code=int(row[4]), response_time_ms=int(row[5]),
                    user_id=int(row[6]), message=row[7]
                )
                counts[entry.level] = counts.get(entry.level, 0) + 1
        else:
            for line in f:
                record = json.loads(line)
                entry = LogEntry(
                    timestamp=record['timestamp'], level=record['level'], method=record['method'],
                    endpoint=record['endpoint'], status_code=int(record['status_code']),
                    response_time_ms=int(record['response_time_ms']), user_id=int(record['user_id']),
                    message=record['message']
                )
                counts[entry.level] = counts.get(entry.level, 0) + 1
                
    return sum(counts.values())

@benchmark
def python_stream(file_path: str, target_level: str = None, chunk_size: int = 100_000):
    """Pure Python Data Pipeline: Yields chunks of Dataclasses."""
    file_type = _detect_format(file_path)
    total_count = 0
    current_chunk = []
    
    with open(file_path, 'r', newline='', encoding='utf-8') as f:
        if file_type == 'csv':
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if target_level and row[1] != target_level: continue
                
                entry = LogEntry(
                    timestamp=row[0], level=row[1], method=row[2], endpoint=row[3],
                    status_code=int(row[4]), response_time_ms=int(row[5]),
                    user_id=int(row[6]), message=row[7]
                )
                current_chunk.append(entry)
                if len(current_chunk) == chunk_size:
                    total_count += len(current_chunk)
                    current_chunk = []
        else:
            for line in f:
                record = json.loads(line)
                if target_level and record['level'] != target_level: continue
                
                entry = LogEntry(
                    timestamp=record['timestamp'], level=record['level'], method=record['method'],
                    endpoint=record['endpoint'], status_code=int(record['status_code']),
                    response_time_ms=int(record['response_time_ms']), user_id=int(record['user_id']),
                    message=record['message']
                )
                current_chunk.append(entry)
                if len(current_chunk) == chunk_size:
                    total_count += len(current_chunk)
                    current_chunk = []
                    
        if current_chunk:
            total_count += len(current_chunk)
            
    return total_count


def run_comparison(test_file: str):
    print(f"\n=======================================================")
    print(f" BENCHMARKING DATASET: {test_file}")
    print(f"=======================================================\n")
    target = "ERROR"
    
    try:
        # 1. Aggregation Benchmark (The Compute Engine)
        print("1. Aggregation Engine (Count all log levels)")
        print("  [Python (Strict Types)]")
        py_agg_count = python_aggregate(test_file)
        print("  [Rust (Zero-Copy Engine)]")
        rust_agg_count = rust_aggregate(test_file)
        assert py_agg_count == rust_agg_count, "Data mismatch in aggregation!"

        # 2. Pipeline Streaming Benchmark (No Filter)
        print("\n2. Data Pipeline Streaming (No Filter, chunk=100k)")
        print("  [Python (Strict Types)]")
        py_stream_count = python_stream(test_file, None)
        print("  [Rust (PyO3 Engine)]")
        rust_stream_count = rust_stream(test_file, None)
        assert py_stream_count == rust_stream_count, "Data mismatch in streaming!"

        # 3. Pipeline Streaming Benchmark (Filtered)
        print(f"\n3. Data Pipeline Streaming (Filtered: {target}, chunk=100k)")
        print("  [Python (Strict Types)]")
        py_filter_count = python_stream(test_file, target)
        print("  [Rust (PyO3 Engine)]")
        rust_filter_count = rust_stream(test_file, target)
        assert py_filter_count == rust_filter_count, "Data mismatch in filtering!"

    except Exception as e:
        print(f"Error during execution: {e}")

if __name__ == "__main__":
    print("--- Log Cruncher Benchmark Showcase ---")
    
    if os.path.exists("massive_logs.jsonl"):
        run_comparison("massive_logs.jsonl")
    if os.path.exists("massive_logs.csv"):
        run_comparison("massive_logs.csv")
    