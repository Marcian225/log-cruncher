import argparse
import time
import sys
import log_cruncher

def run_aggregation(file_path: str):
    """Executes the Rust compute engine to count log levels."""
    print(f"Analyzing {file_path}...")
    start_time = time.perf_counter()
    
    try:
        if file_path.endswith(".csv"):
            results = log_cruncher.process_csv_aggregate(file_path)
        elif file_path.endswith(".jsonl"):
            results = log_cruncher.process_json_aggregate(file_path)
        else:
            print("Error: Unsupported file extension. Use .csv or .jsonl")
            sys.exit(1)
            
        duration = time.perf_counter() - start_time
        
        print("\n--- Aggregation Results ---")
        for level, count in sorted(results.items()):
            print(f"{level.ljust(8)}: {count:,}")
        print(f"---------------------------")
        print(f"Completed in {duration:.4f} seconds")
        
    except OSError as e:
        print(f"I/O Error: {e}")
        sys.exit(1)

def run_batch_streaming(file_path: str, target: str = None, chunk_size: int = 100_000):
    """Executes the Rust BatchLogProcessor to safely stream objects into Python."""
    print(f"Streaming {file_path} (Chunk size: {chunk_size:,})...")
    start_time = time.perf_counter()
    
    try:
        # Signature: file_path, target_level, chunk_size
        # BatchLogProcessor handles format detection (CSV or JSONL)
        processor = log_cruncher.BatchLogProcessor(file_path, target, chunk_size)
        
        total_rows = 0
        chunk_count = 0
        
        for chunk in processor:
            chunk_count += 1
            total_rows += len(chunk)
            # to do something with data
            print(f"  Received chunk {chunk_count} | Rows in chunk: {len(chunk):,}")
            if chunk_count == 1 and len(chunk) > 0:
                sample = chunk[0]
                print("\n    --- Sample Row Structure ---")
                print(f"    Timestamp: {sample.timestamp}")
                print(f"    Request:   {sample.method} {sample.endpoint}")
                print(f"    Status:    {sample.status_code}")
                print(f"    User ID:   {sample.user_id}")
                print(f"    Latency:   {sample.response_time_ms}ms")
                print("    ----------------------------\n")
            
        duration = time.perf_counter() - start_time
        print(f"\n--- Streaming Complete ---")
        print(f"Total chunks processed: {chunk_count:,}")
        print(f"Total rows retrieved:   {total_rows:,}")
        print(f"Completed in {duration:.4f} seconds")
        
    except OSError as e:
        print(f"I/O Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Execution Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log Cruncher - High Performance Log Analysis")
    
    # Core arguments
    parser.add_argument("file", type=str, help="Path to the log file (.csv or .jsonl)")
    parser.add_argument("--mode", type=str, choices=["aggregate", "stream"], default="aggregate", 
                        help="Execution mode: 'aggregate' (default) for fast summaries, 'stream' for pipeline data.")
    
    # Optional arguments for streaming
    parser.add_argument("--target", type=str, default=None, help="Filter by specific log level (e.g., ERROR)")
    parser.add_argument("--chunk-size", type=int, default=100_000, help="Number of rows per batch (streaming mode only)")

    args = parser.parse_args()

    if args.mode == "aggregate":
        run_aggregation(args.file)
    elif args.mode == "stream":
        run_batch_streaming(args.file, args.target, args.chunk_size)