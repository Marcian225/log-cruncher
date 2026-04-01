
import log_cruncher
import csv
import time

total_rows = 2_000_000

def main():
    # processed_logs = log_cruncher.process_logs_csv("test_logs.csv")
    # processed_logs = log_cruncher.get_error_logs("test_logs.csv")
    processed_logs = log_cruncher.process_logs_json("test_logs.jsonl")
    
    for log in processed_logs:  
        print(log)
    
    
    print("first log level: ", processed_logs[0].level)


def stream_logs_csvreader(file_path, target_level):

    with open(file_path, newline='') as f:
        reader = csv.reader(f, delimiter=',')

        header = next(reader) 
        print(f"Skipping these columns: {header}")
        for row in reader:
            timestamp, level, message, user_id = row
            if level == target_level:
                yield row


def stream_logs_dictreader(file_path, target_level):

    with open(file_path, newline='') as f:
        reader = csv.DictReader(f)

        for row in reader:
            if row['level'] == target_level:
                yield row

def parse_csvreader():
    start = time.perf_counter()
    errors = list(stream_logs_csvreader("massive_logs.csv", "WARN"))
    print(f"csv.reader Total errors: {len(errors)}")
    print(f"csv.reader Time: {time.perf_counter() - start:.4f} seconds\n")

def parse_dictreader():
    start = time.perf_counter()
    errors = list(stream_logs_dictreader("massive_logs.csv", "WARN"))
    print(f"csv.DictReader Total errors: {len(errors)}")
    print(f"csv.DictReader Time: {time.perf_counter() - start:.4f} seconds\n")

def rust_csv_parser():
    start = time.perf_counter()
    error_logs = log_cruncher.filter_csv_by_level("massive_logs.csv", "WARN")
    print(f"Rust CSV Total errors: {len(error_logs)}")
    print(f"Rust CSV Time: {time.perf_counter() - start:.4f} seconds\n")

def rust_json_parser():
    start = time.perf_counter()
    error_logs = log_cruncher.filter_json_by_level("massive_logs.jsonl", "WARN")
    print(f"Rust JSONL Total errors: {len(error_logs)}")
    print(f"Rust JSONL Time: {time.perf_counter() - start:.4f} seconds\n")

def rust_batch_parser():
    start = time.perf_counter()

    # We can specify the target level, and optionally override the 100,000 default chunk size
    processor = log_cruncher.BatchLogProcessor("massive_logs.jsonl", "WARN", 500_000)

    total_logs = 0

    # The Rust iterator yields one filtered chunk (list) at a time
    for chunk in processor:
        total_logs += len(chunk)

    duration = time.perf_counter() - start
    print(f"Rust Batch Total logs: {total_logs}")
    print(f"Rust Batch Time taken: {duration:.4f} seconds")

if __name__ == "__main__":
    print("Starting Benchmarks...\n")
    parse_csvreader()
    parse_dictreader()
    rust_csv_parser()
    rust_json_parser()
    rust_batch_parser()