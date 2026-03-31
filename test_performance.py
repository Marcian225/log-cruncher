
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


def stream_errors_csvreader(file_path):

    with open(file_path, newline='') as f:
        reader = csv.reader(f, delimiter=',')

        header = next(reader) 
        print(f"Skipping these columns: {header}")
        for row in reader:
            timestamp, level, message, user_id = row
            if level == 'ERROR':
                yield row



def stream_errors_dictreader(file_path):

    with open(file_path, newline='') as f:
        reader = csv.DictReader(f)

        for row in reader:
            if row['level'] == 'ERROR':
                yield row

def parse_csvreader():
    start = time.perf_counter()
    errors = list(stream_errors_csvreader("massive_logs.csv"))
    print(f"csv.reader Total errors: {len(errors)}")
    print(f"csv.reader Time: {time.perf_counter() - start:.4f} seconds\n")

def parse_dictreader():
    start = time.perf_counter()
    errors = list(stream_errors_dictreader("massive_logs.csv"))
    print(f"csv.DictReader Total errors: {len(errors)}")
    print(f"csv.DictReader Time: {time.perf_counter() - start:.4f} seconds\n")

def rust_parser():
    start = time.perf_counter()
    error_logs = log_cruncher.get_error_logs("massive_logs.csv")
    print(f"Rust Total errors: {len(error_logs)}")
    print(f"Rust Time: {time.perf_counter() - start:.4f} seconds\n")


if __name__ == "__main__":
    print("Starting Benchmarks...\n")
    parse_csvreader()
    parse_dictreader()
    rust_parser()