import csv
import json
import random
from datetime import datetime, timedelta

BUFFER_SIZE = 128 * 1024
NUM_ROWS = 2_000_000  

# Realistic distributions
LEVELS = ['INFO', 'WARN', 'ERROR', 'DEBUG', 'FATAL']
LEVEL_WEIGHTS = [0.70, 0.15, 0.10, 0.04, 0.01]

METHODS = ['GET', 'POST', 'PUT', 'DELETE']
METHOD_WEIGHTS = [0.60, 0.30, 0.05, 0.05]

ENDPOINTS = ['/api/v1/checkout', '/api/v1/catalog', '/api/v1/login', '/api/v1/profile']
ENDPOINT_WEIGHTS = [0.20, 0.50, 0.10, 0.20]

def generate_record(current_time: datetime) -> dict:
    level = random.choices(LEVELS, weights=LEVEL_WEIGHTS, k=1)[0]
    method = random.choices(METHODS, weights=METHOD_WEIGHTS, k=1)[0]
    endpoint = random.choices(ENDPOINTS, weights=ENDPOINT_WEIGHTS, k=1)[0]
    
    # Correlate status code and latency with log level for realism
    if level in ['INFO', 'DEBUG']:
        status_code = random.choice([200, 201])
        response_time_ms = int(random.expovariate(1/50)) + 10  # Fast responses
        message = "Request processed successfully"
    elif level == 'WARN':
        status_code = random.choice([400, 401, 403, 404, 429])
        response_time_ms = int(random.expovariate(1/100)) + 20
        message = "Client error or rate limit exceeded"
    elif level == 'ERROR':
        status_code = random.choice([500, 503, 504])
        response_time_ms = int(random.expovariate(1/1000)) + 500  # Slow responses/timeouts
        message = "Database timeout or internal exception"
    else:  # FATAL
        status_code = 502
        response_time_ms = 5000  # Max timeout
        message = "Payment gateway crashed unexpectedly"
        
    return {
        "timestamp": current_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "level": level,
        "method": method,
        "endpoint": endpoint,
        "status_code": status_code,
        "response_time_ms": response_time_ms,
        "user_id": random.randint(1, 10000),
        "message": message
    }

def generate_csv():
    print("Generating massive_logs.csv...")
    with open('massive_logs.csv', 'w', newline='', buffering=BUFFER_SIZE) as f:
        writer = csv.DictWriter(f, fieldnames=[
            "timestamp", "level", "method", "endpoint", 
            "status_code", "response_time_ms", "user_id", "message"
        ])
        writer.writeheader()
        
        start_time = datetime(2026, 3, 27, 10, 0, 0)
        for i in range(NUM_ROWS):
            record = generate_record(start_time + timedelta(seconds=i))
            writer.writerow(record)
    print("Done generating massive_logs.csv")

def generate_jsonl():
    print("Generating massive_logs.jsonl...")
    with open('massive_logs.jsonl', 'w', buffering=BUFFER_SIZE) as f:
        start_time = datetime(2026, 3, 27, 10, 0, 0)
        for i in range(NUM_ROWS):
            record = generate_record(start_time + timedelta(seconds=i))
            f.write(json.dumps(record) + '\n')
    print("Done generating massive_logs.jsonl")

if __name__ == "__main__":
    generate_csv()
    generate_jsonl()