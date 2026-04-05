# Log-Cruncher

A Python module for processing log files in CSV and JSONL formats, implemented in Rust using PyO3. It provides functions to aggregate log levels and stream data in batches. This is a portfolio project built to explore how to integrate Rust with Python and to understand the performance trade-offs of passing data between the two languages.

The module is designed for log entries with the following fields: timestamp (string), level (string), method (string), endpoint (string), status_code (integer), response_time_ms (integer), user_id (integer), message (string).

## Installation
Requirements: Rust, Python 3.8+, pip.

**System Preparation (Linux/Debian-based):**
1. Install Python: `sudo apt update && sudo apt install python3 python3-venv python3-pip`
2. Install Rust: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
3. Load Rust environment: `source ~/.cargo/env`

**Project Setup:**
1. Clone this repository and navigate to it:
    ```bash
   git clone [https://github.com/Marcian225/log-cruncher.git](https://github.com/Marcian225/log-cruncher.git)
   cd log-cruncher
2. Create and activate a Python virtual environment:
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
3. Install Python dependencies:
   ```bash
    pip install maturin pytest psutil
4. Build and install the Rust extension into the virtual environment:
   ```bash
   maturin develop --release
5. Verify installation by running tests:
   ```bash
   cargo test
    pytest tests.py
## Usage

### Generate sample datasets

Generate both `massive_logs.csv` and `massive_logs.jsonl`:
```
python generate_data.py
```

Aggregate log levels from a CSV file:
```
python main.py test_logs.csv --mode aggregate
```

Stream data from a JSONL file with filtering:
```
python main.py massive_logs.jsonl --mode stream --target ERROR --chunk-size 50000
```

### Python Module

```python
import log_cruncher

# Aggregate log levels
results = log_cruncher.process_csv_aggregate('test_logs.csv')
print(results)  # {'INFO': 4, 'ERROR': 2, ...}

# Stream data in batches
processor = log_cruncher.BatchLogProcessor('massive_logs.jsonl', 'ERROR', 100000)
for chunk in processor:
    print(f"Received chunk with {len(chunk)} rows")
    # The data crosses the FFI boundary as strongly-typed objects
    if chunk:
        sample = chunk[0]
        print(f"Sample: [{sample.timestamp}] {sample.method} {sample.endpoint} -> {sample.status_code}")
```

## Benchmarks
Performance comparison executing on a dataset of 2,000,000 synthetic log records. Python baselines utilize strict `dataclass` type coercion to accurately compare against Rust's strict `struct` parsing.

**Hardware**:  12th Gen Intel(R) Core(TM) i7-12700KF (3.60 GHz) / 32GB DDR4/DDR5 / WSL 2.6.1.0 (Windows 11)

### 1. In-Engine Aggregation (Streaming)
This measures compute efficiency where Rust processes data internally and returns only a final summary.

| Format | Implementation | Time | Throughput | RAM Spike |
|:---|:---|---:|---:|---:|
| **CSV** | Python Baseline | 2.53s | 788,272 rows/s | 0.00 MB |
| **CSV** | Rust Extension | **0.42s** | **4,720,489 rows/s** | **0.00 MB** |
| **JSONL** | Python Baseline | 4.21s | 474,004 rows/s | 0.00 MB |
| **JSONL** | Rust Extension | **0.56s** | **3,571,659 rows/s** | **0.00 MB** |

### 2. PyO3 Data Streaming Pipeline
This test measures cross-language FFI overhead. The pipeline parses the file and yields batches of 100,000 strongly-typed Python objects back to the interpreter. 

*Note: The high RAM spike in the Rust JSONL test illustrates the cost of PyO3 object allocation. The 0.00 MB spike in the subsequent CSV test is an artifact of Python's memory pooling reusing the allocated space.*

| Format | Task | Implementation | Time | Throughput | RAM Spike |
|:---|:---|:---|---:|---:|---:|
| **JSONL** | Process All | Python Baseline | 4.79s | 417,356 rows/s | 8.80 MB |
| **JSONL** | Process All | Rust Extension | **0.84s** | **2,379,445 rows/s** | 47.95 MB |
| **CSV** | Process All | Python Baseline | 2.80s | 713,011 rows/s | 0.00 MB |
| **CSV** | Process All | Rust Extension | **0.62s** | **3,224,019 rows/s** | 0.00 MB |
| **JSONL** | Filter (ERROR) | Python Baseline | 3.35s | 59,377 rows/s | 2.74 MB |
| **JSONL** | Filter (ERROR) | Rust Extension | **0.61s** | **325,635 rows/s** | 0.18 MB |
| **CSV** | Filter (ERROR) | Python Baseline | 1.19s | 166,803 rows/s | 1.95 MB |
| **CSV** | Filter (ERROR) | Rust Extension | **0.42s** | **473,713 rows/s** | 0.04 MB |


A 0.00 MB spike indicates that the operation fit entirely within the memory pages already allocated by the Python interpreter or previously cleared by the garbage collector.
