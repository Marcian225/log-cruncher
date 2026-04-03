use pyo3::prelude::*;
use serde::Deserialize;
use std::io::{BufRead, BufReader, Lines};
use std::fs::{File};
use csv::{DeserializeRecordsIntoIter};
use std::collections::HashMap;


#[pyclass] 
#[derive(Debug, Deserialize, Clone)]
pub struct LogEntry {
    #[pyo3(get, set)]
    pub timestamp: String,
    
    #[pyo3(get, set)]
    pub level: String,
    
    #[pyo3(get, set)]
    pub method: String,

    #[pyo3(get, set)]
    pub endpoint: String,
    
    #[pyo3(get, set)]
    pub status_code: u16,

    #[pyo3(get, set)]
    pub response_time_ms: u32,

    #[pyo3(get, set)]
    pub user_id: u32,

    #[pyo3(get, set)]
    pub message: String,
}

// -------------------------------GENERIC PARSERS--------------------------

// Generic CSV processor that applies a Rust closure to each row
fn process_csv_generic<F>(file_path: &str, mut logic: F) -> Result<(), pyo3::PyErr>
where
    F: FnMut(LogEntry),
{
    let mut reader = csv::Reader::from_path(file_path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        
    for result in reader.deserialize() {
        match result {
            Ok(record) => logic(record),
            Err(e) => eprintln!("CSV Parsing Error: {}", e), // In production, consider logging this
        }
    }
    Ok(())
}

// Generic JSONL processor that applies a Rust closure to each row
fn process_jsonl_generic<F>(file_path: &str, mut logic: F) -> Result<(), pyo3::PyErr>
where
    F: FnMut(LogEntry),
{
    let file = File::open(file_path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        if let Ok(line_str) = line {
            match serde_json::from_str::<LogEntry>(&line_str) {
                Ok(record) => logic(record),
                Err(e) => eprintln!("JSON Parsing Error: {} - {}", e, line_str),
            }
        }
    }
    Ok(())
}

// -------------------------------EXAMPLE USAGE--------------------------

fn aggregate_csv_levels(file_path: &str) -> PyResult<HashMap<String, usize>> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    
    // Inject the counting logic into the generic parser
    process_csv_generic(file_path, |log| {
        *counts.entry(log.level).or_insert(0) += 1;
    })?;
    
    Ok(counts)
}

fn aggregate_jsonl_levels(file_path: &str) -> PyResult<HashMap<String, usize>> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    
    process_jsonl_generic(file_path, |log| {
        *counts.entry(log.level).or_insert(0) += 1;
    })?;
    
    Ok(counts)
}

// -------------------------------STREAMING BATCH PROCESSOR (For raw data pipelines)--------------------------
enum LogReader {
    Csv(DeserializeRecordsIntoIter<File, LogEntry>),
    Jsonl(Lines<BufReader<File>>),
}
#[pyclass]
struct BatchLogProcessor {
    reader: LogReader,
    target_level: Option<String>,
    chunk_size: usize,
}

#[pymethods]
impl BatchLogProcessor {
    #[new]
    #[pyo3(signature = (file_path, target_level = None, chunk_size=100_000))]
    fn new(file_path: &str, target_level: Option<String>, chunk_size: usize) -> PyResult<Self> {
        let reader = if file_path.ends_with(".csv"){
            let csv_reader = csv::Reader::from_path(file_path)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            LogReader::Csv(csv_reader.into_deserialize())
        }   
        else if file_path.ends_with(".jsonl") {
            let file = File::open(file_path)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            LogReader::Jsonl(BufReader::new(file).lines())
        } else {
            return Err(pyo3::exceptions::PyValueError::new_err("Unsupported file format"));
        };

        Ok(BatchLogProcessor { reader, target_level, chunk_size })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<Vec<LogEntry>>> {
        let chunk_size = slf.chunk_size;
        let mut current_chunk = Vec::with_capacity(chunk_size);
        let target_level = slf.target_level.clone();

        match &mut slf.reader {
            LogReader::Csv(reader) => {
                // Read until we fill the chunk OR hit the end of the file
                while current_chunk.len() < chunk_size {
                    match reader.next() {
                        Some(Ok(log)) => {
                            if let Some(target) = &target_level {
                                if &log.level == target {
                                    current_chunk.push(log);
                                }
                            } else {
                                current_chunk.push(log);
                            }
                        },
                        Some(Err(err)) => eprintln!("Failed to parse CSV row: {:?}", err),
                        None => break, // EOF
                    }
                }
            },
            LogReader::Jsonl(lines) => {
                while current_chunk.len() < chunk_size {
                    match lines.next() {
                        Some(Ok(line_str)) => {
                            match serde_json::from_str::<LogEntry>(&line_str) {
                                Ok(log) => {
                                    if let Some(target) = &target_level {
                                        if &log.level == target {
                                            current_chunk.push(log);
                                        }
                                    } else {
                                        current_chunk.push(log);
                                    }
                                },
                                Err(err) => eprintln!("Failed to parse JSON line: {:?}", err),
                            }
                        },
                        Some(Err(err)) => eprintln!("Failed to read line: {:?}", err),
                        None => break, // EOF
                    }
                }
            },
        }

        if current_chunk.is_empty() {
            return Ok(None); // Signal Python iteration to stop
        }

        Ok(Some(current_chunk))
    }
}





// -------------------------------PYTHON MODULE DEFINITION--------------------------
#[pymodule]
mod log_cruncher {
    use pyo3::prelude::*;
    use crate::{aggregate_csv_levels, aggregate_jsonl_levels};

    #[pyfunction]
    fn process_csv_aggregate(file_path: &str) -> PyResult<std::collections::HashMap<String, usize>> {
        aggregate_csv_levels(file_path)
    }

    #[pyfunction]
    fn process_json_aggregate(file_path: &str) -> PyResult<std::collections::HashMap<String, usize>> {
        aggregate_jsonl_levels(file_path)
    }

    #[pymodule_export]
    use crate::BatchLogProcessor;
}

// -------------------------------TESTS--------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_csv_generic() {
        let mut row_count = 0;
        let mut info_count = 0;

        // Test the generic CSV parser by injecting a standard Rust closure
        let result = process_csv_generic("test_logs.csv", |log| {
            row_count += 1;
            if log.level == "INFO" {
                info_count += 1;
            }
        });

        assert!(result.is_ok(), "CSV generic parser returned an unexpected error");
        assert_eq!(row_count, 10, "Should have iterated over exactly 10 rows");
        assert_eq!(info_count, 4, "Should have counted exactly 4 INFO logs");
    }

    #[test]
    fn test_process_jsonl_generic() {
        let mut row_count = 0;
        let mut error_count = 0;

        // Test the generic JSONL parser by injecting a standard Rust closure
        let result = process_jsonl_generic("test_logs.jsonl", |log| {
            row_count += 1;
            if log.level == "ERROR" {
                error_count += 1;
            }
        });

        assert!(result.is_ok(), "JSONL generic parser returned an unexpected error");
        assert_eq!(row_count, 10, "Should have iterated over exactly 10 rows");
        assert_eq!(error_count, 2, "Should have counted exactly 2 ERROR logs");
    }

    #[test]
    fn test_batch_processor_initialization() {
        // Test valid CSV initialization
        let csv_processor = BatchLogProcessor::new("test_logs.csv", Some("ERROR".to_string()), 5);
        assert!(csv_processor.is_ok(), "Failed to initialize CSV processor");

        // Test valid JSONL initialization
        let json_processor = BatchLogProcessor::new("test_logs.jsonl", None, 100);
        assert!(json_processor.is_ok(), "Failed to initialize JSONL processor");
        
        // Note: We do not test invalid files (e.g., "test.txt") here. 
        // PyO3 constructs a PyValueError on failure, which panics without the Python GIL.
        // That edge case belongs in the Python test suite.
    }
}