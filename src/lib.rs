use pyo3::prelude::*;
use serde::Deserialize;
use rayon::prelude::*;
use std::io::{BufRead, BufReader};
use std::fs::File;


/// A Python module implemented in Rust.
#[pymodule]
mod log_cruncher {

    use pyo3::{exceptions::PyValueError, prelude::*};

    use crate::{LogEntry, filter_errors, parse_csv, parse_json_lines};

    #[pyfunction]
    fn process_logs_csv(file_path: &str) -> PyResult<Vec<LogEntry>>{
        parse_csv(file_path).map_err(|e| PyValueError::new_err(e.to_string()))
    }

    #[pyfunction]
    fn get_error_logs(file_path: &str) -> PyResult<Vec<LogEntry>>{
        let logs = parse_csv(file_path).map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(filter_errors(logs))
    }

    #[pyfunction]

    fn process_logs_json(file_path: &str) -> PyResult<Vec<LogEntry>>{
        parse_json_lines(file_path).map_err(|e| PyValueError::new_err(e.to_string()))
    }

}

#[derive(Deserialize, Debug, Clone)]
#[pyclass]
struct LogEntry {
    #[pyo3(get)]
    timestamp: String,
    #[pyo3(get)]
    level: String,
    #[pyo3(get)]
    message: String,
    #[pyo3(get)]
    user_id: u32,
}

#[pymethods]
impl LogEntry {
    fn __repr__(&self) -> String {
        format!("LogEntry {{ timestamp: {}, level: {}, message: {}, user_id: {} }}", self.timestamp, self.level, self.message, self.user_id)
    }
}


// Function to parse CSV file and return a vector of LogEntry structs
fn parse_csv(file_path: &str) -> Result<Vec<LogEntry>, csv::Error>{
    let mut reader = csv::Reader::from_path(file_path)?;
    let mut output: Vec<LogEntry> = vec![];
    
    for result in reader.deserialize(){
        let record = result?;
        output.push(record);
    }
    
    return Ok(output);
}

// function to filter log entries for errors parallel using rayon into_par_iter()
// example function to extract specific log entries 
fn filter_errors(logs: Vec<LogEntry>) -> Vec<LogEntry> {
    logs.into_par_iter().filter(|log| log.level =="ERROR").collect()
}



fn parse_json_lines(file_path: &str) -> Result<Vec<LogEntry>, std::io::Error> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let mut output: Vec<LogEntry> = vec![];

    for line in reader.lines(){
        let line_str = line?;
        
        match serde_json::from_str::<LogEntry>(&line_str) {
            Ok(record) => output.push(record),
            Err(err) => eprintln!("Failed to parse line: {}. Error: {:?}", line_str, err),
        }
    }

    return Ok(output)
}

#[cfg(test)]

mod tests{
    use super::*;

    #[test]
    fn test_parse_json_lines_valid() {
        let logs = parse_json_lines("test_logs.jsonl").expect("Failed to read JSONL file");

        let expected_levels = ["INFO", "ERROR", "WARN", "DEBUG", "INFO", "ERROR", "FATAL", "INFO", "WARN", "DEBUG"];
        let expected_user_ids = [101, 102, 102, 101, 101, 105, 0, 0, 0, 0];

        assert_eq!(logs.len(), 10, "Should have parsed exactly 10 lines");
        for (i, log) in logs.iter().enumerate() {
            assert_eq!(log.level, expected_levels[i], "JSONL line {} level mismatch", i + 1);
            assert_eq!(log.user_id, expected_user_ids[i], "JSONL line {} user_id mismatch", i + 1);
        }

        assert_eq!(logs[0].message, "User logged in");
        assert_eq!(logs[9].message, "Flushing old sessions");
    }

    #[test]
    fn test_parse_csv_valid() {
        let logs = parse_csv("test_logs.csv").expect("Failed to read CSV file");

        let expected_levels = ["INFO", "ERROR", "WARN", "DEBUG", "INFO", "ERROR", "FATAL", "INFO", "WARN", "DEBUG"];
        let expected_user_ids = [101, 102, 102, 101, 101, 105, 0, 0, 0, 0];

        assert_eq!(logs.len(), 10, "Should have parsed exactly 10 lines");
        for (i, log) in logs.iter().enumerate() {
            assert_eq!(log.level, expected_levels[i], "CSV line {} level mismatch", i + 1);
            assert_eq!(log.user_id, expected_user_ids[i], "CSV line {} user_id mismatch", i + 1);
        }

        assert_eq!(logs[0].timestamp, "2026-03-27T10:00:00Z");
        assert_eq!(logs[9].message, "Flushing old sessions");
    }

    #[test]
    fn test_parse_functions_roundtrip() {
        let csv_logs = parse_csv("test_logs.csv").expect("Failed to parse CSV");
        let json_logs = parse_json_lines("test_logs.jsonl").expect("Failed to parse JSONL");

        assert_eq!(csv_logs.len(), 10);
        assert_eq!(json_logs.len(), 10);
        assert_eq!(csv_logs[1].level, "ERROR");
        assert_eq!(json_logs[1].level, "ERROR");

        let errors = filter_errors(csv_logs.clone());
        assert_eq!(errors.len(), 2, "Should be exactly 2 error-level entries (ERROR only)");
    }
}